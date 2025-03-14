package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/n0tB0b17/gomessage/internal/models"
	"github.com/nats-io/nats.go"
)

var (
	ReadBufferSize  int = 1024
	WriteBufferSize int = 1024
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  ReadBufferSize,
	WriteBufferSize: WriteBufferSize,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func TestAPIHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Health check ok"))
}

func ServeWS(h *models.Hub, w http.ResponseWriter, r *http.Request, natAddr string) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Printf("error while upgrading connection: %v \n", err)
		return
	}

	ns, err := nats.Connect(natAddr)
	if err != nil {
		fmt.Printf("error while connecting to nats server: %v \n", err)
		conn.Close()
		return
	}

	username := r.URL.Query().Get("username")
	if username == "" {
		username = fmt.Sprintf("anonymous+%s", uuid.New().String())
	}

	fmt.Printf("client conneted: %s with username: %s\n", conn.RemoteAddr().String(), username)
	clientID := uuid.New().String()
	client := &models.Client{
		Hub:        h,
		Conn:       conn,
		Send:       make(chan []byte, 256),
		Id:         clientID,
		Username:   username,
		NatsConn:   ns,
		LastActive: time.Now(),
	}

	_, err = ns.Subscribe("chat.Broadcast", func(msg *nats.Msg) {
		fmt.Println("Received a chat.Broadcast message")
		select {
		case client.Send <- msg.Data:
		default:
			fmt.Println("client is not ready to receive message")
		}
	})

	if err != nil {
		fmt.Printf("error while subscring to subject: %v \n", err)
		conn.Close()
		return
	}

	_, err = ns.Subscribe("chat.direct", func(msg *nats.Msg) {
		var message models.Message
		if err := json.Unmarshal(msg.Data, &message); err != nil {
			return
		}

		fmt.Printf("Direct message from: %+v \n", message)
		if message.Sender == client.Username || message.Recipient == client.Username {
			select {
			case client.Send <- msg.Data:
			default:
				fmt.Println("client is not ready to receive message")
			}
		}
	})

	if err != nil {
		fmt.Printf("error while subscring to subject: %v \n", err)
		conn.Close()
		return
	}

	client.Hub.Register <- client
	go client.ReadPump()
	go client.WritePump()
}
