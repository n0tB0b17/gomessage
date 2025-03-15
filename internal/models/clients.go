package models

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPerid      = (pongWait * 9) / 10
	maxMessageSize = 1024 * 1024 // 1MB
)

type Client struct {
	Hub        *Hub
	Conn       *websocket.Conn
	Send       chan []byte
	Id         string
	Username   string
	NatsConn   *nats.Conn
	LastActive time.Time
	Mu         sync.Mutex
}

// read message from client's websocket connection to the hub
func (c *Client) ReadPump() {
	defer func() {
		c.Hub.Unregister <- c
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(maxMessageSize)
	c.Conn.SetReadDeadline(time.Now().Add(pongWait))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		// read raw message
		_, msg, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				fmt.Printf("unexpected close error: %v \n", err)
				fmt.Printf("client disconnected: %s with username: %s \n", c.Conn.RemoteAddr().String(), c.Username)
			}

			break
		}

		// update last active time
		c.Mu.Lock()
		c.LastActive = time.Now()
		c.Mu.Unlock()

		// parse message
		var parsedMessage Message
		if err := json.Unmarshal(msg, &parsedMessage); err != nil {
			fmt.Printf("error while parsing message: %v \n", err)
			continue
		}

		// set message meta-data
		parsedMessage.ID = uuid.New().String()
		parsedMessage.Sender = c.Id
		parsedMessage.Timestamp = time.Now()

		// handle different message types, ie > client-to-client or client-to-broadcast
		switch parsedMessage.Type {
		case MESSAGE:
			// publish to nats' direct subject
			if parsedMessage.Recipient != "" {
				docs, _ := json.Marshal(parsedMessage)
				if err := c.NatsConn.Publish("chat.direct", docs); err != nil {
					fmt.Printf("error while publishing message to subject: 'chat.direct' %v \n", err)
				}
			} else {
				// broadcast message
				fmt.Println("[else] > no message type matched, so broadcasting to all connected clients")
				c.Hub.Broadcast <- &parsedMessage
			}
		default:
			fmt.Println("[default] > no message type matched, so broadcasting to all connected clients")
			c.Hub.Broadcast <- &parsedMessage
		}
	}
}

func (c *Client) WritePump() {
	newTicker := time.NewTicker(pingPerid)
	defer func() {
		newTicker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Hub closed
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			writer, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}

			writer.Write(message)
			n := len(c.Send)

			// add pending messages to the current message
			for i := 0; i < n; i++ {
				writer.Write([]byte{'\n'})
				writer.Write(<-c.Send)
			}

			// close writer
			if err := writer.Close(); err != nil {
				return
			}

		case <-newTicker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
