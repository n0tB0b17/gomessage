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
	hub        *Hub
	conn       *websocket.Conn
	send       chan []byte
	id         string
	username   string
	natsConn   *nats.Conn
	lastActive time.Time
	mu         sync.Mutex
}

func (c *Client) ReadPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		// read raw message
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				fmt.Printf("unexpected close error: %v \n", err)
			}
		}

		// update last active time
		c.mu.Lock()
		c.lastActive = time.Now()
		c.mu.Unlock()

		// parse message
		var parsedMessage Message
		if err := json.Unmarshal(msg, &parsedMessage); err != nil {
			fmt.Printf("error while parsing message: %v \n", err)
			continue
		}

		// set message meta-data
		parsedMessage.ID = uuid.New().String()
		parsedMessage.Sender = c.id
		parsedMessage.Timestamp = time.Now()

		// handle different message types, ie > client-to-client or client-to-broadcast
		switch parsedMessage.Type {
		case MESSAGE:
			if parsedMessage.Recipient != "" {
				docs, _ := json.Marshal(parsedMessage)
				if err := c.natsConn.Publish("chat.direct", docs); err != nil {
					fmt.Printf("error while publishing message to subject: 'chat.direct' %v \n", err)
				}
			} else {
				// broadcast message
				c.hub.broadcast <- &parsedMessage
			}
		default:
			c.hub.broadcast <- &parsedMessage
		}
	}
}

func (c *Client) WritePump() {

}
