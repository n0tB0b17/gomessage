package models

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// a central registry for all clients
type Hub struct {
	clients    map[string]*Client
	register   chan *Client
	unregister chan *Client
	broadcast  chan *Message
	natsConn   *nats.Conn
	mu         sync.RWMutex
}

func GetNewHUB(nc *nats.Conn) *Hub {
	return &Hub{
		clients:    make(map[string]*Client),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan *Message),
		natsConn:   nc,
		mu:         sync.RWMutex{},
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register: // when a new client registers
			h.mu.Lock()
			h.clients[client.id] = client
			h.mu.Unlock()

			joinMsg := &Message{
				Type:      JOINED,
				ID:        uuid.New().String(),
				Sender:    client.username,
				Content:   client.username + "has joined the chat",
				Timestamp: time.Now(),
			}

			h.broadcast <- joinMsg // broadcast to all clients
			fmt.Printf("new client registered: %s \n", client.id)

		case client := <-h.unregister: // when client unregisters
			h.mu.Lock()
			if _, ok := h.clients[client.id]; ok {
				delete(h.clients, client.id)
				close(client.send)
				leaveMsg := &Message{
					Type:      LEFT,
					ID:        uuid.New().String(),
					Sender:    client.username,
					Content:   client.username + "has left the chat",
					Timestamp: time.Now(),
				}

				h.mu.Unlock()
				h.broadcast <- leaveMsg
				fmt.Printf("client unregistered: %s \n", client.id)
			} else {
				h.mu.Unlock()
			}

		case msg := <-h.broadcast: // when a message is broadcasted
			docs, err := json.Marshal(msg)
			if err != nil {
				fmt.Printf("error while marshalling message: %v \n", err)
				continue
			}

			if err := h.natsConn.Publish("chat.broadcast", docs); err != nil {
				fmt.Printf("error while publishing message to subject > 'chat.broadcast': %v \n", err)
			}

			if msg.Type == MESSAGE && msg.Recipient != "" {
				// send message to specific recipient
				h.mu.RLock()
				if client, ok := h.clients[msg.Recipient]; ok { // issue here, as we are using msg.Recipient as key instead of id
					select {
					case client.send <- docs:
					default:
						close(client.send)
						delete(h.clients, client.id)
					}
				}

				// send to sender as confirmation
				if client, ok := h.clients[msg.Sender]; ok { // issue here, as we are using msg.Sender as key instead of id
					select {
					case client.send <- docs:
					default:
						close(client.send)
						delete(h.clients, client.id)
					}
				}

				h.mu.RUnlock()
			} else {
				// broadcast to all clients
				h.mu.RLock()
				for _, client := range h.clients {
					select {
					case client.send <- docs:
					default:
						close(client.send)
						h.mu.RUnlock()
						h.mu.Lock()
						delete(h.clients, client.id)
						h.mu.Unlock()
						h.mu.RLock()
					}
				}

				h.mu.RUnlock()
			}

		}
	}
}
