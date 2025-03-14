package models

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// a central registry for all Clients
type Hub struct {
	Clients    map[string]*Client
	Register   chan *Client
	Unregister chan *Client
	Broadcast  chan *Message
	NatsConn   *nats.Conn
	Mu         sync.RWMutex
}

func GetNewHUB(nc *nats.Conn) *Hub {
	return &Hub{
		Clients:    make(map[string]*Client),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Broadcast:  make(chan *Message, 100),
		NatsConn:   nc,
		Mu:         sync.RWMutex{},
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register: // when a new client registers
			h.Mu.Lock()
			h.Clients[client.Id] = client
			h.Mu.Unlock()

			joinMsg := &Message{
				Type:      JOINED,
				ID:        uuid.New().String(),
				Sender:    client.Username,
				Content:   fmt.Sprintf("%s has joined the chat", client.Username),
				Timestamp: time.Now(),
			}

			h.Broadcast <- joinMsg

		case client := <-h.Unregister: // when client unregisters
			h.Mu.Lock()
			if _, ok := h.Clients[client.Id]; ok {
				delete(h.Clients, client.Id)
				close(client.Send)
				leaveMsg := &Message{
					Type:      LEFT,
					ID:        uuid.New().String(),
					Sender:    client.Username,
					Content:   client.Username + "has left the chat",
					Timestamp: time.Now(),
				}

				h.Mu.Unlock()
				h.Broadcast <- leaveMsg
				fmt.Printf("client unregistered: %s \n", client.Id)
			} else {
				h.Mu.Unlock()
			}

		case msg := <-h.Broadcast: // when a message is broadcasted
			docs, err := json.Marshal(msg)
			if err != nil {
				fmt.Printf("error while marshalling message: %v \n", err)
				continue
			}

			if err := h.NatsConn.Publish("chat.Broadcast", docs); err != nil {
				fmt.Printf("error while publishing message to subject > 'chat.Broadcast': %v \n", err)
			}

			if msg.Type == MESSAGE && msg.Recipient != "" {
				// send message to specific recipient
				h.Mu.RLock()
				if client, ok := h.Clients[msg.Recipient]; ok { // issue here, as we are using msg.Recipient as key instead of id
					select {
					case client.Send <- docs:
					default:
						close(client.Send)
						delete(h.Clients, client.Id)
					}
				}

				// send to sender as confirmation
				if client, ok := h.Clients[msg.Sender]; ok { // issue here, as we are using msg.Sender as key instead of id
					select {
					case client.Send <- docs:
					default:
						close(client.Send)
						delete(h.Clients, client.Id)
					}
				}

				h.Mu.RUnlock()
			} else {
				// Broadcast to all Clients
				h.Mu.RLock()
				for _, client := range h.Clients {
					select {
					case client.Send <- docs:
					default:
						close(client.Send)
						h.Mu.RUnlock()
						h.Mu.Lock()
						delete(h.Clients, client.Id)
						h.Mu.Unlock()
						h.Mu.RLock()
					}
				}

				h.Mu.RUnlock()
			}

		}
	}
}
