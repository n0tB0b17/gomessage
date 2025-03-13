package models

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
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
