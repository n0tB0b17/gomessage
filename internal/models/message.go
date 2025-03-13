package models

import "time"

var (
	JOINED  string = "join"
	LEFT           = "leave"
	MESSAGE        = "message"
)

type Message struct {
	Type      string    `json:"type"`
	ID        string    `json:"id"`
	Sender    string    `json:"sender"`
	Recipient string    `json:"recipient,omitempty"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}
