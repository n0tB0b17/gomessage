package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/n0tB0b17/gomessage/internal/models"
	"github.com/nats-io/nats.go"
	"github.com/rs/cors"
)

type APIServer struct {
	Port    int
	NatAddr string
	s       *http.Server
	nats    *nats.Conn
}

func NewApiServer(port int, nats *nats.Conn, natAddr string) *APIServer {
	return &APIServer{
		Port:    port,
		NatAddr: natAddr,
		nats:    nats,
	}
}

// start api server

func (a *APIServer) Start() error {
	addr := fmt.Sprintf(":%d", a.Port)
	router := mux.NewRouter()
	hub := models.GetNewHUB(a.nats)
	go hub.Run()

	router.HandleFunc("/api/v1/status", TestAPIHandler).Methods("GET")
	router.HandleFunc("/api/v1/users", func(w http.ResponseWriter, r *http.Request) {
		hub.Mu.RLock()
		defer hub.Mu.RUnlock()

		users := make([]string, len(hub.Clients))
		for _, clients := range hub.Clients {
			users = append(users, clients.Username)
		}

		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"count": len(users),
			"users": users,
		}

		json.NewEncoder(w).Encode(resp)
	}).Methods("GET")

	router.HandleFunc("/api/v1/message/ws", func(w http.ResponseWriter, r *http.Request) {
		ServeWS(hub, w, r, a.NatAddr)
	})

	c := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST", "OPTIONS"},
		AllowedHeaders: []string{"Content-Type", "Accept", "Content-Length"},
	})

	handler := c.Handler(router)

	a.s = &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	fmt.Printf("Starting API server on address: %s \n", addr)
	fmt.Printf("Connected Nats server name is: %s \n", a.nats.ConnectedServerName())
	return a.s.ListenAndServe()
}

// shutdown api server
func (a *APIServer) Shutdown(ctx context.Context) error {
	contxt, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	return a.s.Shutdown(contxt)
}
