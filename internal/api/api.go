package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/cors"
)

type APIServer struct {
	Port int
	s    *http.Server
}

func NewApiServer(port int) *APIServer {
	return &APIServer{
		Port: port,
	}
}

// start api server

func (a *APIServer) Start() error {
	addr := fmt.Sprintf(":%d", a.Port)
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/message", TestAPIHandler).Methods("GET")

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
	return a.s.ListenAndServe()
}

// shutdown api server
func (a *APIServer) Shutdown(ctx context.Context) error {
	contxt, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	return a.s.Shutdown(contxt)
}
