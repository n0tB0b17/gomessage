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
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type APIServer struct {
	Port      int
	NatAddr   string
	MongoURI  string
	DBName    string
	s         *http.Server
	nats      *nats.Conn
	mongo     *mongo.Client
	userStore *models.UserStore
}

func NewApiServer(port int, nats *nats.Conn, natAddr string) *APIServer {
	return &APIServer{
		Port:     port,
		MongoURI: "mongodb://agentone:password123@localhost:27017",
		DBName:   "fastmsg",
		NatAddr:  natAddr,
		nats:     nats,
	}
}

func (a *APIServer) Start() error {
	err := a.ConnectToDB()
	if err != nil {
		return err
	}

	addr := fmt.Sprintf(":%d", a.Port)
	router := mux.NewRouter()
	hub := models.GetNewHUB(a.nats)
	go hub.Run()

	router.HandleFunc("/api/v1/status", TestAPIHandler).Methods("GET")
	router.HandleFunc("/api/v1/users", func(w http.ResponseWriter, r *http.Request) {
		hub.Mu.RLock()
		defer hub.Mu.RUnlock()

		users := make([]string, 0, len(hub.Clients))
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
	fmt.Printf("Connected to NATS server, name is: %s \n", a.nats.ConnectedServerName())
	fmt.Printf("Connected to Mongodb database: %s \n", a.MongoURI)
	return a.s.ListenAndServe()
}

// shutdown api server
func (a *APIServer) Shutdown(ctx context.Context) error {
	contxt, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if a.mongo != nil {
		a.mongo.Disconnect(ctx)
	}

	if a.s != nil {
		return a.s.Shutdown(contxt)
	}

	return fmt.Errorf("server is already down")
}

func (a *APIServer) ConnectToDB() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(a.MongoURI))
	if err != nil {
		fmt.Printf("error while connecting to mongodb server :%s >> err >> %v \n", a.MongoURI, err)
		return err
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("unable to reach mongodb server at: %s \n", a.MongoURI)
	}

	a.mongo = client
	a.userStore = models.NewUserStore(client, a.DBName)
	return nil
}
