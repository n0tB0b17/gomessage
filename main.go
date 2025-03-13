package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/n0tB0b17/gomessage/internal/api"
	"github.com/nats-io/nats.go"
)

func main() {
	ns, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		fmt.Printf("error while connecting to nats instance: %v \n", err)
		return
	}
	defer ns.Close()

	server := api.NewApiServer(8888, ns)
	if err := server.Start(); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	if err := server.Shutdown(ctx); err != nil {
		fmt.Println("error while shutting down server: ", err)
	}

	fmt.Println("server shutdown successfully")
}
