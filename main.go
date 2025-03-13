package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/n0tB0b17/gomessage/internal/api"
)

func main() {
	server := api.NewApiServer(8888)
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
