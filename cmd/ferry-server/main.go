package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("ferry-server skeleton started; waiting for shutdown signal")
	<-ctx.Done()
	log.Println("ferry-server shutting down")
}
