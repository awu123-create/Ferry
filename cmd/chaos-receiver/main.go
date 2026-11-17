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

	log.Println("chaos-receiver skeleton started; waiting for shutdown signal")
	<-ctx.Done()
	log.Println("chaos-receiver shutting down")
}
