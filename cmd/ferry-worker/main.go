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

	log.Println("ferry-worker skeleton started; waiting for shutdown signal")
	<-ctx.Done()
	log.Println("ferry-worker shutting down")
}
