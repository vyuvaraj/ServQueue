package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"servqueue/pkg/broker"
	"servqueue/pkg/otel"
	"servqueue/pkg/stomp"
	"servqueue/pkg/web"
)

func main() {
	// Initialize OpenTelemetry
	otel.Init()

	engine := broker.NewBrokerEngine()

	// Read credentials and TLS certificate config from environment or default
	stompUser := "admin"
	stompPass := "secret"
	apiToken := "secret-token"

	// TLS config from environment
	tlsCert := os.Getenv("TLS_CERT_FILE")
	tlsKey := os.Getenv("TLS_KEY_FILE")
	
	stompServer := stomp.NewServer(":61613", engine, stompUser, stompPass, tlsCert, tlsKey)
	webServer := web.NewServer(":8082", engine, apiToken, tlsCert, tlsKey)

	log.Println("Starting ServQueue STOMP server on tcp://:61613...")
	go func() {
		if err := stompServer.Start(); err != nil {
			log.Printf("STOMP server error: %v", err)
		}
	}()

	log.Println("Starting ServQueue HTTP management server on http://:8082...")
	go func() {
		if err := webServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Capture shutdown signals
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	<-stopChan
	log.Println("ServQueue: Shutting down gracefully...")

	// 1. Shutdown HTTP Server
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := webServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("ServQueue: HTTP server forced to shutdown: %v", err)
	}

	// 2. Stop STOMP Server
	log.Println("ServQueue: Stopping STOMP server...")
	stompServer.Stop()

	// 3. Stop Broker Engine (stops TimeWheel)
	log.Println("ServQueue: Stopping Broker Engine...")
	engine.Stop()

	log.Println("ServQueue: Shutdown complete")
}
