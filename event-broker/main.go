// Package main — main.go
// AISentry Event Broker & Alerting Pipeline
// Consumes TCP spatial events, saves alerts to embedded SQLite,
// and fans out over WebSockets to dashboards.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	upstreamAddr := flag.String("upstream-addr", "localhost:9100", "Engineer 2's TCP event stream address")
	listenAddr := flag.String("listen-addr", "0.0.0.0:9003", "HTTP address for WebSockets and REST API")
	dbPath := flag.String("db-path", "alerts.db", "Path to SQLite database")

	flag.Parse()

	fmt.Println("╔══════════════════════════════════════════════════════╗")
	fmt.Println("║   AISentry — Event Broker & Alerting Pipeline        ║")
	fmt.Println("╚══════════════════════════════════════════════════════╝")
	fmt.Println()

	// Initialize Store (SQLite)
	store, err := NewAlertStore(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Failed to init SQLite db at %s: %v\n", *dbPath, err)
		os.Exit(1)
	}
	fmt.Printf("[main] Initialized SQLite DB at %s\n", *dbPath)

	// Initialize Hub (WebSockets)
	hub := NewHub()

	// Initialize Pipeline (Orchestrator)
	pipeline := NewPipeline(store, hub)

	// Initialize Consumer (TCP Client)
	consumer := NewConsumer(*upstreamAddr, pipeline)

	// Initialize API (HTTP Server)
	api := NewAPI(*listenAddr, store, hub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		fmt.Printf("\nReceived %v — shutting down gracefully...\n", sig)
		cancel()
	}()

	var wg sync.WaitGroup

	// Run Hub
	wg.Add(1)
	go func() {
		defer wg.Done()
		hub.Run() // Runs forever until process exits
	}()

	// Run Consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumer.Run(ctx)
	}()

	// Run API Server
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := api.Run(ctx); err != nil {
			fmt.Printf("[main] API Server exited: %v\n", err)
		}
	}()

	// Wait for shutdown
	<-ctx.Done()
	wg.Wait()
	fmt.Println("Event Broker shut down cleanly.")
}
