// Package main — main.go
// AISentry Low-Latency Spatial Engine & Geofencer
// Ingests AIS position batches over UDP, classifies them against
// restricted-zone geofences, and publishes violations to Engineer 3.
//
// Usage:
//   go run . --ingest-addr 0.0.0.0:9001 --api-addr 0.0.0.0:9002 --zones-file ../contracts/demo_zones.json
//   go run . --help

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
)

func main() {
	// CLI flags
	ingestAddr := flag.String("ingest-addr", "0.0.0.0:9001", "UDP listen address for AIS batches")
	eventAddr := flag.String("event-addr", "localhost:9100", "TCP address for event output to Engineer 3")
	apiAddr := flag.String("api-addr", "0.0.0.0:9002", "REST API listen address for zone management")
	zonesFile := flag.String("zones-file", "../contracts/demo_zones.json", "Path to initial zone config JSON")
	batchHz := flag.Int("position-batch-hz", 10, "Position batch flush rate to Engineer 3 (Hz)")
	workers := flag.Int("workers", 0, "Number of classification workers (0 = auto-detect CPU cores)")
	cacheTTL := flag.Int("cache-ttl", 300, "Position cache TTL in seconds")
	fixtureFile := flag.String("fixture-file", "../fixtures/sample_events.jsonl", "Path to write event fixtures for Engineer 3")

	flag.Parse()

	// Banner
	fmt.Println("╔══════════════════════════════════════════════════════╗")
	fmt.Println("║   AISentry — Low-Latency Spatial Engine & Geofencer ║")
	fmt.Println("╚══════════════════════════════════════════════════════╝")
	fmt.Println()

	// Auto-detect workers
	numWorkers := *workers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	// Initialize components
	metrics := NewMetrics()
	geofencer := NewGeofencer()
	cache := NewPositionCache(time.Duration(*cacheTTL) * time.Second)

	// Load initial zones
	if err := geofencer.LoadFromFile(*zonesFile); err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: Could not load zones from %s: %v\n", *zonesFile, err)
		fmt.Println("Starting with no zones. Use the REST API to add zones.")
	} else {
		fmt.Printf("Zones:     %d loaded from %s\n", geofencer.ZoneCount(), *zonesFile)
		for _, z := range geofencer.ListZones() {
			fmt.Printf("  • [%s] %s (severity: %s)\n", z.ZoneID, z.Name, z.Severity)
		}
	}

	fmt.Printf("Ingest:    UDP %s\n", *ingestAddr)
	fmt.Printf("Event Bus: TCP %s\n", *eventAddr)
	fmt.Printf("API:       http://%s\n", *apiAddr)
	fmt.Printf("Workers:   %d\n", numWorkers)
	fmt.Printf("Batch Hz:  %d\n", *batchHz)
	fmt.Printf("Cache TTL: %d seconds\n", *cacheTTL)
	fmt.Println()
	fmt.Println("Starting engine...")
	fmt.Println("─────────────────────────────────────────────────────")

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Catch signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		fmt.Printf("\n\nReceived %v — shutting down gracefully...\n", sig)
		cancel()
	}()

	// Done channel for background goroutines
	done := ctx.Done()

	// Start position cache eviction
	cache.StartEviction(done)

	// Create classifier (spawns sharded workers)
	classifier := NewClassifier(numWorkers, geofencer, cache, metrics)

	// Create ingestion listener
	ingestion := NewIngestion(*ingestAddr, classifier.Workers(), metrics)

	// Create event publisher
	publisher := NewEventPublisher(*eventAddr, classifier.Violations(), cache, metrics, *batchHz, *fixtureFile)

	// Create REST API
	api := NewAPI(*apiAddr, geofencer, metrics)

	// Start all components
	var wg sync.WaitGroup

	// Metrics reporter
	wg.Add(1)
	go func() {
		defer wg.Done()
		metrics.ConsoleReporter(done)
	}()

	// REST API
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := api.Run(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "API error: %v\n", err)
		}
	}()

	// Event publisher
	wg.Add(1)
	go func() {
		defer wg.Done()
		publisher.Run(ctx)
	}()

	// Classifier workers
	wg.Add(1)
	go func() {
		defer wg.Done()
		classifier.Run(ctx)
	}()

	// Ingestion listener (blocks until ctx cancelled)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := ingestion.Run(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Ingestion error: %v\n", err)
			cancel()
		}
	}()

	wg.Wait()
	fmt.Println("\nSpatial engine shut down.")
}
