// Package main — main.go
// AISentry High-Velocity AIS Simulator
// CLI entry point with configurable vessel count, message rate, transport mode,
// and scripted violation scenarios.
//
// Usage:
//   go run . --vessels 10000 --rate 50000 --transport udp --target 127.0.0.1:9001
//   go run . --mode replay-gen --output ../fixtures/sample_batches.bin --vessels 5000 --duration 10

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	// CLI flags
	vessels := flag.Int("vessels", 10000, "Number of concurrent simulated vessels")
	rate := flag.Int("rate", 50000, "Target aggregate messages/sec")
	batchSize := flag.Int("batch-size", 256, "Records per AISBatch")
	target := flag.String("target", "127.0.0.1:9001", "Ingestion endpoint address (host:port)")
	transport := flag.String("transport", "udp", "Transport mode: udp, tcp, or file")
	violatorRatio := flag.Float64("violator-ratio", 0.05, "Fraction of vessels that are scripted violators (0.0-1.0)")
	durationSec := flag.Int("duration", 0, "Simulation duration in seconds (0 = run forever)")
	mode := flag.String("mode", "stream", "Operating mode: stream (live send) or replay-gen (write fixtures to file)")
	output := flag.String("output", "../fixtures/sample_batches.bin", "Output file path for replay-gen mode")
	workers := flag.Int("workers", 0, "Number of worker goroutines (0 = auto-detect CPU cores)")

	flag.Parse()

	// Banner
	fmt.Println("╔══════════════════════════════════════════════════════╗")
	fmt.Println("║      AISentry — High-Velocity AIS Simulator         ║")
	fmt.Println("╚══════════════════════════════════════════════════════╝")
	fmt.Println()

	// Determine mode and create sender
	var sender Sender
	var err error

	effectiveMode := strings.ToLower(*mode)
	if effectiveMode == "replay-gen" {
		// Override transport to file mode
		fmt.Printf("Mode:      replay-gen (writing fixtures to %s)\n", *output)
		sender, err = NewFileSender(*output)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: Failed to create file sender: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Live streaming mode
		switch strings.ToLower(*transport) {
		case "udp":
			fmt.Printf("Transport: UDP → %s\n", *target)
			sender, err = NewUDPSender(*target)
		case "tcp":
			fmt.Printf("Transport: TCP → %s\n", *target)
			sender, err = NewTCPSender(*target)
		case "file":
			fmt.Printf("Transport: File → %s\n", *output)
			sender, err = NewFileSender(*output)
		default:
			fmt.Fprintf(os.Stderr, "ERROR: Unknown transport %q (use: udp, tcp, file)\n", *transport)
			os.Exit(1)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: Failed to create sender: %v\n", err)
			os.Exit(1)
		}
	}
	defer sender.Close()

	// Print configuration
	violatorCount := int(float64(*vessels) * *violatorRatio)
	fmt.Printf("Vessels:   %d (%d normal, %d scripted violators)\n", *vessels, *vessels-violatorCount, violatorCount)
	fmt.Printf("Rate:      %d msgs/sec target\n", *rate)
	fmt.Printf("Batch:     %d records/batch\n", *batchSize)
	if *durationSec > 0 {
		fmt.Printf("Duration:  %d seconds\n", *durationSec)
	} else {
		fmt.Printf("Duration:  unlimited (Ctrl+C to stop)\n")
	}
	fmt.Printf("Workers:   %d\n", *workers)
	fmt.Println()
	fmt.Println("Demo zones loaded:")
	for _, z := range DemoZones() {
		fmt.Printf("  • [%s] %s (severity: %s)\n", z.ZoneID, z.Name, z.Severity)
	}
	fmt.Println()
	fmt.Println("Starting simulation...")
	fmt.Println("─────────────────────────────────────────────────────")

	// Build engine config
	cfg := EngineConfig{
		VesselCount:   *vessels,
		TargetRate:    *rate,
		BatchSize:     *batchSize,
		ViolatorRatio: *violatorRatio,
		DurationSec:   *durationSec,
		NumWorkers:    *workers,
	}

	engine := NewEngine(cfg, sender)

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		fmt.Printf("\n\nReceived %v — shutting down gracefully...\n", sig)
		cancel()
	}()

	// Run the simulation
	if err := engine.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Engine stopped with error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\nSimulation complete.")
}
