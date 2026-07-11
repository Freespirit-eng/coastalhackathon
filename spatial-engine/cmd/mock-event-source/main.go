// Mock Event Source for Engineer 3
// Replays ZoneViolationEvent and PositionBatch messages from a JSONL fixture file
// over TCP IPC, so Engineer 3 can develop independently without a live spatial engine.
//
// Usage:
//   go run . --fixture ../../fixtures/sample_events.jsonl --addr localhost:9100 --rate 10

package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	fixture := flag.String("fixture", "../../fixtures/sample_events.jsonl", "Path to JSONL fixture file")
	addr := flag.String("addr", "localhost:9100", "TCP address to listen on")
	rate := flag.Float64("rate", 10, "Events per second replay rate")
	loop := flag.Bool("loop", true, "Loop the fixture file continuously")

	flag.Parse()

	fmt.Println("╔══════════════════════════════════════════════════════╗")
	fmt.Println("║   AISentry — Mock Event Source (Engineer 3 Stub)     ║")
	fmt.Println("╚══════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("Fixture:  %s\n", *fixture)
	fmt.Printf("Listen:   TCP %s\n", *addr)
	fmt.Printf("Rate:     %.1f events/sec\n", *rate)
	fmt.Printf("Loop:     %v\n", *loop)
	fmt.Println()

	// Load fixture file into memory
	events, err := loadFixture(*fixture)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Could not load fixture: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d events from fixture\n", len(events))

	// Listen for connections from Engineer 3
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Could not listen on %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Printf("\nWaiting for Engineer 3 to connect on %s...\n", *addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Accept error: %v\n", err)
			continue
		}

		fmt.Printf("Client connected: %s\n", conn.RemoteAddr())
		go handleClient(conn, events, *rate, *loop)
	}
}

func handleClient(conn net.Conn, events [][]byte, rate float64, loop bool) {
	defer conn.Close()

	interval := time.Duration(float64(time.Second) / rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	idx := 0
	totalSent := 0

	for {
		<-ticker.C

		if idx >= len(events) {
			if loop {
				idx = 0
				fmt.Printf("  Looping fixture (sent %d events so far)\n", totalSent)
			} else {
				fmt.Printf("  Fixture complete (%d events sent)\n", totalSent)
				return
			}
		}

		event := events[idx]
		idx++

		// Send length-prefixed frame
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(event)))

		if _, err := conn.Write(lenBuf); err != nil {
			fmt.Printf("  Client disconnected: %v\n", err)
			return
		}
		if _, err := conn.Write(event); err != nil {
			fmt.Printf("  Client disconnected: %v\n", err)
			return
		}

		totalSent++
	}
}

func loadFixture(path string) ([][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var events [][]byte
	scanner := bufio.NewScanner(f)
	// Increase scanner buffer for large position batches
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		// Make a copy since scanner reuses the buffer
		event := make([]byte, len(line))
		copy(event, line)
		events = append(events, event)
	}

	return events, scanner.Err()
}
