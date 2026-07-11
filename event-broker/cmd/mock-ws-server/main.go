// Mock WebSocket Server for Engineer 4 (Dashboard)
// Reads `sample_events.jsonl` and streams it over WS to simulated clients.

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func main() {
	fixture := flag.String("fixture", "../../fixtures/sample_events.jsonl", "Path to JSONL fixture file")
	addr := flag.String("addr", "0.0.0.0:9003", "HTTP listen address")
	rate := flag.Float64("rate", 10, "Events per second replay rate")
	loop := flag.Bool("loop", true, "Loop the fixture file continuously")

	flag.Parse()

	fmt.Println("╔══════════════════════════════════════════════════════╗")
	fmt.Println("║   AISentry — Mock WebSocket Server (Eng 4 Stub)      ║")
	fmt.Println("╚══════════════════════════════════════════════════════╝")
	
	events, err := loadFixture(*fixture)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Could not load fixture %s: %v\n", *fixture, err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d events from fixture.\n", len(events))
	
	clients := make(map[*websocket.Conn]bool)
	var clientsMu sync.Mutex

	// Start replay goroutine
	go func() {
		interval := time.Duration(float64(time.Second) / *rate)
		ticker := time.NewTicker(interval)
		idx := 0
		
		for {
			<-ticker.C
			if idx >= len(events) {
				if *loop {
					idx = 0
				} else {
					return
				}
			}
			
			// We have a JSON payload from Eng 2 fixture, we need to wrap it in the WS Envelope format
			var raw map[string]interface{}
			if err := json.Unmarshal(events[idx], &raw); err != nil {
				idx++
				continue
			}
			
			// Map Eng 2 TCP event envelope directly to WS envelope (they match)
			wsData, _ := json.Marshal(raw)
			
			clientsMu.Lock()
			for conn := range clients {
				conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
				if err := conn.WriteMessage(websocket.TextMessage, wsData); err != nil {
					conn.Close()
					delete(clients, conn)
				}
			}
			clientsMu.Unlock()
			
			idx++
		}
	}()

	http.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			fmt.Printf("Upgrade failed: %v\n", err)
			return
		}
		
		clientsMu.Lock()
		clients[conn] = true
		fmt.Printf("Client connected. Total: %d\n", len(clients))
		clientsMu.Unlock()
		
		// keep connection alive and read blindly
		go func() {
			defer func() {
				clientsMu.Lock()
				delete(clients, conn)
				clientsMu.Unlock()
				conn.Close()
				fmt.Println("Client disconnected.")
			}()
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					break
				}
			}
		}()
	})

	fmt.Printf("WebSocket server listening on ws://%s/stream\n", *addr)
	http.ListenAndServe(*addr, nil)
}

func loadFixture(path string) ([][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var events [][]byte
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		event := make([]byte, len(line))
		copy(event, line)
		events = append(events, event)
	}

	return events, scanner.Err()
}
