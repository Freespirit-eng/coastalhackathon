// Package main — hub.go
// WebSocket Pub/Sub Hub.
// Manages connected clients and broadcasts events.

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for the dashboard
	},
}

// Client represents a connected WebSocket client.
type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
}

// Hub maintains the set of active clients and broadcasts messages.
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	mu         sync.Mutex // protects clients map
}

// NewHub creates a new WebSocket hub.
func NewHub() *Hub {
	return &Hub{
		broadcast:  make(chan []byte, 1024), // buffer size for incoming messages
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
	}
}

// Run starts the hub loop.
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			fmt.Printf("[hub] Client connected. Total: %d\n", len(h.clients))

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			fmt.Printf("[hub] Client disconnected. Total: %d\n", len(h.clients))

		case message := <-h.broadcast:
			h.mu.Lock()
			for client := range h.clients {
				// Non-blocking send (drops if client is too slow)
				select {
				case client.send <- message:
				default:
					delete(h.clients, client)
					close(client.send)
					fmt.Println("[hub] Client buffer full, dropping connection")
				}
			}
			h.mu.Unlock()
		}
	}
}

// Broadcast sends a JSON-encoded envelope to all clients.
func (h *Hub) Broadcast(envelope interface{}) {
	data, err := json.Marshal(envelope)
	if err != nil {
		return
	}
	
	// Non-blocking broadcast
	select {
	case h.broadcast <- data:
	default:
		// Hub is backed up (rare, but protects the pipeline)
	}
}

// HandleWebSocket handles HTTP requests and upgrades them to WS.
func (h *Hub) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Printf("[hub] Upgrade error: %v\n", err)
		return
	}

	client := &Client{
		hub:  h,
		conn: conn,
		send: make(chan []byte, 256), // buffer up to 256 messages per client
	}
	client.hub.register <- client

	// Start pump goroutines
	go client.writePump()
	go client.readPump() // needed to process close messages from client
}

// writePump pumps messages from the hub to the websocket connection.
func (c *Client) writePump() {
	ticker := time.NewTicker(54 * time.Second) // ping ticker
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				// The hub closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Add queued chat messages to the current websocket message.
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// readPump pumps messages from the websocket connection to the hub.
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(512)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(60 * time.Second)); return nil })
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				fmt.Printf("[hub] error: %v\n", err)
			}
			break
		}
	}
}
