// Package main — api.go
// HTTP server hosting the WebSocket upgrader and REST endpoints.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// API serves the REST endpoints and WS hub.
type API struct {
	addr   string
	store  *AlertStore
	hub    *Hub
	server *http.Server
}

// NewAPI creates the API server.
func NewAPI(addr string, store *AlertStore, hub *Hub) *API {
	return &API{
		addr:  addr,
		store: store,
		hub:   hub,
	}
}

// Run starts the HTTP server. Blocks until ctx is cancelled.
func (a *API) Run(ctx context.Context) error {
	mux := http.NewServeMux()

	// WebSocket endpoint
	mux.HandleFunc("/stream", a.hub.HandleWebSocket)

	// REST endpoint
	mux.HandleFunc("/api/alerts", a.handleGetAlerts)
	
	// Health endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	a.server = &http.Server{
		Addr:    a.addr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		a.server.Shutdown(shutdownCtx)
	}()

	fmt.Printf("[api] HTTP/WS Server listening on http://%s\n", a.addr)
	err := a.server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func (a *API) handleGetAlerts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	filters := QueryFilters{
		VesselID: q.Get("vessel_id"),
		ZoneID:   q.Get("zone_id"),
	}

	if limitStr := q.Get("limit"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil {
			filters.Limit = limit
		}
	}
	if fromStr := q.Get("from"); fromStr != "" {
		if from, err := strconv.ParseInt(fromStr, 10, 64); err == nil {
			filters.FromMs = from
		}
	}
	if toStr := q.Get("to"); toStr != "" {
		if to, err := strconv.ParseInt(toStr, 10, 64); err == nil {
			filters.ToMs = to
		}
	}

	alerts, err := a.store.QueryAlerts(filters)
	if err != nil {
		http.Error(w, fmt.Sprintf("db query error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(alerts)
}
