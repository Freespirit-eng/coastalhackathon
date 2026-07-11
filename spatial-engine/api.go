// Package main — api.go
// REST API for zone management and metrics.
// Endpoints:
//   POST   /api/zones          — add/update zone (hot-reload geofencer)
//   DELETE /api/zones/{zone_id} — remove zone
//   GET    /api/zones          — list all zones
//   GET    /metrics            — Prometheus-compatible metrics

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// API serves the zone management REST API.
type API struct {
	geofencer *Geofencer
	metrics   *Metrics
	addr      string
	server    *http.Server
}

// NewAPI creates a new REST API server.
func NewAPI(addr string, geofencer *Geofencer, metrics *Metrics) *API {
	return &API{
		geofencer: geofencer,
		metrics:   metrics,
		addr:      addr,
	}
}

// Run starts the HTTP server. Blocks until ctx is cancelled.
func (a *API) Run(ctx context.Context) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/zones", a.handleZones)
	mux.HandleFunc("/api/zones/", a.handleZoneByID)
	mux.HandleFunc("/metrics", a.handleMetrics)
	mux.HandleFunc("/health", a.handleHealth)

	a.server = &http.Server{
		Addr:    a.addr,
		Handler: mux,
	}

	// Graceful shutdown on context cancellation
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		a.server.Shutdown(shutdownCtx)
	}()

	fmt.Printf("[api] REST API listening on http://%s\n", a.addr)
	err := a.server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// handleZones handles GET and POST for /api/zones
func (a *API) handleZones(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		a.listZones(w, r)
	case http.MethodPost:
		a.addZone(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleZoneByID handles DELETE for /api/zones/{zone_id}
func (a *API) handleZoneByID(w http.ResponseWriter, r *http.Request) {
	// Extract zone_id from path: /api/zones/{zone_id}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/zones/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "zone_id required", http.StatusBadRequest)
		return
	}
	zoneID := parts[0]

	switch r.Method {
	case http.MethodDelete:
		a.deleteZone(w, r, zoneID)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// listZones returns all active zones as JSON.
func (a *API) listZones(w http.ResponseWriter, r *http.Request) {
	zones := a.geofencer.ListZones()

	// Convert to API response format
	type ZoneResponse struct {
		ZoneID   string       `json:"zone_id"`
		Name     string       `json:"name"`
		Polygon  [][2]float64 `json:"polygon"`
		Severity string       `json:"severity"`
	}

	response := make([]ZoneResponse, len(zones))
	for i, z := range zones {
		response[i] = ZoneResponse{
			ZoneID:   z.ZoneID,
			Name:     z.Name,
			Polygon:  z.Polygon,
			Severity: z.Severity,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(response)
}

// addZone creates or updates a zone (hot-reload).
func (a *API) addZone(w http.ResponseWriter, r *http.Request) {
	var zone Zone
	if err := json.NewDecoder(r.Body).Decode(&zone); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	if zone.ZoneID == "" {
		http.Error(w, "zone_id is required", http.StatusBadRequest)
		return
	}
	if len(zone.Polygon) < 3 {
		http.Error(w, "polygon must have at least 3 points", http.StatusBadRequest)
		return
	}

	a.geofencer.AddZone(zone)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"zone_id": zone.ZoneID,
		"status":  "created",
	})

	fmt.Printf("[api] Zone added/updated: %s (%s)\n", zone.ZoneID, zone.Name)
}

// deleteZone removes a zone by ID.
func (a *API) deleteZone(w http.ResponseWriter, r *http.Request, zoneID string) {
	if a.geofencer.RemoveZone(zoneID) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(map[string]string{
			"zone_id": zoneID,
			"status":  "deleted",
		})
		fmt.Printf("[api] Zone deleted: %s\n", zoneID)
	} else {
		http.Error(w, "zone not found", http.StatusNotFound)
	}
}

// handleMetrics returns Prometheus-compatible metrics.
func (a *API) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte(a.metrics.PrometheusMetrics()))
}

// handleHealth returns a simple health check.
func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":        "ok",
		"zones_loaded":  a.geofencer.ZoneCount(),
		"uptime_sec":    time.Since(a.metrics.StartTime).Seconds(),
	})
}
