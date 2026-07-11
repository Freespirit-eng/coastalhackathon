// Package main — pipeline.go
// Orchestrates data flow: parses payloads, enriches alerts,
// saves to DB, and broadcasts via WS.

package main

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// Pipeline routes incoming events.
type Pipeline struct {
	store *AlertStore
	hub   *Hub
}

// NewPipeline creates the core pipeline.
func NewPipeline(store *AlertStore, hub *Hub) *Pipeline {
	return &Pipeline{
		store: store,
		hub:   hub,
	}
}

// Process handles a typed event payload.
func (p *Pipeline) Process(eventType string, rawPayload []byte) {
	switch eventType {
	case "VIOLATION":
		p.handleViolation(rawPayload)
	case "POSITION_BATCH":
		p.handlePositionBatch(rawPayload)
	default:
		fmt.Printf("[pipeline] Unknown event type: %s\n", eventType)
	}
}

func (p *Pipeline) handleViolation(rawPayload []byte) {
	var v struct {
		VesselID  string  `json:"vessel_id"`
		ZoneID    string  `json:"zone_id"`
		ZoneName  string  `json:"zone_name"`
		Severity  string  `json:"severity"`
		Lat       float64 `json:"lat"`
		Lon       float64 `json:"lon"`
		TSUnixMs  int64   `json:"ts_unix_ms"`
		EventType string  `json:"event_type"`
	}

	if err := json.Unmarshal(rawPayload, &v); err != nil {
		fmt.Printf("[pipeline] VIOLATION parse error: %v\n", err)
		return
	}

	// Generate Alert ID (enrichment)
	alertID := uuid.New().String()

	alert := AlertRecord{
		AlertID:   alertID,
		VesselID:  v.VesselID,
		ZoneID:    v.ZoneID,
		ZoneName:  v.ZoneName,
		Severity:  v.Severity,
		EventType: v.EventType,
		Lat:       v.Lat,
		Lon:       v.Lon,
		TSUnixMs:  v.TSUnixMs,
	}

	// Persist to DB
	if err := p.store.SaveAlert(alert); err != nil {
		fmt.Printf("[pipeline] DB Save Error: %v\n", err)
	}

	// Broadcast to WS clients (matching WS spec)
	envelope := map[string]interface{}{
		"type":    "ALERT",
		"payload": alert,
	}
	p.hub.Broadcast(envelope)
}

func (p *Pipeline) handlePositionBatch(rawPayload []byte) {
	// For position batches, we just pass through to WS clients.
	// The incoming payload schema (from Engineer 2) exactly matches
	// the WS outbound schema for POSITION_BATCH.
	
	// Fast-path: Just create the envelope and broadcast the raw JSON byte map
	// to avoid decoding/encoding the heavy position array twice.
	
	envelope := map[string]interface{}{
		"type":    "POSITION_BATCH",
		"payload": json.RawMessage(rawPayload),
	}
	p.hub.Broadcast(envelope)
}
