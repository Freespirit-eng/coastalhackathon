// Package main — event_publisher.go
// Publishes violation events and batched position snapshots to Engineer 3
// via TCP IPC with length-prefixed JSON encoding.
// Also writes events to a JSONL fixture file for Engineer 3's mock replay.

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

// EventEnvelope wraps events for the TCP IPC protocol.
type EventEnvelope struct {
	Type    string      `json:"type"`    // "VIOLATION" | "POSITION_BATCH"
	Payload interface{} `json:"payload"`
}

// PositionBatchPayload contains batched vessel positions.
type PositionBatchPayload struct {
	Snapshots []PositionSnapshot `json:"snapshots"`
	TSUnixMs  int64              `json:"ts_unix_ms"`
}

// PositionSnapshot matches the contract/spatial_event.proto schema.
type PositionSnapshot struct {
	VesselID   string  `json:"vessel_id"`
	Lat        float64 `json:"lat"`
	Lon        float64 `json:"lon"`
	SOGKnots   float32 `json:"sog_knots"`
	COGDegrees float32 `json:"cog_degrees"`
	TSUnixMs   int64   `json:"ts_unix_ms"`
}

// EventPublisher sends events to Engineer 3 and writes fixture files.
type EventPublisher struct {
	eventAddr     string
	violations    <-chan ViolationEvent
	cache         *PositionCache
	metrics       *Metrics
	batchHz       int
	fixtureFile   string

	conn   net.Conn
	connMu sync.Mutex
}

// NewEventPublisher creates an event publisher.
func NewEventPublisher(eventAddr string, violations <-chan ViolationEvent, cache *PositionCache, metrics *Metrics, batchHz int, fixtureFile string) *EventPublisher {
	return &EventPublisher{
		eventAddr:   eventAddr,
		violations:  violations,
		cache:       cache,
		metrics:     metrics,
		batchHz:     batchHz,
		fixtureFile: fixtureFile,
	}
}

// Run starts the event publisher. Blocks until ctx is cancelled.
func (ep *EventPublisher) Run(ctx context.Context) {
	var wg sync.WaitGroup

	// Open fixture file for writing
	var fixtureWriter *os.File
	if ep.fixtureFile != "" {
		var err error
		fixtureWriter, err = os.Create(ep.fixtureFile)
		if err != nil {
			fmt.Printf("[event-pub] WARNING: Could not create fixture file %s: %v\n", ep.fixtureFile, err)
		} else {
			defer fixtureWriter.Close()
			fmt.Printf("[event-pub] Writing events to fixture file: %s\n", ep.fixtureFile)
		}
	}

	// Goroutine 1: Forward violation events
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-ep.violations:
				if !ok {
					return
				}

				envelope := EventEnvelope{
					Type:    "VIOLATION",
					Payload: event,
				}

				ep.sendEvent(envelope)

				// Write to fixture file
				if fixtureWriter != nil {
					data, err := json.Marshal(envelope)
					if err == nil {
						fixtureWriter.Write(data)
						fixtureWriter.Write([]byte("\n"))
					}
				}
			}
		}
	}()

	// Goroutine 2: Periodic position batch flush
	wg.Add(1)
	go func() {
		defer wg.Done()

		interval := time.Duration(float64(time.Second) / float64(ep.batchHz))
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				entries := ep.cache.Snapshot()
				if len(entries) == 0 {
					continue
				}

				// Update active vessel count metric
				ep.metrics.ActiveVessels.Store(int64(len(entries)))

				// Convert to snapshots
				snapshots := make([]PositionSnapshot, len(entries))
				for i, e := range entries {
					snapshots[i] = PositionSnapshot{
						VesselID:   e.VesselID,
						Lat:        e.Lat,
						Lon:        e.Lon,
						SOGKnots:   e.SOGKnots,
						COGDegrees: e.COGDegrees,
						TSUnixMs:   e.TSUnixMs,
					}
				}

				envelope := EventEnvelope{
					Type: "POSITION_BATCH",
					Payload: PositionBatchPayload{
						Snapshots: snapshots,
						TSUnixMs:  time.Now().UnixMilli(),
					},
				}

				ep.sendEvent(envelope)

				// Write to fixture file
				if fixtureWriter != nil {
					data, err := json.Marshal(envelope)
					if err == nil {
						fixtureWriter.Write(data)
						fixtureWriter.Write([]byte("\n"))
					}
				}
			}
		}
	}()

	wg.Wait()
}

// sendEvent sends a JSON-encoded event over the TCP connection.
// Uses length-prefixed framing: [4-byte LE length][JSON bytes].
// Handles reconnection if the connection drops.
func (ep *EventPublisher) sendEvent(envelope EventEnvelope) {
	data, err := json.Marshal(envelope)
	if err != nil {
		return
	}

	ep.connMu.Lock()
	defer ep.connMu.Unlock()

	// Lazy connect / reconnect
	if ep.conn == nil {
		ep.conn, err = net.DialTimeout("tcp", ep.eventAddr, 2*time.Second)
		if err != nil {
			// Engineer 3 not running yet — that's OK, just skip
			return
		}
		fmt.Printf("[event-pub] Connected to event bus at %s\n", ep.eventAddr)
	}

	// Write length-prefixed frame
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))

	if _, err := ep.conn.Write(lenBuf); err != nil {
		ep.conn.Close()
		ep.conn = nil
		return
	}
	if _, err := ep.conn.Write(data); err != nil {
		ep.conn.Close()
		ep.conn = nil
		return
	}
}
