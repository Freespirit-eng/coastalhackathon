// Package main — classifier.go
// Sharded worker pool for geofence classification.
// Each worker receives AIS records, runs the geofence check,
// updates the position cache, and publishes violation events.

package main

import (
	"context"
	"sync"
	"time"

	pb "github.com/aisentry/simulator/proto"
)

// ViolationEvent represents a geofence violation to send to Engineer 3.
type ViolationEvent struct {
	VesselID  string `json:"vessel_id"`
	ZoneID    string `json:"zone_id"`
	ZoneName  string `json:"zone_name"`
	Severity  string `json:"severity"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	TSUnixMs  int64  `json:"ts_unix_ms"`
	EventType string `json:"event_type"` // "ZONE_ENTER" | "ZONE_HEARTBEAT" | "ZONE_EXIT"
}

// Classifier manages the sharded worker pool for geofence classification.
type Classifier struct {
	geofencer  *Geofencer
	cache      *PositionCache
	metrics    *Metrics
	workers    []chan pb.AISRecord
	violations chan ViolationEvent
	numWorkers int

	// Per-vessel zone tracking for edge detection (ENTER/EXIT)
	// Each worker owns its own tracker shard — no lock contention
	trackers []map[string]map[string]time.Time // [workerID][vesselID][zoneID] -> last seen time
}

// NewClassifier creates a classifier with N sharded workers.
func NewClassifier(numWorkers int, geofencer *Geofencer, cache *PositionCache, metrics *Metrics) *Classifier {
	workers := make([]chan pb.AISRecord, numWorkers)
	trackers := make([]map[string]map[string]time.Time, numWorkers)

	for i := 0; i < numWorkers; i++ {
		workers[i] = make(chan pb.AISRecord, 4096) // buffered for burst absorption
		trackers[i] = make(map[string]map[string]time.Time)
	}

	return &Classifier{
		geofencer:  geofencer,
		cache:      cache,
		metrics:    metrics,
		workers:    workers,
		violations: make(chan ViolationEvent, 8192),
		numWorkers: numWorkers,
		trackers:   trackers,
	}
}

// Workers returns the worker input channels for the ingestion layer to dispatch to.
func (c *Classifier) Workers() []chan pb.AISRecord {
	return c.workers
}

// Violations returns the channel where violation events are published.
func (c *Classifier) Violations() <-chan ViolationEvent {
	return c.violations
}

// Run starts all classification workers. Blocks until ctx is cancelled.
func (c *Classifier) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for i := 0; i < c.numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.workerLoop(ctx, workerID)
		}(i)
	}

	// Start zone-exit detection goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.exitDetector(ctx)
	}()

	wg.Wait()
	close(c.violations)
}

// workerLoop is the hot path for each classification worker.
func (c *Classifier) workerLoop(ctx context.Context, workerID int) {
	tracker := c.trackers[workerID]
	heartbeatInterval := 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			return

		case record, ok := <-c.workers[workerID]:
			if !ok {
				return
			}

			// Update position cache
			c.cache.Update(PositionEntry{
				VesselID:   record.VesselID,
				Lat:        record.Lat,
				Lon:        record.Lon,
				SOGKnots:   record.SOGKnots,
				COGDegrees: record.COGDegrees,
				TSUnixMs:   record.TSUnixMs,
			})

			// Run geofence check
			hits := c.geofencer.Check(record.Lat, record.Lon)
			c.metrics.RecordsClassified.Add(1)

			// Track which zones this vessel is currently in
			vesselZones, exists := tracker[record.VesselID]
			if !exists {
				vesselZones = make(map[string]time.Time)
				tracker[record.VesselID] = vesselZones
			}

			// Build set of current zone hits for this record
			currentHits := make(map[string]bool, len(hits))

			for _, hit := range hits {
				currentHits[hit.ZoneID] = true
				lastSeen, wasInZone := vesselZones[hit.ZoneID]
				now := time.Now()

				if !wasInZone {
					// ZONE_ENTER — first time this vessel is seen in this zone
					c.metrics.ViolationsFound.Add(1)
					c.emitViolation(ViolationEvent{
						VesselID:  record.VesselID,
						ZoneID:    hit.ZoneID,
						ZoneName:  hit.ZoneName,
						Severity:  hit.Severity,
						Lat:       record.Lat,
						Lon:       record.Lon,
						TSUnixMs:  record.TSUnixMs,
						EventType: "ZONE_ENTER",
					})
					vesselZones[hit.ZoneID] = now
				} else if now.Sub(lastSeen) >= heartbeatInterval {
					// ZONE_HEARTBEAT — vessel is still in zone, periodic update
					c.emitViolation(ViolationEvent{
						VesselID:  record.VesselID,
						ZoneID:    hit.ZoneID,
						ZoneName:  hit.ZoneName,
						Severity:  hit.Severity,
						Lat:       record.Lat,
						Lon:       record.Lon,
						TSUnixMs:  record.TSUnixMs,
						EventType: "ZONE_HEARTBEAT",
					})
					vesselZones[hit.ZoneID] = now
				}
			}

			// Check for ZONE_EXIT — vessel was in a zone but no longer
			for zoneID := range vesselZones {
				if !currentHits[zoneID] {
					// Vessel has exited this zone
					c.emitViolation(ViolationEvent{
						VesselID:  record.VesselID,
						ZoneID:    zoneID,
						ZoneName:  zoneID, // zone name not available from tracker, use ID
						Lat:       record.Lat,
						Lon:       record.Lon,
						TSUnixMs:  record.TSUnixMs,
						EventType: "ZONE_EXIT",
					})
					delete(vesselZones, zoneID)
				}
			}
		}
	}
}

// exitDetector periodically cleans up stale vessel-zone tracking entries.
func (c *Classifier) exitDetector(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			staleThreshold := time.Now().Add(-60 * time.Second)
			for _, tracker := range c.trackers {
				for vesselID, zones := range tracker {
					for zoneID, lastSeen := range zones {
						if lastSeen.Before(staleThreshold) {
							delete(zones, zoneID)
						}
					}
					if len(zones) == 0 {
						delete(tracker, vesselID)
					}
				}
			}
		}
	}
}

// emitViolation sends a violation event to the output channel.
func (c *Classifier) emitViolation(event ViolationEvent) {
	select {
	case c.violations <- event:
	default:
		// Channel full — drop oldest by reading one and pushing new
		select {
		case <-c.violations:
		default:
		}
		c.violations <- event
	}
}
