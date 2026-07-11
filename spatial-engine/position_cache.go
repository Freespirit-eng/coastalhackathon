// Package main — position_cache.go
// Per-vessel last-known-position cache with TTL-based expiry.
// Uses sync.Map for lock-free concurrent reads (optimized for read-heavy workloads).

package main

import (
	"sync"
	"sync/atomic"
	"time"
)

// PositionEntry stores the last-known position of a vessel.
type PositionEntry struct {
	VesselID   string
	Lat        float64
	Lon        float64
	SOGKnots   float32
	COGDegrees float32
	TSUnixMs   int64
	UpdatedAt  time.Time
}

// PositionCache is a thread-safe cache of vessel positions.
type PositionCache struct {
	entries sync.Map // map[string]*PositionEntry (keyed by vessel_id)
	ttl     time.Duration
	count   atomic.Int64
}

// NewPositionCache creates a position cache with the given TTL.
// Starts a background goroutine to evict expired entries.
func NewPositionCache(ttl time.Duration) *PositionCache {
	pc := &PositionCache{
		ttl: ttl,
	}
	return pc
}

// StartEviction starts the background eviction goroutine.
// Call this after creating the cache. It runs until the done channel is closed.
func (pc *PositionCache) StartEviction(done <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				pc.evictExpired()
			}
		}
	}()
}

// Update stores or updates a vessel's position.
func (pc *PositionCache) Update(entry PositionEntry) {
	entry.UpdatedAt = time.Now()

	_, loaded := pc.entries.LoadOrStore(entry.VesselID, &entry)
	if loaded {
		// Already existed, update in place
		pc.entries.Store(entry.VesselID, &entry)
	} else {
		pc.count.Add(1)
	}
}

// Get retrieves a vessel's last-known position.
func (pc *PositionCache) Get(vesselID string) (*PositionEntry, bool) {
	val, ok := pc.entries.Load(vesselID)
	if !ok {
		return nil, false
	}
	entry := val.(*PositionEntry)

	// Check if expired
	if time.Since(entry.UpdatedAt) > pc.ttl {
		pc.entries.Delete(vesselID)
		pc.count.Add(-1)
		return nil, false
	}

	return entry, true
}

// Count returns the approximate number of active vessels.
func (pc *PositionCache) Count() int64 {
	return pc.count.Load()
}

// Snapshot returns all non-expired positions as a slice.
// Used for periodic batch flush to Engineer 3.
func (pc *PositionCache) Snapshot() []PositionEntry {
	now := time.Now()
	var entries []PositionEntry

	pc.entries.Range(func(key, value interface{}) bool {
		entry := value.(*PositionEntry)
		if now.Sub(entry.UpdatedAt) <= pc.ttl {
			entries = append(entries, *entry)
		}
		return true
	})

	return entries
}

// evictExpired removes all entries older than TTL.
func (pc *PositionCache) evictExpired() {
	now := time.Now()
	var evicted int64

	pc.entries.Range(func(key, value interface{}) bool {
		entry := value.(*PositionEntry)
		if now.Sub(entry.UpdatedAt) > pc.ttl {
			pc.entries.Delete(key)
			evicted++
		}
		return true
	})

	if evicted > 0 {
		pc.count.Add(-evicted)
	}
}
