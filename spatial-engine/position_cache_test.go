package main

import (
	"testing"
	"time"
)

func TestPositionCache_UpdateAndGet(t *testing.T) {
	pc := NewPositionCache(1 * time.Minute)

	entry := PositionEntry{
		VesselID: "v1",
		Lat:      10.0,
		Lon:      20.0,
	}

	pc.Update(entry)

	if pc.Count() != 1 {
		t.Errorf("expected count 1, got %d", pc.Count())
	}

	got, ok := pc.Get("v1")
	if !ok {
		t.Fatal("expected to find v1")
	}
	if got.Lat != 10.0 || got.Lon != 20.0 {
		t.Errorf("unexpected coords: %f, %f", got.Lat, got.Lon)
	}
}

func TestPositionCache_Eviction(t *testing.T) {
	// Use a very short TTL
	pc := NewPositionCache(10 * time.Millisecond)

	entry := PositionEntry{
		VesselID: "v1",
		Lat:      10.0,
		Lon:      20.0,
	}

	pc.Update(entry)

	// Wait for TTL to expire
	time.Sleep(20 * time.Millisecond)

	// Explicit eviction run
	pc.evictExpired()

	if pc.Count() != 0 {
		t.Errorf("expected count 0 after eviction, got %d", pc.Count())
	}

	_, ok := pc.Get("v1")
	if ok {
		t.Error("expected v1 to be evicted")
	}
}

func TestPositionCache_Snapshot(t *testing.T) {
	pc := NewPositionCache(1 * time.Minute)

	pc.Update(PositionEntry{VesselID: "v1", Lat: 10.0})
	pc.Update(PositionEntry{VesselID: "v2", Lat: 20.0})

	snap := pc.Snapshot()
	if len(snap) != 2 {
		t.Errorf("expected 2 entries in snapshot, got %d", len(snap))
	}
}
