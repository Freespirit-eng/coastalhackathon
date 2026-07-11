package main

import (
	"testing"
)

func TestPointInPolygon(t *testing.T) {
	// Simple square from (0,0) to (10,10)
	square := [][2]float64{
		{0.0, 0.0},
		{0.0, 10.0},
		{10.0, 10.0},
		{10.0, 0.0},
		{0.0, 0.0},
	}

	tests := []struct {
		lat, lon float64
		inside   bool
	}{
		{5.0, 5.0, true},    // inside
		{15.0, 5.0, false},  // outside top
		{-5.0, 5.0, false},  // outside bottom
		{5.0, -5.0, false},  // outside left
		{5.0, 15.0, false},  // outside right
		// Point on boundary is technically undefined in standard ray-casting,
		// but typically ray-casting returns false or true depending on edge direction.
		// We'll just test clear inside/outside cases.
	}

	for i, tc := range tests {
		got := pointInPolygon(tc.lat, tc.lon, square)
		if got != tc.inside {
			t.Errorf("test %d (lat:%f, lon:%f): got %v, want %v", i, tc.lat, tc.lon, got, tc.inside)
		}
	}
}

func TestGeofencer_Check(t *testing.T) {
	g := NewGeofencer()

	z := Zone{
		ZoneID:   "z1",
		Name:     "Test Zone",
		Severity: "HIGH",
		Polygon: [][2]float64{
			{10.0, 10.0},
			{10.0, 20.0},
			{20.0, 20.0},
			{20.0, 10.0},
			{10.0, 10.0},
		},
	}
	g.AddZone(z)

	// Test inside
	hits := g.Check(15.0, 15.0)
	if len(hits) != 1 || hits[0].ZoneID != "z1" {
		t.Errorf("expected 1 hit for z1, got %v", hits)
	}

	// Test outside (coarse filter will reject)
	hits = g.Check(0.0, 0.0)
	if len(hits) != 0 {
		t.Errorf("expected 0 hits, got %v", hits)
	}
}

func TestGeofencer_AddRemoveList(t *testing.T) {
	g := NewGeofencer()

	z1 := Zone{ZoneID: "z1", Name: "Zone 1", Polygon: [][2]float64{{0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}}}
	z2 := Zone{ZoneID: "z2", Name: "Zone 2", Polygon: [][2]float64{{0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}}}

	g.AddZone(z1)
	g.AddZone(z2)

	if g.ZoneCount() != 2 {
		t.Errorf("expected 2 zones, got %d", g.ZoneCount())
	}

	zones := g.ListZones()
	if len(zones) != 2 {
		t.Errorf("expected 2 zones in list, got %d", len(zones))
	}

	ok := g.RemoveZone("z1")
	if !ok {
		t.Error("expected RemoveZone to return true")
	}

	if g.ZoneCount() != 1 {
		t.Errorf("expected 1 zone after removal, got %d", g.ZoneCount())
	}

	ok = g.RemoveZone("nonexistent")
	if ok {
		t.Error("expected RemoveZone to return false for nonexistent zone")
	}
}

func BenchmarkGeofencer_Check_Inside(b *testing.B) {
	g := NewGeofencer()
	g.AddZone(Zone{
		ZoneID: "z1",
		Polygon: [][2]float64{
			{10.0, 10.0},
			{10.0, 20.0},
			{20.0, 20.0},
			{20.0, 10.0},
			{10.0, 10.0},
		},
	})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = g.Check(15.0, 15.0)
	}
}

func BenchmarkGeofencer_Check_Outside(b *testing.B) {
	g := NewGeofencer()
	g.AddZone(Zone{
		ZoneID: "z1",
		Polygon: [][2]float64{
			{10.0, 10.0},
			{10.0, 20.0},
			{20.0, 20.0},
			{20.0, 10.0},
			{10.0, 10.0},
		},
	})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// This will be rejected by the AABB coarse filter
		_ = g.Check(5.0, 5.0)
	}
}
