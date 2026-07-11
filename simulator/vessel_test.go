package main

import (
	"math"
	"math/rand"
	"testing"
)

func TestNewVessel_Normal(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	v := NewVessel(1, false, rng)

	if v.ID != "vessel-00000001" {
		t.Errorf("unexpected ID: %s", v.ID)
	}
	if v.MMSI != "200000001" {
		t.Errorf("unexpected MMSI: %s", v.MMSI)
	}
	if v.IsViolator {
		t.Error("expected normal vessel, got violator")
	}
	if v.Lat < 0 || v.Lat > 20 {
		t.Errorf("lat out of bounds: %f", v.Lat)
	}
	if v.Lon < 100 || v.Lon > 125 {
		t.Errorf("lon out of bounds: %f", v.Lon)
	}
	if v.SOG < 1 || v.SOG > 25 {
		t.Errorf("speed out of bounds: %f", v.SOG)
	}
}

func TestNewVessel_Violator(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	v := NewVessel(2, true, rng)

	if !v.IsViolator {
		t.Error("expected violator vessel")
	}
	if len(v.Waypoints) == 0 {
		t.Error("violator should have waypoints")
	}
}

func TestVessel_Tick_BoundsCheck(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	v := NewVessel(1, false, rng)

	// Tick many times to ensure vessel stays within bounds
	for i := 0; i < 10000; i++ {
		record := v.Tick(0.1)

		if record.Lat < -90 || record.Lat > 90 {
			t.Fatalf("lat out of valid range after tick %d: %f", i, record.Lat)
		}
		if record.Lon < -180 || record.Lon > 180 {
			t.Fatalf("lon out of valid range after tick %d: %f", i, record.Lon)
		}
	}
}

func TestVessel_Tick_Violator_MovesTowardWaypoint(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	v := NewVessel(1, true, rng)

	if len(v.Waypoints) == 0 {
		t.Skip("no waypoints assigned")
	}

	target := v.Waypoints[0]
	initialDist := math.Sqrt(math.Pow(v.Lat-target.Lat, 2) + math.Pow(v.Lon-target.Lon, 2))

	// Tick several times
	for i := 0; i < 100; i++ {
		v.Tick(1.0)
	}

	finalDist := math.Sqrt(math.Pow(v.Lat-target.Lat, 2) + math.Pow(v.Lon-target.Lon, 2))

	// Violator should have moved closer to (or reached) the first waypoint
	if finalDist > initialDist+0.1 { // small tolerance for noise
		t.Errorf("violator moved away from waypoint: initial dist=%.4f, final dist=%.4f", initialDist, finalDist)
	}
}

func TestDemoZones_Count(t *testing.T) {
	zones := DemoZones()
	if len(zones) != 7 {
		t.Errorf("expected 7 demo zones, got %d", len(zones))
	}
}

func TestGetViolatorRoute(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	route := GetViolatorRoute(rng)
	if len(route) < 2 {
		t.Errorf("expected at least 2 waypoints in route, got %d", len(route))
	}
}

func BenchmarkVesselTick(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	v := NewVessel(1, false, rng)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = v.Tick(0.1)
	}
}
