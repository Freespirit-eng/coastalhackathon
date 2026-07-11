// Package main — vessel.go
// Vessel simulation with realistic movement models.
// Normal vessels: bounded random walk with smooth heading changes.
// Scripted violators: follow waypoint routes through restricted zones.

package main

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	pb "github.com/aisentry/simulator/proto"
)

// Vessel represents a single simulated maritime vessel.
type Vessel struct {
	ID   string
	MMSI string

	// Current state
	Lat float64
	Lon float64
	SOG float32 // speed over ground in knots
	COG float32 // course over ground in degrees (0-359)

	// Movement model
	IsViolator     bool
	Waypoints      []Waypoint // for scripted violators
	WaypointIdx    int
	HeadingChangeT float64 // time until next random heading change (seconds)

	// Boundaries for normal vessels
	LatMin, LatMax float64
	LonMin, LonMax float64

	rng *rand.Rand
}

// Waypoint is a target position for scripted violator routes.
type Waypoint struct {
	Lat float64
	Lon float64
}

// NewVessel creates a new vessel with random initial position within the simulation bounds.
func NewVessel(id int, isViolator bool, rng *rand.Rand) *Vessel {
	// South China Sea / Indian Ocean region: lat 0-20°N, lon 100-125°E
	latMin, latMax := 0.0, 20.0
	lonMin, lonMax := 100.0, 125.0

	v := &Vessel{
		ID:             fmt.Sprintf("vessel-%08d", id),
		MMSI:           fmt.Sprintf("%09d", 200000000+id),
		Lat:            latMin + rng.Float64()*(latMax-latMin),
		Lon:            lonMin + rng.Float64()*(lonMax-lonMin),
		SOG:            float32(5.0 + rng.Float64()*15.0), // 5-20 knots
		COG:            float32(rng.Float64() * 360.0),
		IsViolator:     isViolator,
		LatMin:         latMin,
		LatMax:         latMax,
		LonMin:         lonMin,
		LonMax:         lonMax,
		HeadingChangeT: 5.0 + rng.Float64()*10.0, // change heading every 5-15 seconds
		rng:            rng,
	}

	if isViolator {
		// Assign a scripted violation route
		route := GetViolatorRoute(rng)
		v.Waypoints = route
		v.WaypointIdx = 0
		// Start near the first waypoint
		v.Lat = route[0].Lat + (rng.Float64()-0.5)*0.5
		v.Lon = route[0].Lon + (rng.Float64()-0.5)*0.5
		v.SOG = float32(8.0 + rng.Float64()*10.0) // violators tend to be faster
	}

	return v
}

// Tick advances the vessel by dt seconds and returns the current AIS record.
func (v *Vessel) Tick(dt float64) pb.AISRecord {
	if v.IsViolator {
		v.tickViolator(dt)
	} else {
		v.tickNormal(dt)
	}

	return pb.AISRecord{
		VesselID:           v.ID,
		MMSI:               v.MMSI,
		Lat:                v.Lat,
		Lon:                v.Lon,
		SOGKnots:           v.SOG,
		COGDegrees:         v.COG,
		TSUnixMs:           time.Now().UnixMilli(),
		IsScriptedViolator: v.IsViolator,
	}
}

// tickNormal implements bounded random walk movement.
func (v *Vessel) tickNormal(dt float64) {
	// Advance position
	sogMS := float64(v.SOG) * 0.514444 // knots to m/s
	distM := sogMS * dt

	cogRad := float64(v.COG) * math.Pi / 180.0
	// Approximate: 1 degree lat ≈ 111,320 m, 1 degree lon ≈ 111,320 * cos(lat) m
	dLat := (distM * math.Cos(cogRad)) / 111320.0
	dLon := (distM * math.Sin(cogRad)) / (111320.0 * math.Cos(v.Lat*math.Pi/180.0))

	v.Lat += dLat
	v.Lon += dLon

	// Random heading perturbation
	v.HeadingChangeT -= dt
	if v.HeadingChangeT <= 0 {
		// Small random course adjustment: ±30 degrees
		delta := (v.rng.Float64() - 0.5) * 60.0
		v.COG = float32(math.Mod(float64(v.COG)+delta+360.0, 360.0))
		// Slight speed variation: ±2 knots
		v.SOG += float32((v.rng.Float64() - 0.5) * 4.0)
		if v.SOG < 1.0 {
			v.SOG = 1.0
		}
		if v.SOG > 25.0 {
			v.SOG = 25.0
		}
		v.HeadingChangeT = 5.0 + v.rng.Float64()*10.0
	}

	// Boundary reflection — keep vessels within simulation area
	if v.Lat < v.LatMin {
		v.Lat = v.LatMin + 0.1
		v.COG = float32(math.Mod(360.0-float64(v.COG)+360.0, 360.0)) // reflect
	}
	if v.Lat > v.LatMax {
		v.Lat = v.LatMax - 0.1
		v.COG = float32(math.Mod(360.0-float64(v.COG)+360.0, 360.0))
	}
	if v.Lon < v.LonMin {
		v.Lon = v.LonMin + 0.1
		v.COG = float32(math.Mod(180.0-float64(v.COG)+360.0, 360.0))
	}
	if v.Lon > v.LonMax {
		v.Lon = v.LonMax - 0.1
		v.COG = float32(math.Mod(180.0-float64(v.COG)+360.0, 360.0))
	}
}

// tickViolator steers toward the next waypoint on the scripted route.
func (v *Vessel) tickViolator(dt float64) {
	if len(v.Waypoints) == 0 {
		v.tickNormal(dt)
		return
	}

	target := v.Waypoints[v.WaypointIdx]

	// Calculate bearing to target
	dLat := target.Lat - v.Lat
	dLon := target.Lon - v.Lon
	bearing := math.Atan2(dLon, dLat) * 180.0 / math.Pi
	if bearing < 0 {
		bearing += 360.0
	}
	v.COG = float32(bearing)

	// Check if we've reached the waypoint (within ~500m)
	dist := math.Sqrt(dLat*dLat + dLon*dLon)
	if dist < 0.005 { // ~500m in degrees
		v.WaypointIdx = (v.WaypointIdx + 1) % len(v.Waypoints)
	}

	// Advance position toward waypoint
	sogMS := float64(v.SOG) * 0.514444
	distM := sogMS * dt
	cogRad := float64(v.COG) * math.Pi / 180.0
	dLatM := (distM * math.Cos(cogRad)) / 111320.0
	dLonM := (distM * math.Sin(cogRad)) / (111320.0 * math.Cos(v.Lat*math.Pi/180.0))

	v.Lat += dLatM
	v.Lon += dLonM

	// Add small noise for realism
	v.Lat += (v.rng.Float64() - 0.5) * 0.0001
	v.Lon += (v.rng.Float64() - 0.5) * 0.0001
}
