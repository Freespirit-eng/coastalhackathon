// Package main — geofencer.go
// Two-tier geofence classification engine.
// Tier 1 (coarse): Axis-aligned bounding box (AABB) check — 4 float comparisons per zone.
// Tier 2 (precise): Ray-casting point-in-polygon for points inside a zone's AABB.
// Thread-safe with RWMutex for hot-reload support.

package main

import (
	"encoding/json"
	"math"
	"os"
	"sync"
)

// Zone represents a restricted geofence zone.
type Zone struct {
	ZoneID   string       `json:"zone_id"`
	Name     string       `json:"name"`
	Severity string       `json:"severity"`
	Polygon  [][2]float64 `json:"polygon"` // [lat, lon] pairs

	// Pre-computed AABB for coarse filter
	MinLat, MaxLat float64
	MinLon, MaxLon float64
}

// ZoneHit is the result of a geofence match.
type ZoneHit struct {
	ZoneID   string
	ZoneName string
	Severity string
}

// Geofencer checks whether a geographic point falls inside any restricted zone.
type Geofencer struct {
	mu    sync.RWMutex
	zones []Zone
}

// NewGeofencer creates an empty geofencer.
func NewGeofencer() *Geofencer {
	return &Geofencer{}
}

// LoadFromFile reads zones from a JSON file and replaces the current zone set.
func (g *Geofencer) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var rawZones []Zone
	if err := json.Unmarshal(data, &rawZones); err != nil {
		return err
	}

	// Pre-compute AABBs
	for i := range rawZones {
		computeAABB(&rawZones[i])
	}

	g.mu.Lock()
	g.zones = rawZones
	g.mu.Unlock()

	return nil
}

// AddZone adds or updates a single zone. Thread-safe.
func (g *Geofencer) AddZone(z Zone) {
	computeAABB(&z)

	g.mu.Lock()
	defer g.mu.Unlock()

	// Update existing or append
	for i, existing := range g.zones {
		if existing.ZoneID == z.ZoneID {
			g.zones[i] = z
			return
		}
	}
	g.zones = append(g.zones, z)
}

// RemoveZone removes a zone by ID. Returns true if found and removed.
func (g *Geofencer) RemoveZone(zoneID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	for i, z := range g.zones {
		if z.ZoneID == zoneID {
			g.zones = append(g.zones[:i], g.zones[i+1:]...)
			return true
		}
	}
	return false
}

// ListZones returns a copy of all zones.
func (g *Geofencer) ListZones() []Zone {
	g.mu.RLock()
	defer g.mu.RUnlock()

	result := make([]Zone, len(g.zones))
	copy(result, g.zones)
	return result
}

// ZoneCount returns the number of active zones.
func (g *Geofencer) ZoneCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.zones)
}

// Check tests a lat/lon point against all zones.
// Returns a list of zones the point falls inside (typically 0 or 1, but handles overlaps).
// Uses two-tier filtering: AABB coarse filter, then ray-casting precision check.
func (g *Geofencer) Check(lat, lon float64) []ZoneHit {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var hits []ZoneHit

	for i := range g.zones {
		z := &g.zones[i]

		// Tier 1: AABB coarse filter — 4 float comparisons
		if lat < z.MinLat || lat > z.MaxLat || lon < z.MinLon || lon > z.MaxLon {
			continue // outside bounding box, skip
		}

		// Tier 2: Ray-casting point-in-polygon precision check
		if pointInPolygon(lat, lon, z.Polygon) {
			hits = append(hits, ZoneHit{
				ZoneID:   z.ZoneID,
				ZoneName: z.Name,
				Severity: z.Severity,
			})
		}
	}

	return hits
}

// computeAABB pre-computes the axis-aligned bounding box for a zone polygon.
func computeAABB(z *Zone) {
	if len(z.Polygon) == 0 {
		return
	}

	z.MinLat = math.MaxFloat64
	z.MaxLat = -math.MaxFloat64
	z.MinLon = math.MaxFloat64
	z.MaxLon = -math.MaxFloat64

	for _, pt := range z.Polygon {
		lat, lon := pt[0], pt[1]
		if lat < z.MinLat {
			z.MinLat = lat
		}
		if lat > z.MaxLat {
			z.MaxLat = lat
		}
		if lon < z.MinLon {
			z.MinLon = lon
		}
		if lon > z.MaxLon {
			z.MaxLon = lon
		}
	}
}

// pointInPolygon uses the ray-casting algorithm to determine if a point
// is inside a polygon. The polygon is defined as a slice of [lat, lon] pairs.
// The last point should equal the first (closed polygon), but handles open polygons too.
func pointInPolygon(lat, lon float64, polygon [][2]float64) bool {
	n := len(polygon)
	if n < 3 {
		return false
	}

	inside := false
	j := n - 1

	for i := 0; i < n; i++ {
		yi, xi := polygon[i][0], polygon[i][1]
		yj, xj := polygon[j][0], polygon[j][1]

		// Ray-casting: count intersections of a horizontal ray from (lat, lon)
		// going to the right with each edge of the polygon
		if ((yi > lat) != (yj > lat)) &&
			(lon < (xj-xi)*(lat-yi)/(yj-yi)+xi) {
			inside = !inside
		}

		j = i
	}

	return inside
}
