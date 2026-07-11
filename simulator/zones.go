// Package main — zones.go
// Defines demo restricted zones and scripted violator waypoint routes.
// These zones match contracts/demo_zones.json for cross-engineer consistency.

package main

import "math/rand"

// DemoZone represents a restricted/illegal-fishing zone polygon.
type DemoZone struct {
	ZoneID   string
	Name     string
	Severity string
	Polygon  [][2]float64 // [lat, lon] pairs forming a closed polygon
}

// ViolatorRoute is a sequence of waypoints that deliberately transit through
// one or more restricted zones, used for scripted violation scenarios.
type ViolatorRoute struct {
	Name      string
	Waypoints []Waypoint
}

// DemoZones returns the hardcoded set of restricted zones matching contracts/demo_zones.json.
func DemoZones() []DemoZone {
	return []DemoZone{
		{
			ZoneID:   "zone-001",
			Name:     "Spratly Islands Exclusion Zone",
			Severity: "HIGH",
			Polygon:  [][2]float64{{10.0, 113.0}, {10.0, 115.0}, {12.0, 115.0}, {12.0, 113.0}, {10.0, 113.0}},
		},
		{
			ZoneID:   "zone-002",
			Name:     "Paracel Islands Protected Area",
			Severity: "HIGH",
			Polygon:  [][2]float64{{15.5, 111.0}, {15.5, 113.0}, {17.0, 113.0}, {17.0, 111.0}, {15.5, 111.0}},
		},
		{
			ZoneID:   "zone-003",
			Name:     "Gulf of Thailand Fishing Restriction",
			Severity: "MEDIUM",
			Polygon:  [][2]float64{{7.0, 100.0}, {7.0, 102.5}, {9.5, 102.5}, {9.5, 100.0}, {7.0, 100.0}},
		},
		{
			ZoneID:   "zone-004",
			Name:     "Malacca Strait Security Zone",
			Severity: "HIGH",
			Polygon:  [][2]float64{{1.5, 103.0}, {1.5, 104.5}, {3.0, 104.5}, {3.0, 103.0}, {1.5, 103.0}},
		},
		{
			ZoneID:   "zone-005",
			Name:     "Sulu Sea Conservation Area",
			Severity: "MEDIUM",
			Polygon:  [][2]float64{{6.0, 119.0}, {6.0, 121.0}, {8.0, 121.0}, {8.0, 119.0}, {6.0, 119.0}},
		},
		{
			ZoneID:   "zone-006",
			Name:     "Natuna Islands Exclusive Zone",
			Severity: "LOW",
			Polygon:  [][2]float64{{3.0, 107.0}, {3.0, 109.0}, {5.0, 109.0}, {5.0, 107.0}, {3.0, 107.0}},
		},
		{
			ZoneID:   "zone-007",
			Name:     "Scarborough Shoal Restricted Area",
			Severity: "HIGH",
			Polygon:  [][2]float64{{14.5, 117.0}, {14.5, 118.5}, {16.0, 118.5}, {16.0, 117.0}, {14.5, 117.0}},
		},
	}
}

// violatorRoutes defines scripted paths that transit through restricted zones.
// Each route starts outside a zone, enters it, transits through, and exits.
var violatorRoutes = []ViolatorRoute{
	{
		Name: "Spratly Fishing Intrusion",
		Waypoints: []Waypoint{
			{Lat: 9.0, Lon: 112.0},   // approach from southwest
			{Lat: 11.0, Lon: 114.0},  // inside zone-001 (Spratly)
			{Lat: 11.5, Lon: 114.5},  // deep inside zone
			{Lat: 13.0, Lon: 116.0},  // exit northeast
		},
	},
	{
		Name: "Paracel Islands Sweep",
		Waypoints: []Waypoint{
			{Lat: 14.5, Lon: 110.0},  // approach from south
			{Lat: 16.0, Lon: 112.0},  // inside zone-002 (Paracel)
			{Lat: 16.5, Lon: 112.5},  // transit through
			{Lat: 18.0, Lon: 114.0},  // exit north
		},
	},
	{
		Name: "Gulf of Thailand Trawler",
		Waypoints: []Waypoint{
			{Lat: 6.0, Lon: 99.0},    // approach from west
			{Lat: 8.0, Lon: 101.0},   // inside zone-003 (Gulf of Thailand)
			{Lat: 8.5, Lon: 101.5},   // loiter inside
			{Lat: 8.0, Lon: 102.0},   // still inside
			{Lat: 10.0, Lon: 103.0},  // exit north
		},
	},
	{
		Name: "Malacca Strait Runner",
		Waypoints: []Waypoint{
			{Lat: 0.5, Lon: 102.0},   // approach from south
			{Lat: 2.0, Lon: 103.5},   // inside zone-004 (Malacca)
			{Lat: 2.5, Lon: 104.0},   // transit through
			{Lat: 4.0, Lon: 105.0},   // exit northeast
		},
	},
	{
		Name: "Sulu Sea Poacher",
		Waypoints: []Waypoint{
			{Lat: 5.0, Lon: 118.0},   // approach from west
			{Lat: 7.0, Lon: 120.0},   // inside zone-005 (Sulu Sea)
			{Lat: 7.5, Lon: 120.5},   // deep inside
			{Lat: 9.0, Lon: 122.0},   // exit northeast
		},
	},
	{
		Name: "Multi-Zone Violator (Natuna → Malacca)",
		Waypoints: []Waypoint{
			{Lat: 2.0, Lon: 106.0},   // approach Natuna from west
			{Lat: 4.0, Lon: 108.0},   // inside zone-006 (Natuna)
			{Lat: 3.5, Lon: 107.5},   // loiter in Natuna
			{Lat: 2.5, Lon: 105.0},   // head toward Malacca
			{Lat: 2.0, Lon: 103.5},   // inside zone-004 (Malacca)
			{Lat: 1.0, Lon: 102.0},   // exit south
		},
	},
	{
		Name: "Scarborough Shoal Intrusion",
		Waypoints: []Waypoint{
			{Lat: 13.5, Lon: 116.0},  // approach from southwest
			{Lat: 15.0, Lon: 117.5},  // inside zone-007 (Scarborough)
			{Lat: 15.5, Lon: 118.0},  // transit through
			{Lat: 17.0, Lon: 119.0},  // exit northeast
		},
	},
}

// GetViolatorRoute returns a randomly selected violator route.
func GetViolatorRoute(rng *rand.Rand) []Waypoint {
	route := violatorRoutes[rng.Intn(len(violatorRoutes))]
	return route.Waypoints
}
