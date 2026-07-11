package main

import (
	"os"
	"testing"
)

func TestAlertStore(t *testing.T) {
	// Create temporary db
	dbPath := "test_alerts.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := NewAlertStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.db.Close()

	// 1. Insert alert
	alert1 := AlertRecord{
		AlertID:   "a1",
		VesselID:  "v1",
		ZoneID:    "z1",
		ZoneName:  "Test Zone",
		Severity:  "HIGH",
		EventType: "ZONE_ENTER",
		Lat:       1.0,
		Lon:       2.0,
		TSUnixMs:  1000,
	}
	if err := store.SaveAlert(alert1); err != nil {
		t.Fatalf("Failed to save alert1: %v", err)
	}

	// Insert another
	alert2 := AlertRecord{
		AlertID:   "a2",
		VesselID:  "v1",
		ZoneID:    "z2",
		ZoneName:  "Zone Two",
		Severity:  "LOW",
		EventType: "ZONE_ENTER",
		Lat:       3.0,
		Lon:       4.0,
		TSUnixMs:  2000,
	}
	if err := store.SaveAlert(alert2); err != nil {
		t.Fatalf("Failed to save alert2: %v", err)
	}

	// 2. Query all (should sort DESC by default)
	alerts, err := store.QueryAlerts(QueryFilters{})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(alerts) != 2 {
		t.Fatalf("Expected 2 alerts, got %d", len(alerts))
	}
	if alerts[0].AlertID != "a2" || alerts[1].AlertID != "a1" {
		t.Errorf("Unexpected sort order: %v", alerts)
	}

	// 3. Query by Vessel ID
	vesselAlerts, err := store.QueryAlerts(QueryFilters{VesselID: "v1"})
	if err != nil || len(vesselAlerts) != 2 {
		t.Errorf("Expected 2 alerts for vessel v1, got %d", len(vesselAlerts))
	}

	// 4. Query by Zone ID
	zoneAlerts, err := store.QueryAlerts(QueryFilters{ZoneID: "z1"})
	if err != nil || len(zoneAlerts) != 1 || zoneAlerts[0].AlertID != "a1" {
		t.Errorf("Expected 1 alert for zone z1, got %d", len(zoneAlerts))
	}

	// 5. Query with Time Range
	timeAlerts, err := store.QueryAlerts(QueryFilters{FromMs: 1500, ToMs: 2500})
	if err != nil || len(timeAlerts) != 1 || timeAlerts[0].AlertID != "a2" {
		t.Errorf("Expected 1 alert in time range, got %d", len(timeAlerts))
	}

	// 6. Query with Limit
	limitAlerts, err := store.QueryAlerts(QueryFilters{Limit: 1})
	if err != nil || len(limitAlerts) != 1 {
		t.Errorf("Expected 1 alert with limit=1, got %d", len(limitAlerts))
	}
}
