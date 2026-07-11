// Package main — store.go
// Embedded SQLite database for alert persistence.
// Uses modernc.org/sqlite (CGO-free).

package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

// AlertRecord represents an alert stored in the database.
type AlertRecord struct {
	AlertID   string  `json:"alert_id"`
	VesselID  string  `json:"vessel_id"`
	ZoneID    string  `json:"zone_id"`
	ZoneName  string  `json:"zone_name"`
	Severity  string  `json:"severity"`
	EventType string  `json:"event_type"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	TSUnixMs  int64   `json:"ts_unix_ms"`
}

// AlertStore handles database operations.
type AlertStore struct {
	db *sql.DB
}

// NewAlertStore initializes the SQLite database and schema.
func NewAlertStore(dbPath string) (*AlertStore, error) {
	// Enable WAL mode for better concurrency
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	store := &AlertStore{db: db}
	if err := store.initSchema(); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *AlertStore) initSchema() error {
	query := `
	CREATE TABLE IF NOT EXISTS alerts (
		alert_id TEXT PRIMARY KEY,
		vessel_id TEXT NOT NULL,
		zone_id TEXT NOT NULL,
		zone_name TEXT NOT NULL,
		severity TEXT NOT NULL,
		event_type TEXT NOT NULL,
		lat REAL NOT NULL,
		lon REAL NOT NULL,
		ts_unix_ms INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_alerts_vessel ON alerts(vessel_id);
	CREATE INDEX IF NOT EXISTS idx_alerts_zone ON alerts(zone_id);
	CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts_unix_ms);
	`
	_, err := s.db.Exec(query)
	return err
}

// SaveAlert inserts a new alert into the database.
func (s *AlertStore) SaveAlert(alert AlertRecord) error {
	query := `
	INSERT INTO alerts (alert_id, vessel_id, zone_id, zone_name, severity, event_type, lat, lon, ts_unix_ms)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	_, err := s.db.Exec(query,
		alert.AlertID, alert.VesselID, alert.ZoneID, alert.ZoneName,
		alert.Severity, alert.EventType, alert.Lat, alert.Lon, alert.TSUnixMs,
	)
	return err
}

// QueryFilters holds optional filters for querying alerts.
type QueryFilters struct {
	VesselID string
	ZoneID   string
	FromMs   int64
	ToMs     int64
	Limit    int
}

// QueryAlerts retrieves historical alerts based on filters.
func (s *AlertStore) QueryAlerts(filters QueryFilters) ([]AlertRecord, error) {
	var conditions []string
	var args []interface{}

	if filters.VesselID != "" {
		conditions = append(conditions, "vessel_id = ?")
		args = append(args, filters.VesselID)
	}
	if filters.ZoneID != "" {
		conditions = append(conditions, "zone_id = ?")
		args = append(args, filters.ZoneID)
	}
	if filters.FromMs > 0 {
		conditions = append(conditions, "ts_unix_ms >= ?")
		args = append(args, filters.FromMs)
	}
	if filters.ToMs > 0 {
		conditions = append(conditions, "ts_unix_ms <= ?")
		args = append(args, filters.ToMs)
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	limit := filters.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100 // default limit
	}

	query := fmt.Sprintf("SELECT alert_id, vessel_id, zone_id, zone_name, severity, event_type, lat, lon, ts_unix_ms FROM alerts %s ORDER BY ts_unix_ms DESC LIMIT ?", whereClause)
	args = append(args, limit)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var alerts []AlertRecord
	for rows.Next() {
		var a AlertRecord
		err := rows.Scan(
			&a.AlertID, &a.VesselID, &a.ZoneID, &a.ZoneName,
			&a.Severity, &a.EventType, &a.Lat, &a.Lon, &a.TSUnixMs,
		)
		if err != nil {
			return nil, err
		}
		alerts = append(alerts, a)
	}

	if alerts == nil {
		alerts = []AlertRecord{} // return empty slice instead of null
	}
	return alerts, nil
}
