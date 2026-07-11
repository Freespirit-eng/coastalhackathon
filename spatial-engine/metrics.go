// Package main — metrics.go
// Atomic counters for ingestion throughput, classification latency,
// violation counts, and active vessel tracking.
// Provides both a console reporter and a /metrics endpoint.

package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Metrics tracks all observable counters for the spatial engine.
type Metrics struct {
	// Ingestion
	RecordsReceived atomic.Int64
	BatchesReceived atomic.Int64
	BytesReceived   atomic.Int64
	MalformedCount  atomic.Int64

	// Classification
	RecordsClassified atomic.Int64
	ViolationsFound   atomic.Int64
	CoarseFiltered    atomic.Int64 // records that passed AABB but failed ray-cast (boundary cells)

	// Cache
	ActiveVessels atomic.Int64

	// Timing
	StartTime time.Time
}

// NewMetrics creates a new metrics instance.
func NewMetrics() *Metrics {
	return &Metrics{
		StartTime: time.Now(),
	}
}

// ConsoleReporter prints metrics to stdout every second.
func (m *Metrics) ConsoleReporter(done <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastReceived int64
	var lastClassified int64

	for {
		select {
		case <-done:
			m.printFinal()
			return
		case <-ticker.C:
			received := m.RecordsReceived.Load()
			classified := m.RecordsClassified.Load()
			ingestRate := received - lastReceived
			classifyRate := classified - lastClassified
			lastReceived = received
			lastClassified = classified

			elapsed := time.Since(m.StartTime).Seconds()

			fmt.Printf("[%6.1fs] Ingest: %6d/sec | Classify: %6d/sec | Violations: %6d | Vessels: %6d | Malformed: %d\n",
				elapsed,
				ingestRate,
				classifyRate,
				m.ViolationsFound.Load(),
				m.ActiveVessels.Load(),
				m.MalformedCount.Load(),
			)
		}
	}
}

// printFinal prints a summary when the engine shuts down.
func (m *Metrics) printFinal() {
	elapsed := time.Since(m.StartTime).Seconds()
	totalReceived := m.RecordsReceived.Load()
	avgRate := float64(totalReceived) / elapsed

	fmt.Println("\n════════════════════════════════════════════════════════")
	fmt.Println("  AISentry Spatial Engine — Final Report")
	fmt.Println("════════════════════════════════════════════════════════")
	fmt.Printf("  Duration:         %.2f seconds\n", elapsed)
	fmt.Printf("  Records Received: %d\n", totalReceived)
	fmt.Printf("  Records Classified: %d\n", m.RecordsClassified.Load())
	fmt.Printf("  Avg Ingest Rate:  %.0f msgs/sec\n", avgRate)
	fmt.Printf("  Violations Found: %d\n", m.ViolationsFound.Load())
	fmt.Printf("  Active Vessels:   %d\n", m.ActiveVessels.Load())
	fmt.Printf("  Malformed Packets: %d\n", m.MalformedCount.Load())
	fmt.Printf("  Batches Received: %d\n", m.BatchesReceived.Load())
	fmt.Printf("  Bytes Received:   %s\n", humanBytes(m.BytesReceived.Load()))
	fmt.Println("════════════════════════════════════════════════════════")
}

// PrometheusMetrics returns metrics in Prometheus exposition format.
func (m *Metrics) PrometheusMetrics() string {
	elapsed := time.Since(m.StartTime).Seconds()
	totalReceived := m.RecordsReceived.Load()
	avgRate := float64(totalReceived) / elapsed

	return fmt.Sprintf(`# HELP aisentry_records_received_total Total AIS records received
# TYPE aisentry_records_received_total counter
aisentry_records_received_total %d

# HELP aisentry_records_classified_total Total records classified by geofencer
# TYPE aisentry_records_classified_total counter
aisentry_records_classified_total %d

# HELP aisentry_violations_total Total geofence violations detected
# TYPE aisentry_violations_total counter
aisentry_violations_total %d

# HELP aisentry_active_vessels Current number of tracked vessels
# TYPE aisentry_active_vessels gauge
aisentry_active_vessels %d

# HELP aisentry_ingest_rate_avg Average ingestion rate in msgs/sec
# TYPE aisentry_ingest_rate_avg gauge
aisentry_ingest_rate_avg %.2f

# HELP aisentry_malformed_total Total malformed packets received
# TYPE aisentry_malformed_total counter
aisentry_malformed_total %d

# HELP aisentry_uptime_seconds Engine uptime in seconds
# TYPE aisentry_uptime_seconds gauge
aisentry_uptime_seconds %.2f
`,
		totalReceived,
		m.RecordsClassified.Load(),
		m.ViolationsFound.Load(),
		m.ActiveVessels.Load(),
		avgRate,
		m.MalformedCount.Load(),
		elapsed,
	)
}

// humanBytes formats byte counts in human-readable form.
func humanBytes(b int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
