// Package main — ingestion.go
// UDP socket listener that consumes AISBatch datagrams from Engineer 1's simulator.
// Reads datagrams, deserializes using the shared binary format,
// and dispatches individual records to classification workers.

package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"net"

	pb "github.com/aisentry/simulator/proto"
)

// Ingestion listens for AISBatch UDP datagrams and dispatches records.
type Ingestion struct {
	addr       string
	workers    []chan pb.AISRecord
	numWorkers int
	metrics    *Metrics
}

// NewIngestion creates an ingestion listener.
func NewIngestion(addr string, workers []chan pb.AISRecord, metrics *Metrics) *Ingestion {
	return &Ingestion{
		addr:       addr,
		workers:    workers,
		numWorkers: len(workers),
		metrics:    metrics,
	}
}

// Run starts the UDP listener. Blocks until ctx is cancelled.
func (ing *Ingestion) Run(ctx context.Context) error {
	pc, err := net.ListenPacket("udp", ing.addr)
	if err != nil {
		return fmt.Errorf("listen UDP %s: %w", ing.addr, err)
	}
	defer pc.Close()

	// Set read buffer to 4MB for burst absorption
	if conn, ok := pc.(*net.UDPConn); ok {
		_ = conn.SetReadBuffer(4 * 1024 * 1024)
	}

	fmt.Printf("[ingestion] Listening on UDP %s\n", ing.addr)

	// Pre-allocate a read buffer (64KB — max UDP datagram is ~65535 bytes)
	buf := make([]byte, 65536)

	// Listen in a goroutine that respects context cancellation
	go func() {
		<-ctx.Done()
		pc.Close() // unblock ReadFrom
	}()

	for {
		n, _, err := pc.ReadFrom(buf)
		if err != nil {
			// Check if we're shutting down
			select {
			case <-ctx.Done():
				return nil
			default:
				// Transient error, log and continue
				ing.metrics.MalformedCount.Add(1)
				continue
			}
		}

		ing.metrics.BytesReceived.Add(int64(n))

		// Deserialize the AISBatch
		batch := &pb.AISBatch{}
		if err := batch.UnmarshalBinary(buf[:n]); err != nil {
			ing.metrics.MalformedCount.Add(1)
			continue
		}

		ing.metrics.BatchesReceived.Add(1)
		ing.metrics.RecordsReceived.Add(int64(len(batch.Records)))

		// Dispatch records to sharded workers by vessel_id hash
		for i := range batch.Records {
			record := batch.Records[i]

			// Validate lat/lon range
			if record.Lat < -90 || record.Lat > 90 || record.Lon < -180 || record.Lon > 180 {
				ing.metrics.MalformedCount.Add(1)
				continue
			}

			// Hash vessel_id to determine worker shard
			shard := hashVesselID(record.VesselID) % ing.numWorkers

			// Non-blocking send — drop if worker is backed up (backpressure)
			select {
			case ing.workers[shard] <- record:
			default:
				// Worker queue full, drop record (acceptable for position updates)
			}
		}
	}
}

// hashVesselID produces a consistent hash for sharding.
func hashVesselID(vesselID string) int {
	h := fnv.New32a()
	h.Write([]byte(vesselID))
	return int(h.Sum32())
}
