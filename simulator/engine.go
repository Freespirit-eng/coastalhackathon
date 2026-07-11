// Package main — engine.go
// Core simulation engine with sharded worker pool.
// Each worker owns a shard of vessels, ticks them independently,
// batches records, and sends via the configured Sender.
// Uses a token-bucket rate limiter for precise aggregate throughput control.

package main

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/aisentry/simulator/proto"
)

// EngineConfig holds the simulation parameters.
type EngineConfig struct {
	VesselCount    int
	TargetRate     int     // target msgs/sec aggregate
	BatchSize      int     // records per AISBatch
	ViolatorRatio  float64 // fraction of vessels that are scripted violators
	DurationSec    int     // 0 = run forever
	NumWorkers     int     // 0 = use runtime.NumCPU()
}

// Engine orchestrates the vessel simulation across sharded workers.
type Engine struct {
	cfg     EngineConfig
	sender  Sender
	vessels []*Vessel

	// Metrics (atomic for lock-free reads)
	totalSent     atomic.Int64
	totalBatches  atomic.Int64
	totalErrors   atomic.Int64
	startTime     time.Time
}

// NewEngine creates a new simulation engine.
func NewEngine(cfg EngineConfig, sender Sender) *Engine {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = runtime.NumCPU()
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}

	// Create vessels
	vessels := make([]*Vessel, cfg.VesselCount)
	violatorCount := int(float64(cfg.VesselCount) * cfg.ViolatorRatio)

	for i := 0; i < cfg.VesselCount; i++ {
		isViolator := i < violatorCount
		rng := rand.New(rand.NewSource(int64(i) + time.Now().UnixNano()))
		vessels[i] = NewVessel(i, isViolator, rng)
	}

	return &Engine{
		cfg:     cfg,
		sender:  sender,
		vessels: vessels,
	}
}

// Run starts the simulation, blocking until ctx is cancelled or duration expires.
func (e *Engine) Run(ctx context.Context) error {
	e.startTime = time.Now()

	// Apply duration limit if configured
	if e.cfg.DurationSec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(e.cfg.DurationSec)*time.Second)
		defer cancel()
	}

	// Calculate per-worker rate
	perWorkerRate := e.cfg.TargetRate / e.cfg.NumWorkers
	if perWorkerRate < 1 {
		perWorkerRate = 1
	}

	// Shard vessels across workers
	shards := make([][]*Vessel, e.cfg.NumWorkers)
	for i, v := range e.vessels {
		shard := i % e.cfg.NumWorkers
		shards[shard] = append(shards[shard], v)
	}

	// Start stats reporter
	go e.reportStats(ctx)

	// Launch workers
	var wg sync.WaitGroup
	for i := 0; i < e.cfg.NumWorkers; i++ {
		wg.Add(1)
		go func(workerID int, myVessels []*Vessel, rate int) {
			defer wg.Done()
			e.workerLoop(ctx, workerID, myVessels, rate)
		}(i, shards[i], perWorkerRate)
	}

	wg.Wait()
	return nil
}

// workerLoop is the hot path for each worker goroutine.
// It ticks its vessels, batches records, and sends via the Sender.
func (e *Engine) workerLoop(ctx context.Context, workerID int, vessels []*Vessel, targetRate int) {
	if len(vessels) == 0 {
		return
	}

	batchSize := e.cfg.BatchSize
	batch := &pb.AISBatch{
		Records: make([]pb.AISRecord, 0, batchSize),
	}
	var batchSeq int32

	// Rate control: send `targetRate` records per second.
	// We calculate a tick interval based on how many records per second this worker should produce.
	// Each tick, we cycle through vessels round-robin.
	vesselIdx := 0
	dt := 1.0 // simulate 1-second time steps for movement model

	// Calculate sleep interval between batches to hit target rate
	batchesPerSec := float64(targetRate) / float64(batchSize)
	if batchesPerSec < 1 {
		batchesPerSec = 1
	}
	sleepPerBatch := time.Duration(float64(time.Second) / batchesPerSec)

	ticker := time.NewTicker(sleepPerBatch)
	defer ticker.Stop()

	lastTick := time.Now()

	for {
		select {
		case <-ctx.Done():
			// Flush remaining records
			if len(batch.Records) > 0 {
				batchSeq++
				batch.BatchSeq = batchSeq
				if err := e.sender.Send(batch); err != nil {
					e.totalErrors.Add(1)
				} else {
					e.totalSent.Add(int64(len(batch.Records)))
					e.totalBatches.Add(1)
				}
			}
			return

		case now := <-ticker.C:
			elapsed := now.Sub(lastTick).Seconds()
			if elapsed < 0.001 {
				elapsed = 0.001
			}
			lastTick = now

			// Fill a batch by ticking vessels
			batch.Records = batch.Records[:0]
			for j := 0; j < batchSize; j++ {
				v := vessels[vesselIdx%len(vessels)]
				record := v.Tick(dt * elapsed)
				batch.Records = append(batch.Records, record)
				vesselIdx++
			}

			batchSeq++
			batch.BatchSeq = batchSeq

			if err := e.sender.Send(batch); err != nil {
				e.totalErrors.Add(1)
			} else {
				e.totalSent.Add(int64(len(batch.Records)))
				e.totalBatches.Add(1)
			}
		}
	}
}

// reportStats prints throughput metrics to stdout every second.
func (e *Engine) reportStats(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastSent int64

	for {
		select {
		case <-ctx.Done():
			e.printFinalStats()
			return
		case <-ticker.C:
			currentSent := e.totalSent.Load()
			rate := currentSent - lastSent
			lastSent = currentSent

			elapsed := time.Since(e.startTime).Seconds()
			avgRate := float64(currentSent) / elapsed

			senderStats := e.sender.Stats()

			fmt.Printf("[%6.1fs] Rate: %6d msgs/sec | Avg: %8.0f msgs/sec | Total: %10d msgs | Batches: %8d | Errors: %d | Bytes: %s\n",
				elapsed,
				rate,
				avgRate,
				currentSent,
				e.totalBatches.Load(),
				e.totalErrors.Load(),
				humanBytes(senderStats.BytesSent),
			)
		}
	}
}

// printFinalStats prints a summary when the simulation ends.
func (e *Engine) printFinalStats() {
	elapsed := time.Since(e.startTime).Seconds()
	totalSent := e.totalSent.Load()
	avgRate := float64(totalSent) / elapsed
	senderStats := e.sender.Stats()

	fmt.Println("\n════════════════════════════════════════════════════════")
	fmt.Println("  AISentry Simulator — Final Report")
	fmt.Println("════════════════════════════════════════════════════════")
	fmt.Printf("  Duration:       %.2f seconds\n", elapsed)
	fmt.Printf("  Total Records:  %d\n", totalSent)
	fmt.Printf("  Total Batches:  %d\n", e.totalBatches.Load())
	fmt.Printf("  Avg Throughput: %.0f msgs/sec\n", avgRate)
	fmt.Printf("  Total Bytes:    %s\n", humanBytes(senderStats.BytesSent))
	fmt.Printf("  Errors:         %d\n", e.totalErrors.Load())
	fmt.Printf("  Vessels:        %d (%d violators)\n",
		e.cfg.VesselCount,
		int(float64(e.cfg.VesselCount)*e.cfg.ViolatorRatio))
	fmt.Println("════════════════════════════════════════════════════════")
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
