package main

import (
	"context"
	"os"
	"testing"
	"time"

	pb "github.com/aisentry/simulator/proto"
)

// mockSender records all sent batches for testing.
type mockSender struct {
	batches []pb.AISBatch
	stats   SenderStats
}

func (m *mockSender) Send(batch *pb.AISBatch) error {
	m.batches = append(m.batches, *batch)
	m.stats.BatchesSent++
	m.stats.RecordsSent += int64(len(batch.Records))
	return nil
}

func (m *mockSender) Close() error { return nil }

func (m *mockSender) Stats() SenderStats { return m.stats }

func TestEngine_BasicRun(t *testing.T) {
	sender := &mockSender{}

	cfg := EngineConfig{
		VesselCount:   100,
		TargetRate:    10000,
		BatchSize:     50,
		ViolatorRatio: 0.1,
		DurationSec:   2,
		NumWorkers:    2,
	}

	engine := NewEngine(cfg, sender)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := engine.Run(ctx)
	if err != nil {
		t.Fatalf("engine.Run failed: %v", err)
	}

	if sender.stats.BatchesSent == 0 {
		t.Error("no batches were sent")
	}
	if sender.stats.RecordsSent == 0 {
		t.Error("no records were sent")
	}

	t.Logf("Sent %d batches, %d records in 2 seconds",
		sender.stats.BatchesSent, sender.stats.RecordsSent)
}

func TestEngine_ViolatorRatio(t *testing.T) {
	sender := &mockSender{}

	cfg := EngineConfig{
		VesselCount:   1000,
		TargetRate:    5000,
		BatchSize:     100,
		ViolatorRatio: 0.1,
		DurationSec:   1,
		NumWorkers:    1,
	}

	engine := NewEngine(cfg, sender)

	violatorCount := 0
	for _, v := range engine.vessels {
		if v.IsViolator {
			violatorCount++
		}
	}

	expected := int(float64(cfg.VesselCount) * cfg.ViolatorRatio)
	if violatorCount != expected {
		t.Errorf("expected %d violators, got %d", expected, violatorCount)
	}
}

func TestFileSender_WriteAndRead(t *testing.T) {
	tmpFile := "test_fixture.bin"
	defer os.Remove(tmpFile)

	sender, err := NewFileSender(tmpFile)
	if err != nil {
		t.Fatalf("NewFileSender failed: %v", err)
	}

	batch := &pb.AISBatch{
		BatchSeq: 1,
		Records: []pb.AISRecord{
			{
				VesselID: "test-vessel",
				MMSI:     "123456789",
				Lat:      10.5,
				Lon:      110.5,
				SOGKnots: 12.0,
				COGDegrees: 180.0,
				TSUnixMs: time.Now().UnixMilli(),
			},
		},
	}

	err = sender.Send(batch)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	err = sender.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify file was created and has content
	info, err := os.Stat(tmpFile)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() == 0 {
		t.Error("fixture file is empty")
	}

	stats := sender.Stats()
	if stats.BatchesSent != 1 {
		t.Errorf("expected 1 batch sent, got %d", stats.BatchesSent)
	}
	if stats.RecordsSent != 1 {
		t.Errorf("expected 1 record sent, got %d", stats.RecordsSent)
	}
}
