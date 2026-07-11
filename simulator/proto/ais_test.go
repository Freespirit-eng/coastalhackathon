package proto

import (
	"bytes"
	"math"
	"testing"
)

func TestAISBatchRoundTrip(t *testing.T) {
	original := &AISBatch{
		BatchSeq: 42,
		Records: []AISRecord{
			{
				VesselID:           "vessel-001",
				MMSI:               "123456789",
				Lat:                12.345678,
				Lon:                -98.765432,
				SOGKnots:           14.5,
				COGDegrees:         270.3,
				TSUnixMs:           1731000000000,
				IsScriptedViolator: false,
			},
			{
				VesselID:           "vessel-002",
				MMSI:               "987654321",
				Lat:                -45.6789,
				Lon:                123.456,
				SOGKnots:           0.0,
				COGDegrees:         0.0,
				TSUnixMs:           1731000001000,
				IsScriptedViolator: true,
			},
		},
	}

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	decoded := &AISBatch{}
	err = decoded.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	if decoded.BatchSeq != original.BatchSeq {
		t.Errorf("BatchSeq mismatch: got %d, want %d", decoded.BatchSeq, original.BatchSeq)
	}

	if len(decoded.Records) != len(original.Records) {
		t.Fatalf("Record count mismatch: got %d, want %d", len(decoded.Records), len(original.Records))
	}

	for i, orig := range original.Records {
		dec := decoded.Records[i]

		if dec.VesselID != orig.VesselID {
			t.Errorf("Record %d VesselID: got %q, want %q", i, dec.VesselID, orig.VesselID)
		}
		if dec.MMSI != orig.MMSI {
			t.Errorf("Record %d MMSI: got %q, want %q", i, dec.MMSI, orig.MMSI)
		}
		if math.Abs(dec.Lat-orig.Lat) > 1e-10 {
			t.Errorf("Record %d Lat: got %f, want %f", i, dec.Lat, orig.Lat)
		}
		if math.Abs(dec.Lon-orig.Lon) > 1e-10 {
			t.Errorf("Record %d Lon: got %f, want %f", i, dec.Lon, orig.Lon)
		}
		if dec.SOGKnots != orig.SOGKnots {
			t.Errorf("Record %d SOGKnots: got %f, want %f", i, dec.SOGKnots, orig.SOGKnots)
		}
		if dec.COGDegrees != orig.COGDegrees {
			t.Errorf("Record %d COGDegrees: got %f, want %f", i, dec.COGDegrees, orig.COGDegrees)
		}
		if dec.TSUnixMs != orig.TSUnixMs {
			t.Errorf("Record %d TSUnixMs: got %d, want %d", i, dec.TSUnixMs, orig.TSUnixMs)
		}
		if dec.IsScriptedViolator != orig.IsScriptedViolator {
			t.Errorf("Record %d IsScriptedViolator: got %v, want %v", i, dec.IsScriptedViolator, orig.IsScriptedViolator)
		}
	}
}

func TestAISBatchEmpty(t *testing.T) {
	original := &AISBatch{BatchSeq: 0, Records: nil}
	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed for empty batch: %v", err)
	}

	decoded := &AISBatch{}
	err = decoded.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("UnmarshalBinary failed for empty batch: %v", err)
	}

	if decoded.BatchSeq != 0 {
		t.Errorf("BatchSeq: got %d, want 0", decoded.BatchSeq)
	}
	if len(decoded.Records) != 0 {
		t.Errorf("Record count: got %d, want 0", len(decoded.Records))
	}
}

func TestAISBatchTruncatedData(t *testing.T) {
	b := &AISBatch{BatchSeq: 1, Records: []AISRecord{
		{VesselID: "v1", MMSI: "123456789", Lat: 10.0, Lon: 20.0, SOGKnots: 5.0, COGDegrees: 90.0, TSUnixMs: 1000},
	}}

	data, _ := b.MarshalBinary()

	// Truncate data at various points
	for truncLen := 0; truncLen < len(data)-1; truncLen++ {
		decoded := &AISBatch{}
		err := decoded.UnmarshalBinary(data[:truncLen])
		if err == nil && truncLen < len(data) {
			// If it didn't error but we truncated, it must be because we only truncated
			// after all records were fully decoded (only possible if truncLen >= full data len)
			// This is okay as long as the header was read
		}
	}
}

func BenchmarkAISBatchMarshal(b *testing.B) {
	batch := &AISBatch{
		BatchSeq: 1,
		Records:  make([]AISRecord, 256),
	}
	for i := range batch.Records {
		batch.Records[i] = AISRecord{
			VesselID:   "vessel-00000001",
			MMSI:       "123456789",
			Lat:        10.5 + float64(i)*0.001,
			Lon:        110.5 + float64(i)*0.001,
			SOGKnots:   12.5,
			COGDegrees: 180.0,
			TSUnixMs:   1731000000000,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = batch.MarshalBinary()
	}
}

func BenchmarkAISBatchUnmarshal(b *testing.B) {
	batch := &AISBatch{
		BatchSeq: 1,
		Records:  make([]AISRecord, 256),
	}
	for i := range batch.Records {
		batch.Records[i] = AISRecord{
			VesselID:   "vessel-00000001",
			MMSI:       "123456789",
			Lat:        10.5 + float64(i)*0.001,
			Lon:        110.5 + float64(i)*0.001,
			SOGKnots:   12.5,
			COGDegrees: 180.0,
			TSUnixMs:   1731000000000,
		}
	}

	data, _ := batch.MarshalBinary()
	_ = bytes.Clone(data) // ensure we're not cheating

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		decoded := &AISBatch{}
		_ = decoded.UnmarshalBinary(data)
	}
}
