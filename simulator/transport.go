// Package main — transport.go
// Network transport layer for sending AIS batches.
// Implements UDP (primary), gRPC streaming (fallback), and File (fixture generation) senders.

package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"

	pb "github.com/aisentry/simulator/proto"
)

// Sender is the interface for sending serialized AIS batches.
type Sender interface {
	Send(batch *pb.AISBatch) error
	Close() error
	Stats() SenderStats
}

// SenderStats tracks send-side metrics.
type SenderStats struct {
	BatchesSent int64
	RecordsSent int64
	BytesSent   int64
	Errors      int64
}

// ---------------------------------------------------------------------------
// UDP Sender
// ---------------------------------------------------------------------------

// UDPSender sends serialized AISBatch messages as UDP datagrams.
// Each datagram contains one complete AISBatch.
type UDPSender struct {
	conn  *net.UDPConn
	addr  *net.UDPAddr
	pool  sync.Pool
	mu    sync.Mutex
	stats SenderStats
}

// NewUDPSender creates a UDP sender targeting the specified address.
func NewUDPSender(target string) (*UDPSender, error) {
	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return nil, fmt.Errorf("resolve UDP addr %q: %w", target, err)
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, fmt.Errorf("dial UDP %q: %w", target, err)
	}

	// Set write buffer to 4MB for burst absorption
	_ = conn.SetWriteBuffer(4 * 1024 * 1024)

	return &UDPSender{
		conn: conn,
		addr: addr,
		pool: sync.Pool{
			New: func() interface{} {
				// Pre-allocate buffer for typical batch (~256 records × ~50 bytes)
				b := make([]byte, 0, 16384)
				return &b
			},
		},
	}, nil
}

func (s *UDPSender) Send(batch *pb.AISBatch) error {
	data, err := batch.MarshalBinary()
	if err != nil {
		s.mu.Lock()
		s.stats.Errors++
		s.mu.Unlock()
		return fmt.Errorf("marshal batch: %w", err)
	}

	_, err = s.conn.Write(data)
	if err != nil {
		s.mu.Lock()
		s.stats.Errors++
		s.mu.Unlock()
		return fmt.Errorf("UDP write: %w", err)
	}

	s.mu.Lock()
	s.stats.BatchesSent++
	s.stats.RecordsSent += int64(len(batch.Records))
	s.stats.BytesSent += int64(len(data))
	s.mu.Unlock()

	return nil
}

func (s *UDPSender) Close() error {
	return s.conn.Close()
}

func (s *UDPSender) Stats() SenderStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// ---------------------------------------------------------------------------
// File Sender (for replay-gen / fixture generation)
// ---------------------------------------------------------------------------

// FileSender writes length-prefixed serialized AISBatch messages to a file.
// Format: [4-byte LE length][payload bytes] repeated.
// This produces the fixture file that Engineer 2 can replay.
type FileSender struct {
	writer io.WriteCloser
	mu     sync.Mutex
	stats  SenderStats
}

// NewFileSender creates a file sender writing to the specified path.
func NewFileSender(path string) (*FileSender, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create file %q: %w", path, err)
	}
	return &FileSender{writer: f}, nil
}

func (s *FileSender) Send(batch *pb.AISBatch) error {
	data, err := batch.MarshalBinary()
	if err != nil {
		s.mu.Lock()
		s.stats.Errors++
		s.mu.Unlock()
		return fmt.Errorf("marshal batch: %w", err)
	}

	// Write length prefix (4-byte little-endian)
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.writer.Write(lenBuf); err != nil {
		s.stats.Errors++
		return fmt.Errorf("write length: %w", err)
	}
	if _, err := s.writer.Write(data); err != nil {
		s.stats.Errors++
		return fmt.Errorf("write data: %w", err)
	}

	s.stats.BatchesSent++
	s.stats.RecordsSent += int64(len(batch.Records))
	s.stats.BytesSent += int64(4 + len(data))

	return nil
}

func (s *FileSender) Close() error {
	return s.writer.Close()
}

func (s *FileSender) Stats() SenderStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// ---------------------------------------------------------------------------
// TCP Sender (simple framed TCP as gRPC-lite alternative)
// ---------------------------------------------------------------------------

// TCPSender sends length-prefixed AISBatch messages over a persistent TCP connection.
// This serves as a simple, reliable alternative to UDP when packet loss is unacceptable
// and gRPC overhead is not desired.
type TCPSender struct {
	conn  net.Conn
	mu    sync.Mutex
	stats SenderStats
}

// NewTCPSender creates a TCP sender targeting the specified address.
func NewTCPSender(target string) (*TCPSender, error) {
	conn, err := net.Dial("tcp", target)
	if err != nil {
		return nil, fmt.Errorf("dial TCP %q: %w", target, err)
	}
	return &TCPSender{conn: conn}, nil
}

func (s *TCPSender) Send(batch *pb.AISBatch) error {
	data, err := batch.MarshalBinary()
	if err != nil {
		s.mu.Lock()
		s.stats.Errors++
		s.mu.Unlock()
		return fmt.Errorf("marshal batch: %w", err)
	}

	// Length-prefixed frame: [4-byte LE length][payload]
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.conn.Write(lenBuf); err != nil {
		s.stats.Errors++
		return fmt.Errorf("TCP write length: %w", err)
	}
	if _, err := s.conn.Write(data); err != nil {
		s.stats.Errors++
		return fmt.Errorf("TCP write data: %w", err)
	}

	s.stats.BatchesSent++
	s.stats.RecordsSent += int64(len(batch.Records))
	s.stats.BytesSent += int64(4 + len(data))

	return nil
}

func (s *TCPSender) Close() error {
	return s.conn.Close()
}

func (s *TCPSender) Stats() SenderStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}
