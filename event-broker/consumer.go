// Package main — consumer.go
// TCP client that consumes length-prefixed JSON frames from Engineer 2.

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

// Consumer connects to the upstream TCP socket and reads frames.
type Consumer struct {
	addr     string
	pipeline *Pipeline
}

// NewConsumer creates a TCP event consumer.
func NewConsumer(addr string, pipeline *Pipeline) *Consumer {
	return &Consumer{
		addr:     addr,
		pipeline: pipeline,
	}
}

// Run starts the consumer loop with auto-reconnect.
func (c *Consumer) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, err := net.Dial("tcp", c.addr)
		if err != nil {
			fmt.Printf("[consumer] Connection failed to %s, retrying in 2s...\n", c.addr)
			time.Sleep(2 * time.Second)
			continue
		}

		fmt.Printf("[consumer] Connected to upstream event stream at %s\n", c.addr)
		
		err = c.readStream(ctx, conn)
		conn.Close()
		
		if err != nil && err != io.EOF {
			fmt.Printf("[consumer] Stream error: %v. Reconnecting...\n", err)
		} else {
			fmt.Println("[consumer] Upstream closed connection. Reconnecting...")
		}
		
		time.Sleep(1 * time.Second)
	}
}

func (c *Consumer) readStream(ctx context.Context, conn net.Conn) error {
	lenBuf := make([]byte, 4)

	// Context cancellation check goroutine
	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	for {
		// Read 4-byte length prefix (Little Endian)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return err
		}

		msgLen := binary.LittleEndian.Uint32(lenBuf)
		if msgLen == 0 || msgLen > 10*1024*1024 { // max 10MB sanity check
			return fmt.Errorf("invalid message length: %d", msgLen)
		}

		// Read payload
		payloadBuf := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, payloadBuf); err != nil {
			return err
		}

		// Decode Envelope
		var envelope struct {
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(payloadBuf, &envelope); err != nil {
			fmt.Printf("[consumer] Invalid envelope JSON: %v\n", err)
			continue
		}

		// Pass to pipeline
		c.pipeline.Process(envelope.Type, envelope.Payload)
	}
}
