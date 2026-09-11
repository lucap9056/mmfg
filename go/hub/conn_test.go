//go:build linux

package hub

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

func TestConnWriteRead(t *testing.T) {
	h, err := NewHub()
	if err != nil {
		t.Fatalf("Failed to create hub: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := h.Request(ctx, 1, true)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer conn.Close()

	data := []byte("hello world")
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	buf := make([]byte, len(data))
	if _, err := conn.Read(buf); err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(data, buf) {
		t.Fatalf("Mismatch: got %s, want %s", buf, data)
	}
}

func TestConnWriteAt(t *testing.T) {
	h, err := NewHub()
	if err != nil {
		t.Fatalf("Failed to create hub: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := h.Request(ctx, 1, true)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer conn.Close()

	data := []byte("hello")
	if _, err := conn.WriteAt(data, 10); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	buf := make([]byte, 5)
	if _, err := conn.ReadAt(buf, 10); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	if !bytes.Equal(data, buf) {
		t.Fatalf("Mismatch: got %s, want %s", buf, data)
	}
}

func TestConnExpansion(t *testing.T) {
	h, err := NewHub()
	if err != nil {
		t.Fatalf("Failed to create hub: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := h.Request(ctx, 1, true)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer conn.Close()

	largeData := make([]byte, 10*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	if _, err := conn.Write(largeData); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	buf := make([]byte, len(largeData))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(largeData, buf) {
		t.Fatal("Data mismatch after expansion")
	}
}

func TestConnReadAll(t *testing.T) {
	h, err := NewHub()
	if err != nil {
		t.Fatalf("Failed to create hub: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := h.Request(ctx, 1, true)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer conn.Close()

	data := []byte("testing io.ReadAll functionality in mmfg")
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read all data using io.ReadAll
	// Note: busConn must support ReadUntilEOF or have a mechanism that allows io.ReadAll to finish.
	// Since busConn implements io.Reader, ReadAll will try to read until EOF.
	// We need to ensure the connection behaves correctly when Read is called.
	buf, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(data, buf) {
		t.Fatalf("Mismatch: got %s, want %s", buf, data)
	}
}
