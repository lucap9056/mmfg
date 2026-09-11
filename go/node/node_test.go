//go:build linux

package node

import (
	"bytes"
	"io"
	"testing"

	"github.com/nautrouds/mmfg/v2/go/shm"
)

func TestNodeConnWriteRead(t *testing.T) {
	blockCount := 2
	blocks := make([][]byte, blockCount)
	for i := range blocks {
		blocks[i] = make([]byte, shm.BlockSize)
	}

	stripe := &shm.Stripe{
		DataLen:    0,
		Sequence:   []int16{0, 1},
		BlockCount: blockCount,
		Blocks:     blocks,
	}

	conn := &nodeConn{
		stripe: stripe,
	}

	// Test Write
	data := []byte("hello node")
	n, err := conn.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected %d bytes written, got %d", len(data), n)
	}

	// Test Read
	buf := make([]byte, len(data))
	conn.readOff = 0
	n, err = conn.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected %d bytes read, got %d", len(data), n)
	}

	if !bytes.Equal(data, buf) {
		t.Fatalf("Mismatch: got %s, want %s", buf, data)
	}
}

func TestNodeConnWriteAtReadAt(t *testing.T) {
	blockCount := 2
	blocks := make([][]byte, blockCount)
	for i := range blocks {
		blocks[i] = make([]byte, shm.BlockSize)
	}

	stripe := &shm.Stripe{
		DataLen:    0,
		Sequence:   []int16{0, 1},
		BlockCount: blockCount,
		Blocks:     blocks,
	}

	conn := &nodeConn{
		stripe: stripe,
	}

	// Test WriteAt
	data := []byte("hello")
	off := int64(10)
	n, err := conn.WriteAt(data, off)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected %d bytes written, got %d", len(data), n)
	}

	// Test ReadAt
	buf := make([]byte, len(data))
	n, err = conn.ReadAt(buf, off)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected %d bytes read, got %d", len(data), n)
	}

	if !bytes.Equal(data, buf) {
		t.Fatalf("Mismatch: got %s, want %s", buf, data)
	}
}

func TestNodeConnReadAll(t *testing.T) {
	blockCount := 2
	blocks := make([][]byte, blockCount)
	for i := range blocks {
		blocks[i] = make([]byte, shm.BlockSize)
	}

	stripe := &shm.Stripe{
		DataLen:    0,
		Sequence:   []int16{0, 1},
		BlockCount: blockCount,
		Blocks:     blocks,
	}

	conn := &nodeConn{
		stripe: stripe,
	}

	data := []byte("testing io.ReadAll functionality in nodeConn")
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	buf, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(data, buf) {
		t.Fatalf("Mismatch: got %s, want %s", buf, data)
	}
}
