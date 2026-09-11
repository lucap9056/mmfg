//go:build linux

package shm_test

import (
	"bytes"
	"testing"

	"github.com/nautrouds/mmfg/v2/go/shm"
)

func TestViewer(t *testing.T) {
	chunk, err := shm.NewChunk("test_viewer")
	if err != nil {
		t.Fatalf("Failed to create chunk: %v", err)
	}
	defer chunk.Close()

	bus, err := shm.NewBus(chunk, func(c *shm.Chunk) {})
	if err != nil {
		t.Fatalf("Failed to create bus: %v", err)
	}

	// Allocate 3 blocks to ensure we cross boundaries
	stripe, err := bus.AllocStripe(3)
	if err != nil {
		t.Fatalf("Failed to alloc stripe: %v", err)
	}

	// Write data that crosses block boundaries (BlockSize = 4KB)
	// Block 0: Header
	// Block 1: offset 0-4095
	// Block 2: offset 4096-8191
	data := make([]byte, 8192)
	for i := range data {
		data[i] = byte(i % 251) // prime number to avoid pattern alignment
	}

	_, err = stripe.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Test View
	err = stripe.View(4000, 200, func(v *shm.Viewer) error {
		// 4000 to 4200 spans Block 1 and Block 2
		if v.Length != 200 {
			t.Errorf("Expected length 200, got %d", v.Length)
		}

		// Test ByteAt
		for i := 0; i < 200; i++ {
			b, err := v.ByteAt(i)
			if err != nil {
				t.Fatalf("ByteAt(%d) failed: %v", i, err)
			}
			if b != data[4000+i] {
				t.Errorf("ByteAt(%d) mismatch: expected %d, got %d", i, data[4000+i], b)
			}
		}

		// Test SetByteAt
		err = v.SetByteAt(50, 0xFF)
		if err != nil {
			t.Errorf("SetByteAt failed: %v", err)
		}
		b, _ := v.ByteAt(50)
		if b != 0xFF {
			t.Errorf("SetByteAt verify failed: expected 255, got %d", b)
		}
		// Reset
		v.SetByteAt(50, data[4050])

		// Test Compare
		if !v.Compare(data[4000:4200]) {
			t.Error("Compare failed")
		}

		// Test Index
		target := string(data[4090:4100]) // Spans boundary
		idx := v.Index(target)
		if idx != 90 {
			t.Errorf("Index failed: expected 90, got %d", idx)
		}

		// Test Iterator
		it := v.Iterator()
		count := 0
		for {
			b, ok := it()
			if !ok {
				break
			}
			if b != data[4000+count] {
				t.Errorf("Iterator mismatch at %d", count)
			}
			count++
		}
		if count != 200 {
			t.Errorf("Iterator count mismatch: expected 200, got %d", count)
		}

		return nil
	})

	if err != nil {
		t.Fatalf("View failed: %v", err)
	}
}

func TestViewerLargeRange(t *testing.T) {
	chunk, err := shm.NewChunk("test_viewer_large")
	if err != nil {
		t.Fatalf("Failed to create chunk: %v", err)
	}
	defer chunk.Close()

	bus, err := shm.NewBus(chunk, func(c *shm.Chunk) {})
	if err != nil {
		t.Fatalf("Failed to create bus: %v", err)
	}

	// 11 blocks (1 Header + 10 Data blocks = 40KB data capacity)
	stripe, err := bus.AllocStripe(11)
	if err != nil {
		t.Fatalf("Failed to alloc stripe: %v", err)
	}

	data := bytes.Repeat([]byte("ABCDEFGHIJ"), 4000) // 40KB
	n, err := stripe.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Short write: %d", n)
	}

	err = stripe.View(0, len(data), func(v *shm.Viewer) error {
		// Index near the end
		target := "GHIJABCDEF"
		expectedIdx := bytes.Index(data, []byte(target))
		idx := v.Index(target)
		if idx != expectedIdx {
			t.Errorf("Large View Index failed: expected %d, got %d", expectedIdx, idx)
		}

		// Compare
		if !v.Compare(data) {
			t.Error("Large View Compare failed")
		}

		return nil
	})

	if err != nil {
		t.Fatalf("Large View failed: %v", err)
	}
}
