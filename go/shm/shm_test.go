//go:build linux

package shm_test

import (
	"testing"

	"github.com/nautrouds/mmfg/v2/go/shm"
)

func TestBusAndStripe(t *testing.T) {
	chunk, err := shm.NewChunk("test_bus")
	if err != nil {
		t.Fatalf("Failed to create chunk: %v", err)
	}
	defer chunk.Close()

	bus, err := shm.NewBus(chunk, func(c *shm.Chunk) {})
	if err != nil {
		t.Fatalf("Failed to create bus: %v", err)
	}

	// Allocate 2 blocks for a stripe
	stripe, err := bus.AllocStripe(2)
	if err != nil {
		t.Fatalf("Failed to alloc stripe: %v", err)
	}

	if len(stripe.Blocks) != 2 {
		t.Errorf("Expected 2 blocks, got %d", len(stripe.Blocks))
	}

	// Test WriteAt/ReadAt
	data := []byte("hello stripe across blocks")
	// Offsets that might span blocks (BlockSize is 4KB, 4000 is near the end)
	n, err := stripe.WriteAt(data, 4000)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Short write: %d", n)
	}

	readBuf := make([]byte, len(data))
	n, err = stripe.ReadAt(readBuf, 4000)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if string(readBuf) != string(data) {
		t.Errorf("Data mismatch: %s", string(readBuf))
	}
}

func TestControlWithStripe(t *testing.T) {
	chunk, err := shm.NewChunk("test_ctrl_stripe")
	if err != nil {
		t.Fatalf("Failed to create chunk: %v", err)
	}
	defer chunk.Close()
	bus, err := shm.NewBus(chunk, func(c *shm.Chunk) {})
	if err != nil {
		t.Fatalf("Failed to create bus: %v", err)
	}

	stripe, _ := bus.AllocStripe(256) // 1MB for control
	ctrl := shm.NewControl(stripe)
	ctrl.Init()

	slotID, ok := ctrl.AllocSlot()
	if !ok {
		t.Fatal("Failed to alloc slot")
	}

	ctrl.SetStripe(slotID, shm.StripeStatusBusy, 0, 1024, 1)
	if ctrl.GetStripeStatus(slotID) != shm.StripeStatusBusy {
		t.Error("Status mismatch")
	}

	ctrl.FreeSlot(slotID)
	if ctrl.GetStripeStatus(slotID) != shm.StripeStatusIdle {
		t.Error("Failed to free slot")
	}
}

func TestStripeExpansion(t *testing.T) {
	chunk, err := shm.NewChunk("test_expansion")
	if err != nil {
		t.Fatalf("Failed to create chunk: %v", err)
	}
	defer chunk.Close()

	bus, err := shm.NewBus(chunk, func(c *shm.Chunk) {})
	if err != nil {
		t.Fatalf("Failed to create bus: %v", err)
	}

	// Start with 1 block
	stripe, err := bus.AllocStripe(1)
	if err != nil {
		t.Fatalf("Failed to alloc stripe: %v", err)
	}

	if len(stripe.Blocks) != 1 {
		t.Fatalf("Expected 1 block, got %d", len(stripe.Blocks))
	}

	// Expand by 2 more blocks
	err = bus.AllocBlocksForStripe(stripe, 3)
	if err != nil {
		t.Fatalf("Failed to expand stripe: %v", err)
	}

	if len(stripe.Blocks) != 4 {
		t.Errorf("Expected 4 blocks after expansion, got %d", len(stripe.Blocks))
	}

	// Test writing to the new blocks
	data := make([]byte, 8000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Write at 2KB offset, spanning 1st, 2nd, and 3rd blocks (total 2KB + 8KB = 10KB)
	// 10KB / 4KB = 2.5 blocks. So it uses 3 blocks.
	n, err := stripe.WriteAt(data, 2048)
	if err != nil {
		t.Fatalf("WriteAt expanded stripe failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Short write: %d", n)
	}

	readBuf := make([]byte, len(data))
	n, err = stripe.ReadAt(readBuf, 2048)
	if err != nil {
		t.Fatalf("ReadAt expanded stripe failed: %v", err)
	}
	for i := range data {
		if readBuf[i] != data[i] {
			t.Fatalf("Data mismatch at index %d", i)
		}
	}

}
