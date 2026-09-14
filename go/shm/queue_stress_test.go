//go:build linux

package shm_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nautrouds/mmfg/v2/go/shm"
)

func TestQueuePushPopNoLoss(t *testing.T) {
	chunk, err := shm.NewChunk("test_queue_stress")
	if err != nil {
		t.Fatalf("Failed to create chunk: %v", err)
	}
	defer chunk.Close()

	blkCnt := shm.ControlStripeSize / shm.BlockSize
	stripe := &shm.Stripe{
		BlockCount: blkCnt,
		Blocks:     make([][]byte, blkCnt),
	}
	for i := range blkCnt {
		off := i * shm.BlockSize
		stripe.Blocks[i] = chunk.Data[off : off+shm.BlockSize]
	}

	ctrl := shm.NewControl(stripe)
	ctrl.Init()

	const numProducers = 2000
	const qOff = uintptr(shm.OFF_RESP_QUEUE)

	var seen sync.Map
	var poppedCount atomic.Int64
	var pushedOK atomic.Int64

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				for {
					slotID, _, ok := ctrl.Pop(qOff)
					if !ok {
						return
					}
					seen.Store(slotID, true)
					poppedCount.Add(1)
				}
			default:
			}
			slotID, _, ok := ctrl.Pop(qOff)
			if !ok {
				time.Sleep(time.Millisecond)
				continue
			}
			seen.Store(slotID, true)
			poppedCount.Add(1)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(numProducers)
	for i := range numProducers {
		go func(id uint32) {
			defer wg.Done()
			for !ctrl.Push(qOff, id, shm.CMD_PROCESS) {
				time.Sleep(time.Millisecond)
			}
			pushedOK.Add(1)
		}(uint32(i + 1))
	}

	wg.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for poppedCount.Load() < int64(numProducers) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	<-done

	if pushedOK.Load() != int64(numProducers) {
		t.Fatalf("expected %d successful pushes, got %d", numProducers, pushedOK.Load())
	}

	missing := 0
	for i := 1; i <= numProducers; i++ {
		if _, ok := seen.Load(uint32(i)); !ok {
			missing++
			if missing <= 20 {
				t.Logf("missing slotID=%d", i)
			}
		}
	}
	if missing > 0 {
		t.Fatalf("lost %d/%d entries: pushed=%d popped=%d", missing, numProducers, pushedOK.Load(), poppedCount.Load())
	}
}
