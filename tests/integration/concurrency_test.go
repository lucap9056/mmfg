//go:build linux

package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/nautrouds/mmfg/v2/go/shm"
)

func TestConcurrentRequestsRust(t *testing.T) {
	const workers = 300

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	wg.Add(workers)

	for i := range workers {
		go func(id int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn, err := testHub.Request(ctx, 1, true)
			if err != nil {
				errCh <- fmt.Errorf("id %d: request: %w", id, err)
				return
			}
			defer conn.Close()

			target := "rust1"
			if id%2 == 0 {
				target = "rust2"
			}

			payload := randomBytes(64+id%2048, int64(id))
			if _, err := conn.Write(payload); err != nil {
				errCh <- fmt.Errorf("id %d: write: %w", id, err)
				return
			}
			if err := conn.Next(target); err != nil {
				errCh <- fmt.Errorf("id %d: Next(%s): %w", id, target, err)
				return
			}

			buf := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, buf); err != nil {
				errCh <- fmt.Errorf("id %d: read: %w", id, err)
				return
			}
			if !bytes.Equal(buf, payload) {
				errCh <- fmt.Errorf("id %d: data mismatch via %s", id, target)
				return
			}
			errCh <- nil
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Error(err)
		}
	}
}

func TestConcurrentLargeTransfersRust(t *testing.T) {
	const workers = 64

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	wg.Add(workers)

	for i := range workers {
		go func(id int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn, err := testHub.Request(ctx, 1, true)
			if err != nil {
				errCh <- fmt.Errorf("id %d: request: %w", id, err)
				return
			}
			defer conn.Close()

			target := "rust1"
			if id%2 == 0 {
				target = "rust2"
			}

			size := shm.BlockSize + (id * shm.BlockSize / 8)
			payload := randomBytes(size, int64(id)+1000)
			if _, err := conn.Write(payload); err != nil {
				errCh <- fmt.Errorf("id %d: write: %w", id, err)
				return
			}
			if err := conn.Next(target); err != nil {
				errCh <- fmt.Errorf("id %d: Next(%s): %w", id, target, err)
				return
			}

			buf := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, buf); err != nil {
				errCh <- fmt.Errorf("id %d: read: %w", id, err)
				return
			}
			if !bytes.Equal(buf, payload) {
				errCh <- fmt.Errorf("id %d: data mismatch via %s (size %d)", id, target, size)
				return
			}
			errCh <- nil
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Error(err)
		}
	}
}

func TestSerialRepetitionRust(t *testing.T) {
	const iterations = 1500

	for i := range iterations {
		conn := request(t)

		payload := fmt.Appendf(nil, "serial-msg-%d", i)
		if _, err := conn.Write(payload); err != nil {
			conn.Close()
			t.Fatalf("iter %d: write: %v", i, err)
		}
		if err := conn.Next("rust1"); err != nil {
			conn.Close()
			t.Fatalf("iter %d: Next(rust1): %v", i, err)
		}

		buf := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, buf); err != nil {
			conn.Close()
			t.Fatalf("iter %d: read: %v", i, err)
		}
		if !bytes.Equal(buf, payload) {
			conn.Close()
			t.Fatalf("iter %d: mismatch: got %q want %q", i, buf, payload)
		}
		conn.Close()
	}
}
