//go:build unix

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nautrouds/mmfg/v2/go/hub"
	"github.com/nautrouds/mmfg/v2/go/node"
)

func TestMain(t *testing.T) {
	const (
		sockA = "/tmp/mmfg_node_a.sock"
		sockB = "/tmp/mmfg_node_b.sock"
	)

	// 1. Setup Node A: Uppercase Handler
	nodeA := node.NewNode(node.WithHandler(func(conn node.Connection) {
		buf := make([]byte, 64)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			upper := bytes.ToUpper(buf[:n])
			_, err = conn.Write(upper)
			if err != nil {
				return
			}
		}
	}))
	go nodeA.Listen(sockA)

	// 2. Setup Node B: Suffix Handler
	nodeB := node.NewNode(node.WithHandler(func(conn node.Connection) {
		data, err := io.ReadAll(conn)
		if err != nil && err != io.EOF {
			return
		}
		res := append(data, []byte("_PROCESSED")...)
		conn.Write(res)
	}))
	go nodeB.Listen(sockB)

	time.Sleep(500 * time.Millisecond)

	// 3. Setup Hub
	h, err := hub.NewHub()
	if err != nil {
		t.Fatalf("Failed to create Hub: %v", err)
	}

	if err := h.Dial("NodeA", sockA); err != nil {
		t.Fatalf("Failed to dial NodeA: %v", err)
	}
	if err := h.Dial("NodeB", sockB); err != nil {
		t.Fatalf("Failed to dial NodeB: %v", err)
	}

	testBasic(t, h)
	testHandoff(t, h)
	testExpansion(t, h)
	testMegaExpansion(t, h)
	testSerialRepetition(t, h)
	testConcurrency(t, h)
}

func testMegaExpansion(t *testing.T, h *hub.Hub) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	conn, err := h.Request(ctx, 1, true)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer conn.Close()

	chars := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
	// 2MB of data
	megaData := make([]byte, 2*1024*1024)
	for i := range megaData {
		megaData[i] = chars[i%len(chars)]
	}

	const numWaiters = 10
	errCh := make(chan error, numWaiters)

	go func() {
		for range numWaiters {
			if _, err := conn.Write(megaData); err != nil {
				errCh <- fmt.Errorf("Mega write failed: %v", err)
			}

			if err := conn.Next("NodeA"); err != nil {
				errCh <- fmt.Errorf("Next(NodeA) failed: %v", err)
			}

			buf := make([]byte, 1024)

			n, _ := conn.Read(buf)
			if !bytes.Equal(buf[:n], bytes.ToUpper(megaData[:n])) {
				errCh <- fmt.Errorf("Mega data head mismatch")
			}

			errCh <- nil
		}
	}()

	for i := 0; i < numWaiters; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("Mega expansion test failed: %v", err)
		}
	}
}

func testSerialRepetition(t *testing.T, h *hub.Hub) {

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		func(id int) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			conn, err := h.Request(ctx, 1, true)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			defer conn.Close()

			input := fmt.Sprintf("msg-%d", id)

			if _, err := conn.Write([]byte(input)); err != nil {
				t.Fatalf("Iteration %d: Write failed: %v", id, err)
			}

			if err := conn.Next("NodeA"); err != nil {
				t.Fatalf("Iteration %d: Next(NodeA) failed: %v", id, err)
			}

			buf := make([]byte, 64)
			n, err := conn.Read(buf)
			if err != nil {
				t.Fatalf("Iteration %d: Read failed: %v", id, err)
			}

			expected := strings.ToUpper(input)
			if string(buf[:n]) != expected {
				t.Fatalf("Iteration %d: Mismatch, got %s, want %s", id, string(buf[:n]), expected)
			}
		}(i)
	}
}

func testConcurrency(t *testing.T, h *hub.Hub) {
	const numWaiters = 2000
	errCh := make(chan error, numWaiters)

	var wg sync.WaitGroup
	wg.Add(numWaiters)

	for i := 0; i < numWaiters; i++ {
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			conn, err := h.Request(ctx, 1, true)
			if err != nil {
				errCh <- fmt.Errorf("Request failed: %v", err)
				return
			}
			defer conn.Close()

			msg := fmt.Sprintf("Concurrent Request %d", id)
			if _, err := conn.Write([]byte(msg)); err != nil {
				errCh <- fmt.Errorf("id %d: Write failed: %v", id, err)
				return
			}

			if err := conn.Next("NodeA"); err != nil {
				errCh <- fmt.Errorf("id %d: Next(NodeA) failed: %v", id, err)
				return
			}

			res := make([]byte, 64)
			n, err := conn.Read(res)
			if err != nil {
				errCh <- fmt.Errorf("id %d: Read failed: %v", id, err)
				return
			}
			expected := strings.ToUpper(msg)
			if string(res[:n]) != expected {
				errCh <- fmt.Errorf("id %d: mismatch, got %s, want %s", id, string(res[:n]), expected)
				return
			}
			errCh <- nil
		}(i)
	}

	wg.Wait()

	for i := 0; i < numWaiters; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("Concurrency test failed: %v", err)
		}
	}
}

func testExpansion(t *testing.T, h *hub.Hub) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	conn, err := h.Request(ctx, 1, true)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer conn.Close()

	// 20KB of data, exceeds initial 8KB (1 header + 1 data block)
	largeData := make([]byte, 20*1024)
	for i := range largeData {
		largeData[i] = 'a' + byte(i%26)
	}

	if _, err := conn.Write(largeData); err != nil {
		t.Fatalf("Large write failed: %v", err)
	}

	if err := conn.Next("NodeA"); err != nil {
		t.Fatalf("Next(NodeA) failed: %v", err)
	}

	buf := make([]byte, len(largeData))
	n, err := io.ReadFull(conn, buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read failed: %v", err)
	}

	if n != len(largeData) {
		t.Fatalf("Expected %d bytes, got %d", len(largeData), n)
	}

	for i := range largeData {
		expected := bytes.ToUpper([]byte{largeData[i]})[0]
		if buf[i] != expected {
			t.Fatalf("Data mismatch at index %d: expected %c, got %c", i, expected, buf[i])
		}
	}
}

func testBasic(t *testing.T, h *hub.Hub) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	conn, err := h.Request(ctx, 1, true)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer conn.Close()

	input := []byte("hello mmfg")
	for _, b := range input {
		conn.WriteByte(b)
	}

	if err := conn.Next("NodeA"); err != nil {
		t.Fatalf("Next(NodeA) failed: %v", err)
	}

	buf := make([]byte, 64)
	n, _ := conn.Read(buf)
	res := string(buf[:n])

	if strings.TrimSpace(res) != "HELLO MMFG" {
		t.Fatalf("Basic test failed: expected HELLO MMFG, got %s", res)
	}
}

func testHandoff(t *testing.T, h *hub.Hub) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	conn, err := h.Request(ctx, 1, true)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer conn.Close()

	input := "chain"
	conn.Write([]byte(input))
	if err := conn.Next("NodeA"); err != nil {
		t.Fatalf("Next(NodeA) failed: %v", err)
	}

	if err := conn.Next("NodeB"); err != nil {
		t.Fatalf("Next(NodeB) failed: %v", err)
	}

	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	res := string(buf[:n])

	expected := "CHAIN_PROCESSED"
	if strings.TrimSpace(res) != expected {
		t.Fatalf("Handoff test failed: expected %s, got %s", expected, res)
	}
}
