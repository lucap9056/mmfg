//go:build unix

package main

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/nautrouds/mmfg/v2/go/hub"
	"github.com/nautrouds/mmfg/v2/go/node"
)

func TestHandoffIntegration(t *testing.T) {
	const (
		sockA = "/tmp/mmfg_test_handoff_a.sock"
		sockB = "/tmp/mmfg_test_handoff_b.sock"
	)
	os.Remove(sockA)
	os.Remove(sockB)

	nodeA := node.NewNode(node.WithHandler(func(conn node.Connection) {
		data, err := io.ReadAll(conn)
		if err != nil && err != io.EOF {
			return
		}
		conn.Write(append(data, []byte("-A")...))
	}))
	go nodeA.Listen(sockA)

	nodeB := node.NewNode(node.WithHandler(func(conn node.Connection) {
		data, err := io.ReadAll(conn)
		if err != nil && err != io.EOF {
			return
		}
		conn.Write(append(data, []byte("-B")...))
	}))
	go nodeB.Listen(sockB)

	time.Sleep(200 * time.Millisecond)

	h, err := hub.NewHub()
	if err != nil {
		t.Fatalf("Hub err: %v", err)
	}
	h.Dial("A", sockA)
	h.Dial("B", sockB)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	conn, _ := h.Request(ctx, 1, true)
	defer conn.Close()

	conn.Write([]byte("data"))
	conn.Next("A")

	if err := conn.Next("B"); err != nil {
		t.Fatalf("Handoff err: %v", err)
	}

	buf := make([]byte, 64)
	n, _ := conn.Read(buf)
	res := string(buf[:n])
	if res != "data-A-B" {
		t.Errorf("Mismatch: got %s", res)
	}
}

func BenchmarkSingleMessage(b *testing.B) {
	const sock = "/tmp/mmfg_bench_single.sock"
	os.Remove(sock)

	n := node.NewNode(node.WithHandler(func(conn node.Connection) {
		buf := make([]byte, 1)
		for {
			_, err := conn.Read(buf)
			if err != nil {
				return
			}
			conn.Write(buf)
		}
	}))
	go n.Listen(sock)
	time.Sleep(200 * time.Millisecond)

	h, _ := hub.NewHub()
	h.Dial("bench", sock)

	ctx, cancel := context.WithTimeout(b.Context(), time.Second)
	defer cancel()
	conn, _ := h.Request(ctx, 1, true)
	defer conn.Close()

	payload := []byte("x")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Write(payload)
		conn.Next("bench")
		conn.Read(make([]byte, 1))
	}
}
