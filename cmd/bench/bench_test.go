//go:build linux

package main

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/nautrouds/mmfg/v2/go/hub"
	"github.com/nautrouds/mmfg/v2/go/node"
)

var msg = []byte("Hello MMFG!")

func BenchmarkIPC_Latency(b *testing.B) {
	const sock = "/tmp/mmfg_bench.sock"
	const nodeName = "bench"

	nodeA := node.NewNode(node.WithHandler(func(conn node.Connection) {
		// Handler to echo or just consume
		io.Copy(io.Discard, conn)
	}))
	go nodeA.Listen(sock)

	time.Sleep(200 * time.Millisecond)

	h, err := hub.NewHub()
	if err != nil {
		b.Fatalf("Failed to create hub: %v", err)
	}
	if err := h.Dial(nodeName, sock); err != nil {
		b.Fatalf("Failed to dial node: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn, err := h.Request(b.Context(), 1, true)
		if err != nil {
			b.Fatalf("Request error at index %d: %v", i, err)
		}

		if _, err := conn.Write(msg); err != nil {
			b.Fatalf("Write error at index %d: %v", i, err)
		}

		if err := conn.Next(nodeName); err != nil {
			b.Fatalf("Next error at index %d: %v", i, err)
		}
		conn.Close()
	}

	b.StopTimer()
}

func BenchmarkIPC_Latency_Reuse(b *testing.B) {
	const sock = "/tmp/mmfg_bench_reuse.sock"
	const nodeName = "bench_reuse"

	nodeA := node.NewNode(node.WithHandler(func(conn node.Connection) {

	}))
	go nodeA.Listen(sock)

	time.Sleep(200 * time.Millisecond)

	h, err := hub.NewHub()
	if err != nil {
		b.Fatalf("Failed to create hub: %v", err)
	}
	if err := h.Dial(nodeName, sock); err != nil {
		b.Fatalf("Failed to dial node: %v", err)
	}

	conn, err := h.Request(b.Context(), 1, true)
	if err != nil {
		b.Fatalf("Failed to request connection: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write(msg); err != nil {
		b.Fatalf("Write failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := conn.Next(nodeName); err != nil {
			b.Fatalf("Next failed at %d: %v", i, err)
		}
	}

	b.StopTimer()
}

func BenchmarkIPC_Async_10Nodes(b *testing.B) {
	const numNodes = 10
	sockets := make([]string, numNodes)
	nodeNames := make([]string, numNodes)

	h, err := hub.NewHub()
	if err != nil {
		b.Fatalf("Failed to create hub: %v", err)
	}

	for i := 0; i < numNodes; i++ {
		nodeNames[i] = fmt.Sprintf("node_async_%d", i)
		sockets[i] = fmt.Sprintf("/tmp/mmfg_async_%d.sock", i)
		n := node.NewNode(node.WithHandler(func(conn node.Connection) {
			// Just consume
			io.Copy(io.Discard, conn)
		}))
		go n.Listen(sockets[i])
		time.Sleep(50 * time.Millisecond)
		if err := h.Dial(nodeNames[i], sockets[i]); err != nil {
			b.Fatalf("Failed to dial node %d: %v", i, err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			target := nodeNames[i%numNodes]
			conn, err := h.Request(b.Context(), 1, true)
			if err != nil {
				b.Errorf("Request error: %v", err)
				return
			}
			if _, err := conn.Write(msg); err != nil {
				b.Errorf("Write error: %v", err)
				return
			}
			if err := conn.Next(target); err != nil {
				b.Errorf("Next error: %v", err)
				return
			}
			conn.Close()
			i++
		}
	})
	b.StopTimer()
}

func BenchmarkIPC_Chained_10Nodes(b *testing.B) {
	const numNodes = 10
	sockets := make([]string, numNodes)
	nodeNames := make([]string, numNodes)

	h, err := hub.NewHub()
	if err != nil {
		b.Fatalf("Failed to create hub: %v", err)
	}

	for i := 0; i < numNodes; i++ {
		nodeNames[i] = fmt.Sprintf("node_chain_%d", i)
		sockets[i] = fmt.Sprintf("/tmp/mmfg_chain_%d.sock", i)
		n := node.NewNode(node.WithHandler(func(conn node.Connection) {
			// Pass-through
		}))
		go n.Listen(sockets[i])
		time.Sleep(50 * time.Millisecond)
		if err := h.Dial(nodeNames[i], sockets[i]); err != nil {
			b.Fatalf("Failed to dial node %d: %v", i, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := h.Request(b.Context(), 1, true)
		if err != nil {
			b.Fatalf("Request error: %v", err)
		}
		if _, err := conn.Write(msg); err != nil {
			b.Fatalf("Write error: %v", err)
		}

		for j := 0; j < numNodes; j++ {
			if err := conn.Next(nodeNames[j]); err != nil {
				b.Fatalf("Next error at node %d: %v", j, err)
			}
		}
		conn.Close()
	}
	b.StopTimer()
}
