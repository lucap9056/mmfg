//go:build linux

package integration

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/nautrouds/mmfg/v2/go/shm"
)

func TestHandoffGoRustGo(t *testing.T) {
	conn := request(t)
	defer conn.Close()

	input := []byte("payload")
	if _, err := conn.Write(input); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := conn.Next("goA"); err != nil {
		t.Fatalf("Next(goA): %v", err)
	}
	if err := conn.Next("rust1"); err != nil {
		t.Fatalf("Next(rust1): %v", err)
	}
	if err := conn.Next("goB"); err != nil {
		t.Fatalf("Next(goB): %v", err)
	}

	want := "payload-A-B"
	buf := make([]byte, len(want))
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf[:n]) != want {
		t.Fatalf("mismatch: got %q want %q", buf[:n], want)
	}
}

func TestHandoffRustToRust(t *testing.T) {
	conn := request(t)
	defer conn.Close()

	input := randomBytes(8192, 99)
	if _, err := conn.Write(input); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := conn.Next("rust1"); err != nil {
		t.Fatalf("Next(rust1): %v", err)
	}
	if err := conn.Next("rust2"); err != nil {
		t.Fatalf("Next(rust2): %v", err)
	}

	buf := make([]byte, len(input))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(buf, input) {
		t.Fatal("data corrupted across rust1 -> rust2 handoff")
	}
}

func TestHandoffMultiHopLargePayload(t *testing.T) {
	conn := request(t)
	defer conn.Close()

	input := randomBytes(3*shm.BlockSize+17, 12345)
	if _, err := conn.Write(input); err != nil {
		t.Fatalf("write: %v", err)
	}

	for _, hop := range []string{"goA", "rust1", "goB", "rust2"} {
		if err := conn.Next(hop); err != nil {
			t.Fatalf("Next(%s): %v", hop, err)
		}
	}

	want := append(append([]byte{}, input...), []byte("-A-B")...)
	buf := make([]byte, len(want))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(buf, want) {
		t.Fatal("multi-hop payload corrupted")
	}
}

func TestRustNodeDisconnectCleanup(t *testing.T) {
	const sockPath = "/tmp/mmfg_it_rust_transient.sock"

	rn, err := startRustNode(sockPath)
	if err != nil {
		t.Fatalf("start transient rust node: %v", err)
	}

	if err := testHub.Dial("rust_transient", sockPath); err != nil {
		t.Fatalf("dial rust_transient: %v", err)
	}

	func() {
		conn := request(t)
		defer conn.Close()

		if _, err := conn.Write([]byte("ping")); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := conn.Next("rust_transient"); err != nil {
			t.Fatalf("Next(rust_transient): %v", err)
		}
		buf := make([]byte, 4)
		if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
			t.Fatalf("unexpected echo: %q err=%v", buf, err)
		}
	}()

	stopRustNode(rn)
	time.Sleep(200 * time.Millisecond)

	conn := request(t)
	defer conn.Close()

	payload := []byte("still-alive")
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := conn.Next("rust1"); err != nil {
		t.Fatalf("Next(rust1) after sibling crash: %v", err)
	}
	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, buf); err != nil || !bytes.Equal(buf, payload) {
		t.Fatalf("rust1 broken after sibling crash: %q err=%v", buf, err)
	}
}
