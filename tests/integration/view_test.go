//go:build linux

package integration

import (
	"bytes"
	"io"
	"strconv"
	"testing"

	"github.com/nautrouds/mmfg/v2/go/shm"
)

func TestRustNodeView(t *testing.T) {
	conn := request(t)
	defer conn.Close()

	haystack := make([]byte, 5000)
	for i := range haystack {
		haystack[i] = byte('a' + i%26)
	}
	needlePos := shm.BlockSize - 3 // straddles the block boundary
	copy(haystack[needlePos:], []byte("NEEDLE"))

	payload := append([]byte("__MMFG_TEST_VIEWFIND__:"), haystack...)
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := conn.Next("rust1"); err != nil {
		t.Fatalf("Next(rust1): %v", err)
	}

	want := strconv.Itoa(needlePos)
	buf := make([]byte, len(want))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != want {
		t.Fatalf("VIEWFIND mismatch: got %q want %q", buf, want)
	}
}

func TestRustNodeViewNotFound(t *testing.T) {
	conn := request(t)
	defer conn.Close()

	haystack := bytes.Repeat([]byte("x"), 5000)
	payload := append([]byte("__MMFG_TEST_VIEWFIND__:"), haystack...)
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := conn.Next("rust1"); err != nil {
		t.Fatalf("Next(rust1): %v", err)
	}

	buf := make([]byte, 2)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "-1" {
		t.Fatalf("expected -1, got %q", buf)
	}
}
