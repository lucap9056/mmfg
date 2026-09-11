//go:build linux

package integration

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestRustNodeHandlerPanicRecovery(t *testing.T) {
	conn := request(t)
	defer conn.Close()

	if _, err := conn.Write([]byte("__MMFG_TEST_PANIC__")); err != nil {
		t.Fatalf("write: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- conn.Next("rust1") }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Next(rust1) after handler panic: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Next(rust1) hung after handler panic -- hub never got a response")
	}

	conn2 := request(t)
	defer conn2.Close()

	payload := []byte("still-alive-after-panic")
	if _, err := conn2.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := conn2.Next("rust1"); err != nil {
		t.Fatalf("Next(rust1) after panic recovery: %v", err)
	}
	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(conn2, buf); err != nil || !bytes.Equal(buf, payload) {
		t.Fatalf("rust1 broken after handler panic: %q err=%v", buf, err)
	}
}
