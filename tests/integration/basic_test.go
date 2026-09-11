//go:build linux

package integration

import (
	"bytes"
	"io"
	"testing"
)

func TestBasicEchoRust(t *testing.T) {
	for _, name := range []string{"rust1", "rust2"} {
		t.Run(name, func(t *testing.T) {
			conn := request(t)
			defer conn.Close()

			payload := []byte("hello-from-go-" + name)
			if _, err := conn.Write(payload); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := conn.Next(name); err != nil {
				t.Fatalf("Next(%s): %v", name, err)
			}

			buf := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, buf); err != nil {
				t.Fatalf("read: %v", err)
			}
			if !bytes.Equal(buf, payload) {
				t.Fatalf("mismatch: got %q want %q", buf, payload)
			}
		})
	}
}
