//go:build linux

package integration

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/nautrouds/mmfg/v2/go/shm"
)

func TestByteExactRoundTrip(t *testing.T) {
	sizes := []int{
		0,
		1,
		shm.BlockSize - 1,
		shm.BlockSize,
		shm.BlockSize + 1,
		3 * shm.BlockSize,
		100 * shm.BlockSize,
		shm.StripeDataSizeLimit,
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("%dB", size), func(t *testing.T) {
			conn := request(t)
			defer conn.Close()

			payload := randomBytes(size, int64(size)+1)
			if _, err := conn.Write(payload); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := conn.Next("rust1"); err != nil {
				t.Fatalf("Next(rust1): %v", err)
			}

			if got := int(conn.DataLen()); got != size {
				t.Fatalf("DataLen mismatch: got %d want %d", got, size)
			}

			buf := make([]byte, size)
			if _, err := io.ReadFull(conn, buf); err != nil {
				t.Fatalf("read: %v", err)
			}
			if !bytes.Equal(buf, payload) {
				t.Fatalf("data mismatch at size %d", size)
			}
		})
	}
}
