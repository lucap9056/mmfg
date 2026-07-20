//go:build unix

package sync

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Eventfd provides a lightweight inter-process signaling mechanism.
type Eventfd struct {
	fd int
}

func NewEventfd() (*Eventfd, error) {
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("eventfd creation failed: %w", err)
	}
	return &Eventfd{fd: fd}, nil
}

func AttachEventfd(fd int) *Eventfd {
	return &Eventfd{fd: fd}
}

// Notify increments the eventfd counter.
func (e *Eventfd) Notify() error {
	var val uint64 = 1
	data := (*[8]byte)(unsafe.Pointer(&val))[:]
	_, err := unix.Write(e.fd, data)
	if err != nil {
		return fmt.Errorf("eventfd notify failed: %w", err)
	}
	return nil
}

// Wait blocks until the eventfd counter is non-zero.
func (e *Eventfd) Wait() error {
	var buf [8]byte
	_, err := unix.Read(e.fd, buf[:])
	if err != nil {
		return fmt.Errorf("eventfd wait failed: %w", err)
	}
	return nil
}

func (e *Eventfd) Fd() int {
	return e.fd
}

func (e *Eventfd) Close() error {
	if e.fd >= 0 {
		err := unix.Close(e.fd)
		e.fd = -1
		return err
	}
	return nil
}
