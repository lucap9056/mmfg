//go:build linux

package shm

import (
	"golang.org/x/sys/unix"
)

// --- Chunk Support ---

type Chunk struct {
	Data []byte
	Fd   int
}

func NewChunk(name string) (*Chunk, error) {
	fd, err := unix.MemfdCreate(name, unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, err
	}
	fdNum := int(fd)
	if err := unix.Ftruncate(fdNum, int64(ChunkSize)); err != nil {
		unix.Close(fdNum)
		return nil, err
	}
	chunk, err := AttachChunk(fdNum)
	if err != nil {
		unix.Close(fdNum)
		return nil, err
	}
	return chunk, nil
}

func AttachChunk(fd int) (*Chunk, error) {
	data, err := unix.Mmap(fd, 0, ChunkSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &Chunk{Data: data, Fd: fd}, nil
}

func (c *Chunk) Close() error {
	if c == nil {
		return nil
	}
	if c.Data != nil {
		unix.Munmap(c.Data)
		c.Data = nil
	}
	if c.Fd >= 0 {
		err := unix.Close(c.Fd)
		c.Fd = -1
		return err
	}
	return nil
}
