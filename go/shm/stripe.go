//go:build unix

package shm

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"sync/atomic"
	"unsafe"
)

// Block represents a single physical memory block in a Chunk.
type Block struct {
	ChunkID  int16
	BlockIdx int16
	Data     []byte // The mmap'd slice of the chunk for this block
}

// Stripe represents a collection of blocks forming a logical communication unit.
type Stripe struct {
	io.WriterAt
	io.ReaderAt
	Sequence   []int16
	BlockCount int
	Blocks     [][]byte
	DataLen    uint32
}

// GetPointer returns a direct pointer to the memory at a stripe-relative offset.
// It assumes the requested data does not cross block boundaries (BlockSize = 4KB).
func (s *Stripe) GetPointer(offset uintptr) unsafe.Pointer {
	bIdx := offset / BlockSize
	if bIdx >= uintptr(len(s.Blocks)) {
		return nil
	}
	innerOff := offset % BlockSize
	return unsafe.Pointer(&s.Blocks[bIdx][innerOff])
}

func (s *Stripe) updateDataLen(newLen uint32) {
	ptr := unsafe.Pointer(&s.Blocks[0][HEADER_DATA_LEN_OFF])
	atomic.StoreUint32((*uint32)(ptr), newLen)
	s.DataLen = newLen
}

func (s *Stripe) Truncate(newLen uint32) {
	if newLen > s.DataLen {
		return
	}
	s.updateDataLen(newLen)
}

// ReadAt implements io.ReaderAt for a Stripe.
// ... existing ReadAt ...
func (s *Stripe) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("negative offset")
	}

	if off >= int64(s.DataLen) {
		return 0, io.EOF
	}

	remaining := int64(len(p))
	if off+remaining > int64(s.DataLen) {
		remaining = int64(s.DataLen) - off
	}
	off += BlockSize // Skip Header Block

	for remaining > 0 {
		bIdx := off / BlockSize
		if bIdx >= int64(len(s.Blocks)) {
			return n, io.EOF
		}
		innerOff := off % BlockSize
		avail := BlockSize - innerOff
		copyLen := avail
		if copyLen > remaining {
			copyLen = remaining
		}

		copy(p[n:n+int(copyLen)], s.Blocks[bIdx][innerOff:innerOff+copyLen])
		n += int(copyLen)
		off += int64(copyLen)
		remaining -= copyLen
	}
	return n, nil
}

func (s *Stripe) ReadByteAt(off int64) (byte, error) {
	if off < 0 || off >= int64(s.DataLen) {
		if off < 0 {
			return 0, errors.New("negative offset")
		}
		return 0, io.EOF
	}

	off += BlockSize // Skip Header Block

	bIdx := off / BlockSize
	if bIdx >= int64(len(s.Blocks)) {
		return 0, io.EOF
	}
	innerOff := off % BlockSize
	return s.Blocks[bIdx][innerOff], nil
}

// WriteAt implements io.WriterAt for a Stripe.
func (s *Stripe) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("negative offset")
	}

	defer func() {
		if n > 0 {
			if newEnd := uint32(off - BlockSize); newEnd > s.DataLen {
				s.updateDataLen(newEnd)
			}
		}
	}()

	off += BlockSize // Skip Header Block

	remaining := len(p)
	for remaining > 0 {
		bIdx := off / BlockSize
		if bIdx >= int64(len(s.Blocks)) {
			return n, errors.New("stripe capacity exceeded")
		}
		innerOff := off % BlockSize
		avail := BlockSize - innerOff
		copyLen := int(avail)
		if copyLen > remaining {
			copyLen = remaining
		}

		copy(s.Blocks[bIdx][innerOff:innerOff+int64(copyLen)], p[n:n+copyLen])
		n += copyLen
		off += int64(copyLen)
		remaining -= copyLen
	}

	return n, nil
}

func (s *Stripe) WriteByteAt(b byte, off int64) error {
	if off < 0 {
		return errors.New("negative offset")
	}

	off += BlockSize

	bIdxInt := off / BlockSize

	if bIdxInt >= int64(len(s.Blocks)) {
		return errors.New("stripe capacity exceeded")
	}

	innerOff := off % BlockSize

	s.Blocks[bIdxInt][innerOff] = b
	if newEnd := uint32(off - BlockSize + 1); newEnd > s.DataLen {
		s.updateDataLen(newEnd)
	}
	return nil
}

type Viewer struct {
	Segments [][]byte
	offsets  []int
	Length   int
}

func (v *Viewer) findSegment(offset int) (int, int) {
	idx, _ := slices.BinarySearch(v.offsets, offset)
	if idx > 0 && (idx == len(v.offsets) || v.offsets[idx] > offset) {
		idx--
	}
	return idx, offset - v.offsets[idx]
}

func (v *Viewer) ByteAt(offset int) (byte, error) {
	if offset < 0 || offset >= v.Length {
		return 0, errors.New("out of bounds")
	}

	idx, innerOff := v.findSegment(offset)
	return v.Segments[idx][innerOff], nil
}

func (v *Viewer) SetByteAt(offset int, val byte) error {
	if offset < 0 || offset >= v.Length {
		return errors.New("out of bounds")
	}

	idx, innerOff := v.findSegment(offset)
	v.Segments[idx][innerOff] = val
	return nil
}

func (v *Viewer) Iterator() func() (byte, bool) {
	segIdx := 0
	innerIdx := 0

	return func() (byte, bool) {
		for segIdx < len(v.Segments) {
			if innerIdx < len(v.Segments[segIdx]) {
				val := v.Segments[segIdx][innerIdx]
				innerIdx++
				return val, true
			}
			segIdx++
			innerIdx = 0
		}
		return 0, false
	}
}

func (v *Viewer) Compare(data []byte) bool {
	if len(data) != v.Length {
		return false
	}

	for i, seg := range v.Segments {
		start := v.offsets[i]
		if !bytes.Equal(seg, data[start:start+len(seg)]) {
			return false
		}
	}
	return true
}

func (v *Viewer) Index(s string) int {
	if len(s) == 0 {
		return 0
	}

	target := unsafe.Slice(unsafe.StringData(s), len(s))

	targetLen := len(target)
	firstByte := target[0]

	for i, seg := range v.Segments {
		currentOff := v.offsets[i]
		segLen := len(seg)

		for startIdx := 0; startIdx < segLen; {
			matchPos := bytes.IndexByte(seg[startIdx:], firstByte)
			if matchPos == -1 {
				break
			}

			absMatchPos := startIdx + matchPos
			startIdx = absMatchPos + 1

			if absMatchPos+targetLen <= segLen {
				if bytes.Equal(seg[absMatchPos:absMatchPos+targetLen], target) {
					return currentOff + absMatchPos
				}
			} else {
				if v.matchAcrossSegments(i, absMatchPos, target) {
					return currentOff + absMatchPos
				}
			}
		}
	}

	return -1
}

func (v *Viewer) matchAcrossSegments(segIdx, posInSeg int, target []byte) bool {
	targetLen := len(target)
	currSegIdx := segIdx
	currOff := posInSeg

	for i := 0; i < targetLen; i++ {
		if currOff >= len(v.Segments[currSegIdx]) {
			currSegIdx++
			currOff = 0
			if currSegIdx >= len(v.Segments) {
				return false
			}
		}

		if v.Segments[currSegIdx][currOff] != target[i] {
			return false
		}
		currOff++
	}
	return true
}

func (s *Stripe) View(offset, length int, call func(*Viewer) error) error {
	if offset < 0 {
		return errors.New("negative offset")
	}

	if offset >= int(s.DataLen) {
		return io.EOF
	}

	n := min(uint32(offset+length), s.DataLen)
	actualLen := int(n) - offset

	if actualLen <= 0 {
		return io.EOF
	}

	startOff := int64(offset) + BlockSize
	endOff := int64(n) + BlockSize

	var segments [][]byte

	for off := startOff; off < endOff; {
		bIdx := off / BlockSize
		if bIdx >= int64(len(s.Blocks)) {
			break
		}

		innerOff := off % BlockSize
		avail := int64(BlockSize) - innerOff
		copyLen := avail
		if off+copyLen > endOff {
			copyLen = endOff - off
		}

		segments = append(segments, s.Blocks[bIdx][innerOff:innerOff+copyLen])

		off += copyLen
	}

	viewer := &Viewer{
		Segments: segments,
		offsets:  make([]int, len(segments)),
		Length:   actualLen,
	}

	curr := 0
	for i, seg := range segments {
		viewer.offsets[i] = curr
		curr += len(seg)
	}

	return call(viewer)
}
