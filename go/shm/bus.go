//go:build linux

package shm

import (
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
)

// Bus is the Local resource manager (Hub-side only).
type Bus struct {
	lastSearchPos atomic.Uint64
	chunkCount    atomic.Uint32
	_             [52]byte // Cache Line Padding

	bitmap atomic.Pointer[[]atomic.Uint32] //[]atomic.Uint32 Flattened bitmap: [Chunk0-Block0...Chunk0-Block1023, Chunk1-Block0...]

	mu                sync.RWMutex
	tryAppendChunkMux sync.Mutex

	chunks          []*Chunk
	onChunkAppended func(*Chunk)
}

func NewBus(firstChunk *Chunk, onChunkAppended func(*Chunk)) (*Bus, error) {
	if firstChunk == nil {
		return nil, fmt.Errorf("firstChunk cannot be nil")
	}
	if onChunkAppended == nil {
		return nil, fmt.Errorf("onChunkAppended cannot be nil")
	}

	chunks := []*Chunk{firstChunk}

	bus := &Bus{
		chunks:          chunks,
		onChunkAppended: onChunkAppended,
	}

	bus.chunkCount.Add(1)
	bitmap := make([]atomic.Uint32, BlocksPerChunk/32)
	bus.bitmap.Store(&bitmap)

	return bus, nil
}

// AllocBlocks allocates a number of blocks from the available chunks (Local only).
func (b *Bus) AllocBlocks(count int) ([]Block, error) {
	allocated := make([]Block, 0, count)
	remaining := count

	for {
		b.mu.RLock()
		bitmapSlice := *b.bitmap.Load()
		chunksSnapshot := b.chunks

		numWords := uint64(len(bitmapSlice))
		startWord := b.lastSearchPos.Load() % numWords

		for i := uint64(0); i < numWords && remaining > 0; i++ {
			wordIdx := (startWord + i) % numWords
			wordPtr := &bitmapSlice[wordIdx]

			for {
				old := wordPtr.Load()
				if old == 0xFFFFFFFF {
					break
				}

				newVal := old
				var foundBits [32]int
				foundCount := 0

				tempWord := ^old
				for tempWord != 0 && foundCount < remaining {
					bitIdx := bits.TrailingZeros32(tempWord)
					newVal |= (1 << uint(bitIdx))
					foundBits[foundCount] = bitIdx
					foundCount++
					tempWord &= ^(1 << uint(bitIdx))
				}

				if foundCount == 0 {
					break
				}

				if wordPtr.CompareAndSwap(old, newVal) {
					for j := 0; j < foundCount; j++ {
						bitIdx := foundBits[j]
						bitPos := wordIdx*32 + uint64(bitIdx)
						cid, bidx := ParseBlockAddress(bitPos)
						offset := uint32(bidx) * BlockSize

						allocated = append(allocated, Block{
							ChunkID:  cid,
							BlockIdx: bidx,
							Data:     chunksSnapshot[^cid].Data[offset : offset+BlockSize],
						})
					}
					remaining -= foundCount
					if remaining == 0 {
						b.lastSearchPos.Store((wordIdx + 1) % numWords)
						b.mu.RUnlock()
						return allocated, nil
					}
					break
				}
			}
		}
		b.mu.RUnlock()

		if err := b.tryAppendChunk(); err != nil {
			for _, res := range allocated {
				b.FreeBlock(res.ChunkID, res.BlockIdx)
			}
			return nil, err
		}
	}
}

func (b *Bus) tryAppendChunk() error {
	count := b.ChunkCount()

	if count >= MaxChunks {
		return fmt.Errorf("failed to allocate %d blocks", count)
	}

	b.tryAppendChunkMux.Lock()
	defer b.tryAppendChunkMux.Unlock()

	if count != b.ChunkCount() {
		return nil
	}

	chunk, err := NewChunk(fmt.Sprintf("mmfg_chunk%d", count))
	if err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.chunks = append(b.chunks, chunk)

	// Expand bitmap by 1024 bits (32 uint32s)
	bitmap := *b.bitmap.Load()
	updatedBitmap := append(bitmap, make([]atomic.Uint32, BlocksPerChunk/32)...)
	b.bitmap.Store(&updatedBitmap)
	b.chunkCount.Add(1)

	b.onChunkAppended(chunk)
	return nil

}

func (b *Bus) FreeBlock(chunkID, blockIdx int16) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	bitmap := *b.bitmap.Load()

	globalBlockIdx := GetBlockAddress(chunkID, blockIdx)
	wordIdx := globalBlockIdx / 32
	if wordIdx >= len(bitmap) {
		return
	}
	bitIdx := uint(globalBlockIdx % 32)
	wordPtr := &bitmap[wordIdx]

	mosk := ^uint32(1 << bitIdx)
	for {
		old := wordPtr.Load()
		if wordPtr.CompareAndSwap(old, old&mosk) {
			break
		}
	}
}

// GetBlock returns the physical block info for a given chunk/block index.
func (b *Bus) GetBlock(chunkID, blockIdx int16) (Block, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	realCID := int(^chunkID)
	if realCID >= len(b.chunks) {
		return Block{}, fmt.Errorf("chunk %d not found", realCID)
	}

	chunk := b.chunks[realCID]
	offset := uint32(blockIdx) * BlockSize
	return Block{
		ChunkID:  chunkID,
		BlockIdx: blockIdx,
		Data:     chunk.Data[offset : offset+BlockSize],
	}, nil
}

// GetChunk returns a specific chunk.
func (b *Bus) GetChunk(id int16) *Chunk {
	realID := int(^id)
	b.mu.RLock()
	defer b.mu.RUnlock()
	if realID < 0 || realID >= len(b.chunks) {
		return nil
	}
	return b.chunks[realID]
}

// ChunkCount returns the number of chunks currently managed by the bus.
func (b *Bus) ChunkCount() int {
	return int(b.chunkCount.Load())
}

// AllocStripe creates a Stripe by allocating blocks and retrieving their memory.
func (b *Bus) AllocStripe(blockCount int) (*Stripe, error) {
	blocks, err := b.AllocBlocks(blockCount)
	if err != nil {
		return nil, err
	}

	stripe := &Stripe{
		Sequence:   make([]int16, 0),
		BlockCount: blockCount,
		Blocks:     make([][]byte, blockCount),
	}

	var chunkID int16
	for i, blk := range blocks {
		if blk.ChunkID != chunkID {
			stripe.Sequence = append(stripe.Sequence, blk.ChunkID)
			chunkID = blk.ChunkID
		}
		stripe.Sequence = append(stripe.Sequence, blk.BlockIdx)
		stripe.Blocks[i] = blk.Data
	}

	UpdateStripeHeader(stripe)
	return stripe, nil
}

// AllocBlocksForStripe allocates count blocks and appends them to the existing stripe.
func (b *Bus) AllocBlocksForStripe(s *Stripe, count int) error {
	blocks, err := b.AllocBlocks(count)
	if err != nil {
		return err
	}

	var lastChunkID int16
	for i := len(s.Sequence) - 1; i >= 0; i-- {
		if s.Sequence[i] < 0 {
			lastChunkID = s.Sequence[i]
			break
		}
	}
	for _, blk := range blocks {
		if blk.ChunkID != lastChunkID {
			s.Sequence = append(s.Sequence, blk.ChunkID)
			lastChunkID = blk.ChunkID
		}
		s.Sequence = append(s.Sequence, blk.BlockIdx)

		s.Blocks = append(s.Blocks, blk.Data)
		s.BlockCount++
	}

	UpdateStripeHeader(s)
	return nil
}

func (b *Bus) FreeStripe(s *Stripe) {
	var chunkID int16
	for _, id := range s.Sequence {
		if id < 0 {
			chunkID = id
			continue
		}
		b.FreeBlock(chunkID, id)
	}
}
