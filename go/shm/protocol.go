//go:build linux

package shm

import (
	"encoding/binary"
)

func UpdateStripeHeader(stripe *Stripe) {
	headerData := stripe.Blocks[0]
	binary.LittleEndian.PutUint32(headerData[HEADER_DATA_LEN_OFF:], stripe.DataLen)
	binary.LittleEndian.PutUint16(headerData[HEADER_BLK_CNT_OFF:], uint16(stripe.BlockCount))
	binary.LittleEndian.PutUint16(headerData[HEADER_SEQ_LEN_OFF:], uint16(len(stripe.Sequence)))
	for i, val := range stripe.Sequence {
		off := HEADER_SEQ_START_OFF + i*2
		if off+2 > len(headerData) {
			break // Safety: should not happen with range encoding
		}
		binary.LittleEndian.PutUint16(headerData[off:], uint16(val))
	}
}

// DecodeSequence parses the resource sequence from a Header Block data.
func DecodeHeader(headerData []byte) (uint32, int, []int16) {
	dataLen := binary.LittleEndian.Uint32(headerData[HEADER_DATA_LEN_OFF:])
	blkCnt := int(binary.LittleEndian.Uint16(headerData[HEADER_BLK_CNT_OFF:]))
	seqLen := int(binary.LittleEndian.Uint16(headerData[HEADER_SEQ_LEN_OFF:]))
	seq := make([]int16, seqLen)

	for i := 0; i < seqLen; i++ {
		off := HEADER_SEQ_START_OFF + i*2
		if off+2 > len(headerData) {
			break
		}
		seq[i] = int16(binary.LittleEndian.Uint16(headerData[off:]))
	}
	return dataLen, blkCnt, seq
}

// GetBlockAddress returns the global bit index for a block.
func GetBlockAddress(chunkID, blockIdx int16) int {
	readCID := ^chunkID
	return int(readCID)*BlocksPerChunk + int(blockIdx)
}

// ParseBlockAddress returns the chunk ID and block index from a global bit index.
func ParseBlockAddress(address uint64) (chunkID, blockIdx int16) {
	return ^int16(address / BlocksPerChunk), int16(address % BlocksPerChunk)
}
