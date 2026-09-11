//go:build linux

package shm

const (
	// Design specs
	ChunkSize      = 4 * 1024 * 1024 // 4MB
	BlockSize      = 4 * 1024        // 4KB
	BlocksPerChunk = 1024

	MaxNodes      = 64
	MaxChunks     = 64
	QueueSize     = 510  // entries per queue
	MaxTotalSlots = 4096 // Total slots in registry

	// ControlStripeSize is the default size requested for the Control region.
	// 256 blocks = 1MB.
	ControlStripeSize = 256 * BlockSize

	StripeDataSizeLimit = 512 * BlockSize // 2MB of data per stripe (max 512 blocks)
)

// --- Control Region Offsets ---
// These are relative to the start of the Control Stripe
const (
	// Response Queue (starts at 4KB)
	OFF_RESP_QUEUE  = 4096
	RESP_QUEUE_SIZE = 4096 // Each node gets a 4KB queue region

	// Stripe Registry (Registry of Stripes)
	// 4096 slots * 32 bytes = 128KB.
	OFF_STRIPE_REGISTRY = 524288
)

// --- Queue Internal Offsets (Relative to Queue start) ---
const (
	Q_OFF_HEAD    = 0          // 4 bytes
	Q_OFF_TAIL    = 4          // 4 bytes
	Q_OFF_ENTRIES = 16         // Start of entry array (8 bytes per entry)
	Q_READY_BIT   = 0x80000000 // High bit of the Command field signals entry is ready
)

// Command constants
const (
	CMD_PROCESS        uint32 = 1
	CMD_RELEASE        uint32 = 2
	CMD_REQUEST_EXPAND uint32 = 3
	CMD_EXPAND_READY   uint32 = 4
	CMD_EXPAND_ERROR   uint32 = 5
)

// --- Stripe Registry Entry Offsets (The "Slot") ---
const (
	STRIPE_ENTRY_SIZE = 16
	STRIPE_OFF_STATUS = 0
	STRIPE_OFF_HEADER = 4 // ChunkID of the Header Block
	STRIPE_OFF_OWNER  = 8 // NodeID or HubID
)

const (
	StripeStatusIdle  = 0
	StripeStatusBusy  = 1
	StripeStatusReady = 2
	StripeStatusDone  = 3
)

// --- Header Block Structure ---
// Stored at the beginning of the first block of every Stripe.
const (
	HEADER_DATA_LEN_OFF  = 0 // 4 bytes: Total valid data length in the stripe
	HEADER_BLK_CNT_OFF   = 4 // 2 bytes: Number of blocks in the stripe
	HEADER_SEQ_LEN_OFF   = 6 // 2 bytes: Sequence Length (number of int16 entries)
	HEADER_SEQ_START_OFF = 8 // Start of []int16 sequence
)

// GetNodeReqQueueOffset returns the start of a specific node's request queue.
func GetNodeReqQueueOffset(nodeID int) uintptr {
	return uintptr(OFF_RESP_QUEUE + (nodeID * RESP_QUEUE_SIZE))
}

// GetStripeEntryOffset returns the start of a specific stripe registry entry.
func GetStripeEntryOffset(slotID uint32) uintptr {
	return uintptr(OFF_STRIPE_REGISTRY + (uintptr(slotID-1) * STRIPE_ENTRY_SIZE))
}
