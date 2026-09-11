// Constants defined in go/shm/layout.go
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4MB
pub const BLOCK_SIZE: usize = 4 * 1024;        // 4KB
pub const BLOCKS_PER_CHUNK: usize = 1024;

pub const MAX_NODES: usize = 64;
pub const MAX_CHUNKS: usize = 64;
pub const QUEUE_SIZE: usize = 510;
pub const MAX_TOTAL_SLOTS: usize = 4096;

pub const CONTROL_STRIPE_SIZE: usize = 256 * BLOCK_SIZE;
pub const STRIPE_DATA_SIZE_LIMIT: usize = 512 * BLOCK_SIZE;

// --- Control Region Offsets ---
pub const OFF_RESP_QUEUE: usize = 4096;
pub const RESP_QUEUE_SIZE: usize = 4096;

pub const OFF_STRIPE_REGISTRY: usize = 524288;

// --- Queue Internal Offsets ---
pub const Q_OFF_HEAD: usize = 0;
pub const Q_OFF_TAIL: usize = 4;
pub const Q_OFF_ENTRIES: usize = 16;
pub const Q_READY_BIT: u32 = 0x80000000;

// Command constants
pub const CMD_PROCESS: u32 = 1;
pub const CMD_RELEASE: u32 = 2;
pub const CMD_REQUEST_EXPAND: u32 = 3;
pub const CMD_EXPAND_READY: u32 = 4;
pub const CMD_EXPAND_ERROR: u32 = 5;

// --- Stripe Registry Entry Offsets ---
pub const STRIPE_ENTRY_SIZE: usize = 16;
pub const STRIPE_OFF_STATUS: usize = 0;
pub const STRIPE_OFF_HEADER: usize = 4;
pub const STRIPE_OFF_OWNER: usize = 8;

pub const STRIPE_STATUS_IDLE: u32 = 0;
pub const STRIPE_STATUS_BUSY: u32 = 1;
pub const STRIPE_STATUS_READY: u32 = 2;
pub const STRIPE_STATUS_DONE: u32 = 3;

// --- Header Block Structure ---
pub const HEADER_DATA_LEN_OFF: usize = 0;
pub const HEADER_BLK_CNT_OFF: usize = 4;
pub const HEADER_SEQ_LEN_OFF: usize = 6;
pub const HEADER_SEQ_START_OFF: usize = 8;

pub fn get_node_req_queue_offset(node_id: usize) -> usize {
    OFF_RESP_QUEUE + (node_id * RESP_QUEUE_SIZE)
}

pub fn get_stripe_entry_offset(slot_id: u32) -> usize {
    OFF_STRIPE_REGISTRY + ((slot_id as usize - 1) * STRIPE_ENTRY_SIZE)
}

#[cfg(test)]
#[path = "layout_test.rs"]
mod layout_test;
