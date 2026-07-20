use std::sync::Arc;
use parking_lot::Mutex;
use std::os::unix::io::RawFd;
use crate::layout;
use crate::sync::Eventfd;
use crate::control::ControlRegion;
use crate::node::NodeState;
use crate::error::{Result, MmfgError};
use tokio::sync::oneshot;
use std::sync::atomic::Ordering;

pub struct Stripe {
    pub slot_id: u32,
    pub sequence: Arc<Vec<i16>>,
    pub block_count: usize,
    pub data_len: u32,
    control: Arc<ControlRegion>,
    state: Arc<Mutex<NodeState>>,
}

impl Stripe {
    pub fn new(slot_id: u32, control: Arc<ControlRegion>, state: Arc<Mutex<NodeState>>) -> Self {
        Self { 
            slot_id, 
            sequence: Arc::new(Vec::new()),
            block_count: 0,
            data_len: 0,
            control, 
            state,
        }
    }

    pub fn register_waiter(&self, tx: oneshot::Sender<bool>) -> Result<()> {
        let mut state = self.state.lock();
        state.waiters.insert(self.slot_id, tx);
        Ok(())
    }

    pub fn update_data_len(&self, data_len: u32) {
        let (cid, off) = self.control.get_stripe_header(self.slot_id);
        let state = self.state.lock();
        if let Some(chunk) = state.chunks.get(!cid as usize) {
            let base = (off as usize) * layout::BLOCK_SIZE;
            unsafe {
                let ptr = chunk.as_ptr().add(base + layout::HEADER_DATA_LEN_OFF) as *const std::sync::atomic::AtomicU32;
                (*ptr).store(data_len, Ordering::Release);
            }
        }
    }

    pub fn request_expand_hub(&self, hub_ev_fd: RawFd) -> Result<()> {
        let hub_req_q_off = layout::OFF_RESP_QUEUE; // Hub is at offset OFF_RESP_QUEUE (node 0)
        if !self.control.push(hub_req_q_off, self.slot_id, layout::CMD_REQUEST_EXPAND) {
            return Err(MmfgError::Expansion("Failed to push expansion request to Hub".to_string()));
        }
        
        // Notify Hub
        let hub_ev = Eventfd::attach(hub_ev_fd);
        hub_ev.notify()?;
        std::mem::forget(hub_ev); // Keep FD open
        Ok(())
    }

    pub fn get_meta(&self) -> (u32, usize) {
        let (cid, off) = self.control.get_stripe_header(self.slot_id);
        let state = self.state.lock();
        if let Some(chunk) = state.chunks.get(!cid as usize) {
            let base = (off as usize) * layout::BLOCK_SIZE;
            unsafe {
                let data_len_ptr = chunk.as_ptr().add(base + layout::HEADER_DATA_LEN_OFF) as *const std::sync::atomic::AtomicU32;
                let blk_cnt_ptr = chunk.as_ptr().add(base + layout::HEADER_BLK_CNT_OFF) as *const std::sync::atomic::AtomicU16;
                
                let data_len = (*data_len_ptr).load(Ordering::Acquire);
                let blk_cnt = (*blk_cnt_ptr).load(Ordering::Acquire) as usize;
                return (data_len, blk_cnt);
            }
        }
        (0, 0)
    }

    /// Decodes the block sequence from the header block.
    pub fn refresh_sequence(&mut self) -> Result<()> {
        let (cid, off) = self.control.get_stripe_header(self.slot_id);
        let state = self.state.lock();
        
        if let Some(chunk) = state.chunks.get(!cid as usize) {
            let base = (off as usize) * layout::BLOCK_SIZE;
            unsafe {
                let data_len_ptr = chunk.as_ptr().add(base + layout::HEADER_DATA_LEN_OFF) as *const std::sync::atomic::AtomicU32;
                let blk_cnt_ptr = chunk.as_ptr().add(base + layout::HEADER_BLK_CNT_OFF) as *const std::sync::atomic::AtomicU16;
                let seq_len_ptr = chunk.as_ptr().add(base + layout::HEADER_SEQ_LEN_OFF) as *const std::sync::atomic::AtomicU16;
                
                self.data_len = (*data_len_ptr).load(Ordering::Acquire);
                self.block_count = (*blk_cnt_ptr).load(Ordering::Acquire) as usize;
                let seq_len = (*seq_len_ptr).load(Ordering::Acquire) as usize;
                
                let mut seq = Vec::with_capacity(seq_len);
                for i in 0..seq_len {
                    let ptr = chunk.as_ptr().add(base + layout::HEADER_SEQ_START_OFF + i * 2) as *const std::sync::atomic::AtomicI16;
                    seq.push((*ptr).load(Ordering::Acquire));
                }
                self.sequence = Arc::new(seq);
            }
            Ok(())
        } else {
            Err(MmfgError::Shm(format!("Header chunk {} not found", !cid)))
        }
    }

    pub fn read_data(&mut self, offset: usize, buffer: &mut [u8]) -> usize {
        let _ = self.refresh_sequence();
        let seq = Arc::clone(&self.sequence);
        if seq.is_empty() { return 0; }

        let mut remaining = buffer.len();
        let mut buf_ptr = buffer.as_mut_ptr();
        
        let mut off_in_stripe = offset + layout::BLOCK_SIZE; // Skip Header Block
        let state = self.state.lock();

        let mut current_cid = -1i16;
        for &val in seq.iter() {
            if val < 0 {
                current_cid = val;
                continue;
            }
            
            // It's a block index
            if off_in_stripe >= layout::BLOCK_SIZE {
                off_in_stripe -= layout::BLOCK_SIZE;
                continue;
            }

            // This block contains part of our data
            let chunk_idx = !current_cid as usize;
            if let Some(chunk) = state.chunks.get(chunk_idx) {
                let take = std::cmp::min(remaining, layout::BLOCK_SIZE - off_in_stripe);
                unsafe {
                    let src = chunk.as_ptr().add((val as usize * layout::BLOCK_SIZE) + off_in_stripe);
                    std::ptr::copy_nonoverlapping(src, buf_ptr, take);
                    buf_ptr = buf_ptr.add(take);
                }
                remaining -= take;
                off_in_stripe = 0;
            }

            if remaining == 0 { break; }
        }

        buffer.len() - remaining
    }

    pub fn write_data(&mut self, offset: usize, buffer: &[u8]) -> usize {
        let _ = self.refresh_sequence();
        let seq = Arc::clone(&self.sequence);
        if seq.is_empty() { return 0; }

        let mut remaining = buffer.len();
        let mut buf_ptr = buffer.as_ptr();
        
        let mut off_in_stripe = offset + layout::BLOCK_SIZE; // Skip Header Block
        let state = self.state.lock();

        let mut current_cid = -1i16;
        for &val in seq.iter() {
            if val < 0 {
                current_cid = val;
                continue;
            }
            
            if off_in_stripe >= layout::BLOCK_SIZE {
                off_in_stripe -= layout::BLOCK_SIZE;
                continue;
            }

            let chunk_idx = !current_cid as usize;
            if let Some(chunk) = state.chunks.get(chunk_idx) {
                let take = std::cmp::min(remaining, layout::BLOCK_SIZE - off_in_stripe);
                unsafe {
                    let dest = chunk.as_ptr().add((val as usize * layout::BLOCK_SIZE) + off_in_stripe) as *mut u8;
                    std::ptr::copy_nonoverlapping(buf_ptr, dest, take);
                    buf_ptr = buf_ptr.add(take);
                }
                remaining -= take;
                off_in_stripe = 0;
            }

            if remaining == 0 { break; }
        }

        buffer.len() - remaining
    }
}
