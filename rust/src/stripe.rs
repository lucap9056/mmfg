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

    fn resolve_segments(&self, seq: &[i16], start_off_in_stripe: usize, want: usize) -> Vec<(*mut u8, usize)> {
        if seq.is_empty() || want == 0 {
            return Vec::new();
        }

        let mut off_in_stripe = start_off_in_stripe;
        let mut remaining = want;
        let mut out = Vec::new();

        let mut state = self.state.lock();
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
            if let Some(chunk) = state.chunks.get_mut(chunk_idx) {
                let take = std::cmp::min(remaining, layout::BLOCK_SIZE - off_in_stripe);
                unsafe {
                    let ptr = chunk.as_mut_ptr().add((val as usize * layout::BLOCK_SIZE) + off_in_stripe);
                    out.push((ptr, take));
                }
                remaining -= take;
                off_in_stripe = 0;
            }

            if remaining == 0 { break; }
        }

        out
    }

    pub fn read_data(&mut self, offset: usize, buffer: &mut [u8]) -> usize {
        let _ = self.refresh_sequence();
        let seq = Arc::clone(&self.sequence);
        let segments = self.resolve_segments(&seq, offset + layout::BLOCK_SIZE, buffer.len());

        let mut buf_ptr = buffer.as_mut_ptr();
        let mut copied = 0usize;
        for (src, len) in segments {
            unsafe {
                std::ptr::copy_nonoverlapping(src as *const u8, buf_ptr, len);
                buf_ptr = buf_ptr.add(len);
            }
            copied += len;
        }
        copied
    }

    pub fn view<F: FnMut(&mut Viewer) -> Result<()>>(&mut self, offset: usize, length: usize, mut call: F) -> Result<()> {
        let _ = self.refresh_sequence();
        let seq = Arc::clone(&self.sequence);

        if seq.is_empty() || offset >= self.data_len as usize {
            return Err(MmfgError::Shm("view offset out of bounds".to_string()));
        }

        let end = std::cmp::min((offset + length) as u32, self.data_len) as usize;
        if end <= offset {
            return Err(MmfgError::Shm("empty view range".to_string()));
        }
        let actual_len = end - offset;

        let raw = self.resolve_segments(&seq, offset + layout::BLOCK_SIZE, actual_len);
        if raw.is_empty() {
            return Err(MmfgError::Shm("view produced no segments".to_string()));
        }

        let segments: Vec<&mut [u8]> = raw
            .into_iter()
            .map(|(ptr, len)| unsafe { std::slice::from_raw_parts_mut(ptr, len) })
            .collect();

        let mut offsets = Vec::with_capacity(segments.len());
        let mut curr = 0usize;
        for seg in &segments {
            offsets.push(curr);
            curr += seg.len();
        }

        let mut viewer = Viewer { segments, offsets, length: actual_len };
        call(&mut viewer)
    }

    pub fn write_data(&mut self, offset: usize, buffer: &[u8]) -> usize {
        let _ = self.refresh_sequence();
        let seq = Arc::clone(&self.sequence);
        let segments = self.resolve_segments(&seq, offset + layout::BLOCK_SIZE, buffer.len());

        let mut buf_ptr = buffer.as_ptr();
        let mut copied = 0usize;
        for (dest, len) in segments {
            unsafe {
                std::ptr::copy_nonoverlapping(buf_ptr, dest, len);
                buf_ptr = buf_ptr.add(len);
            }
            copied += len;
        }
        copied
    }
}

pub struct Viewer<'a> {
    pub segments: Vec<&'a mut [u8]>,
    offsets: Vec<usize>,
    pub length: usize,
}

impl<'a> Viewer<'a> {
    fn find_segment(&self, offset: usize) -> (usize, usize) {
        let idx = match self.offsets.binary_search(&offset) {
            Ok(idx) => idx,
            Err(idx) => idx - 1,
        };
        (idx, offset - self.offsets[idx])
    }

    pub fn byte_at(&self, offset: usize) -> Result<u8> {
        if offset >= self.length {
            return Err(MmfgError::Shm("view offset out of bounds".to_string()));
        }
        let (idx, inner) = self.find_segment(offset);
        Ok(self.segments[idx][inner])
    }

    pub fn set_byte_at(&mut self, offset: usize, val: u8) -> Result<()> {
        if offset >= self.length {
            return Err(MmfgError::Shm("view offset out of bounds".to_string()));
        }
        let (idx, inner) = self.find_segment(offset);
        self.segments[idx][inner] = val;
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = u8> + '_ {
        self.segments.iter().flat_map(|seg| seg.iter().copied())
    }

    pub fn compare(&self, data: &[u8]) -> bool {
        if data.len() != self.length {
            return false;
        }
        let mut pos = 0;
        for seg in &self.segments {
            if &data[pos..pos + seg.len()] != *seg {
                return false;
            }
            pos += seg.len();
        }
        true
    }

    pub fn index(&self, target: &[u8]) -> Option<usize> {
        if target.is_empty() {
            return Some(0);
        }
        let target_len = target.len();
        let first_byte = target[0];

        for (i, seg) in self.segments.iter().enumerate() {
            let curr_off = self.offsets[i];
            let seg_len = seg.len();
            let mut start_idx = 0;

            while start_idx < seg_len {
                let rel = match seg[start_idx..].iter().position(|&b| b == first_byte) {
                    Some(p) => p,
                    None => break,
                };
                let abs_match_pos = start_idx + rel;
                start_idx = abs_match_pos + 1;

                if abs_match_pos + target_len <= seg_len {
                    if &seg[abs_match_pos..abs_match_pos + target_len] == target {
                        return Some(curr_off + abs_match_pos);
                    }
                } else if self.match_across_segments(i, abs_match_pos, target) {
                    return Some(curr_off + abs_match_pos);
                }
            }
        }

        None
    }

    fn match_across_segments(&self, seg_idx: usize, pos_in_seg: usize, target: &[u8]) -> bool {
        let mut curr_seg_idx = seg_idx;
        let mut curr_off = pos_in_seg;

        for &want in target {
            if curr_off >= self.segments[curr_seg_idx].len() {
                curr_seg_idx += 1;
                curr_off = 0;
                if curr_seg_idx >= self.segments.len() {
                    return false;
                }
            }
            if self.segments[curr_seg_idx][curr_off] != want {
                return false;
            }
            curr_off += 1;
        }
        true
    }
}

#[cfg(test)]
mod viewer_tests {
    use super::Viewer;

    fn build_viewer(bufs: &mut [Vec<u8>]) -> Viewer<'_> {
        let mut offsets = Vec::with_capacity(bufs.len());
        let mut curr = 0usize;
        for b in bufs.iter() {
            offsets.push(curr);
            curr += b.len();
        }
        let length = curr;
        let segments = bufs.iter_mut().map(|b| b.as_mut_slice()).collect();
        Viewer { segments, offsets, length }
    }

    #[test]
    fn byte_at_and_set_byte_at_cross_segment() {
        let mut data = vec![vec![1u8, 2, 3], vec![4u8, 5, 6, 7]];
        let mut v = build_viewer(&mut data);

        assert_eq!(v.length, 7);
        assert_eq!(v.byte_at(0).unwrap(), 1);
        assert_eq!(v.byte_at(2).unwrap(), 3);
        assert_eq!(v.byte_at(3).unwrap(), 4);
        assert_eq!(v.byte_at(6).unwrap(), 7);
        assert!(v.byte_at(7).is_err());

        v.set_byte_at(3, 0xFF).unwrap();
        assert_eq!(v.byte_at(3).unwrap(), 0xFF);
    }

    #[test]
    fn compare_and_iter() {
        let expected: Vec<u8> = (0u8..20).collect();
        let mut data = vec![expected[0..7].to_vec(), expected[7..20].to_vec()];
        let v = build_viewer(&mut data);

        assert!(v.compare(&expected));
        assert!(!v.compare(&expected[..5]));

        let collected: Vec<u8> = v.iter().collect();
        assert_eq!(collected, expected);
    }

    #[test]
    fn index_spans_segment_boundary() {
        let mut data = vec![b"ABCDE".to_vec(), b"FGHIJ".to_vec(), b"KLMNO".to_vec()];
        let v = build_viewer(&mut data);

        assert_eq!(v.index(b"CDEFG"), Some(2));
        assert_eq!(v.index(b"KLMNO"), Some(10));
        assert_eq!(v.index(b"NOTFOUND"), None);
        assert_eq!(v.index(b""), Some(0));
    }
}
