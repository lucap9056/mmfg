use crate::layout;
use crate::shm::SharedMemory;
use std::sync::atomic::Ordering;

pub struct ControlRegion {
    shm: SharedMemory,
}

impl ControlRegion {
    pub fn new(shm: SharedMemory) -> Self {
        Self { shm }
    }

    pub fn read_u32(&self, offset: usize) -> u32 {
        // Stripe registry meta can use Acquire to ensure we see the latest updates
        self.shm.atomic_read_u32(offset, Ordering::Acquire)
    }

    pub fn get_node_req_queue_head(&self, node_id: usize) -> u32 {
        let offset = layout::get_node_req_queue_offset(node_id) + layout::Q_OFF_HEAD;
        self.shm.atomic_read_u32(offset, Ordering::Acquire)
    }

    pub fn get_node_req_queue_tail(&self, node_id: usize) -> u32 {
        let offset = layout::get_node_req_queue_offset(node_id) + layout::Q_OFF_TAIL;
        self.shm.atomic_read_u32(offset, Ordering::Acquire)
    }

    pub fn pop(&self, q_off: usize) -> Option<(u32, u32)> {
        let head = self.shm.atomic_read_u32(q_off + layout::Q_OFF_HEAD, Ordering::Acquire);
        let tail = self.shm.atomic_read_u32(q_off + layout::Q_OFF_TAIL, Ordering::Acquire);

        if head == tail {
            return None;
        }

        let entry_idx = (head % layout::QUEUE_SIZE as u32) as usize;
        let entry_off = q_off + layout::Q_OFF_ENTRIES + (entry_idx * 8);

        let mut spin = 0;
        let packed = loop {
            let val = self.shm.atomic_read_u64(entry_off, Ordering::Acquire);

            if (val >> 32) & (layout::Q_READY_BIT as u64) != 0 {
                break val;
            }

            spin += 1;
            if spin > 1000000 {
                eprintln!("CRITICAL: Timeout waiting for Q_READY_BIT at head {}", head);
                return None;
            }
            std::hint::spin_loop();
        };

        let cmd_with_bit = (packed >> 32) as u32;
        let slot_id = (packed & 0xFFFFFFFF) as u32;
        let cmd = cmd_with_bit & !layout::Q_READY_BIT;

        // Clear entry
        self.shm.atomic_write_u64(entry_off, 0, Ordering::Release);

        self.shm.atomic_write_u32(q_off + layout::Q_OFF_HEAD, head.wrapping_add(1), Ordering::Release);

        Some((slot_id, cmd))
    }

    pub fn set_stripe_status(&self, slot_id: u32, status: u32) {
        let base = layout::get_stripe_entry_offset(slot_id);
        self.shm.atomic_write_u32(base + layout::STRIPE_OFF_STATUS, status, Ordering::Release);
    }

    pub fn free_slot(&self, slot_id: u32) {
        self.set_stripe_status(slot_id, layout::STRIPE_STATUS_IDLE);
    }

    pub fn push(&self, q_off: usize, slot_id: u32, cmd: u32) -> bool {
        let mut spin = 0u32;
        loop {
            let head = self.shm.atomic_read_u32(q_off + layout::Q_OFF_HEAD, Ordering::Acquire);
            let tail = self.shm.atomic_read_u32(q_off + layout::Q_OFF_TAIL, Ordering::Acquire);

            if tail.wrapping_sub(head) >= layout::QUEUE_SIZE as u32 - 1 {
                spin += 1;
                if spin > 1_000_000 {
                    return false; // Full
                }
                std::hint::spin_loop();
                continue;
            }

            let next_tail = tail.wrapping_add(1);
            if self.shm.atomic_cas_u32(q_off + layout::Q_OFF_TAIL, tail, next_tail, Ordering::AcqRel) {
                let entry_idx = (tail % layout::QUEUE_SIZE as u32) as usize;
                let entry_off = q_off + layout::Q_OFF_ENTRIES + (entry_idx * 8);

                let packed = (u64::from(cmd | layout::Q_READY_BIT) << 32) | u64::from(slot_id);
                self.shm.atomic_write_u64(entry_off, packed, Ordering::Release);
                return true;
            }
        }
    }

    pub fn get_stripe_header(&self, slot_id: u32) -> (i16, i16) {
        let base = layout::get_stripe_entry_offset(slot_id);
        let val = self.read_u32(base + layout::STRIPE_OFF_HEADER);
        let cid = (val >> 16) as i16;
        let off = (val & 0xFFFF) as i16;
        (cid, off)
    }
}
