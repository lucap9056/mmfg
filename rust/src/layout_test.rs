#[cfg(test)]
mod tests {
    use crate::layout;

    #[test]
    fn test_layout_constants() {
        assert_eq!(layout::BLOCK_SIZE, 4096);
    }

    #[test]
    fn test_offsets() {
        let node_id = 0;
        let offset = layout::get_node_req_queue_offset(node_id);
        assert_eq!(offset, 4096);
        
        let slot_id = 1;
        let stripe_off = layout::get_stripe_entry_offset(slot_id);
        assert_eq!(stripe_off, 524288);

        let slot_id = 2;
        let stripe_off = layout::get_stripe_entry_offset(slot_id);
        assert_eq!(stripe_off, 524288 + layout::STRIPE_ENTRY_SIZE);
    }
}
