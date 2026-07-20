use std::os::unix::io::RawFd;
use crate::net::recv_msg_with_fds;
use crate::layout;
use crate::error::{Result, MmfgError};

pub const CONN_HEADER: &[u8; 4] = b"MMFG";
pub const CONN_VERSION: u8 = 2;
const MSG_HANDSHAKE: u8 = 1;

const MAX_HANDSHAKE_FDS: usize = 2 + layout::MAX_CHUNKS;

pub struct HandshakeResult {
    pub node_id: usize,
    pub node_ev_fd: RawFd,
    pub hub_ev_fd: RawFd,
    pub chunk_fds: Vec<RawFd>,
}

fn close_all(fds: &[RawFd]) {
    for &fd in fds {
        unsafe { libc::close(fd) };
    }
}

pub fn perform_handshake(fd: RawFd) -> Result<HandshakeResult> {
    let mut header = [0u8; 7];
    let (n, fds) = recv_msg_with_fds(fd, &mut header, MAX_HANDSHAKE_FDS)?;

    if n != 7 {
        close_all(&fds);
        return Err(MmfgError::Protocol(format!("short header: got {} bytes, want 7", n)));
    }

    if &header[0..4] != CONN_HEADER {
        close_all(&fds);
        return Err(MmfgError::Protocol(format!("invalid handshake header: {:?}", &header[0..4])));
    }

    if header[4] != CONN_VERSION {
        close_all(&fds);
        return Err(MmfgError::Protocol(format!("unsupported protocol version: {}", header[4])));
    }

    if header[5] != MSG_HANDSHAKE {
        close_all(&fds);
        return Err(MmfgError::Protocol(format!("unexpected message type in handshake: {}", header[5])));
    }

    let node_id = header[6] as usize;
    if node_id == 0 || node_id >= layout::MAX_NODES {
        close_all(&fds);
        return Err(MmfgError::Protocol(format!("invalid nodeID {}", node_id)));
    }

    if fds.len() < 3 {
        close_all(&fds);
        return Err(MmfgError::Protocol(format!("expected at least 3 fds, got {}", fds.len())));
    }

    let node_ev_fd = fds[0];
    let hub_ev_fd = fds[1];
    let chunk_fds = fds[2..].to_vec();

    Ok(HandshakeResult { node_id, node_ev_fd, hub_ev_fd, chunk_fds })
}
