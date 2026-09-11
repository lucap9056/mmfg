use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::future::Future;

use crate::layout;
use crate::shm::{Chunk, SharedMemory};
use crate::sync::Eventfd;
use crate::control::ControlRegion;
use crate::stripe::Stripe;
use crate::connection::{ShmConnection, Connection};
use crate::handshake::perform_handshake;
use crate::net;

use crate::error::{Result, MmfgError};

const MSG_NEW_CHUNK: u8 = 2;
const MSG_HEARTBEAT: u8 = 3;
const MSG_RELEASE: u8 = 4;

pub type Handler = Arc<dyn Fn(Box<dyn Connection>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub struct NodeState {
    pub chunks: Vec<Chunk>,
    pub waiters: HashMap<u32, oneshot::Sender<bool>>,
}

pub struct Node {
    pub node_id: usize,
    pub control: Arc<ControlRegion>,
    pub node_ev: Arc<Eventfd>,
    pub hub_ev: Arc<Eventfd>,
    pub handler: Handler,
    pub conn_fd: RawFd,
    pub state: Arc<Mutex<NodeState>>,
}

impl Node {
    pub async fn serve(listen_fd: RawFd, handler: Handler) -> Result<()> {
        let rt = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || -> Result<()> {
            loop {
                let conn_fd = match net::accept(listen_fd) {
                    Ok(fd) => fd,
                    Err(e) => {
                        eprintln!("accept failed: {}", e);
                        continue;
                    }
                };
                let handler = Arc::clone(&handler);
                rt.spawn(async move {
                    match Node::new(conn_fd, handler) {
                        Ok(node) => {
                            if let Err(e) = Arc::new(node).start_event_loop().await {
                                eprintln!("Node event loop error: {}", e);
                            }
                        }
                        Err(e) => eprintln!("Node handshake failed: {}", e),
                    }
                });
            }
        })
        .await
        .map_err(|e| MmfgError::Internal(format!("serve task panicked: {}", e)))?
    }

    pub async fn listen(socket_path: &str, handler: Handler) -> Result<()> {
        let listen_fd = net::listen_seqpacket(socket_path)?;
        Self::serve(listen_fd, handler).await
    }

    pub fn new(conn_fd: RawFd, handler: Handler) -> Result<Self> {
        let handshake = match perform_handshake(conn_fd) {
            Ok(hs) => hs,
            Err(e) => {
                unsafe { libc::close(conn_fd) };
                return Err(e);
            }
        };

        let mut chunks = Vec::with_capacity(handshake.chunk_fds.len());
        for &fd in &handshake.chunk_fds {
            match Chunk::attach(fd, layout::CHUNK_SIZE) {
                Ok(chunk) => chunks.push(chunk),
                Err(e) => {
                    unsafe {
                        libc::close(handshake.node_ev_fd);
                        libc::close(handshake.hub_ev_fd);
                        libc::close(conn_fd);
                    }
                    return Err(e.into());
                }
            }
        }

        if chunks.is_empty() {
            unsafe {
                libc::close(handshake.node_ev_fd);
                libc::close(handshake.hub_ev_fd);
                libc::close(conn_fd);
            }
            return Err(MmfgError::Protocol("handshake carried no chunk fds".to_string()));
        }

        let control_shm = SharedMemory::new(
            chunks[0].as_ptr() as *mut u8,
            layout::CONTROL_STRIPE_SIZE,
        );
        let control = Arc::new(ControlRegion::new(control_shm));

        let node_ev = Eventfd::attach(handshake.node_ev_fd);
        let hub_ev = Eventfd::attach(handshake.hub_ev_fd);

        let state = NodeState {
            chunks,
            waiters: HashMap::new(),
        };

        Ok(Self {
            node_id: handshake.node_id,
            control,
            node_ev: Arc::new(node_ev),
            hub_ev: Arc::new(hub_ev),
            handler,
            conn_fd,
            state: Arc::new(Mutex::new(state)),
        })
    }

    pub async fn start_event_loop(self: Arc<Self>) -> Result<()> {
        println!("Rust Node {} starting event loops...", self.node_id);

        let this_ev = Arc::clone(&self);
        let ev_handle = tokio::spawn(async move {
            loop {
                match this_ev.wait_node_ev().await {
                    Ok(_) => {
                        let q_off = layout::get_node_req_queue_offset(this_ev.node_id);
                        while let Some((slot_id, cmd)) = this_ev.control.pop(q_off) {
                            match cmd {
                                layout::CMD_PROCESS => {
                                    this_ev.handle_process(slot_id);
                                }
                                layout::CMD_RELEASE => {
                                    this_ev.control.free_slot(slot_id);
                                }
                                layout::CMD_EXPAND_READY => {
                                    let mut state = this_ev.state.lock();
                                    if let Some(tx) = state.waiters.remove(&slot_id) {
                                        let _ = tx.send(true);
                                    }
                                }
                                layout::CMD_EXPAND_ERROR => {
                                    let mut state = this_ev.state.lock();
                                    if let Some(tx) = state.waiters.remove(&slot_id) {
                                        let _ = tx.send(false);
                                    }
                                }
                                _ => {
                                    println!("Unhandled command: {}", cmd);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Eventfd wait error: {}", e);
                        break;
                    }
                }
            }
        });

        // recvmsg with ancillary FDs has no safe async equivalent in tokio.
        let this_msg = Arc::clone(&self);
        let result = tokio::task::spawn_blocking(move || this_msg.message_loop())
            .await
            .map_err(|e| MmfgError::Internal(format!("message loop panicked: {}", e)))?;

        ev_handle.abort();
        unsafe { libc::close(self.conn_fd) };
        result
    }

    async fn wait_node_ev(&self) -> Result<()> {
        let ev = Arc::clone(&self.node_ev);
        tokio::task::spawn_blocking(move || ev.wait())
            .await
            .map_err(|e| MmfgError::Internal(format!("Spawn blocking failed: {}", e)))?
            .map_err(|e| e.into())
    }

    fn message_loop(&self) -> Result<()> {
        let max_fds = layout::MAX_CHUNKS;
        loop {
            let mut header = [0u8; 1];
            let (n, fds) = match net::recv_msg_with_fds(self.conn_fd, &mut header, max_fds) {
                Ok(r) => r,
                Err(e) => {
                    println!("Node {}: connection closed: {}", self.node_id, e);
                    return Ok(());
                }
            };

            if n == 0 {
                println!("Node {}: connection closed", self.node_id);
                return Ok(());
            }

            match header[0] {
                MSG_NEW_CHUNK => {
                    let mut state = self.state.lock();
                    for fd in fds {
                        match Chunk::attach(fd, layout::CHUNK_SIZE) {
                            Ok(chunk) => state.chunks.push(chunk),
                            Err(e) => eprintln!("Node {}: failed to attach new chunk: {}", self.node_id, e),
                        }
                    }
                }
                MSG_HEARTBEAT | MSG_RELEASE => {
                    for fd in fds {
                        unsafe { libc::close(fd) };
                    }
                }
                other => {
                    println!("Node {}: unknown socket message: {}", self.node_id, other);
                    for fd in fds {
                        unsafe { libc::close(fd) };
                    }
                }
            }
        }
    }

    fn handle_process(&self, slot_id: u32) {
        let control = Arc::clone(&self.control);
        let hub_ev = Arc::clone(&self.hub_ev);
        let state = Arc::clone(&self.state);
        let handler = Arc::clone(&self.handler);

        tokio::spawn(async move {
            let stripe = Stripe::new(slot_id, control.clone(), state);
            let conn = ShmConnection::new(stripe, hub_ev.fd());

            (handler)(Box::new(conn)).await;

            control.set_stripe_status(slot_id, layout::STRIPE_STATUS_DONE);
            let hub_resp_q_off = layout::OFF_RESP_QUEUE;
            if !control.push(hub_resp_q_off, slot_id, layout::CMD_PROCESS) {
                eprintln!("Node: FAILED to push slot {} to hub response queue (queue full)", slot_id);
                return;
            }
            if let Err(e) = hub_ev.notify() {
                eprintln!("Failed to notify hub: {}", e);
            }
        });
    }
}
