use std::io;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio::net::UnixStream;
use tokio::io::AsyncReadExt;
use std::os::unix::io::FromRawFd;
use std::pin::Pin;
use std::future::Future;

use crate::layout;
use crate::shm::{Chunk, SharedMemory};
use crate::sync::Eventfd;
use crate::control::ControlRegion;
use crate::stripe::Stripe;
use crate::connection::{ShmConnection, Connection};
use crate::handshake::perform_handshake;

use crate::error::Result;

pub type Handler = Arc<dyn Fn(Box<dyn Connection>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub struct NodeState {
    pub chunks: Vec<Chunk>,
    pub waiters: HashMap<u32, oneshot::Sender<()>>,
}

pub struct Node {
    pub node_id: usize,
    pub control: Arc<ControlRegion>,
    pub node_ev: Arc<Eventfd>,
    pub hub_ev: Arc<Eventfd>,
    pub handler: Handler,
    pub stream: tokio::sync::Mutex<UnixStream>,
    pub state: Arc<Mutex<NodeState>>,
}

impl Node {
    /// Accepts from listener and spawns a task per connection into `Node::new`.
    /// For a listener shared with other protocols, demux externally and call
    /// `Node::new` directly instead.
    pub async fn serve(listener: std::os::unix::net::UnixListener, handler: Handler) -> Result<()> {
        let rt = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || -> Result<()> {
            for stream in listener.incoming() {
                let stream = stream?;
                let handler = Arc::clone(&handler);
                rt.spawn(async move {
                    match Node::new(stream, handler) {
                        Ok(node) => {
                            if let Err(e) = Arc::new(node).start_event_loop().await {
                                eprintln!("Node event loop error: {}", e);
                            }
                        }
                        Err(e) => eprintln!("Node handshake failed: {}", e),
                    }
                });
            }
            Ok(())
        })
        .await
        .map_err(|e| crate::error::MmfgError::Internal(format!("serve task panicked: {}", e)))?
    }

    /// Binds socket_path and serves it. Convenience wrapper around `serve`.
    pub async fn listen(socket_path: &str, handler: Handler) -> Result<()> {
        if std::fs::metadata(socket_path).is_ok() {
            std::fs::remove_file(socket_path)?;
        }
        let listener = std::os::unix::net::UnixListener::bind(socket_path)?;
        Self::serve(listener, handler).await
    }

    /// Runs the Hub handshake over an accepted stream and builds a `Node`.
    /// Callers are responsible for routing only mmfg traffic here.
    pub fn new(stream: std::os::unix::net::UnixStream, handler: Handler) -> Result<Self> {
        let mut stream_sync = stream;
        let handshake = perform_handshake(&mut stream_sync)?;
        
        let chunk0 = Chunk::attach(handshake.fds[0], layout::CHUNK_SIZE)?;
        let node_ev = Eventfd::attach(handshake.fds[1]);
        let hub_ev = Eventfd::attach(handshake.fds[2]);

        let control_shm = SharedMemory::new(
            chunk0.as_ptr() as *mut u8,
            layout::CONTROL_STRIPE_SIZE,
        );
        let control = Arc::new(ControlRegion::new(control_shm));

        let state = NodeState {
            chunks: vec![chunk0],
            waiters: HashMap::new(),
        };

        // Convert sync stream to tokio stream
        stream_sync.set_nonblocking(true)?;
        let stream = UnixStream::from_std(stream_sync)?;

        Ok(Self {
            node_id: handshake.node_id,
            control,
            node_ev: Arc::new(node_ev),
            hub_ev: Arc::new(hub_ev),
            handler,
            stream: tokio::sync::Mutex::new(stream),
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
                                        let _ = tx.send(());
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

        let mut stream = self.stream.lock().await;
        loop {
            let mut msg_header = [0u8; 1];
            match stream.read_exact(&mut msg_header).await {
                Ok(_) => {
                    if !self.handle_socket_msg(msg_header[0], &mut stream).await? {
                        break;
                    }
                }
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    }
                    return Err(e.into());
                }
            }
        }

        ev_handle.abort();
        Ok(())
    }

    async fn wait_node_ev(&self) -> Result<()> {
        let ev = Arc::clone(&self.node_ev);
        tokio::task::spawn_blocking(move || ev.wait())
            .await
            .map_err(|e| crate::error::MmfgError::Internal(format!("Spawn blocking failed: {}", e)))?
            .map_err(|e| e.into())
    }

    async fn handle_socket_msg(&self, msg_type: u8, stream: &mut UnixStream) -> Result<bool> {
        match msg_type {
            2 => { // MsgNewChunk
                use std::os::unix::io::AsRawFd;
                let fd = stream.as_raw_fd();
                
                let temp_stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
                let res = crate::net::receive_fds(&temp_stream, 1);
                std::mem::forget(temp_stream);
                
                let fds = res?;
                let chunk = Chunk::attach(fds[0], layout::CHUNK_SIZE)?;
                let mut state = self.state.lock();
                state.chunks.push(chunk);
            }
            3 => { // Heartbeat
                // Ignore
            }
            5 => { // MsgTakeover
                let mut id_buf = [0u8; 4];
                stream.read_exact(&mut id_buf).await?;
                let slot_id = u32::from_le_bytes(id_buf);
                self.handle_process(slot_id);
            }
            _ => {
                println!("Unknown socket message: {}", msg_type);
            }
        }
        Ok(true)
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
            control.push(hub_resp_q_off, slot_id, layout::CMD_PROCESS);
            if let Err(e) = hub_ev.notify() {
                eprintln!("Failed to notify hub: {}", e);
            }
        });
    }
}
