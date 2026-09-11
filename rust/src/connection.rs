use std::io::{self, Read, Write};
use std::os::unix::io::RawFd;
use tokio::sync::oneshot;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::future::Future;
use crate::stripe::{Stripe, Viewer};
use crate::layout;
use crate::error::{Result, MmfgError};

pub trait Connection: AsyncRead + AsyncWrite + Send + Unpin {
    fn data_len(&self) -> u32;
    fn request_expand(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
    fn view(&mut self, offset: usize, length: usize, call: &mut dyn FnMut(&mut Viewer) -> Result<()>) -> Result<()>;
}

pub struct ShmConnection {
    pub stripe: Stripe,
    pub read_pos: usize,
    pub write_pos: usize,
    pub hub_ev_fd: RawFd,
    expansion_rx: Option<oneshot::Receiver<bool>>,
}

impl ShmConnection {
    pub fn new(stripe: Stripe, hub_ev_fd: RawFd) -> Self {
        Self {
            stripe,
            read_pos: 0,
            write_pos: 0,
            hub_ev_fd,
            expansion_rx: None,
        }
    }
}

impl AsyncRead for ShmConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let (data_len, _) = self.stripe.get_meta();
        if self.read_pos >= data_len as usize {
            return Poll::Ready(Ok(()));
        }
        let remaining = data_len as usize - self.read_pos;
        let n = std::cmp::min(remaining, buf.remaining());
        if n == 0 {
            return Poll::Ready(Ok(()));
        }
        
        let read_pos = self.read_pos;
        
        // Safety: We ensure we only write to initialized or soon-to-be-initialized memory
        let read = unsafe {
            let unfilled = buf.unfilled_mut();
            let slice = std::slice::from_raw_parts_mut(unfilled.as_mut_ptr() as *mut u8, n);
            let r = self.stripe.read_data(read_pos, slice);
            buf.assume_init(r);
            r
        };
        
        buf.advance(read);
        self.read_pos += read;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ShmConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(mut rx) = self.expansion_rx.take() {
            match Pin::new(&mut rx).poll(cx) {
                Poll::Ready(Ok(true)) => {
                    // Success, continue write
                }
                Poll::Ready(Ok(false)) | Poll::Ready(Err(_)) => {
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, "Expansion failed")));
                }
                Poll::Pending => {
                    self.expansion_rx = Some(rx);
                    return Poll::Pending;
                }
            }
        }

        let (_, blk_cnt) = self.stripe.get_meta();
        let user_cap = (blk_cnt - 1) * layout::BLOCK_SIZE;

        if self.write_pos + buf.len() > user_cap {
            let (tx, rx) = oneshot::channel();
            if let Err(e) = self.stripe.register_waiter(tx) {
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e.to_string())));
            }
            if let Err(e) = self.stripe.request_expand_hub(self.hub_ev_fd) {
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e.to_string())));
            }
            self.expansion_rx = Some(rx);
            // We need to poll the receiver once to register the waker
            return self.poll_write(cx, buf);
        }

        let write_pos = self.write_pos;
        let written = self.stripe.write_data(write_pos, buf);
        self.write_pos += written;

        let (cur_len, _) = self.stripe.get_meta();
        if self.write_pos as u32 > cur_len {
            self.stripe.update_data_len(self.write_pos as u32);
        }

        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Connection for ShmConnection {
    fn data_len(&self) -> u32 {
        let (data_len, _) = self.stripe.get_meta();
        data_len
    }

    fn view(&mut self, offset: usize, length: usize, call: &mut dyn FnMut(&mut Viewer) -> Result<()>) -> Result<()> {
        self.stripe.view(offset, length, call)
    }

    fn request_expand(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.stripe.register_waiter(tx) {
            return Box::pin(async move { Err(e) });
        }
        if let Err(e) = self.stripe.request_expand_hub(self.hub_ev_fd) {
            return Box::pin(async move { Err(e) });
        }
        
        Box::pin(async move {
            match rx.await {
                Ok(true) => Ok(()),
                Ok(false) => Err(MmfgError::Expansion("Expansion failed: hub reported error".to_string())),
                Err(_) => Err(MmfgError::Expansion("Expansion failed: Hub closed connection".to_string())),
            }
        })
    }
}

// Keep Read/Write for compatibility if needed
impl Read for ShmConnection {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let (data_len, _) = self.stripe.get_meta();
        if self.read_pos >= data_len as usize {
            return Ok(0);
        }
        let remaining = data_len as usize - self.read_pos;
        let n = std::cmp::min(remaining, buf.len());
        let read_pos = self.read_pos;
        let read = self.stripe.read_data(read_pos, &mut buf[..n]);
        self.read_pos += read;
        Ok(read)
    }
}

impl Write for ShmConnection {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let (_, blk_cnt) = self.stripe.get_meta();
        let user_cap = (blk_cnt - 1) * layout::BLOCK_SIZE;

        if self.write_pos + buf.len() > user_cap {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "Expansion needed, use AsyncWrite"));
        }

        let write_pos = self.write_pos;
        let written = self.stripe.write_data(write_pos, buf);
        self.write_pos += written;

        let (cur_len, _) = self.stripe.get_meta();
        if self.write_pos as u32 > cur_len {
            self.stripe.update_data_len(self.write_pos as u32);
        }

        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
