use memmap2::MmapMut;
use std::os::unix::io::RawFd;
use std::fs::File;
use std::os::unix::io::FromRawFd;
use std::io;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

pub struct Chunk {
    pub mmap: MmapMut,
    pub fd: RawFd,
}

impl Chunk {
    pub fn attach(fd: RawFd, _size: usize) -> io::Result<Self> {
        let file = unsafe { File::from_raw_fd(fd) };
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        // file is dropped here, closing the FD
        Ok(Self { mmap, fd: -1 })
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.mmap
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.mmap
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.mmap.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.mmap.as_mut_ptr()
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        if self.fd >= 0 {
            unsafe {
                libc::close(self.fd);
            }
            self.fd = -1;
        }
    }
}

pub struct SharedMemory {
    ptr: *mut u8,
    len: usize,
}

impl SharedMemory {
    pub fn new(ptr: *mut u8, len: usize) -> Self {
        Self { ptr, len }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    pub fn read_u32(&self, offset: usize) -> u32 {
        assert!(offset + 4 <= self.len, "SHM read out of bounds: {} / {}", offset, self.len);
        unsafe {
            let ptr = self.ptr.add(offset) as *const u32;
            std::ptr::read_volatile(ptr)
        }
    }

    pub fn write_u32(&self, offset: usize, value: u32) {
        assert!(offset + 4 <= self.len, "SHM write out of bounds: {} / {}", offset, self.len);
        unsafe {
            let ptr = self.ptr.add(offset) as *mut u32;
            std::ptr::write_volatile(ptr, value);
        }
    }

    pub fn atomic_read_u32(&self, offset: usize, order: Ordering) -> u32 {
        assert!(offset + 4 <= self.len, "SHM atomic read out of bounds");
        unsafe {
            let ptr = self.ptr.add(offset) as *const AtomicU32;
            (*ptr).load(order)
        }
    }

    pub fn atomic_write_u32(&self, offset: usize, value: u32, order: Ordering) {
        assert!(offset + 4 <= self.len, "SHM atomic write out of bounds");
        unsafe {
            let ptr = self.ptr.add(offset) as *const AtomicU32;
            (*ptr).store(value, order)
        }
    }

    pub fn atomic_cas_u32(&self, offset: usize, current: u32, new: u32, order: Ordering) -> bool {
        assert!(offset + 4 <= self.len, "SHM atomic cas out of bounds");
        unsafe {
            let ptr = self.ptr.add(offset) as *const AtomicU32;
            (*ptr).compare_exchange(current, new, order, Ordering::Acquire).is_ok()
        }
    }

    pub fn atomic_read_u64(&self, offset: usize, order: Ordering) -> u64 {
        assert!(offset + 8 <= self.len, "SHM atomic read out of bounds");
        unsafe {
            let ptr = self.ptr.add(offset) as *const AtomicU64;
            (*ptr).load(order)
        }
    }

    pub fn atomic_write_u64(&self, offset: usize, value: u64, order: Ordering) {
        assert!(offset + 8 <= self.len, "SHM atomic write out of bounds");
        unsafe {
            let ptr = self.ptr.add(offset) as *const AtomicU64;
            (*ptr).store(value, order)
        }
    }
}

unsafe impl Send for SharedMemory {}
unsafe impl Sync for SharedMemory {}
