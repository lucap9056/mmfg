use std::ffi::CString;
use std::io;
use std::os::unix::io::RawFd;
use crate::error::{Result, MmfgError};

/// Sends msg + fds in one sendmsg(2) call — on SEQPACKET both must be in the same datagram or the peer's single recvmsg loses the fds.
pub fn send_msg_with_fds(fd: RawFd, msg: &[u8], fds: &[RawFd]) -> Result<()> {
    let mut iov = libc::iovec {
        iov_base: msg.as_ptr() as *mut libc::c_void,
        iov_len: msg.len(),
    };

    let mut msghdr = unsafe { std::mem::zeroed::<libc::msghdr>() };
    msghdr.msg_iov = &mut iov;
    msghdr.msg_iovlen = 1;

    let mut cmsg_buf;
    if !fds.is_empty() {
        let cmsg_len = unsafe { libc::CMSG_SPACE((fds.len() * std::mem::size_of::<RawFd>()) as u32) as usize };
        cmsg_buf = vec![0u8; cmsg_len];
        msghdr.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
        msghdr.msg_controllen = cmsg_len;

        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msghdr);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN((fds.len() * std::mem::size_of::<RawFd>()) as u32) as usize;
            let data_ptr = libc::CMSG_DATA(cmsg) as *mut RawFd;
            for (i, &f) in fds.iter().enumerate() {
                std::ptr::write_unaligned(data_ptr.add(i), f);
            }
        }
    }

    let n = unsafe { libc::sendmsg(fd, &msghdr, 0) };
    if n < 0 {
        return Err(MmfgError::Io(io::Error::last_os_error()));
    }
    Ok(())
}

/// Receives a message and its ancillary fds in one recvmsg(2) call; closes any parsed fds if MSG_CTRUNC fires.
pub fn recv_msg_with_fds(fd: RawFd, buf: &mut [u8], max_fds: usize) -> Result<(usize, Vec<RawFd>)> {
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: buf.len(),
    };

    let cmsg_cap = if max_fds > 0 {
        unsafe { libc::CMSG_SPACE((max_fds * std::mem::size_of::<RawFd>()) as u32) as usize }
    } else {
        0
    };
    let mut cmsg_buf = vec![0u8; cmsg_cap];

    let mut msghdr = unsafe { std::mem::zeroed::<libc::msghdr>() };
    msghdr.msg_iov = &mut iov;
    msghdr.msg_iovlen = 1;
    if cmsg_cap > 0 {
        msghdr.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
        msghdr.msg_controllen = cmsg_cap;
    }

    let n = unsafe { libc::recvmsg(fd, &mut msghdr, 0) };
    if n < 0 {
        return Err(MmfgError::Io(io::Error::last_os_error()));
    }

    let mut fds = Vec::new();
    if cmsg_cap > 0 {
        let mut cmsg = unsafe { libc::CMSG_FIRSTHDR(&msghdr) };
        while !cmsg.is_null() {
            let is_rights = unsafe {
                (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS
            };
            if is_rights {
                let data_ptr = unsafe { libc::CMSG_DATA(cmsg) as *const RawFd };
                let num_fds = unsafe {
                    ((*cmsg).cmsg_len as usize - libc::CMSG_LEN(0) as usize) / std::mem::size_of::<RawFd>()
                };
                for i in 0..num_fds {
                    fds.push(unsafe { std::ptr::read_unaligned(data_ptr.add(i)) });
                }
            }
            cmsg = unsafe { libc::CMSG_NXTHDR(&msghdr, cmsg) };
        }
    }

    if msghdr.msg_flags & libc::MSG_CTRUNC != 0 {
        for f in &fds {
            unsafe { libc::close(*f) };
        }
        return Err(MmfgError::Protocol("control message truncated: too many fds".to_string()));
    }

    Ok((n as usize, fds))
}

/// Binds/listens a SOCK_SEQPACKET socket via libc — std::os::unix::net has no SEQPACKET support.
pub fn listen_seqpacket(path: &str) -> Result<RawFd> {
    let _ = std::fs::remove_file(path);

    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(MmfgError::Io(io::Error::last_os_error()));
    }

    let c_path = CString::new(path)
        .map_err(|_| MmfgError::Protocol("socket path contains a null byte".to_string()))?;
    let path_bytes = c_path.as_bytes_with_nul();

    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    if path_bytes.len() > addr.sun_path.len() {
        unsafe { libc::close(fd) };
        return Err(MmfgError::Protocol("socket path too long".to_string()));
    }
    for (i, &b) in path_bytes.iter().enumerate() {
        addr.sun_path[i] = b as libc::c_char;
    }
    let addr_len = (std::mem::size_of::<libc::sa_family_t>() + path_bytes.len()) as libc::socklen_t;

    let ret = unsafe { libc::bind(fd, &addr as *const _ as *const libc::sockaddr, addr_len) };
    if ret < 0 {
        let e = io::Error::last_os_error();
        unsafe { libc::close(fd) };
        return Err(MmfgError::Io(e));
    }

    let ret = unsafe { libc::listen(fd, 128) };
    if ret < 0 {
        let e = io::Error::last_os_error();
        unsafe { libc::close(fd) };
        return Err(MmfgError::Io(e));
    }

    Ok(fd)
}

pub fn accept(listen_fd: RawFd) -> Result<RawFd> {
    let fd = unsafe {
        libc::accept4(listen_fd, std::ptr::null_mut(), std::ptr::null_mut(), libc::SOCK_CLOEXEC)
    };
    if fd < 0 {
        return Err(MmfgError::Io(io::Error::last_os_error()));
    }
    Ok(fd)
}
