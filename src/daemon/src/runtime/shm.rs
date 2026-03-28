use std::ffi::CString;

use nixie_common::shm::{Shm, ShmGuard};

use nix::libc;

use crate::error::DaemonError;

pub(crate) fn open_shm(path: String) -> Result<ShmGuard, DaemonError> {
    tracing::debug!("open_shm({})", path);
    let cpath = CString::new(path).unwrap();
    let shm_fd =
        unsafe { libc::shm_open(cpath.as_ptr(), libc::O_RDWR, libc::S_IRUSR | libc::S_IWUSR) };
    if shm_fd < 0 {
        return Err(DaemonError::Errno(
            "open shared memory failed",
            nix::errno::Errno::last(),
        ));
    }
    let shm = ShmGuard::new(unsafe {
        Shm::open_copy_at(shm_fd, nixie_common::shm::Shm::SHM_STRUCT_SIZE).map_err(|e| {
            DaemonError::Errno("open_shm(): mmap failed", nix::errno::Errno::from_raw(e))
        })?
    });
    unsafe {
        libc::close(shm_fd);
        let errno = libc::shm_unlink(cpath.as_ptr());
        if errno != 0 {
            tracing::warn!("Failed to unlink shared memory: {}", errno);
        }
    }
    Ok(shm)
}
