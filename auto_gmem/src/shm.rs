use std::ffi::CString;

use auto_gmem_ipc::shm::{Shm, ShmGuard};

use nix::libc;

use crate::error::AutoGMemError;

pub(crate) fn open_shm(path: String) -> Result<ShmGuard, AutoGMemError> {
    tracing::debug!("open_shm({})", path);
    let cpath = CString::new(path).unwrap();
    let shm_fd =
        unsafe { libc::shm_open(cpath.as_ptr(), libc::O_RDWR, libc::S_IRUSR | libc::S_IWUSR) };
    if shm_fd == -1 {
        return Err(AutoGMemError::Errno(
            nix::errno::Errno::last(),
            "open shared memory failed",
        ));
    }
    let shm = ShmGuard::new(unsafe {
        Shm::open_copy_at(shm_fd, auto_gmem_ipc::shm::Shm::SHM_STRUCT_SIZE).map_err(|e| {
            AutoGMemError::Errno(nix::errno::Errno::from_raw(e), "open_shm(): mmap failed")
        })?
    });
    unsafe {
        libc::close(shm_fd);
        let errno = libc::unlink(cpath.as_ptr());
        if errno != 0 {
            tracing::warn!("Failed to unlink shared memory: {}", errno);
        }
    }
    Ok(shm)
}
