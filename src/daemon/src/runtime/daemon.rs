use nix::libc::c_int;
use std::{
    os::fd::{FromRawFd, OwnedFd},
    path::{Path, PathBuf},
};
use syscalls::{syscall, Sysno};
use tokio::net::UnixListener;

use crate::{error::NihilphaseError, runtime::daemon_server::DaemonServer};

pub struct Daemon {
    control_path: PathBuf,
}

impl Daemon {
    pub fn new() -> Self {
        Self {
            control_path: PathBuf::from("/tmp/nihilphase.sock"),
        }
    }

    pub fn start(self) {
        crate::logging::init_tracing();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let r: Result<(), NihilphaseError> = rt.block_on(async move {
            tokio::select! {
                r = self.mainloop() => r,
                _ = tokio::signal::ctrl_c() => Ok(())
            }
        });

        if let Err(e) = r {
            tracing::error!("Error: {}", e);
        }
    }

    async fn mainloop(self) -> Result<(), NihilphaseError> {
        let controller = UnixListenerGuard::new(self.control_path.as_path())?;
        tracing::info!("Daemon started at {:?}", self.control_path);
        loop {
            let (stream, _) = controller.get_listener().accept().await?;
            DaemonServer::launch(stream);
        }
    }
}

fn duplicate_peer_fd(pid: i32, remote_fd: i32) -> Result<(OwnedFd, OwnedFd), NihilphaseError> {
    let pid_fd = match unsafe { syscall!(Sysno::pidfd_open, pid, nix::libc::PIDFD_NONBLOCK) } {
        Ok(fd) => fd as c_int,
        Err(e) => {
            return Err(NihilphaseError::Errno(
                nix::errno::Errno::from_raw(e.into_raw()),
                "pidfd_open",
            ));
        }
    };
    match unsafe { syscall!(Sysno::pidfd_getfd, pid_fd, remote_fd, 0) } {
        Ok(fd) => {
            let pid_fd = unsafe { OwnedFd::from_raw_fd(pid_fd) };
            let uvm_fd = unsafe { OwnedFd::from_raw_fd(fd as c_int) };
            Ok((pid_fd, uvm_fd))
        }
        Err(e) => {
            let _ = nix::unistd::close(pid_fd);
            Err(NihilphaseError::Errno(
                nix::errno::Errno::from_raw(e.into_raw()),
                "pidfd_getfd",
            ))
        }
    }
}

// Utils

struct UnixListenerGuard {
    path: PathBuf,
    listener: Option<UnixListener>,
}

impl UnixListenerGuard {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, NihilphaseError> {
        let path = path.as_ref().to_path_buf();
        let listener = UnixListener::bind(&path)?;
        if let Some((_, uid, gid)) = get_user_info() {
            nix::unistd::chown(&path, Some(uid.into()), Some(gid.into()))
                .map_err(|e| NihilphaseError::Errno(e, "chown"))?;
        }
        Ok(Self {
            path,
            listener: Some(listener),
        })
    }
    pub fn get_listener(&self) -> &UnixListener {
        self.listener.as_ref().unwrap()
    }
}

impl Drop for UnixListenerGuard {
    fn drop(&mut self) {
        self.listener = None;
        if let Err(e) = std::fs::remove_file(&self.path) {
            tracing::error!("Error when removing unix domain socket: {}", e)
        }
    }
}

fn get_user_info() -> Option<(String, nix::libc::uid_t, nix::libc::gid_t)> {
    let user_name = unsafe { nix::libc::getlogin() };
    if user_name.is_null() {
        return None;
    }
    let name = unsafe { std::ffi::CStr::from_ptr(user_name) }
        .to_string_lossy()
        .into_owned();
    let user_info = unsafe { nix::libc::getpwnam(user_name) };
    if user_info.is_null() {
        return None;
    }
    let uid = unsafe { (*user_info).pw_uid };
    let gid = unsafe { (*user_info).pw_gid };
    Some((name, uid, gid))
}
