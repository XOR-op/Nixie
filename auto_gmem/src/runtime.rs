use nix::libc::c_int;
use std::{
    os::fd::{FromRawFd, OwnedFd},
    path::{Path, PathBuf},
};
use syscalls::{syscall, Sysno};

use tokio::{io::AsyncReadExt, net::UnixListener};

use crate::error::AutoGMemError;
use auto_gmem_ipc::Message;

pub struct Runtime {
    control_path: PathBuf,
}

impl Runtime {
    pub fn new() -> Self {
        Self {
            control_path: PathBuf::from("/tmp/auto_gmem.sock"),
        }
    }

    pub fn start(self) {
        crate::logging::init_tracing();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let r: Result<(), AutoGMemError> = rt.block_on(async move {
            tokio::select! {
                r = self.mainloop() => r,
                _ = tokio::signal::ctrl_c() => Ok(())
            }
        });

        if let Err(e) = r {
            tracing::error!("Error: {}", e);
        }
    }

    async fn mainloop(self) -> Result<(), AutoGMemError> {
        let controller = UnixListenerGuard::new(self.control_path.as_path())?;
        tracing::info!("Runtime started at {:?}", self.control_path);
        loop {
            let (stream, _) = controller.get_listener().accept().await?;
            tokio::spawn(async move {
                let _ = Self::serve_conn(stream).await;
            });
        }
    }

    async fn serve_conn(mut stream: tokio::net::UnixStream) -> Result<(), AutoGMemError> {
        let mut length_buf = [0u8; 4];
        let mut peer_pid = None;

        while stream.read_exact(&mut length_buf).await.is_ok() {
            // read entire message
            let length = u32::from_le_bytes(length_buf);
            let mut buf = vec![0u8; length as usize];
            stream.read_exact(&mut buf).await?;
            let message = bincode::deserialize(&buf)?;

            // make sure the peer process has registered itself
            if peer_pid.is_none() && !matches!(message, Message::ClientHello(_)) {
                return Err(AutoGMemError::InvalidMessage);
            }
            match message {
                Message::ClientHello(hello) => {
                    peer_pid = Some(hello.pid);
                    tracing::info!("Client[pid={}] connected", hello.pid);
                }
                Message::UvmFd(fd) => {
                    tracing::debug!("UvmFd: {:?}", fd);
                }
            }
        }
        Ok(())
    }
}

fn get_peer_fd(pid: i32, remote_fd: i32) -> Result<OwnedFd, AutoGMemError> {
    let pid_fd = match unsafe { syscall!(Sysno::pidfd_open, pid, 0) } {
        Ok(fd) => fd as c_int,
        Err(e) => {
            return Err(AutoGMemError::Errno(
                nix::errno::Errno::from_raw(e.into_raw()),
                "pidfd_open",
            ));
        }
    };
    match unsafe { syscall!(Sysno::pidfd_getfd, pid_fd, remote_fd, 0) } {
        Ok(fd) => {
            let _ = nix::unistd::close(pid_fd);
            Ok(unsafe { OwnedFd::from_raw_fd(fd as c_int) })
        }
        Err(e) => {
            let _ = nix::unistd::close(pid_fd);
            Err(AutoGMemError::Errno(
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
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, AutoGMemError> {
        let path = path.as_ref().to_path_buf();
        let listener = UnixListener::bind(&path)?;
        if let Some((_, uid, gid)) = get_user_info() {
            nix::unistd::chown(&path, Some(uid.into()), Some(gid.into()))
                .map_err(|e| AutoGMemError::Errno(e, "chown"))?;
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
