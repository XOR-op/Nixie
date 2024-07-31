use nix::libc::c_int;
use std::{
    os::fd::{FromRawFd, OwnedFd},
    path::{Path, PathBuf},
};
use syscalls::{syscall, Sysno};
use tokio::{
    io::{unix::AsyncFd, AsyncReadExt},
    net::UnixListener,
};

use crate::{error::AutoGMemError, uvm::event_queue::EventQueue};
use auto_gmem_ipc::C2SMessage;

use super::{proc_ctl::ProcessControlBuilder, shm::open_shm};

pub struct Daemon {
    control_path: PathBuf,
    dylib_path: String,
}

impl Daemon {
    pub fn new(dylib_path: String) -> Self {
        Self {
            control_path: PathBuf::from("/tmp/auto_gmem.sock"),
            dylib_path,
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
        tracing::info!("Daemon started at {:?}", self.control_path);
        loop {
            let (stream, _) = controller.get_listener().accept().await?;
            let dylib_path = self.dylib_path.clone();
            tokio::spawn(async move {
                match Self::serve_conn(stream, dylib_path).await {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!("Client[pid={}] {}", e.1.unwrap_or(-1), e.0);
                    }
                }
            });
        }
    }

    async fn serve_conn(
        stream: tokio::net::UnixStream,
        dylib_path: String,
    ) -> Result<(), (AutoGMemError, Option<i32>)> {
        let mut length_buf = [0u8; 4];
        let mut peer_pid = None;
        let (mut uds_recv, uds_send) = stream.into_split();
        let mut builder = ProcessControlBuilder::new(uds_send, dylib_path);

        while uds_recv.read_exact(&mut length_buf).await.is_ok() {
            // read entire message
            let length = u32::from_le_bytes(length_buf);
            let mut buf = vec![0u8; length as usize];
            uds_recv
                .read_exact(&mut buf)
                .await
                .map_err(|e| (AutoGMemError::from(e), peer_pid))?;
            let message =
                bincode::deserialize(&buf).map_err(|e| (AutoGMemError::from(e), peer_pid))?;

            // make sure the peer process has registered itself
            if peer_pid.is_none() && !matches!(message, C2SMessage::ClientHello(_)) {
                return Err((
                    AutoGMemError::Invalid("ClientHello message is required"),
                    None,
                ));
            }
            match message {
                C2SMessage::ClientHello(hello) => {
                    peer_pid = Some(hello.pid);
                    tracing::info!("Client[pid={}] connected", hello.pid);
                }
                C2SMessage::UvmFd(fd) => {
                    tracing::debug!("UvmFd: {:?}", fd);
                    let (pid_fd, uvm_fd) =
                        duplicate_peer_fd(peer_pid.unwrap(), fd.fd).map_err(|e| (e, peer_pid))?;
                    let event_queue = EventQueue::new(uvm_fd, 1024).map_err(|e| (e, peer_pid))?;
                    builder
                        .with_pid(peer_pid.unwrap())
                        .with_pid_fd(
                            AsyncFd::new(pid_fd).map_err(|e| (AutoGMemError::Io(e), peer_pid))?,
                        )
                        .with_event_queue(event_queue);
                    if let Some(ctl) = builder.build() {
                        tokio::spawn(async move {
                            ctl.run().await;
                        });
                    }
                }
                C2SMessage::ShmPath(path) => {
                    let shmem = open_shm(path.path).map_err(|e| (e, peer_pid))?;
                    builder.with_shm(shmem);
                    if let Some(ctl) = builder.build() {
                        tokio::spawn(async move {
                            ctl.run().await;
                        });
                    }
                }
                C2SMessage::MemoryUsage(_) => todo!(),
            }
        }
        Ok(())
    }
}

fn duplicate_peer_fd(pid: i32, remote_fd: i32) -> Result<(OwnedFd, OwnedFd), AutoGMemError> {
    let pid_fd = match unsafe { syscall!(Sysno::pidfd_open, pid, nix::libc::PIDFD_NONBLOCK) } {
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
            let pid_fd = unsafe { OwnedFd::from_raw_fd(pid_fd) };
            let uvm_fd = unsafe { OwnedFd::from_raw_fd(fd as c_int) };
            Ok((pid_fd, uvm_fd))
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
