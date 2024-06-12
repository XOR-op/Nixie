#![allow(non_upper_case_globals)]
use nix::libc::c_int;
use std::{
    os::fd::{FromRawFd, OwnedFd},
    path::{Path, PathBuf},
};
use syscalls::{syscall, Sysno};
use tokio::{io::AsyncReadExt, net::UnixListener};

use crate::{
    error::AutoGMemError,
    uvm::{event_queue::EventQueue, uvm_binding::UvmEventType_UvmEventTypeGpuFault},
};
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
                match Self::serve_conn(stream).await {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!("Client[pid={}] {}", e.1.unwrap_or(-1), e.0);
                    }
                }
            });
        }
    }

    async fn serve_conn(
        mut stream: tokio::net::UnixStream,
    ) -> Result<(), (AutoGMemError, Option<i32>)> {
        let mut length_buf = [0u8; 4];
        let mut peer_pid = None;

        while stream.read_exact(&mut length_buf).await.is_ok() {
            // read entire message
            let length = u32::from_le_bytes(length_buf);
            let mut buf = vec![0u8; length as usize];
            stream
                .read_exact(&mut buf)
                .await
                .map_err(|e| (AutoGMemError::from(e), peer_pid))?;
            let message =
                bincode::deserialize(&buf).map_err(|e| (AutoGMemError::from(e), peer_pid))?;

            // make sure the peer process has registered itself
            if peer_pid.is_none() && !matches!(message, Message::ClientHello(_)) {
                return Err((
                    AutoGMemError::Invalid("ClientHello message is required"),
                    None,
                ));
            }
            match message {
                Message::ClientHello(hello) => {
                    peer_pid = Some(hello.pid);
                    tracing::info!("Client[pid={}] connected", hello.pid);
                }
                Message::UvmFd(fd) => {
                    tracing::debug!("UvmFd: {:?}", fd);
                    let (pid_fd, uvm_fd) =
                        duplicate_peer_fd(peer_pid.unwrap(), fd.fd).map_err(|e| (e, peer_pid))?;
                    let event_queue = EventQueue::new(uvm_fd, 1024).map_err(|e| (e, peer_pid))?;
                    let peer_pid2 = peer_pid.unwrap();
                    tokio::spawn(async move {
                        tracing::info!("Monitoring process [pid={}]", peer_pid2);
                        if let Err(e) = Self::monitor_process(peer_pid2, pid_fd, event_queue).await
                        {
                            tracing::error!("Client[pid={}] {}", peer_pid2, e);
                        }
                    });
                }
                Message::ShmPath(path) => {
                    tracing::debug!("ShmPath: {:?}", path);
                    todo!()
                }
            }
        }
        Ok(())
    }

    async fn monitor_process(
        peer_pid: i32,
        pid_fd: OwnedFd,
        mut event_queue: EventQueue,
    ) -> Result<(), AutoGMemError> {
        event_queue
            .enable_event(UvmEventType_UvmEventTypeGpuFault)
            .map_err(|e| AutoGMemError::from(e))?;

        tracing::info!("Listen events from process [pid={}]", peer_pid);
        loop {
            let _ = tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let mut write_cnt = 0;
            let mut first = true;
            let n_completed = event_queue.read_events(|event| {
                let event_type = unsafe { event.__bindgen_anon_1.eventData.eventType };
                match event_type as u32 {
                    UvmEventType_UvmEventTypeGpuFault => {
                        let event_ref = unsafe { &event.__bindgen_anon_1.eventData.gpuFault };
                        const UVM_FAULT_TYPE_WRITE: u8 = 3;
                        match event_ref.faultType {
                            UVM_FAULT_TYPE_WRITE => write_cnt += 1,
                            _ => {}
                        }
                        if first && event_ref.faultType == UVM_FAULT_TYPE_WRITE {
                            tracing::info!(
                                "fault: addr={:#018x}, fault_type={}",
                                event_ref.address,
                                event_ref.faultType
                            );
                            first = false;
                        }
                        true
                    }
                    _ => {
                        tracing::warn!("Unknown event type: {}", event_type);
                        false
                    }
                }
            });
            if n_completed > 0 {
                tracing::info!(
                    "[pid={}] Received {} events: write={}",
                    peer_pid,
                    n_completed,
                    write_cnt
                );
            }
        }
    }
}

fn duplicate_peer_fd(pid: i32, remote_fd: i32) -> Result<(OwnedFd, OwnedFd), AutoGMemError> {
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
