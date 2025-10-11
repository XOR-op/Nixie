use crate::{
    error::{DaemonError, UvmError},
    runtime::{proc_ctl::ProcessControl, schedule::control::ScheduleControlReq, shm::open_shm},
};
use futures::StreamExt;
use nihil_common::{
    GlobalDeviceId, HandshakeResponse, ProcessLocalDeviceId,
    rpc::{Daemon, SidecarClient, rpc_multiplex_twoway},
};
use nix::libc::c_int;
use std::{
    collections::HashMap,
    future::Future,
    os::fd::{FromRawFd, OwnedFd},
    sync::Arc,
    task::{Poll, ready},
};
use syscalls::{Sysno, syscall};
use tarpc::{
    context::Context,
    server::{BaseChannel, Channel},
    tokio_util::codec::LengthDelimitedCodec,
};
use tokio::{
    io::unix::AsyncFd,
    net::UnixStream,
    sync::{Mutex, mpsc},
    task::JoinHandle,
};

use super::ProcCtlReq;

macro_rules! extract_guard {
    ($state:expr, $expected:path, $funcname: literal) => {
        match &mut *$state {
            $expected(v) => v,
            _ => {
                tracing::error!("[{}] bad state: {}", $funcname, $state.state_name());
                return;
            }
        }
    };
}

#[allow(unused_macros)]
macro_rules! ensure_guard {
    ($state:expr, $expected:pat, $funcname: literal) => {
        match *$state {
            $expected => (),
            _ => {
                tracing::error!("[{}] bad state: {}", $funcname, $state.state_name());
                return;
            }
        }
    };
}

macro_rules! checked {
    ($result:expr) => {
        match $result {
            Ok(val) => val,
            Err((err, pid)) => {
                tracing::error!("[pid={}] {}", pid, err);
                return None;
            }
        }
    };
}

pub(super) struct DaemonServerHandleFuture {
    client: SidecarClient,
    pid: Option<i32>,
    task_rx: mpsc::Receiver<(JoinHandle<()>, i32, Arc<DeviceOrdinalMapping>)>,
    inst_tx: mpsc::UnboundedSender<ProcCtlReq>,
}

impl Future for DaemonServerHandleFuture {
    type Output = Option<DaemonServerHandle>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        let val = ready!(self.task_rx.poll_recv(cx));
        Poll::Ready(val.map(|(task, pid, dev_mapping)| DaemonServerHandle {
            client: self.client.clone(),
            pid,
            dev_mapping,
            task,
            inst_tx: self.inst_tx.clone(),
        }))
    }
}

pub(crate) struct DaemonServerHandle {
    client: SidecarClient,
    pid: i32,
    dev_mapping: Arc<DeviceOrdinalMapping>,
    task: JoinHandle<()>,
    /// TX to ProcessControl
    inst_tx: mpsc::UnboundedSender<ProcCtlReq>,
}

impl DaemonServerHandle {
    pub fn is_closed(&self) -> bool {
        self.task.is_finished()
    }

    pub fn client(&self) -> SidecarClient {
        self.client.clone()
    }

    pub fn pid(&self) -> i32 {
        self.pid
    }

    pub fn inst_tx(&self) -> mpsc::UnboundedSender<ProcCtlReq> {
        self.inst_tx.clone()
    }

    pub(super) fn dev_mapping(&self) -> Arc<DeviceOrdinalMapping> {
        self.dev_mapping.clone()
    }
}

/// For each client process, we have a corresponding daemon server to manage its state.
#[derive(Clone)]
pub(crate) struct DaemonServer {
    state: Arc<Mutex<ServerState>>,
}

impl DaemonServer {
    pub fn launch(
        conn: UnixStream,
        exit_tx: mpsc::UnboundedSender<i32>,
        rpc_data_tx: mpsc::UnboundedSender<(i32, nihil_common::ActivityUpdate)>,
        sched_ctl_tx: mpsc::UnboundedSender<ScheduleControlReq>,
        buffer_shmem_path: String,
        buffer_len: usize,
    ) -> DaemonServerHandleFuture {
        // construct a bidirectional RPC tunnel based on single UDS connection
        let mut codec_builder = LengthDelimitedCodec::builder();
        codec_builder.max_frame_length(64 * 1024 * 1024);
        let framed = codec_builder.new_framed(conn);
        let transport = tarpc::serde_transport::new(framed, tokio_serde::formats::Cbor::default());
        let (server_ret, client_ret, inbound_fut, outbound_fut) = rpc_multiplex_twoway(transport);
        tokio::spawn(inbound_fut);
        tokio::spawn(outbound_fut);
        // daemon to client
        let client = SidecarClient::new(Default::default(), client_ret).spawn();
        let client_handle = client.clone();
        let (handle_tx, handle_rx) = mpsc::channel(1);
        let (inst_tx, inst_rx) = mpsc::unbounded_channel();
        // client to daemon
        let server = Self {
            state: Arc::new(Mutex::new(ServerState::Start(StateOfStarting {
                rpc_client: client,
                ret: handle_tx,
                inst_rx,
                rpc_data_tx,
                sched_ctl_tx,
                exit_tx,
                buffer_shmem_path,
                buffer_len,
            }))),
        };
        tokio::spawn(
            BaseChannel::with_defaults(server_ret)
                .execute(server.serve())
                .for_each(|response| async move {
                    tokio::spawn(response);
                }),
        );
        DaemonServerHandleFuture {
            client: client_handle,
            pid: None,
            task_rx: handle_rx,
            inst_tx,
        }
    }
}

struct StateOfStarting {
    rpc_client: SidecarClient,
    inst_rx: mpsc::UnboundedReceiver<ProcCtlReq>,
    exit_tx: mpsc::UnboundedSender<i32>,
    rpc_data_tx: mpsc::UnboundedSender<(i32, nihil_common::ActivityUpdate)>,
    sched_ctl_tx: mpsc::UnboundedSender<ScheduleControlReq>,
    ret: mpsc::Sender<(JoinHandle<()>, i32, Arc<DeviceOrdinalMapping>)>,
    buffer_shmem_path: String,
    buffer_len: usize,
}

struct DaemonServerState {
    client_pid: i32,
    rpc_data_tx: mpsc::UnboundedSender<(i32, nihil_common::ActivityUpdate)>,
}

enum ServerState {
    Start(StateOfStarting),
    Launched(DaemonServerState),
    // Used for ownership workaround
    Partial,
}

impl ServerState {
    fn state_name(&self) -> &'static str {
        match self {
            ServerState::Start(_) => "Start",
            ServerState::Launched(_) => "Launched",
            ServerState::Partial => "Inconsistent state when lock is hold",
        }
    }
}

impl nihil_common::rpc::Daemon for DaemonServer {
    async fn handshake(
        self,
        _ctx: Context,
        params: nihil_common::Handshake,
    ) -> Option<HandshakeResponse> {
        let mut state_guard = self.state.lock().await;
        let state = std::mem::replace(&mut *state_guard, ServerState::Partial);
        let state = if let ServerState::Start(state) = state {
            state
        } else {
            tracing::error!("Handshake called in wrong state: {}", state.state_name());
            return None;
        };
        let rpc_client = state.rpc_client.clone();
        let process_name = std::fs::read_to_string(format!("/proc/{}/comm", params.pid))
            .map(|s| s.trim().to_string())
            .ok();
        tracing::info!(
            "Client[pid={}, comm={:?}] connected",
            params.pid,
            process_name.unwrap_or_else(|| "Unknown".to_string())
        );

        let peer_pid = params.pid;
        let (pid_fd, _) = checked!(duplicate_peer_fd(peer_pid, None).map_err(|e| (e, peer_pid)));
        let pid_fd = checked!(
            AsyncFd::new(pid_fd).map_err(|e| (UvmError::Io("Create PID fd", e), peer_pid))
        );

        // open shared memory
        let shmem = checked!(open_shm(params.shm_path).map_err(|e| (e, peer_pid)));

        // parse CUDA_VISIBLE_DEVICES
        let device_mapping = Arc::new(checked!(
            DeviceOrdinalMapping::new(&params.visible_devices).map_err(|e| (e, peer_pid))
        ));
        let ctl = ProcessControl::new(
            peer_pid,
            pid_fd,
            shmem,
            (*device_mapping).clone(),
            rpc_client.clone(),
            state.inst_rx,
            state.sched_ctl_tx,
            state.exit_tx,
        );
        let task = tokio::spawn(async move {
            ctl.run().await;
        });
        // should no have problem since state transition only happens once
        let _ = state.ret.try_send((task, peer_pid, device_mapping.clone()));
        *state_guard = ServerState::Launched(DaemonServerState {
            client_pid: peer_pid,
            rpc_data_tx: state.rpc_data_tx,
        });
        let mapped_mem_sizes = {
            let mem_size = crate::runtime::get_allowed_devices_mem().ok()?;
            mem_size
                .into_iter()
                .filter_map(|(dev_id, size)| {
                    device_mapping
                        .real_to_visible(dev_id)
                        .map(|visible_dev| (visible_dev, size))
                })
                .collect::<Vec<_>>()
        };
        Some(HandshakeResponse {
            available_vram_sizes: mapped_mem_sizes,
            buffer_shm_path: state.buffer_shmem_path,
            buffer_length: state.buffer_len as u64,
        })
    }

    async fn notify_activity(self, _context: Context, params: nihil_common::ActivityUpdate) {
        let mut state_guard = self.state.lock().await;
        let state = extract_guard!(state_guard, ServerState::Launched, "notify_activity");
        let _ = state.rpc_data_tx.send((state.client_pid, params));
    }
}

// if `remote_fd` is Some, the `Ok` result must be a tuple of (pid_fd, ,Some(uvm_fd))
fn duplicate_peer_fd(
    pid: i32,
    remote_fd: Option<i32>,
) -> Result<(OwnedFd, Option<OwnedFd>), DaemonError> {
    let pid_fd = match unsafe { syscall!(Sysno::pidfd_open, pid, nix::libc::PIDFD_NONBLOCK) } {
        Ok(fd) => fd as c_int,
        Err(e) => {
            return Err(DaemonError::Errno(
                "pidfd_open",
                nix::errno::Errno::from_raw(e.into_raw()),
            ));
        }
    };
    if let Some(remote_fd) = remote_fd {
        match unsafe { syscall!(Sysno::pidfd_getfd, pid_fd, remote_fd, 0) } {
            Ok(fd) => {
                let pid_fd = unsafe { OwnedFd::from_raw_fd(pid_fd) };
                let uvm_fd = unsafe { OwnedFd::from_raw_fd(fd as c_int) };
                Ok((pid_fd, Some(uvm_fd)))
            }
            Err(e) => {
                let _ = nix::unistd::close(pid_fd);
                Err(DaemonError::Errno(
                    "pidfd_getfd",
                    nix::errno::Errno::from_raw(e.into_raw()),
                ))
            }
        }
    } else {
        Ok((unsafe { OwnedFd::from_raw_fd(pid_fd) }, None))
    }
}

// Mapping between real GPU indices and indices exposed to processes
#[derive(Debug, Clone)]
pub(super) struct DeviceOrdinalMapping {
    real_to_visible: HashMap<GlobalDeviceId, ProcessLocalDeviceId>,
    visible_to_real: HashMap<ProcessLocalDeviceId, GlobalDeviceId>,
}

impl DeviceOrdinalMapping {
    pub fn new(visible_devices: &str) -> Result<Self, DaemonError> {
        let mut real_to_visible = HashMap::new();
        let mut visible_to_real = HashMap::new();
        if visible_devices.is_empty() {
            // no CUDA_VISIBLE_DEVICES set, use default mapping
            let num_dev = {
                let mut num_dev = 0;
                let res = unsafe { cudarc::driver::sys::cuDeviceGetCount(&mut num_dev as *mut _) };
                if res != cudarc::driver::sys::cudaError_enum::CUDA_SUCCESS {
                    return Err(DaemonError::Cuda("cuDeviceGetCount", res));
                }
                num_dev
            };
            for i in 0..num_dev {
                real_to_visible.insert(GlobalDeviceId(i), ProcessLocalDeviceId(i));
                visible_to_real.insert(ProcessLocalDeviceId(i), GlobalDeviceId(i));
            }
        } else {
            let device_mapping = get_device_uuid_mapping();
            for (visible_dev, real_str) in visible_devices.split(',').enumerate() {
                let real_dev = if let Ok(dev) = real_str.parse::<i32>() {
                    dev
                } else {
                    let mut real_dev = None;
                    if real_str.starts_with("GPU-") {
                        // GPU UUID
                        if let Some(dev) =
                            device_mapping.get(real_str.to_ascii_lowercase().as_str())
                        {
                            real_dev = Some(*dev);
                        }
                    }
                    match real_dev {
                        Some(dev) => dev,
                        None => {
                            return Err(DaemonError::Cuda(
                                "parse visible devices",
                                cudarc::driver::sys::cudaError_enum::CUDA_ERROR_INVALID_VALUE,
                            ));
                        }
                    }
                };
                let real_dev = GlobalDeviceId(real_dev);
                let visible_dev = ProcessLocalDeviceId(visible_dev as i32);
                real_to_visible.insert(real_dev, visible_dev);
                visible_to_real.insert(visible_dev, real_dev);
            }
        }
        Ok(Self {
            real_to_visible,
            visible_to_real,
        })
    }

    pub fn real_to_visible(&self, real: GlobalDeviceId) -> Option<ProcessLocalDeviceId> {
        self.real_to_visible.get(&real).copied()
    }

    pub fn visible_to_real(&self, visible: ProcessLocalDeviceId) -> Option<GlobalDeviceId> {
        self.visible_to_real.get(&visible).copied()
    }

    pub fn from_real_to_visible_map(map: HashMap<GlobalDeviceId, ProcessLocalDeviceId>) -> Self {
        let mut visible_to_real = HashMap::new();
        for (real, visible) in &map {
            visible_to_real.insert(*visible, *real);
        }
        Self {
            real_to_visible: map,
            visible_to_real,
        }
    }
}

fn get_device_uuid_mapping() -> HashMap<String, i32> {
    let mut uuid_mapping = HashMap::new();
    let mut num_dev = 0;
    let res = unsafe { cudarc::driver::sys::cuDeviceGetCount(&mut num_dev as *mut _) };
    if res != cudarc::driver::sys::cudaError_enum::CUDA_SUCCESS {
        tracing::error!("Failed to get device count: {:?}", res);
        return uuid_mapping;
    }
    for i in 0..num_dev {
        let mut uuid = [0u8; 16];
        let res =
            unsafe { cudarc::driver::sys::cuDeviceGetUuid_v2(uuid.as_mut_ptr() as *mut _, i) };
        if res != cudarc::driver::sys::cudaError_enum::CUDA_SUCCESS {
            tracing::error!("Failed to get device {} UUID: {:?}", i, res);
            continue;
        }
        let uuid_str = format_uuid(&uuid);
        uuid_mapping.insert(uuid_str, i);
    }
    uuid_mapping
}

fn format_uuid(uuid: &[u8; 16]) -> String {
    let mut uuid_str = String::new();
    uuid_str.push_str("gpu-");
    for (i, byte) in uuid.iter().enumerate() {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            uuid_str.push('-');
        }
        uuid_str.push_str(&format!("{:02x}", byte));
    }
    uuid_str
}
