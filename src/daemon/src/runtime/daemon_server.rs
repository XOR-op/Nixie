use crate::{
    error::{DaemonError, UvmError},
    runtime::shm::open_shm,
    uvm::event_queue::EventQueue,
};
use futures::StreamExt;
use nihilipc::rpc::{rpc_multiplex_twoway, Daemon, SidecarClient};
use nix::libc::c_int;
use std::{
    future::Future,
    os::fd::{FromRawFd, OwnedFd},
    sync::Arc,
    task::{ready, Poll},
};
use syscalls::{syscall, Sysno};
use tarpc::{
    context::Context,
    server::{BaseChannel, Channel},
    tokio_util::codec::LengthDelimitedCodec,
};
use tokio::{
    io::unix::AsyncFd,
    net::UnixStream,
    sync::{mpsc, Mutex},
    task::JoinHandle,
};

use super::{proc_ctl::ProcessControlBuilder, ProcCtlReq};

macro_rules! extract_guard_and_swap {
    ($state:expr, $expected:path, $funcname: literal) => {
        match &*$state {
            $expected(_) => {
                let state = std::mem::replace(&mut *$state, ServerState::Partial);
                if let $expected(val) = state {
                    val
                } else {
                    unreachable!()
                }
            }
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
                return;
            }
        }
    };
}

pub(super) struct DaemonServerHandleFuture {
    client: SidecarClient,
    pid: Option<i32>,
    task_rx: mpsc::Receiver<(JoinHandle<()>, i32)>,
    inst_tx: mpsc::UnboundedSender<ProcCtlReq>,
}

impl Future for DaemonServerHandleFuture {
    type Output = Option<DaemonServerHandle>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        let val = ready!(self.task_rx.poll_recv(cx));
        return Poll::Ready(val.map(|(task, pid)| DaemonServerHandle {
            client: self.client.clone(),
            pid,
            task,
            inst_tx: self.inst_tx.clone(),
        }));
    }
}

pub(crate) struct DaemonServerHandle {
    client: SidecarClient,
    pid: i32,
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
}

/// For each client process, we have a corresponding daemon server to manage its state.
#[derive(Clone)]
pub(crate) struct DaemonServer {
    state: Arc<Mutex<ServerState>>,
}

impl DaemonServer {
    pub fn launch(conn: UnixStream) -> DaemonServerHandleFuture {
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
    ret: mpsc::Sender<(JoinHandle<()>, i32)>,
}

struct StateOfBuilding {
    client_pid: i32,
    builder: ProcessControlBuilder,
    ret: mpsc::Sender<(JoinHandle<()>, i32)>,
}

struct DaemonServerState {
    client_pid: i32,
}

enum ServerState {
    Start(StateOfStarting),
    Building(StateOfBuilding),
    Launched(DaemonServerState),
    // Used for ownership workaround
    Partial,
}

impl ServerState {
    fn state_name(&self) -> &'static str {
        match self {
            ServerState::Start(_) => "Start",
            ServerState::Building(_) => "Building",
            ServerState::Launched(_) => "Launched",
            ServerState::Partial => "Inconsistent state when lock is hold",
        }
    }
}

impl nihilipc::rpc::Daemon for DaemonServer {
    async fn init_client(self, _ctx: Context, params: nihilipc::InitClient) {
        let mut state_guard = self.state.lock().await;
        let state = extract_guard_and_swap!(state_guard, ServerState::Start, "init_client");
        let rpc_client = state.rpc_client.clone();
        tracing::info!("Client[pid={}] connected", params.pid);
        let mut builder = ProcessControlBuilder::new(rpc_client, state.inst_rx);
        builder.with_pid(params.pid);
        *state_guard = ServerState::Building(StateOfBuilding {
            client_pid: params.pid,
            builder,
            ret: state.ret.clone(),
        });
    }

    async fn set_uvm_fd(self, _ctx: Context, params: nihilipc::UvmFd) {
        let mut state_guard = self.state.lock().await;
        let mut state = extract_guard_and_swap!(state_guard, ServerState::Building, "set_uvm_fd");
        let peer_pid = state.client_pid;
        let (pid_fd, uvm_fd) =
            checked!(duplicate_peer_fd(peer_pid, params.fd).map_err(|e| (e, peer_pid)));
        let event_queue = checked!(EventQueue::new(uvm_fd, 1024).map_err(|e| (e, peer_pid)));
        state
            .builder
            .with_pid_fd(checked!(
                AsyncFd::new(pid_fd).map_err(|e| (UvmError::Io("Create PID fd", e), peer_pid))
            ))
            .with_event_queue(event_queue);
        if let Some(ctl) = state.builder.build() {
            let task = tokio::spawn(async move {
                ctl.run().await;
            });
            // should no have problem since state transition only happens once
            let _ = state.ret.try_send((task, peer_pid));
            *state_guard = ServerState::Launched(DaemonServerState {
                client_pid: peer_pid,
            });
        } else {
            *state_guard = ServerState::Building(state);
        }
    }

    async fn set_shm_path(self, _ctx: Context, params: nihilipc::ShmPath) {
        let mut state_guard = self.state.lock().await;
        let mut state = extract_guard_and_swap!(state_guard, ServerState::Building, "set_uvm_fd");
        let peer_pid = state.client_pid;
        let shmem = checked!(open_shm(params.path).map_err(|e| (e, peer_pid)));
        state.builder.with_shm(shmem);
        if let Some(ctl) = state.builder.build() {
            let task = tokio::spawn(async move {
                ctl.run().await;
            });
            let _ = state.ret.try_send((task, peer_pid));
            *state_guard = ServerState::Launched(DaemonServerState {
                client_pid: peer_pid,
            });
        } else {
            *state_guard = ServerState::Building(state);
        }
    }
}

fn duplicate_peer_fd(pid: i32, remote_fd: i32) -> Result<(OwnedFd, OwnedFd), DaemonError> {
    let pid_fd = match unsafe { syscall!(Sysno::pidfd_open, pid, nix::libc::PIDFD_NONBLOCK) } {
        Ok(fd) => fd as c_int,
        Err(e) => {
            return Err(DaemonError::Errno(
                "pidfd_open",
                nix::errno::Errno::from_raw(e.into_raw()),
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
            Err(DaemonError::Errno(
                "pidfd_getfd",
                nix::errno::Errno::from_raw(e.into_raw()),
            ))
        }
    }
}
