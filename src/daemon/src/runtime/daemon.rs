use futures::StreamExt;
use hashlink::LinkedHashMap;
use nihilipc::PrefetchArgs;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tarpc::{
    context::Context,
    server::{BaseChannel, Channel},
    tokio_util::codec::LengthDelimitedCodec,
};
use tokio::{
    net::UnixListener,
    sync::{mpsc, RwLock},
};

use crate::{
    control::{self, Controllable, PrefetchMsg, ReadDupMsg},
    error::{DaemonError, NihilphaseError},
    runtime::daemon_server::DaemonServer,
};

use super::{daemon_server::DaemonServerHandle, socket_chown};

#[derive(Clone)]
struct DaemonData {
    processes: Arc<RwLock<LinkedHashMap<i32, DaemonServerHandle>>>,
}

impl DaemonData {
    pub fn new() -> Self {
        Self {
            processes: Arc::new(RwLock::new(LinkedHashMap::new())),
        }
    }
}

pub struct Daemon {
    daemon_path: PathBuf,
    control_path: PathBuf,
    data: Arc<DaemonData>,
}

impl Daemon {
    pub fn new() -> Self {
        Self {
            daemon_path: PathBuf::from("/tmp/nihilphase.sock"),
            control_path: PathBuf::from(control::CONTROL_PATH),
            data: Arc::new(DaemonData::new()),
        }
    }

    pub fn run(self) {
        crate::logging::init_tracing();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let r: Result<(), NihilphaseError> = rt.block_on(async move {
            tokio::select! {
                r = self.run_body() => r,
                _ = tokio::signal::ctrl_c() => Ok(())
            }
        });

        if let Err(e) = r {
            tracing::error!("Error: {}", e);
        }
    }

    async fn run_body(self) -> Result<(), NihilphaseError> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(Self::handle_processes(self.daemon_path, tx));
        let list_handle = self.data.processes.clone();
        tokio::spawn(async move {
            while let Some(handle) = rx.recv().await {
                list_handle.write().await.insert(handle.pid(), handle);
            }
        });

        let listener = UnixListener::bind(&self.control_path)
            .map_err(|e| DaemonError::Io("bind control listener", e))?;
        socket_chown(&self.control_path)?;

        // listen for client
        while let Ok((stream, _)) = listener.accept().await {
            let conn = tarpc::serde_transport::new(
                LengthDelimitedCodec::builder().new_framed(stream),
                tarpc::tokio_serde::formats::Cbor::default(),
            );
            let server = ControllableDaemon {
                data: self.data.clone(),
            };
            tokio::spawn(
                BaseChannel::with_defaults(conn)
                    .execute(server.serve())
                    .for_each(|response| async move {
                        tokio::spawn(response);
                    }),
            );
        }

        Ok(())
    }

    async fn handle_processes(
        daemon_path: PathBuf,
        ret_tx: mpsc::UnboundedSender<DaemonServerHandle>,
    ) -> Result<(), DaemonError> {
        let controller = UnixListenerGuard::new(daemon_path.as_path())?;
        tracing::info!("Daemon started at {:?}", daemon_path);
        loop {
            let (stream, _) = controller
                .get_listener()
                .accept()
                .await
                .map_err(|e| DaemonError::Io("accept connection", e))?;
            let future = DaemonServer::launch(stream);
            let tx = ret_tx.clone();
            tokio::spawn(async move {
                let val = future.await;
                if let Some(val) = val {
                    let _ = tx.send(val);
                }
            });
        }
    }
}

#[derive(Clone)]
struct ControllableDaemon {
    data: Arc<DaemonData>,
}

impl Controllable for ControllableDaemon {
    async fn list_processes(self, _context: Context) {
        todo!()
    }

    async fn read_dup(self, _context: Context, args: ReadDupMsg) {
        let guard = self.data.processes.read().await;
        let Some(handle) = guard.get(&args.pid) else {
            return;
        };
        let inst_tx = handle.inst_tx();
        drop(guard);
        let _ = inst_tx.send(args);
    }

    async fn prefetch(self, _context: Context, args: PrefetchMsg) {
        let guard = self.data.processes.read().await;
        let Some(handle) = guard.get(&args.pid) else {
            return;
        };
        let client = handle.client();
        drop(guard);
        tracing::warn!("prefetch half implemented: %addr and %device");
        let _ = client
            .prefetch(
                Context::current(),
                PrefetchArgs {
                    addr: 0,
                    len: args.size_low.unwrap_or(0),
                    device: 0,
                },
            )
            .await;
    }
}
// Utils

struct UnixListenerGuard {
    path: PathBuf,
    listener: Option<UnixListener>,
}

impl UnixListenerGuard {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, DaemonError> {
        let path = path.as_ref().to_path_buf();
        let listener =
            UnixListener::bind(&path).map_err(|e| DaemonError::Io("bind listener", e))?;
        if let Some((_, uid, gid)) = get_user_info() {
            nix::unistd::chown(&path, Some(uid.into()), Some(gid.into()))
                .map_err(|e| DaemonError::Errno("chown listener", e))?;
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
