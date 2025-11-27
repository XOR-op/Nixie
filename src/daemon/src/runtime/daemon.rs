use futures::StreamExt;
use hashlink::LinkedHashMap;
use nihil_common::{ActivityUpdate, general::pretty_size};
use std::{
    collections::HashMap,
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
    sync::{RwLock, mpsc},
};

use crate::{
    config::{Config, ConfigurableArgs, load_config, update_config},
    control::{
        self, Controllable, DataManagerMetadata, GetHistoryArgs, GetHistoryResult, PrefetchArgs,
        PrefetchResponse, SetPriorityArgs, SetPriorityResponse,
    },
    error::{DaemonError, NihilphaseError},
    runtime::{
        ProcCtlReq,
        daemon_server::DaemonServer,
        migration::{
            AllocationCapacity, BufferId, DataManagerHandle, HostMemBufferManager,
            ShmBufferManager, StorageBufferManager,
        },
        schedule::control::ScheduleControlReq,
    },
};
use nihil_common::general::{CallFuture, CallParameter};

use super::{
    ProcessMetadata, daemon_server::DaemonServerHandle, schedule::Scheduler, socket_chown,
};

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
    buffer_path: PathBuf,
    shm_buffer_size: usize,
    ram_buffer_size: usize,
    shm_buffer_path: String,
    data: Arc<DaemonData>,
}

impl Daemon {
    pub fn new(shm_buffer_size: usize, ram_buffer_size: usize) -> Self {
        Self {
            daemon_path: PathBuf::from("/tmp/nihilphase.sock"),
            control_path: PathBuf::from(control::CONTROL_PATH),
            buffer_path: PathBuf::from("/tmp/nihilphase.pagebuffer"),
            shm_buffer_size,
            ram_buffer_size,
            shm_buffer_path: String::from("/nihilphase_shm_buffer"),
            data: Arc::new(DaemonData::new()),
        }
    }

    pub fn run(self) {
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
        let shm_buffer = Arc::new(
            ShmBufferManager::new(&self.shm_buffer_path, self.shm_buffer_size)
                .map_err(|e| DaemonError::Io("create shared memory buffer", e))?,
        );
        let hostmem_buffer = Arc::new(HostMemBufferManager::new(
            self.ram_buffer_size,
            1024 * 1024 * 1024,
            load_config().preallocate_hostmem,
        ));
        let storage_buffer = Arc::new(
            StorageBufferManager::new(&self.buffer_path).map_err(DaemonError::HybridBuffer)?,
        );
        let data_manager = DataManagerHandle {
            shm: shm_buffer,
            hostmem: hostmem_buffer,
            storage: storage_buffer,
        };
        tracing::info!(
            "Shared memory buffer created at {}, size = {}",
            self.shm_buffer_path,
            pretty_size(self.shm_buffer_size as u64),
        );
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (exit_tx, mut exit_rx) = mpsc::unbounded_channel();
        let (rpc_data_tx, rpc_data_rx) = mpsc::unbounded_channel();
        let (sched_ctl_tx, sched_ctl_rx) = mpsc::unbounded_channel();
        let shm_buffer_path = self.shm_buffer_path.clone();
        // accept app connections
        tokio::spawn(Self::handle_processes(
            self.daemon_path,
            tx,
            exit_tx,
            sched_ctl_tx.clone(),
            rpc_data_tx,
            shm_buffer_path,
            self.shm_buffer_size,
        ));
        let list_handle = self.data.processes.clone();
        {
            let shm_buffer = data_manager.shm.clone();
            let hostmem_buffer = data_manager.hostmem.clone();
            let storage_buffer = data_manager.storage.clone();
            tokio::spawn(async move {
                // maintain app list
                loop {
                    tokio::select! {
                        Some(handle) = rx.recv() => {
                            list_handle.write().await.insert(handle.pid(), handle);
                        }
                        Some(pid) = exit_rx.recv() => {
                            list_handle.write().await.remove(&pid);
                            // release used buffer by the exited process
                            shm_buffer.release_process_residual(pid);
                            hostmem_buffer.release_process_residual(pid);
                            storage_buffer.release_process_residual(pid);
                        }
                    }
                }
            });
        }
        {
            let list_handle = self.data.processes.clone();
            let data_manager = data_manager.clone();
            tokio::spawn(async move {
                Scheduler::new(list_handle, rpc_data_rx, sched_ctl_rx, data_manager)
                    .run()
                    .await;
            });
        }

        let listener_guard = UnixListenerGuard::new(&self.control_path)?;

        // listen for client
        while let Ok((stream, _)) = listener_guard.get_listener().accept().await {
            let conn = tarpc::serde_transport::new(
                LengthDelimitedCodec::builder().new_framed(stream),
                tarpc::tokio_serde::formats::Cbor::default(),
            );
            let server = ControllableDaemon {
                data: self.data.clone(),
                control_tx: sched_ctl_tx.clone(),
                data_mgr_handle: data_manager.clone(),
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
        exit_tx: mpsc::UnboundedSender<i32>,
        sched_ctl_tx: mpsc::UnboundedSender<ScheduleControlReq>,
        rpc_data_tx: mpsc::UnboundedSender<(i32, ActivityUpdate)>,
        shm_buffer_path: String,
        shm_buffer_size: usize,
    ) -> Result<(), DaemonError> {
        let controller = UnixListenerGuard::new(daemon_path.as_path())?;
        tracing::info!("Daemon started at {:?}", daemon_path);
        loop {
            let (stream, _) = controller
                .get_listener()
                .accept()
                .await
                .map_err(|e| DaemonError::Io("accept connection", e))?;
            let future = DaemonServer::launch(
                stream,
                exit_tx.clone(),
                rpc_data_tx.clone(),
                sched_ctl_tx.clone(),
                shm_buffer_path.clone(),
                shm_buffer_size,
            );
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
    data_mgr_handle: DataManagerHandle,
    control_tx: mpsc::UnboundedSender<ScheduleControlReq>,
}

impl Controllable for ControllableDaemon {
    async fn list_pid(self, _context: Context) -> Vec<i32> {
        self.data.processes.read().await.keys().copied().collect()
    }

    async fn list_processes(self, _context: Context) -> Vec<ProcessMetadata> {
        let guard = self.data.processes.read().await;
        let handles: Vec<mpsc::UnboundedSender<ProcCtlReq>> =
            guard.values().map(|h| h.inst_tx()).collect();
        drop(guard);
        let futs: Vec<CallFuture<ProcessMetadata>> = handles
            .into_iter()
            .map(|tx| {
                let (para, fut) = CallParameter::new(());
                let _ = tx.send(ProcCtlReq::List(para));
                fut
            })
            .collect();
        let results = futures::future::join_all(futs).await;
        results.into_iter().flatten().collect()
    }

    async fn set_priority(
        self,
        _context: Context,
        args: SetPriorityArgs,
    ) -> Result<SetPriorityResponse, ()> {
        let guard = self.data.processes.read().await;
        if !guard.contains_key(&args.pid) {
            tracing::warn!("Process with pid {} not found", args.pid);
            return Err(());
        }
        drop(guard);
        let (para, ret_rx) = CallParameter::new(args);
        self.control_tx
            .send(ScheduleControlReq::SetPriority(para))
            .map_err(|_| ())?;
        ret_rx.await.ok_or(())
    }

    async fn get_history(
        self,
        _context: Context,
        args: GetHistoryArgs,
    ) -> Result<GetHistoryResult, ()> {
        let guard = self.data.processes.read().await;
        if !guard.contains_key(&args.pid) {
            tracing::warn!("Process with pid {} not found", args.pid);
            return Err(());
        }
        drop(guard);
        let (para, ret_rx) = CallParameter::new(args);
        self.control_tx
            .send(ScheduleControlReq::GetHistory(para))
            .map_err(|_| ())?;
        ret_rx.await.ok_or(())
    }

    async fn prefetch(self, _context: Context, args: PrefetchArgs) -> Result<PrefetchResponse, ()> {
        let guard = self.data.processes.read().await;
        for msg in &args.list {
            if !guard.contains_key(&msg.pid) {
                tracing::warn!("Process with pid {} not found", msg.pid);
                return Err(());
            }
        }
        drop(guard);
        let (para, ret_rx) = CallParameter::new(args);
        self.control_tx
            .send(ScheduleControlReq::Prefetch(para))
            .map_err(|_| ())?;
        ret_rx.await.ok_or(())
    }

    async fn update_config(self, _context: Context, config: ConfigurableArgs) {
        update_config(config);
    }

    async fn get_config(self, _context: Context) -> Config {
        crate::config::load_config().as_ref().clone()
    }

    async fn data_details(self, _context: Context) -> DataManagerMetadata {
        fn to_meta(
            buffers: HashMap<BufferId, AllocationCapacity>,
        ) -> Vec<control::ProcessDataMeta> {
            let mut procs = HashMap::<i32, Vec<control::DataBlockMeta>>::new();
            for (buf_id, size) in buffers {
                let entry = procs.entry(buf_id.pid).or_default();
                entry.push(control::DataBlockMeta {
                    device_id: buf_id.device_id,
                    size: size.0 as u64,
                });
            }
            procs
                .into_iter()
                .map(|(pid, blocks)| control::ProcessDataMeta {
                    pid,
                    data_blocks: blocks,
                })
                .collect()
        }
        let shm_size = self.data_mgr_handle.shm.capacity() as u64;
        let hostmem_size = self.data_mgr_handle.hostmem.capacity() as u64;
        DataManagerMetadata {
            shm: to_meta(self.data_mgr_handle.shm.dump_buffers()),
            hostmem: to_meta(self.data_mgr_handle.hostmem.dump_buffers()),
            storage: to_meta(self.data_mgr_handle.storage.dump_buffers()),
            shm_capacity: shm_size,
            hostmem_capacity: hostmem_size,
        }
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
        socket_chown(&path)?;
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
