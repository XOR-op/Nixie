use std::{
    collections::{BTreeSet, HashMap, HashSet},
    io::{IoSlice, IoSliceMut},
    num::NonZeroU32,
    sync::Arc,
    time::Duration,
};

use itertools::Itertools;
use nihil_common::{
    GlobalDeviceId, MigrationArgs, MigrationResponse, ProcessLocalDeviceId, general::pretty_size,
    rpc::SidecarClient,
};
use tokio::sync::mpsc;

use crate::{
    error::HybridBufferError,
    runtime::{
        daemon_server::DeviceOrdinalMapping,
        migration::{
            BufferLocation, DataManagerHandle,
            channel::{
                InDataReadyRx, InDataReadyTx, OutDataReadyRx, OutDataReadyTx, RequestForSpaceRx,
                RequestForSpaceTx, create_data_ready_channel, create_request_for_space_channel,
            },
            hostmem_buffer::HostMemBufferManager,
            shm_buffer::ShmBlock,
            storage_buffer::StorageBufferManager,
        },
    },
};

use super::{BufferId, ShmBufferManager};

macro_rules! warn_on_send_error {
    ($res:expr) => {
        if let Err(_) = $res {
            tracing::warn!("Failed to send on channel: {}", stringify!($res));
        }
    };
}

#[derive(Debug, Clone)]
pub struct MigrationSpecEntry {
    pub size: u32,
    pub handle_idx: NonZeroU32,
    // When ready is true, the buffer should be on GPU or in shm.
    pub ready_for_pcie_xfer: bool,
}

impl MigrationSpecEntry {
    pub fn to_buffer_id(&self, pid: i32, device_id: GlobalDeviceId) -> BufferId {
        BufferId {
            pid,
            device_id,
            block_id: self.handle_idx,
            size: self.size,
        }
    }
}

pub struct MigrationSpec {
    pub device_map: HashMap<GlobalDeviceId, Vec<MigrationSpecEntry>>,
}

pub struct DataMigrationTask<Client, Handle> {
    // movement involving client processes
    pub(super) out_from_gpu: Vec<(i32, MigrationSpec, Client, Arc<DeviceOrdinalMapping>)>,
    pub(super) into_gpu: Option<(i32, MigrationSpec, Client, Arc<DeviceOrdinalMapping>)>,

    // reorganization of buffers within daemon
    pub(super) storage_to_shm: Vec<BufferId>,
    pub(super) hostmem_to_shm: Vec<BufferId>,
    pub(super) shm_to_backend: HashMap<BufferId, BufferLocation>,
    pub(super) storage_to_hostmem: Vec<BufferId>,
    pub(super) hostmem_to_storage: Vec<BufferId>,

    pub(super) data_manager: Handle,
}

impl<Client, Handle> DataMigrationTask<Client, Handle> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        out_from_gpu: Vec<(i32, MigrationSpec, Client, Arc<DeviceOrdinalMapping>)>,
        into_gpu: Option<(i32, MigrationSpec, Client, Arc<DeviceOrdinalMapping>)>,
        storage_to_shm: Vec<BufferId>,
        host_mem_to_shm: Vec<BufferId>,
        shm_to_backend: HashMap<BufferId, BufferLocation>,
        storage_to_hostmem: Vec<BufferId>,
        hostmem_to_storage: Vec<BufferId>,
        data_manager: Handle,
    ) -> Self {
        Self {
            out_from_gpu,
            into_gpu,
            storage_to_shm,
            hostmem_to_shm: host_mem_to_shm,
            shm_to_backend,
            storage_to_hostmem,
            hostmem_to_storage,
            data_manager,
        }
    }

    pub fn json_summary(&self) -> String {
        let into_gpu_size = self
            .into_gpu
            .as_ref()
            .map(|into_gpu| {
                into_gpu
                    .1
                    .device_map
                    .values()
                    .flatten()
                    .map(|e| e.size as u64)
                    .sum::<u64>()
            })
            .unwrap_or_default();
        let income_pid_str = self
            .into_gpu
            .as_ref()
            .map(|(pid, _, _, _)| format!("{}", *pid))
            .unwrap_or("N/A".to_string());
        // size per pid
        let out_from_gpu_size = self
            .out_from_gpu
            .iter()
            .map(|(pid, specs, _, _)| {
                (
                    format!("{}", pid),
                    pretty_size(
                        specs
                            .device_map
                            .values()
                            .flatten()
                            .map(|e| e.size as u64)
                            .sum::<u64>(),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        let mut data = HashMap::new();
        if self.into_gpu.is_some() {
            data.insert(
                "shm -> gpu",
                HashMap::from([(income_pid_str.clone(), pretty_size(into_gpu_size))]),
            );
        }
        if !self.out_from_gpu.is_empty() {
            data.insert("gpu -> shm", out_from_gpu_size);
        }
        if !self.hostmem_to_shm.is_empty() {
            let hostmem_to_shm_mapping = self
                .hostmem_to_shm
                .iter()
                .map(|b| (format!("{}", b.pid), b.size))
                .into_group_map()
                .into_iter()
                .map(|(pid, sizes)| {
                    (
                        pid,
                        pretty_size(sizes.iter().map(|x| *x as u64).sum::<u64>()),
                    )
                })
                .collect::<HashMap<_, _>>();
            data.insert("hostmem -> shm", hostmem_to_shm_mapping);
        }
        if !self.storage_to_shm.is_empty() {
            let storage_to_shm_mapping = self
                .storage_to_shm
                .iter()
                .map(|b| (format!("{}", b.pid), b.size))
                .into_group_map()
                .into_iter()
                .map(|(pid, sizes)| {
                    (
                        pid,
                        pretty_size(sizes.iter().map(|x| *x as u64).sum::<u64>()),
                    )
                })
                .collect::<HashMap<_, _>>();
            data.insert("storage -> shm", storage_to_shm_mapping);
        }
        if !self.shm_to_backend.is_empty() {
            let shm_to_backend_mapping = self
                .shm_to_backend
                .keys()
                .map(|b| (format!("{}", b.pid), b.size))
                .into_group_map()
                .into_iter()
                .map(|(pid, sizes)| {
                    (
                        pid,
                        pretty_size(sizes.iter().map(|x| *x as u64).sum::<u64>()),
                    )
                })
                .collect::<HashMap<_, _>>();
            data.insert("shm -> backend", shm_to_backend_mapping);
        }
        if !self.storage_to_hostmem.is_empty() {
            let storage_to_hostmem_mapping = self
                .storage_to_hostmem
                .iter()
                .map(|b| (format!("{}", b.pid), b.size))
                .into_group_map()
                .into_iter()
                .map(|(pid, sizes)| {
                    (
                        pid,
                        pretty_size(sizes.iter().map(|x| *x as u64).sum::<u64>()),
                    )
                })
                .collect::<HashMap<_, _>>();
            data.insert("storage -> hostmem", storage_to_hostmem_mapping);
        }
        if !self.hostmem_to_storage.is_empty() {
            let hostmem_to_storage_mapping = self
                .hostmem_to_storage
                .iter()
                .map(|b| (format!("{}", b.pid), b.size))
                .into_group_map()
                .into_iter()
                .map(|(pid, sizes)| {
                    (
                        pid,
                        pretty_size(sizes.iter().map(|x| *x as u64).sum::<u64>()),
                    )
                })
                .collect::<HashMap<_, _>>();
            data.insert("hostmem -> storage", hostmem_to_storage_mapping);
        }
        serde_json::to_string(&data).unwrap_or_default()
    }
}

impl DataMigrationTask<SidecarClient, DataManagerHandle> {
    pub fn get_out_from_gpu(
        &self,
    ) -> &[(i32, MigrationSpec, SidecarClient, Arc<DeviceOrdinalMapping>)] {
        &self.out_from_gpu
    }

    pub async fn run(mut self) {
        let mut largest_transfer_size = [
            self.hostmem_to_shm
                .iter()
                .map(|b| b.size as u64)
                .sum::<u64>(),
            self.storage_to_shm
                .iter()
                .map(|b| b.size as u64)
                .sum::<u64>(),
            self.shm_to_backend
                .keys()
                .map(|b| b.size as u64)
                .sum::<u64>(),
            self.storage_to_hostmem
                .iter()
                .map(|b| b.size as u64)
                .sum::<u64>(),
            self.hostmem_to_storage
                .iter()
                .map(|b| b.size as u64)
                .sum::<u64>(),
        ]
        .into_iter()
        .max()
        .unwrap_or(0);

        // clustering by global device ID
        #[allow(clippy::type_complexity)]
        let mut src_per_device: HashMap<
            GlobalDeviceId,
            Vec<(
                i32,
                ProcessLocalDeviceId,
                SidecarClient,
                Vec<MigrationSpecEntry>,
            )>,
        > = HashMap::new();
        for (pid, spec, rpc_client, mapping) in self.out_from_gpu {
            for (device_id, entries) in spec.device_map {
                largest_transfer_size =
                    largest_transfer_size.max(entries.iter().map(|e| e.size as u64).sum::<u64>());
                src_per_device.entry(device_id).or_insert(Vec::new()).push((
                    pid,
                    mapping
                        .real_to_visible(device_id)
                        .unwrap_or_else(|| todo!("Handle missing device mapping")),
                    rpc_client.clone(),
                    entries,
                ));
            }
        }

        let (in_tx, device_junction, out_rx) = {
            let incoming_dev_map = self
                .into_gpu
                .as_ref()
                .map(|(_, spec, _, _)| spec.device_map.clone())
                .unwrap_or_default();
            create_data_ready_channel(
                src_per_device
                    .keys()
                    .chain(incoming_dev_map.keys())
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    .into_iter(),
                self.shm_to_backend.clone(),
            )
        };
        let (req_shm_tx, req_shm_rx) = create_request_for_space_channel();
        let mut task_handles = Vec::new();
        for (device, (in_rx, out_tx)) in device_junction {
            let src_list = src_per_device.remove(&device).unwrap_or_default();
            let shm_buffer_mgr = self.data_manager.shm.clone();
            let req_shm_tx = req_shm_tx.clone();
            let into_gpu = self.into_gpu.as_mut().map(|into_gpu| {
                let dst_entries = into_gpu.1.device_map.remove(&device).unwrap_or_default();
                // Run migration for each device
                let rpc_client = into_gpu.2.clone();
                let device_id = into_gpu
                    .3
                    .real_to_visible(device)
                    .unwrap_or_else(|| todo!("Handle missing device mapping"));
                largest_transfer_size = largest_transfer_size
                    .max(dst_entries.iter().map(|e| e.size as u64).sum::<u64>());
                (into_gpu.0, device_id, rpc_client, dst_entries)
            });

            task_handles.push(tokio::spawn(async move {
                Self::run_for_device(
                    device,
                    src_list,
                    into_gpu,
                    shm_buffer_mgr,
                    in_rx,
                    out_tx,
                    req_shm_tx,
                )
                .await;
            }));
        }
        // interact with host mem and storage
        if !self.hostmem_to_shm.is_empty() {
            task_handles.push({
                let shm_buffer_mgr = self.data_manager.shm.clone();
                let hostmem_buffer_mgr = self.data_manager.hostmem.clone();
                let in_tx = in_tx.clone();
                let req_shm_tx = req_shm_tx.clone();
                tokio::spawn(async move {
                    backend_to_shm_transfer(
                        self.hostmem_to_shm,
                        in_tx,
                        shm_buffer_mgr,
                        BackendManager::HostMem(hostmem_buffer_mgr),
                        req_shm_tx,
                    )
                    .await
                })
            });
        }

        if !self.storage_to_shm.is_empty() {
            task_handles.push({
                let shm_buffer_mgr = self.data_manager.shm.clone();
                let storage_buffer_mgr = self.data_manager.storage.clone();
                let in_tx = in_tx.clone();
                let req_shm_tx = req_shm_tx.clone();
                tokio::spawn(async move {
                    backend_to_shm_transfer(
                        self.storage_to_shm,
                        in_tx,
                        shm_buffer_mgr,
                        BackendManager::Storage(storage_buffer_mgr),
                        req_shm_tx,
                    )
                    .await
                })
            });
        }

        // should always be enabled for spill over
        task_handles.push({
            let hostmem_buffer_mgr = self.data_manager.hostmem.clone();
            let storage_buffer_mgr = self.data_manager.storage.clone();
            tokio::spawn(async move {
                shm_to_backend_transfer(
                    self.into_gpu
                        .as_ref()
                        .map(|(pid, _, _, _)| *pid)
                        .into_iter()
                        .collect(),
                    self.shm_to_backend,
                    out_rx,
                    self.data_manager.shm.clone(),
                    hostmem_buffer_mgr,
                    storage_buffer_mgr,
                    req_shm_rx,
                )
                .await
            })
        });

        if !self.hostmem_to_storage.is_empty() {
            task_handles.push({
                let hostmem_buffer_mgr = self.data_manager.hostmem.clone();
                let storage_buffer_mgr = self.data_manager.storage.clone();
                tokio::spawn(async move {
                    hostmem_to_storage_transfer(
                        self.hostmem_to_storage,
                        hostmem_buffer_mgr,
                        storage_buffer_mgr,
                    )
                    .await
                })
            });
        }

        if !self.storage_to_hostmem.is_empty() {
            task_handles.push({
                let hostmem_buffer_mgr = self.data_manager.hostmem.clone();
                let storage_buffer_mgr = self.data_manager.storage.clone();
                tokio::spawn(async move {
                    storage_to_hostmem_transfer(
                        self.storage_to_hostmem,
                        hostmem_buffer_mgr,
                        storage_buffer_mgr,
                    )
                    .await
                })
            });
        }
        drop(in_tx);
        drop(req_shm_tx);

        let ts_start = std::time::Instant::now();
        // Wait for all tasks to complete
        let _ = futures::future::join_all(task_handles).await;
        let elapsed = ts_start.elapsed();
        if largest_transfer_size > 0 {
            tracing::debug!(
                "Data migration completed in {:.3}s, largest transfer size = {}, speed = {:.3} GB/s",
                elapsed.as_secs_f64(),
                pretty_size(largest_transfer_size),
                (largest_transfer_size as f64 / elapsed.as_secs_f64() / 1e9)
            );
        } else {
            tracing::debug!("No migration needed");
        }
    }

    async fn run_for_device(
        global_id: GlobalDeviceId,
        out_from_gpu: Vec<(
            i32,
            ProcessLocalDeviceId,
            SidecarClient,
            Vec<MigrationSpecEntry>,
        )>,
        into_gpu: Option<(
            i32,
            ProcessLocalDeviceId,
            SidecarClient,
            Vec<MigrationSpecEntry>,
        )>,
        shm_buffer_mgr: Arc<ShmBufferManager>,
        in_data_ready_rx: InDataReadyRx,
        out_data_ready_tx: OutDataReadyTx,
        req_shm_tx: RequestForSpaceTx,
    ) {
        let (transfer_token_tx, transfer_token_rx) =
            tokio::sync::mpsc::unbounded_channel::<MigrationResponse>();
        // H2D direction
        let h2d_handle = into_gpu.map(|into_gpu| {
            tokio::spawn(host_to_device_transfer(
                global_id,
                into_gpu,
                shm_buffer_mgr.clone(),
                transfer_token_rx,
                in_data_ready_rx,
            ))
        });
        // D2H direction
        device_to_host_transfer(
            global_id,
            out_from_gpu,
            shm_buffer_mgr,
            transfer_token_tx,
            out_data_ready_tx,
            req_shm_tx,
        )
        .await;
        if let Some(h2d_handle) = h2d_handle {
            let _ = h2d_handle.await;
        }
    }
}

async fn device_to_host_transfer(
    global_id: GlobalDeviceId,
    out_from_gpu: Vec<(
        i32,
        ProcessLocalDeviceId,
        SidecarClient,
        Vec<MigrationSpecEntry>,
    )>,
    shm_buffer_mgr: Arc<ShmBufferManager>,
    gpu_mem_token_tx: mpsc::UnboundedSender<MigrationResponse>,
    out_data_ready_tx: OutDataReadyTx,
    req_for_shm: RequestForSpaceTx,
) {
    let total = out_from_gpu
        .iter()
        .map(|(_, _, _, entries)| entries.len())
        .sum::<usize>();
    let mut moved_cnt = 0;
    for (out_from_gpu_pid, device, rpc_client, out_from_gpu_entries) in out_from_gpu {
        // Migrate each entry
        let timeout = Duration::from_secs(5);
        for out_from_gpu_entry in out_from_gpu_entries {
            let src_buffer_id = BufferId {
                pid: out_from_gpu_pid,
                device_id: global_id,
                block_id: out_from_gpu_entry.handle_idx,
                size: out_from_gpu_entry.size,
            };
            let blocks = match shm_buffer_mgr.try_reserve(&src_buffer_id) {
                Some(blocks) => blocks,
                None => {
                    // No shm is immediately available; wait for shm->backend to free up space
                    req_for_shm.prio_request();
                    match shm_buffer_mgr
                        .reserve_with_max_pending(&src_buffer_id, 0, Some(Duration::from_secs(18)))
                        .await
                    {
                        Ok(blocks) => blocks,
                        Err(_) => {
                            // No shm is available given the plan; we need to notify shm -> backend to free up space
                            req_for_shm.request(1);
                            match shm_buffer_mgr
                                .reserve_with_timeout(&src_buffer_id, Some(timeout))
                                .await
                            {
                                Ok(offset) => offset,
                                Err(_) => {
                                    tracing::warn!(
                                        "Failed to reserve shm for gpu->shm: {:?} for timeout {:?}; moved {}/{}",
                                        src_buffer_id,
                                        timeout,
                                        moved_cnt,
                                        total
                                    );
                                    tokio::time::sleep(Duration::from_secs(3600)).await;
                                    return;
                                }
                            }
                        }
                    }
                }
            };

            let args = MigrationArgs {
                host_buffer_offset: blocks.iter().map(|b| b.offset.0).collect(),
                size: blocks.iter().map(|b| b.data_size).collect(),
                device,
                handle_idx: out_from_gpu_entry.handle_idx,
                host_to_device: false,
            };
            // Send migration request to the source process
            if let Ok(resp) = rpc_client.migrate(tarpc::context::current(), args).await {
                // Rx may close early if the client is requiring space for allocation
                let _ = gpu_mem_token_tx.send(resp);
                // Rx may close early if no shm to backend transfer is needed
                let _ = out_data_ready_tx.send(src_buffer_id);
            } else {
                tracing::warn!("Failed to complete D2H migration RPC to source process");
            }
            moved_cnt += 1;
        }
    }
    drop(gpu_mem_token_tx); // close the channel
    tracing::trace!("D2H migration moved {} buffers", total);
}

macro_rules! warn_no_buffer_id {
    ($res:expr) => {
        if let Err(buf_id) = $res {
            tracing::warn!("Buffer ID {:?} not found in shm buffer manager", buf_id);
            return;
        }
    };
}

#[allow(unused_variables)]
async fn host_to_device_transfer(
    global_id: GlobalDeviceId,
    into_gpu: (
        i32,
        ProcessLocalDeviceId,
        SidecarClient,
        Vec<MigrationSpecEntry>,
    ),
    shm_buffer_mgr: Arc<ShmBufferManager>,
    mut gpu_mem_token_rx: mpsc::UnboundedReceiver<MigrationResponse>,
    mut data_available_rx: InDataReadyRx,
) {
    let (dst_pid, dst_device, dst_rpc_client, dst_entries) = into_gpu;
    let (mut dst_entries, mut pending_dst_entries): (Vec<_>, HashSet<_>) =
        dst_entries.into_iter().rev().partition_map(|e| {
            if e.ready_for_pcie_xfer {
                itertools::Either::Left(e.to_buffer_id(dst_pid, global_id))
            } else {
                itertools::Either::Right(e.to_buffer_id(dst_pid, global_id))
            }
        });

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum BufferSource {
        Ready,
        Pending,
    }

    let dst_count = dst_entries.len();
    let pending_count = pending_dst_entries.len();
    let mut accu_length = 0;
    let mut next_entry = dst_entries.pop().map(|i| (i, BufferSource::Ready));

    let mut dst_processed = 0;
    let mut pending_processed = 0;
    let mut token_received = 0;
    let mut token_not_enough = 0;

    loop {
        if next_entry.is_none() {
            // get next entry when dst_entries is depleted
            while next_entry.is_none()
                && !pending_dst_entries.is_empty()
                && let Some((buffer_id, _)) = data_available_rx.recv().await
            {
                if pending_dst_entries.remove(&buffer_id) {
                    tracing::trace!(
                        "To H2D: {{\"pid\": {}, \"block_id\":,\"{}\"  \"size\": \"{}\"}}",
                        buffer_id.pid,
                        buffer_id.block_id,
                        pretty_size(buffer_id.size)
                    );
                    next_entry = Some((buffer_id, BufferSource::Pending));
                } else {
                    tracing::warn!("Received unexpected buffer ID to H2D: {:?}", buffer_id);
                }
            }
            if next_entry.is_none() {
                // no more entries to process
                tracing::trace!(
                    "H2D migration moved ({}+{})/({}+{}) buffers; token_not_enough = {}",
                    dst_processed,
                    pending_processed,
                    dst_count,
                    pending_count,
                    token_not_enough
                );
                if dst_processed != dst_count || pending_processed != pending_count {
                    tracing::warn!(
                        "H2D migration incomplete: moved ({}+{})/({}+{}) buffers; token_not_enough = {}",
                        dst_processed,
                        pending_processed,
                        dst_count,
                        pending_count,
                        token_not_enough
                    );
                }
                return;
            }
        }

        let (buffer_id, buf_source) = next_entry.as_ref().unwrap();
        let buf_source = *buf_source;
        // get gpu tokens if we need
        if !gpu_mem_token_rx.is_closed()
            && let Some(d2h_resp) = gpu_mem_token_rx.recv().await
        {
            accu_length += d2h_resp.size;

            token_received += 1;

            if accu_length >= buffer_id.size as u64 {
                accu_length -= buffer_id.size as u64;
            } else {
                // no enough vram; wait for more
                token_not_enough += 1;
                continue;
            }
        }
        warn_no_buffer_id!(
            host_to_device_transfer_inner(
                next_entry.take().unwrap().0,
                dst_device,
                &dst_rpc_client,
                &shm_buffer_mgr,
            )
            .await
        );
        match buf_source {
            BufferSource::Ready => dst_processed += 1,
            BufferSource::Pending => pending_processed += 1,
        }
        next_entry = dst_entries.pop().map(|i| (i, BufferSource::Ready));
    }
}

macro_rules! panic_on_error {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(e) => {
                tracing::error!("Hybrid buffer operation failed: {:?}", e);
                panic!("Hybrid buffer operation failed: {:?}", e);
            }
        }
    };
}

async fn host_to_device_transfer_inner(
    buffer_id: BufferId,
    dst_device: ProcessLocalDeviceId,
    rpc_client: &SidecarClient,
    shm_buffer_mgr: &ShmBufferManager,
) -> Result<(), BufferId> {
    let blocks = shm_buffer_mgr
        .get_buffer(&buffer_id)
        .ok_or(buffer_id.clone())?;

    let args = MigrationArgs {
        host_buffer_offset: blocks.iter().map(|b| b.offset.0).collect(),
        size: blocks.iter().map(|b| b.data_size).collect(),
        device: dst_device,
        handle_idx: buffer_id.block_id,
        host_to_device: true,
    };
    if rpc_client
        .migrate(tarpc::context::current(), args)
        .await
        .is_ok()
    {
        shm_buffer_mgr
            .release(&buffer_id)
            .expect("Failed to release buffer after migration");
    } else {
        tracing::warn!("Failed to complete H2D migration RPC to destination process");
    }
    Ok(())
}

enum BackendManager {
    HostMem(Arc<HostMemBufferManager>),
    Storage(Arc<StorageBufferManager>),
}

async fn backend_to_shm_transfer(
    host_mem_to_shm: Vec<BufferId>,
    in_data_ready_tx: InDataReadyTx,
    shm_buffer_mgr: Arc<ShmBufferManager>,
    backend_mgr: BackendManager,
    req_for_shm: RequestForSpaceTx,
) {
    let mut moved_cnt = 0;
    let total = host_mem_to_shm.len();
    let timeout = Duration::from_secs(6);
    for buffer_id in host_mem_to_shm {
        let blocks = match shm_buffer_mgr.try_reserve(&buffer_id) {
            Some(blocks) => blocks,
            None => {
                req_for_shm.prio_request();
                match shm_buffer_mgr
                    .reserve_with_max_pending(&buffer_id, 0, Some(Duration::from_secs(30)))
                    .await
                {
                    Ok(offset) => offset,
                    Err(_) => {
                        // No shm is available given the plan; we need to notify shm -> backend to free up space
                        req_for_shm.request(1);
                        match shm_buffer_mgr
                            .reserve_with_timeout(&buffer_id, Some(timeout))
                            .await
                        {
                            Ok(blocks) => blocks,
                            Err(_) => {
                                tracing::warn!(
                                    "Failed to reserve shm for backend->shm; buffer {:?} after {:?}; moved {}/{}",
                                    buffer_id,
                                    timeout,
                                    moved_cnt,
                                    total
                                );
                                tokio::time::sleep(Duration::from_secs(3600)).await;
                                return;
                            }
                        }
                    }
                }
            }
        };
        let mut buf = unsafe {
            get_buffer_ref_mut(
                convert_to_static(&shm_buffer_mgr), // safety: the lifetime of the buffer will not exceed the end of the block
                &blocks,
            )
        };
        match &backend_mgr {
            BackendManager::HostMem(mgr) => {
                panic_on_error!(mgr.load_to_vectored(&buffer_id, &mut buf));
                warn_on_send_error!(
                    in_data_ready_tx.send(buffer_id.clone(), BufferLocation::HostMem)
                );
            }
            BackendManager::Storage(mgr) => {
                let buf_id = buffer_id.clone();
                let mgr = mgr.clone();
                panic_on_error!(panic_on_error!(
                    tokio::task::spawn_blocking(move || mgr.load_to_vectored(&buf_id, &mut buf))
                        .await
                ));
                warn_on_send_error!(
                    in_data_ready_tx.send(buffer_id.clone(), BufferLocation::Storage)
                );
            }
        }

        moved_cnt += 1;
        tracing::trace!(
            "[Ongoing] Moved {}/{} buffers from backend to shm: {{\"pid\": {}, \"block_id\":,\"{}\"  \"size\": \"{}\"}}",
            moved_cnt,
            total,
            buffer_id.pid,
            buffer_id.block_id,
            pretty_size(buffer_id.size)
        );
    }
    if total > 0 {
        tracing::debug!("Moved {}/{} buffers from backend to shm", moved_cnt, total);
    }
}

async fn shm_to_backend_transfer(
    excluding_list: HashSet<i32>,
    shm_to_hybrid: HashMap<BufferId, BufferLocation>,
    mut out_data_ready_rx: OutDataReadyRx,
    shm_buffer_mgr: Arc<ShmBufferManager>,
    hostmem_buffer_mgr: Arc<HostMemBufferManager>,
    storage_buffer_mgr: Arc<StorageBufferManager>,
    mut req_for_shm_rx: RequestForSpaceRx,
) {
    fn check_location(buffer_id: &BufferId, expected: BufferLocation, actual: BufferLocation) {
        if expected != actual {
            match expected {
                BufferLocation::HostMem => {
                    tracing::debug!(
                        "Buffer {:?} expected to be in host memory, but found in storage",
                        buffer_id
                    );
                }
                BufferLocation::Storage => {
                    tracing::warn!(
                        "Buffer {:?} expected to be in storage, but found in host memory",
                        buffer_id
                    );
                }
                other => {
                    tracing::error!(
                        "Unexpected buffer location: {:?} for {:?}",
                        other,
                        buffer_id
                    );
                }
            }
        }
    }

    let mut extra_move = 0;

    let mut next_shm_handling = HashMap::new();
    for (buf_id, expected_location) in shm_to_hybrid {
        let location = match shm_to_backend_transfer_inner(
            &buf_id,
            &shm_buffer_mgr,
            &hostmem_buffer_mgr,
            &storage_buffer_mgr,
            expected_location,
        )
        .await
        {
            Ok(loc) => loc,
            Err(HybridBufferError::NoBufferId) => {
                next_shm_handling.insert(buf_id, expected_location);
                continue;
            }
            Err(e) => {
                panic!("Hybrid buffer operation failed: {:?}", e);
            }
        };
        check_location(&buf_id, expected_location, location);
    }
    // Handle any remaining buffers that were not found
    while !(next_shm_handling.is_empty()) {
        let Some((buf_id, expected_location)) = out_data_ready_rx.recv().await else {
            break;
        };
        if let Some(expected_location) = next_shm_handling.remove(&buf_id) {
            let shm_buffer_mgr = shm_buffer_mgr.clone();
            let hostmem_buffer_mgr = hostmem_buffer_mgr.clone();
            let storage_buffer_mgr = storage_buffer_mgr.clone();
            // spawn for multithreading
            // TODO: profiling
            tokio::spawn(async move {
                let location = panic_on_error!(
                    shm_to_backend_transfer_inner(
                        &buf_id,
                        &shm_buffer_mgr,
                        &hostmem_buffer_mgr,
                        &storage_buffer_mgr,
                        expected_location
                    )
                    .await
                );
                check_location(&buf_id, expected_location, location);
            });
        } else {
            tracing::warn!(
                "Received unexpected buffer ID to {:?}: {:?}",
                expected_location,
                buf_id
            );
        }
    }
    loop {
        // condition for releasing shm space
        if !shm_buffer_mgr.is_full() {
            match req_for_shm_rx.listen().await {
                // new request
                Some(()) => {}
                None => {
                    // no more needed
                    break;
                }
            }
        }
        // Release space for error in plan
        let Some((buf_id, _)) =
            shm_buffer_mgr.find(|buf_id, _| !excluding_list.contains(&buf_id.pid))
        else {
            tracing::warn!(
                "No buffer can be released to satisfy shm space request for pid {:?}",
                excluding_list
            );
            continue;
        };
        tracing::debug!(
            "Releasing buffer [pid: {}, size: {}] to satisfy shm space request for pid {:?}",
            buf_id.pid,
            pretty_size(buf_id.size),
            excluding_list
        );
        extra_move += 1;

        // always try to move to hostmem first
        panic_on_error!(
            shm_to_backend_transfer_inner(
                &buf_id,
                &shm_buffer_mgr,
                &hostmem_buffer_mgr,
                &storage_buffer_mgr,
                BufferLocation::HostMem
            )
            .await
        );
    }
    tracing::trace!(
        "Shm to backend migration completed with {} extra moves",
        extra_move
    );
}

async fn hostmem_to_storage_transfer(
    list: Vec<BufferId>,
    hostmem_mgr: Arc<HostMemBufferManager>,
    storage_mgr: Arc<StorageBufferManager>,
) {
    for buffer_id in list {
        let Some(buf) = hostmem_mgr.pop_buffer(&buffer_id) else {
            tracing::warn!(
                "Buffer ID {:?} not found in host memory buffer manager",
                buffer_id
            );
            continue;
        };
        let storage_mgr = storage_mgr.clone();
        let buf = panic_on_error!(
            tokio::task::spawn_blocking(move || {
                let buf_vec = buf
                    .iter()
                    .map(|x| IoSlice::new(x.0.iter().as_slice()))
                    .collect::<Vec<_>>();
                panic_on_error!(storage_mgr.store_vectored(&buffer_id, &buf_vec));
                buf
            })
            .await
        );
        hostmem_mgr.put_back_mem(buf);
    }
}

async fn storage_to_hostmem_transfer(
    list: Vec<BufferId>,
    hostmem_mgr: Arc<HostMemBufferManager>,
    storage_mgr: Arc<StorageBufferManager>,
) {
    for buffer_id in list {
        let Some(mut buf) = hostmem_mgr.allocate_empty_buffer(buffer_id.size as usize) else {
            tracing::warn!(
                "No enough free buffer in host memory buffer manager to load buffer ID {:?}",
                buffer_id
            );
            continue;
        };
        assert_eq!(
            buf.iter().map(|x| x.0.len()).sum::<usize>(),
            buffer_id.size as usize
        );
        let storage_mgr = storage_mgr.clone();
        let buf_id = buffer_id.clone();
        let buf = panic_on_error!(
            tokio::task::spawn_blocking(move || {
                let mut slices = buf
                    .iter_mut()
                    .map(|x| IoSliceMut::new(&mut x.0))
                    .collect::<Vec<_>>();
                panic_on_error!(storage_mgr.load_to_vectored(&buf_id, &mut slices));
                buf
            })
            .await
        );
        hostmem_mgr.return_associated_buffer(buffer_id, buf);
    }
}

// Note: converting to &mut [u8] from an immutable reference is actually unsafe,
// but we need this for partial ownership of the buffer.
// The same buffer chunks should no be accessed concurrently. Just no compiler guarantee.
#[allow(clippy::mut_from_ref)]
unsafe fn get_buffer_ref_mut<'a>(
    shm_buffer_mgr: &'a ShmBufferManager,
    blocks: &[ShmBlock],
) -> Vec<IoSliceMut<'a>> {
    let mut res = Vec::with_capacity(blocks.len());
    for block in blocks {
        let slice = unsafe {
            std::slice::from_raw_parts_mut(
                shm_buffer_mgr
                    .at_offset(block.offset.0, block.data_size as usize)
                    .unwrap(),
                block.data_size as usize,
            )
        };
        res.push(IoSliceMut::new(slice));
    }
    res
}

unsafe fn get_buffer_ref<'a>(
    shm_buffer_mgr: &'a ShmBufferManager,
    blocks: &[ShmBlock],
) -> Vec<IoSlice<'a>> {
    let mut res = Vec::with_capacity(blocks.len());
    for block in blocks {
        let slice = unsafe {
            std::slice::from_raw_parts(
                shm_buffer_mgr
                    .at_offset(block.offset.0, block.data_size as usize)
                    .unwrap(),
                block.data_size as usize,
            )
        };
        res.push(IoSlice::new(slice));
    }
    res
}

unsafe fn convert_to_static<T>(r: &T) -> &'static T {
    unsafe { &*(r as *const T) }
}

async fn shm_to_backend_transfer_inner(
    buffer_id: &BufferId,
    shm_buffer_mgr: &ShmBufferManager,
    hostmem_buffer_mgr: &HostMemBufferManager,
    storage_buffer_mgr: &Arc<StorageBufferManager>,
    mut target_loc: BufferLocation,
) -> Result<BufferLocation, HybridBufferError> {
    let blocks = shm_buffer_mgr
        .get_buffer(buffer_id)
        .ok_or(HybridBufferError::NoBufferId)?;
    // Safety: the lifetime of the buffer will not exceed the end of the block
    let buf_ref = unsafe { get_buffer_ref(convert_to_static(shm_buffer_mgr), &blocks) };
    assert!(target_loc == BufferLocation::HostMem || target_loc == BufferLocation::Storage);

    if target_loc == BufferLocation::HostMem {
        match hostmem_buffer_mgr.store_vectored(buffer_id, &buf_ref) {
            Ok(_) => {}
            Err(HybridBufferError::MemoryExhausted) => {
                target_loc = BufferLocation::Storage;
            }
            Err(e) => return Err(e),
        }
    }

    if target_loc == BufferLocation::Storage {
        let buf_id = buffer_id.clone();
        let storage_buffer_mgr = storage_buffer_mgr.clone();
        tokio::task::spawn_blocking(move || storage_buffer_mgr.store_vectored(&buf_id, &buf_ref))
            .await??;
    }

    shm_buffer_mgr
        .release(buffer_id)
        .expect("BufferId released twice");
    Ok(target_loc)
}
