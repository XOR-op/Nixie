use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
    sync::Arc,
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
            BufferLocation,
            channel::{
                InDataReadyRx, InDataReadyTx, OutDataReadyRx, OutDataReadyTx,
                create_data_ready_channel,
            },
            hostmem_buffer::HostMemBufferManager,
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
    pub size: u64,
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

pub struct DataMigrationTask {
    // movement involving client processes
    out_from_gpu: Vec<(i32, MigrationSpec, SidecarClient, Arc<DeviceOrdinalMapping>)>,
    into_gpu: (i32, MigrationSpec, SidecarClient, Arc<DeviceOrdinalMapping>),

    // reorganization of buffers within daemon
    storage_to_shm: Vec<BufferId>,
    hostmem_to_shm: Vec<BufferId>,
    shm_to_backend: HashMap<BufferId, BufferLocation>,
    storage_to_hostmem: Vec<BufferId>,
    hostmem_to_storage: Vec<BufferId>,

    shm_buffer_mgr: Arc<ShmBufferManager>,
    hostmem_buffer_mgr: Arc<HostMemBufferManager>,
    storage_buffer_mgr: Arc<StorageBufferManager>,
}

impl DataMigrationTask {
    pub fn new(
        out_from_gpu: Vec<(i32, MigrationSpec, SidecarClient, Arc<DeviceOrdinalMapping>)>,
        into_gpu: (i32, MigrationSpec, SidecarClient, Arc<DeviceOrdinalMapping>),
        storage_to_shm: Vec<BufferId>,
        host_mem_to_shm: Vec<BufferId>,
        shm_to_backend: HashMap<BufferId, BufferLocation>,
        storage_to_host_mem: Vec<BufferId>,
        host_mem_to_storage: Vec<BufferId>,
        shm_buffer_mgr: Arc<ShmBufferManager>,
        hostmem_buffer_mgr: Arc<HostMemBufferManager>,
        storage_buffer_mgr: Arc<StorageBufferManager>,
    ) -> Self {
        assert!(
            storage_to_host_mem.is_empty() && host_mem_to_storage.is_empty(),
            "not implemented yet"
        );
        Self {
            out_from_gpu,
            into_gpu,
            storage_to_shm,
            hostmem_to_shm: host_mem_to_shm,
            shm_to_backend,
            storage_to_hostmem: storage_to_host_mem,
            hostmem_to_storage: host_mem_to_storage,
            shm_buffer_mgr,
            hostmem_buffer_mgr,
            storage_buffer_mgr,
        }
    }

    pub fn get_out_from_gpu(
        &self,
    ) -> &[(i32, MigrationSpec, SidecarClient, Arc<DeviceOrdinalMapping>)] {
        &self.out_from_gpu
    }

    pub async fn run(mut self) {
        let mut largest_transfer_size = 0;
        // clustering by global device ID
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
                    largest_transfer_size.max(entries.iter().map(|e| e.size).sum::<u64>());
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
        let (in_tx, device_junction, out_rx) = create_data_ready_channel(
            src_per_device
                .keys()
                .chain(self.into_gpu.1.device_map.keys())
                .cloned(),
            self.shm_to_backend.clone(),
        );
        let mut task_handles = Vec::new();
        for (device, (in_rx, out_tx)) in device_junction {
            let src_list = src_per_device.remove(&device).unwrap_or_default();
            let dst_entries = self
                .into_gpu
                .1
                .device_map
                .remove(&device)
                .unwrap_or_default();
            // Run migration for each device
            let shm_buffer_mgr = Arc::clone(&self.shm_buffer_mgr);
            let rpc_client = self.into_gpu.2.clone();
            let device_id = self
                .into_gpu
                .3
                .real_to_visible(device)
                .unwrap_or_else(|| todo!("Handle missing device mapping"));
            largest_transfer_size =
                largest_transfer_size.max(dst_entries.iter().map(|e| e.size).sum::<u64>());
            task_handles.push(tokio::spawn(async move {
                Self::run_for_device(
                    device,
                    src_list,
                    (self.into_gpu.0, device_id, rpc_client, dst_entries),
                    shm_buffer_mgr,
                    in_rx,
                    out_tx,
                )
                .await;
            }));
        }
        // interact with host mem and storage
        task_handles.push({
            let shm_buffer_mgr = self.shm_buffer_mgr.clone();
            let hostmem_buffer_mgr = self.hostmem_buffer_mgr.clone();
            let in_tx = in_tx.clone();
            tokio::spawn(async move {
                hostmem_to_shm_transfer(
                    self.hostmem_to_shm,
                    in_tx,
                    shm_buffer_mgr,
                    hostmem_buffer_mgr,
                )
                .await
            })
        });

        task_handles.push({
            let shm_buffer_mgr = self.shm_buffer_mgr.clone();
            let storage_buffer_mgr = self.storage_buffer_mgr.clone();
            let in_tx = in_tx.clone();
            tokio::spawn(async move {
                storage_to_shm_transfer(
                    self.storage_to_shm,
                    in_tx,
                    shm_buffer_mgr,
                    storage_buffer_mgr,
                )
                .await
            })
        });

        task_handles.push(tokio::spawn(async move {
            shm_to_backend_transfer(
                self.shm_to_backend,
                out_rx,
                self.shm_buffer_mgr.clone(),
                self.hostmem_buffer_mgr.clone(),
                self.storage_buffer_mgr.clone(),
            )
            .await
        }));
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
        into_gpu: (
            i32,
            ProcessLocalDeviceId,
            SidecarClient,
            Vec<MigrationSpecEntry>,
        ),
        shm_buffer_mgr: Arc<ShmBufferManager>,
        in_data_ready_rx: InDataReadyRx,
        out_data_ready_tx: OutDataReadyTx,
    ) {
        let (transfer_token_tx, transfer_token_rx) =
            tokio::sync::mpsc::unbounded_channel::<MigrationResponse>();
        // H2D direction
        let h2d_handle = tokio::spawn(host_to_device_transfer(
            global_id,
            into_gpu,
            shm_buffer_mgr.clone(),
            transfer_token_rx,
            in_data_ready_rx,
        ));
        // D2H direction
        device_to_host_transfer(
            global_id,
            out_from_gpu,
            shm_buffer_mgr,
            transfer_token_tx,
            out_data_ready_tx,
        )
        .await;
        let _ = h2d_handle.await;
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
) {
    for (out_from_gpu_pid, device, rpc_client, out_from_gpu_entries) in out_from_gpu {
        // Migrate each entry
        for out_from_gpu_entry in out_from_gpu_entries {
            let src_buffer_id = BufferId {
                pid: out_from_gpu_pid,
                device_id: global_id,
                block_id: out_from_gpu_entry.handle_idx,
                size: out_from_gpu_entry.size,
            };
            // Reserve shared memory for the migration
            if let Some(offset) = shm_buffer_mgr.try_reserve(&src_buffer_id) {
                let args = MigrationArgs {
                    host_buffer_offset: offset,
                    size: out_from_gpu_entry.size,
                    device,
                    handle_idx: out_from_gpu_entry.handle_idx,
                    host_to_device: false,
                };
                // Send migration request to the source process
                if let Ok(resp) = rpc_client.migrate(tarpc::context::current(), args).await {
                    warn_on_send_error!(gpu_mem_token_tx.send(resp));
                    warn_on_send_error!(out_data_ready_tx.send(src_buffer_id));
                } else {
                    tracing::warn!("Failed to complete D2H migration RPC to source process");
                }
            } else {
                tracing::warn!(
                    "Failed to reserve shared memory for migration: {:?}",
                    src_buffer_id
                );
            }
        }
    }
    drop(gpu_mem_token_tx); // close the channel
}

macro_rules! xfer_fallback {
    ($res:expr, $pending_set:expr) => {
        if let Err(failed_buf_id) = $res {
            tracing::warn!(
                "Failed to transfer buffer {:?} to device; enqueued",
                failed_buf_id
            );
            $pending_set.insert(failed_buf_id);
        }
    };
}

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

    let mut accu_length = 0;
    let mut next_entry = dst_entries.pop();
    while let Some(d2h_resp) = gpu_mem_token_rx.recv().await {
        accu_length += d2h_resp.size;
        if let Some(buffer_id) = next_entry.take() {
            if accu_length >= buffer_id.size {
                accu_length -= buffer_id.size;
                xfer_fallback!(
                    host_to_device_transfer_inner(
                        buffer_id,
                        dst_device,
                        &dst_rpc_client,
                        &shm_buffer_mgr,
                    )
                    .await,
                    pending_dst_entries
                );
                next_entry = dst_entries.pop();
            }
        }
    }
    // If there are remaining entries, we need to handle them
    while let Some(buffer_id) = next_entry {
        xfer_fallback!(
            host_to_device_transfer_inner(buffer_id, dst_device, &dst_rpc_client, &shm_buffer_mgr)
                .await,
            pending_dst_entries
        );
        next_entry = dst_entries.pop();
    }

    // Now we need to process the pending entries
    while !pending_dst_entries.is_empty()
        && let Some((buffer_id, _)) = data_available_rx.recv().await
    {
        if pending_dst_entries.remove(&buffer_id) {
            if let Err(buf_id) = host_to_device_transfer_inner(
                buffer_id,
                dst_device,
                &dst_rpc_client,
                &shm_buffer_mgr,
            )
            .await
            {
                tracing::warn!("Failed to transfer buffer {:?} to device", buf_id);
            }
        } else {
            tracing::warn!("Received unexpected buffer ID to H2D: {:?}", buffer_id);
        }
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
    let offset = shm_buffer_mgr
        .get_buffer(&buffer_id)
        .ok_or(buffer_id.clone())?
        .addr;
    let args = MigrationArgs {
        host_buffer_offset: offset,
        size: buffer_id.size,
        device: dst_device,
        handle_idx: buffer_id.block_id,
        host_to_device: true,
    };
    if let Ok(_) = rpc_client.migrate(tarpc::context::current(), args).await {
        shm_buffer_mgr
            .release(&buffer_id)
            .expect("Failed to release buffer after migration");
    } else {
        tracing::warn!("Failed to complete H2D migration RPC to destination process");
    }
    Ok(())
}

async fn hostmem_to_shm_transfer(
    host_mem_to_shm: Vec<BufferId>,
    in_data_ready_tx: InDataReadyTx,
    shm_buffer_mgr: Arc<ShmBufferManager>,
    hostmem_buffer_mgr: Arc<HostMemBufferManager>,
) {
    for buffer_id in host_mem_to_shm {
        let shm_buf_offset = shm_buffer_mgr.reserve(&buffer_id).await;
        let buf = unsafe { get_buffer_ref_mut(&shm_buffer_mgr, &buffer_id, shm_buf_offset) };
        panic_on_error!(hostmem_buffer_mgr.load_to(&buffer_id, buf));
        warn_on_send_error!(in_data_ready_tx.send(buffer_id, BufferLocation::HostMem));
    }
}

async fn storage_to_shm_transfer(
    storage_to_shm: Vec<BufferId>,
    in_data_ready_tx: InDataReadyTx,
    shm_buffer_mgr: Arc<ShmBufferManager>,
    storage_buffer_mgr: Arc<StorageBufferManager>,
) {
    for buffer_id in storage_to_shm {
        let shm_buf_offset = shm_buffer_mgr.reserve(&buffer_id).await;
        let buf = unsafe { get_buffer_ref_mut(&shm_buffer_mgr, &buffer_id, shm_buf_offset) };
        panic_on_error!(storage_buffer_mgr.load_to(&buffer_id, buf).await);
        warn_on_send_error!(in_data_ready_tx.send(buffer_id, BufferLocation::Storage));
    }
}

async fn shm_to_backend_transfer(
    shm_to_hybrid: HashMap<BufferId, BufferLocation>,
    mut out_data_ready_rx: OutDataReadyRx,
    shm_buffer_mgr: Arc<ShmBufferManager>,
    hostmem_buffer_mgr: Arc<HostMemBufferManager>,
    storage_buffer_mgr: Arc<StorageBufferManager>,
) {
    fn check_location(buffer_id: &BufferId, expected: BufferLocation, actual: BufferLocation) {
        if expected != actual {
            match expected {
                BufferLocation::HostMem => {
                    tracing::warn!(
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
            }
        }
    }
    let mut next_shm_handling = HashMap::new();
    for (buf_id, expected_location) in shm_to_hybrid {
        let location = match shm_to_backend_transfer_inner(
            &buf_id,
            &shm_buffer_mgr,
            &hostmem_buffer_mgr,
            &storage_buffer_mgr,
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
            let location = panic_on_error!(
                shm_to_backend_transfer_inner(
                    &buf_id,
                    &shm_buffer_mgr,
                    &hostmem_buffer_mgr,
                    &storage_buffer_mgr
                )
                .await
            );
            check_location(&buf_id, expected_location, location);
        } else {
            tracing::warn!(
                "Received unexpected buffer ID to {:?}: {:?}",
                expected_location,
                buf_id
            );
        }
    }
}

unsafe fn get_buffer_ref_mut<'a>(
    shm_buffer_mgr: &ShmBufferManager,
    buffer_id: &'a BufferId,
    shm_offset: u64,
) -> &'a mut [u8] {
    unsafe {
        std::slice::from_raw_parts_mut(
            shm_buffer_mgr
                .at_offset(shm_offset, buffer_id.size as usize)
                .unwrap(),
            buffer_id.size as usize,
        )
    }
}

unsafe fn get_buffer_ref<'a>(
    shm_buffer_mgr: &ShmBufferManager,
    buffer_id: &'a BufferId,
    shm_offset: u64,
) -> &'a [u8] {
    unsafe {
        std::slice::from_raw_parts(
            shm_buffer_mgr
                .at_offset(shm_offset, buffer_id.size as usize)
                .unwrap(),
            buffer_id.size as usize,
        )
    }
}

async fn shm_to_backend_transfer_inner(
    buffer_id: &BufferId,
    shm_buffer_mgr: &ShmBufferManager,
    hostmem_buffer_mgr: &HostMemBufferManager,
    storage_buffer_mgr: &StorageBufferManager,
) -> Result<BufferLocation, HybridBufferError> {
    let buf_offset = shm_buffer_mgr
        .get_buffer(&buffer_id)
        .ok_or(HybridBufferError::NoBufferId)?
        .addr;
    let buf_ref = unsafe { get_buffer_ref(shm_buffer_mgr, buffer_id, buf_offset) };
    let mut target_loc = BufferLocation::HostMem;
    match hostmem_buffer_mgr.store(buffer_id, buf_ref) {
        Ok(_) => {}
        Err(HybridBufferError::MemoryExhausted) => {
            storage_buffer_mgr.store(buffer_id, buf_ref).await?;
            target_loc = BufferLocation::Storage;
        }
        Err(e) => return Err(e),
    }
    shm_buffer_mgr
        .release(buffer_id)
        .map_err(|_| HybridBufferError::NoBufferId)?;
    Ok(target_loc)
}
