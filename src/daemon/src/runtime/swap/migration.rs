use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use nihil_common::{
    general::pretty_size, rpc::SidecarClient, GlobalDeviceId, MigrationArgs, MigrationResponse,
    ProcessLocalDeviceId,
};
use tokio::sync::mpsc;

use crate::{error::HybridBufferError, runtime::daemon_server::DeviceOrdinalMapping};

use super::{hybrid_buffer::HybridBufferManager, BufferId, ShmBufferManager};

#[derive(Debug, Clone)]
pub struct ShmMigrationSpecEntry {
    pub size: u64,
    pub handle_idx: NonZeroU32,
}

#[derive(Debug, Clone)]
pub struct HybridMigrationSpecEntry {
    pub size: u64,
    pub handle_idx: NonZeroU32,
}

#[derive(Debug, Clone)]
pub enum MigrationSpecEntry {
    Shm(ShmMigrationSpecEntry),
    HostMem(HybridMigrationSpecEntry),
    Storage(HybridMigrationSpecEntry),
}

impl MigrationSpecEntry {
    pub fn size(&self) -> u64 {
        match self {
            MigrationSpecEntry::Shm(entry) => entry.size,
            MigrationSpecEntry::HostMem(entry) => entry.size,
            MigrationSpecEntry::Storage(entry) => entry.size,
        }
    }
}

pub struct MigrationSpec {
    pub device_map: HashMap<GlobalDeviceId, Vec<MigrationSpecEntry>>,
}

pub struct DataMigrationTask {
    out_from_gpu: Vec<(i32, MigrationSpec, SidecarClient, Arc<DeviceOrdinalMapping>)>,
    into_gpu: (i32, MigrationSpec, SidecarClient, Arc<DeviceOrdinalMapping>),
    shm_buffer_mgr: Arc<ShmBufferManager>,
    hybrid_buffer_mgr: Arc<HybridBufferManager>,
}

impl DataMigrationTask {
    pub fn new(
        out_from_gpu: Vec<(i32, MigrationSpec, SidecarClient, Arc<DeviceOrdinalMapping>)>,
        into_gpu: (i32, MigrationSpec, SidecarClient, Arc<DeviceOrdinalMapping>),
        shm_buffer_mgr: Arc<ShmBufferManager>,
        hybrid_buffer_mgr: Arc<HybridBufferManager>,
    ) -> Self {
        Self {
            out_from_gpu,
            into_gpu,
            shm_buffer_mgr,
            hybrid_buffer_mgr,
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
                    largest_transfer_size.max(entries.iter().map(|e| e.size()).sum::<u64>());
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
        let device_junction = src_per_device
            .keys()
            .chain(self.into_gpu.1.device_map.keys())
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let mut task_handles = Vec::new();
        for device in device_junction {
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
                largest_transfer_size.max(dst_entries.iter().map(|e| e.size()).sum::<u64>());
            task_handles.push(tokio::spawn(async move {
                Self::run_for_device(
                    device,
                    src_list,
                    (self.into_gpu.0, device_id, rpc_client, dst_entries),
                    shm_buffer_mgr,
                )
                .await;
            }));
        }
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
    ) {
        let (transfer_token_tx, transfer_token_rx) =
            tokio::sync::mpsc::unbounded_channel::<MigrationResponse>();
        // H2D direction
        let h2d_handle = tokio::spawn(Self::host_to_device_transfer(
            global_id,
            into_gpu,
            shm_buffer_mgr.clone(),
            transfer_token_rx,
        ));
        // D2H direction
        Self::device_to_host_transfer(global_id, out_from_gpu, shm_buffer_mgr, transfer_token_tx)
            .await;
        let _ = h2d_handle.await;
    }

    async fn device_to_host_transfer(
        global_id: GlobalDeviceId,
        out_from_gpu: Vec<(
            i32,
            ProcessLocalDeviceId,
            SidecarClient,
            Vec<ShmMigrationSpecEntry>,
        )>,
        shm_buffer_mgr: Arc<ShmBufferManager>,
        transfer_token_tx: mpsc::UnboundedSender<MigrationResponse>,
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
                if let Some(offset) = shm_buffer_mgr.reserve(&src_buffer_id) {
                    let args = MigrationArgs {
                        host_buffer_offset: offset,
                        size: out_from_gpu_entry.size,
                        device,
                        handle_idx: out_from_gpu_entry.handle_idx,
                        host_to_device: false,
                    };
                    // Send migration request to the source process
                    if let Ok(resp) = rpc_client.migrate(tarpc::context::current(), args).await {
                        let _ = transfer_token_tx.send(resp);
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
        drop(transfer_token_tx); // close the channel
    }

    async fn host_to_device_transfer(
        global_id: GlobalDeviceId,
        into_gpu: (
            i32,
            ProcessLocalDeviceId,
            SidecarClient,
            Vec<ShmMigrationSpecEntry>,
        ),
        shm_buffer_mgr: Arc<ShmBufferManager>,
        mut notification_rx: mpsc::UnboundedReceiver<MigrationResponse>,
    ) {
        let (dst_pid, dst_device, dst_rpc_client, mut dst_entries) = into_gpu;
        dst_entries.reverse();
        let mut accu_length = 0;
        let mut next_entry = dst_entries.pop();
        while let Some(d2h_resp) = notification_rx.recv().await {
            accu_length += d2h_resp.size;
            if let Some(entry) = next_entry.take() {
                if accu_length >= entry.size {
                    accu_length -= entry.size;
                    host_to_device_transfer_inner(
                        global_id,
                        dst_pid,
                        dst_device,
                        entry,
                        &dst_rpc_client,
                        &shm_buffer_mgr,
                    )
                    .await;
                    next_entry = dst_entries.pop();
                }
            }
        }
        // If there are remaining entries, we need to handle them
        while let Some(entry) = next_entry {
            host_to_device_transfer_inner(
                global_id,
                dst_pid,
                dst_device,
                entry,
                &dst_rpc_client,
                &shm_buffer_mgr,
            )
            .await;
            next_entry = dst_entries.pop();
        }
    }
}

async fn host_to_device_transfer_inner(
    global_id: GlobalDeviceId,
    into_gpu_pid: i32,
    dst_device: ProcessLocalDeviceId,
    into_gpu_entry: ShmMigrationSpecEntry,
    rpc_client: &SidecarClient,
    shm_buffer_mgr: &ShmBufferManager,
) {
    let buffer_id = BufferId {
        pid: into_gpu_pid,
        device_id: global_id,
        block_id: into_gpu_entry.handle_idx,
        size: into_gpu_entry.size,
    };
    let offset = shm_buffer_mgr.get_buffer(&buffer_id).unwrap_or_else(|| {
        panic!("Failed to get buffer for migration: {:?}", buffer_id);
    });
    let args = MigrationArgs {
        host_buffer_offset: offset,
        size: into_gpu_entry.size,
        device: dst_device,
        handle_idx: into_gpu_entry.handle_idx,
        host_to_device: true,
    };
    if let Ok(_) = rpc_client.migrate(tarpc::context::current(), args).await {
        shm_buffer_mgr
            .release(&buffer_id)
            .expect("Failed to release buffer after migration");
    } else {
        tracing::warn!("Failed to complete H2D migration RPC to destination process");
    }
}

async fn hybrid_to_shm_transfer_inner(
    pid: i32,
    device_id: GlobalDeviceId,
    src_entry: HybridMigrationSpecEntry,
    shm_buffer_mgr: &ShmBufferManager,
    shm_offset: u64,
    hybrid_buffer_mgr: &HybridBufferManager,
) -> Result<(), HybridBufferError> {
    let buffer_id = BufferId {
        pid,
        device_id,
        block_id: src_entry.handle_idx,
        size: src_entry.size,
    };
    let buf_ref = unsafe {
        std::slice::from_raw_parts_mut(
            shm_buffer_mgr
                .at_offset(shm_offset, src_entry.size as usize)
                .unwrap(),
            src_entry.size as usize,
        )
    };
    hybrid_buffer_mgr.load_to(&buffer_id, buf_ref).await?;
    Ok(())
}

async fn shm_to_hybrid_transfer_inner(
    pid: i32,
    device_id: GlobalDeviceId,
    src_entry: ShmMigrationSpecEntry,
    shm_buffer_mgr: &ShmBufferManager,
    hybrid_buffer_mgr: &HybridBufferManager,
) -> Result<(), HybridBufferError> {
    let buffer_id = BufferId {
        pid,
        device_id,
        block_id: src_entry.handle_idx,
        size: src_entry.size,
    };
    let buf_offset = shm_buffer_mgr
        .get_buffer(&buffer_id)
        .ok_or(HybridBufferError::NoBufferId)?;
    let buf_ref = unsafe {
        std::slice::from_raw_parts(
            shm_buffer_mgr
                .at_offset(buf_offset, src_entry.size as usize)
                .unwrap(),
            src_entry.size as usize,
        )
    };
    hybrid_buffer_mgr.store(&buffer_id, buf_ref).await?;
    Ok(())
}
