use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use nihil_common::{
    general::pretty_size, rpc::SidecarClient, GlobalDeviceId, MigrationArgs, MigrationResponse,
    ProcessLocalDeviceId,
};

use crate::runtime::daemon_server::DeviceOrdinalMapping;

use super::{buffer_manager::BufferId, ShmBufferManager};

#[derive(Debug, Clone)]
pub struct MigrationSpecEntry {
    pub size: u64,
    pub handle_idx: NonZeroU32,
}

pub struct MigrationSpec {
    pub device_map: HashMap<GlobalDeviceId, Vec<MigrationSpecEntry>>,
}

pub struct DataMigrationTask {
    src: Vec<(i32, MigrationSpec, SidecarClient, Arc<DeviceOrdinalMapping>)>,
    dst: (i32, MigrationSpec, SidecarClient, Arc<DeviceOrdinalMapping>),
    shm_buffer_mgr: Arc<ShmBufferManager>,
}

impl DataMigrationTask {
    pub fn new(
        src: Vec<(i32, MigrationSpec, SidecarClient, Arc<DeviceOrdinalMapping>)>,
        dst: (i32, MigrationSpec, SidecarClient, Arc<DeviceOrdinalMapping>),
        shm_buffer_mgr: Arc<ShmBufferManager>,
    ) -> Self {
        Self {
            src,
            dst,
            shm_buffer_mgr,
        }
    }

    pub fn get_src(&self) -> &[(i32, MigrationSpec, SidecarClient, Arc<DeviceOrdinalMapping>)] {
        &self.src
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
        for (pid, spec, rpc_client, mapping) in self.src {
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
        let device_junction = src_per_device
            .keys()
            .chain(self.dst.1.device_map.keys())
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let mut task_handles = Vec::new();
        for device in device_junction {
            let src_list = src_per_device.remove(&device).unwrap_or_default();
            let dst_entries = self.dst.1.device_map.remove(&device).unwrap_or_default();
            // Run migration for each device
            let shm_buffer_mgr = Arc::clone(&self.shm_buffer_mgr);
            let rpc_client = self.dst.2.clone();
            let device_id = self
                .dst
                .3
                .real_to_visible(device)
                .unwrap_or_else(|| todo!("Handle missing device mapping"));
            largest_transfer_size =
                largest_transfer_size.max(dst_entries.iter().map(|e| e.size).sum::<u64>());
            task_handles.push(tokio::spawn(async move {
                Self::run_for_device(
                    device,
                    src_list,
                    (self.dst.0, device_id, rpc_client, dst_entries),
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
        src: Vec<(
            i32,
            ProcessLocalDeviceId,
            SidecarClient,
            Vec<MigrationSpecEntry>,
        )>,
        dst: (
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
            dst,
            shm_buffer_mgr.clone(),
            transfer_token_rx,
        ));
        // D2H direction
        for (src_pid, src_device, src_rpc_client, entries) in src {
            // Migrate each entry
            for src_entry in entries {
                let src_buffer_id = BufferId {
                    pid: src_pid,
                    device_id: global_id,
                    block_id: src_entry.handle_idx,
                    size: src_entry.size,
                };
                // Reserve shared memory for the migration
                if let Some(offset) = shm_buffer_mgr.reserve(&src_buffer_id) {
                    let args = MigrationArgs {
                        host_buffer_offset: offset,
                        size: src_entry.size,
                        device: src_device,
                        handle_idx: src_entry.handle_idx,
                        host_to_device: false,
                    };
                    // Send migration request to the source process
                    if let Ok(resp) = src_rpc_client
                        .migrate(tarpc::context::current(), args)
                        .await
                    {
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
        let _ = h2d_handle.await;
    }

    async fn host_to_device_transfer(
        global_id: GlobalDeviceId,
        dst: (
            i32,
            ProcessLocalDeviceId,
            SidecarClient,
            Vec<MigrationSpecEntry>,
        ),
        shm_buffer_mgr: Arc<ShmBufferManager>,
        mut notification_rx: tokio::sync::mpsc::UnboundedReceiver<MigrationResponse>,
    ) {
        let (dst_pid, dst_device, dst_rpc_client, mut dst_entries) = dst;
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
    dst_pid: i32,
    dst_device: ProcessLocalDeviceId,
    dst_entry: MigrationSpecEntry,
    rpc_client: &SidecarClient,
    shm_buffer_mgr: &ShmBufferManager,
) {
    let buffer_id = BufferId {
        pid: dst_pid,
        device_id: global_id,
        block_id: dst_entry.handle_idx,
        size: dst_entry.size,
    };
    let offset = shm_buffer_mgr.get_buffer(&buffer_id).unwrap_or_else(|| {
        panic!("Failed to get buffer for migration: {:?}", buffer_id);
    });
    let args = MigrationArgs {
        host_buffer_offset: offset,
        size: dst_entry.size,
        device: dst_device,
        handle_idx: dst_entry.handle_idx,
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
