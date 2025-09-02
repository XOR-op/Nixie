use std::{collections::HashMap, sync::Arc};

use nihil_common::{GlobalDeviceId, MAX_ALLOCATION_SIZE, rpc::SidecarClient};

use crate::{
    control::ProcessResidualData,
    runtime::{
        daemon_server::DeviceOrdinalMapping,
        migration::{BufferId, hybrid_buffer::BufferLocation},
    },
};

use super::{
    ShmBufferManager,
    hybrid_buffer::HybridBufferManager,
    migration::{DataMigrationTask, MigrationSpec, MigrationSpecEntry},
};

#[derive(Debug, Clone)]
pub(crate) enum DeviceRequestArgs {
    ResidualData(ProcessResidualData),
    Allocation(HashMap<GlobalDeviceId, u64>),
}

/// Create a migration task that migrates data at the cost of others being moved out.
/// `out_from_gpu`: the earlier in the list, the more likely to be moved out.
pub(crate) fn two_processes_task(
    into_gpu: (
        i32,
        DeviceRequestArgs,
        SidecarClient,
        Arc<DeviceOrdinalMapping>,
    ),
    out_from_gpu: &[(
        i32,
        ProcessResidualData,
        SidecarClient,
        Arc<DeviceOrdinalMapping>,
    )],
    shm_buffer_mgr: Arc<ShmBufferManager>,
    hybrid_buffer_mgr: Arc<HybridBufferManager>,
) -> DataMigrationTask {
    let mut out_of_gpu_list = Vec::new();
    let into_gpu_requirement = match &into_gpu.1 {
        DeviceRequestArgs::ResidualData(process_residual_data) => process_residual_data
            .allocations
            .iter()
            .map(|(id, entries)| (*id, entries.iter().map(|entry| entry.size).sum::<u64>()))
            .collect::<HashMap<_, _>>(),
        DeviceRequestArgs::Allocation(allocation) => allocation.clone(),
    };
    let mut shm_free_segments = shm_buffer_mgr.free_segments();
    let mut host_mem_free_segments = hybrid_buffer_mgr.free_mem_segments();

    let mut host_mem_to_shm = Vec::new();
    let mut shm_to_host_mem = Vec::new();
    let mut storage_to_shm = Vec::new();
    let mut shm_to_storage = Vec::new();
    // TODO: use these two to reduce migration time
    let storage_to_host_mem = Vec::new();
    let host_mem_to_storage = Vec::new();

    // prepare into_gpu entries, and also update free segments
    let into_gpu_entries = match into_gpu.1 {
        DeviceRequestArgs::ResidualData(process_residual_data) => process_residual_data
            .allocations
            .into_iter()
            .map(|(global_id, entries)| {
                (
                    global_id,
                    entries
                        .into_iter()
                        .filter_map(|data_entry| {
                            let buffer_id = BufferId {
                                pid: into_gpu.0,
                                device_id: global_id,
                                block_id: data_entry.handle_idx,
                                size: data_entry.size,
                            };
                            if let Some(alloc_info) = shm_buffer_mgr.get_buffer(&buffer_id) {
                                shm_free_segments.push(alloc_info.block_size);
                                // in SHM
                                Some(MigrationSpecEntry {
                                    size: data_entry.size,
                                    handle_idx: data_entry.handle_idx,
                                })
                            } else {
                                match hybrid_buffer_mgr.get_buffer_location(&buffer_id) {
                                    Some(location) => {
                                        // in host mem or storage
                                        match location {
                                            BufferLocation::HostMem => {
                                                // TODO: use non-uniform segment sizes
                                                host_mem_free_segments
                                                    .push(MAX_ALLOCATION_SIZE as u64);
                                                host_mem_to_shm.push(buffer_id);
                                            }
                                            BufferLocation::Storage => {
                                                storage_to_shm.push(buffer_id);
                                            }
                                        }
                                        Some(MigrationSpecEntry {
                                            size: data_entry.size,
                                            handle_idx: data_entry.handle_idx,
                                        })
                                    }
                                    None => {
                                        tracing::error!(
                                            "Data to migrate not found in SHM or host mem: {:?}",
                                            buffer_id
                                        );
                                        None
                                    }
                                }
                            }
                        })
                        .collect(),
                )
            })
            .collect::<HashMap<_, _>>(),
        DeviceRequestArgs::Allocation(_) => HashMap::new(),
    };

    // for every dst device
    // TODO: better policy to allocate shm buffer to each device evenly
    for (global_id, into_gpu_required_size) in into_gpu_requirement {
        let mut accu_size = 0;
        // for every src process
        for (out_from_gpu_pid, out_from_gpu_entries, rpc_client, dev_mapping) in out_from_gpu.iter()
        {
            if let Some(entries) = out_from_gpu_entries.allocations.get(&global_id) {
                let mut migration_entries = Vec::new();
                // check per device per src process
                for entry in entries {
                    if accu_size >= into_gpu_required_size {
                        break;
                    }
                    let spec_entry = MigrationSpecEntry {
                        size: entry.size,
                        handle_idx: entry.handle_idx,
                    };
                    // Use SHM first, then host mem, then storage
                    // TODO: handle if segments have variable lengths
                    if let Some(seg) = shm_free_segments.pop() {
                        assert!(
                            seg >= entry.size,
                            "SHM segment {} is smaller than required {}",
                            seg,
                            entry.size
                        );
                    } else if let Some(seg) = host_mem_free_segments.pop() {
                        assert!(
                            seg >= entry.size,
                            "Host mem segment {} is smaller than required {}",
                            seg,
                            entry.size
                        );
                        shm_to_host_mem.push(spec_entry.to_buffer_id(*out_from_gpu_pid, global_id))
                    } else {
                        shm_to_storage.push(spec_entry.to_buffer_id(*out_from_gpu_pid, global_id))
                    };
                    migration_entries.push(spec_entry);
                    accu_size += entry.size;
                }
                if !migration_entries.is_empty() {
                    out_of_gpu_list.push((
                        *out_from_gpu_pid,
                        MigrationSpec {
                            device_map: HashMap::from([(global_id, migration_entries)]),
                        },
                        rpc_client.clone(),
                        Arc::clone(dev_mapping),
                    ));
                }
            }
        }
        if accu_size < into_gpu_required_size {
            tracing::warn!(
                "Not enough data to migrate for device {:?}: required {}, but only {}",
                global_id,
                into_gpu_required_size,
                accu_size
            );
        }
    }

    DataMigrationTask::new(
        out_of_gpu_list,
        (
            into_gpu.0,
            MigrationSpec {
                device_map: into_gpu_entries,
            },
            into_gpu.2,
            Arc::clone(&into_gpu.3),
        ),
        storage_to_shm,
        host_mem_to_shm,
        shm_to_host_mem,
        shm_to_storage,
        storage_to_host_mem,
        host_mem_to_storage,
        shm_buffer_mgr,
        hybrid_buffer_mgr,
    )
}
