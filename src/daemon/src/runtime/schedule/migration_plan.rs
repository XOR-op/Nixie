use std::{collections::HashMap, sync::Arc};

use nihil_common::{rpc::SidecarClient, GlobalDeviceId};

use crate::{control::ProcessResidualData, runtime::daemon_server::DeviceOrdinalMapping};

use super::{
    migration::{DataMigrationTask, MigrationSpec, MigrationSpecEntry},
    ShmBufferManager,
};

#[derive(Debug, Clone)]
pub(super) enum DstRequestArgs {
    ResidualData(ProcessResidualData),
    Allocation(HashMap<GlobalDeviceId, u64>),
}

pub(super) fn two_processes_task(
    dst: (
        i32,
        DstRequestArgs,
        SidecarClient,
        Arc<DeviceOrdinalMapping>,
    ),
    src: &[(
        i32,
        ProcessResidualData,
        SidecarClient,
        Arc<DeviceOrdinalMapping>,
    )],
    shm_buffer_mgr: Arc<ShmBufferManager>,
) -> DataMigrationTask {
    let mut src_list = Vec::new();
    let dst_requirement = match &dst.1 {
        DstRequestArgs::ResidualData(process_residual_data) => process_residual_data
            .allocations
            .iter()
            .map(|(id, entries)| (*id, entries.iter().map(|entry| entry.size).sum::<u64>()))
            .collect::<HashMap<_, _>>(),
        DstRequestArgs::Allocation(allocation) => allocation.clone(),
    };
    // for every dst device
    for (global_id, dst_required_size) in dst_requirement {
        let mut accu_size = 0;
        // for every src process
        for (src_pid, src_entries, src_rpc_client, src_mapping) in src.iter() {
            if let Some(entries) = src_entries.allocations.get(&global_id) {
                let mut migration_entries = Vec::new();
                // check per device per src process
                for entry in entries {
                    if accu_size >= dst_required_size {
                        break;
                    }
                    migration_entries.push(MigrationSpecEntry {
                        size: entry.size,
                        handle_idx: entry.handle_idx,
                    });
                    accu_size += entry.size;
                }
                if !migration_entries.is_empty() {
                    src_list.push((
                        *src_pid,
                        MigrationSpec {
                            device_map: HashMap::from([(global_id, migration_entries)]),
                        },
                        src_rpc_client.clone(),
                        Arc::clone(src_mapping),
                    ));
                }
            }
        }
        if accu_size < dst_required_size {
            tracing::warn!(
                "Not enough data to migrate for device {:?}: required {}, but only {}",
                global_id,
                dst_required_size,
                accu_size
            );
        }
    }

    let dst_entries = match dst.1 {
        DstRequestArgs::ResidualData(process_residual_data) => process_residual_data
            .allocations
            .into_iter()
            .map(|(global_id, entries)| {
                (
                    global_id,
                    entries
                        .into_iter()
                        .map(|data_entry| MigrationSpecEntry {
                            size: data_entry.size,
                            handle_idx: data_entry.handle_idx,
                        })
                        .collect(),
                )
            })
            .collect::<HashMap<_, _>>(),
        DstRequestArgs::Allocation(_) => HashMap::new(),
    };
    DataMigrationTask::new(
        src_list,
        (
            dst.0,
            MigrationSpec {
                device_map: dst_entries,
            },
            dst.2,
            Arc::clone(&dst.3),
        ),
        shm_buffer_mgr,
    )
}
