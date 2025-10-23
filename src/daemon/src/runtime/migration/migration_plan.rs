use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use nihil_common::{GlobalDeviceId, MAX_ALLOCATION_SIZE};

use crate::{
    control::ProcessResidualData,
    runtime::{
        daemon_server::DeviceOrdinalMapping,
        migration::{AllocationCapacity, BufferId, BufferLocation, DataManagerHandle},
    },
};

use super::execution::{DataMigrationTask, MigrationSpec, MigrationSpecEntry};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MigrationRequirement {
    pub from: BufferLocation,
    pub to: BufferLocation,
    pub size: u64,
    pub allow_incomplete: bool, // if true, allow moving with less than size
}

impl MigrationRequirement {
    pub(crate) fn new(
        from: BufferLocation,
        to: BufferLocation,
        size: u64,
        allow_incomplete: bool,
    ) -> Self {
        assert_ne!(from, to, "From and to endpoints must be different");
        Self {
            from,
            to,
            size,
            allow_incomplete,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum DeviceRequestArgs {
    ResidualData(ProcessResidualData),
    Allocation(HashMap<GlobalDeviceId, u64>),
}

pub(crate) trait AbstractDataHandle: Clone {
    fn gpu_free_space(&self, devices: &[GlobalDeviceId]) -> HashMap<GlobalDeviceId, u64>;

    fn shm_alloc_size(&self, buf_id: &BufferId) -> Option<u64>;

    fn shm_contains(&self, buf_id: &BufferId) -> bool {
        self.shm_alloc_size(buf_id).is_some()
    }
    fn hostmem_contains(&self, buf_id: &BufferId) -> bool;
    fn storage_contains(&self, buf_id: &BufferId) -> bool;

    fn shm_buffer_ids(&self) -> HashMap<BufferId, AllocationCapacity>;
    fn hostmem_buffer_ids(&self) -> HashMap<BufferId, AllocationCapacity>;

    fn shm_free_segments(&self) -> Vec<AllocationCapacity>;
    fn hostmem_free_segments(&self) -> Vec<AllocationCapacity>;
}

impl AbstractDataHandle for DataManagerHandle {
    fn gpu_free_space(&self, devices: &[GlobalDeviceId]) -> HashMap<GlobalDeviceId, u64> {
        macro_rules! check_error {
            ($expr:expr, $msg:expr) => {
                match $expr {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!("{} failed with error: {}", $msg, e);
                        return HashMap::new();
                    }
                }
            };
        }
        let mut result = HashMap::new();
        let nvml = crate::staticly::get_nvml();
        for dev in devices.iter() {
            let device = check_error!(nvml.device_by_index(dev.0 as u32), "device_by_index");
            let mem_info = check_error!(device.memory_info(), "memory_info");
            result.insert(*dev, mem_info.free);
        }
        result
    }

    fn hostmem_contains(&self, buf_id: &BufferId) -> bool {
        self.hostmem.contains(buf_id)
    }
    fn storage_contains(&self, buf_id: &BufferId) -> bool {
        self.storage.contains(buf_id)
    }
    fn shm_alloc_size(&self, buf_id: &BufferId) -> Option<AllocationCapacity> {
        self.shm.get_buffer(buf_id).map(|info| info.block_size)
    }

    fn shm_buffer_ids(&self) -> HashMap<BufferId, AllocationCapacity> {
        self.shm.dump_buffers()
    }

    fn hostmem_buffer_ids(&self) -> HashMap<BufferId, AllocationCapacity> {
        self.hostmem.dump_buffers()
    }

    fn shm_free_segments(&self) -> Vec<AllocationCapacity> {
        self.shm.free_segments()
    }
    fn hostmem_free_segments(&self) -> Vec<AllocationCapacity> {
        self.hostmem.free_mem_segments()
    }
}

macro_rules! check_size {
    ($given:expr, $expected:expr, $msg:expr
    ) => {
        if $given < $expected {
            panic!(
                "TODO: {} {} is smaller than expected {}, which not implemented",
                $msg, $given, $expected
            );
        }
    };
}

/// Create a migration task that migrates data at the cost of others being moved out.
/// `out_from_gpu`: the earlier in the list, the more likely to be moved out.
pub(crate) fn realtime_migrate_task<Client, Handle>(
    into_gpu: (i32, DeviceRequestArgs, Client, Arc<DeviceOrdinalMapping>),
    out_from_gpu: &[(i32, ProcessResidualData, Client, Arc<DeviceOrdinalMapping>)],
    data_manager: Handle,
    with_free_memory: bool,
) -> Option<DataMigrationTask<Client, Handle>>
where
    Client: Clone,
    Handle: AbstractDataHandle,
{
    let profiling_timestamp_start = std::time::Instant::now();
    let mut out_of_gpu_list = Vec::new();

    let into_gpu_requirement = {
        let in_gpu_list = match &into_gpu.1 {
            DeviceRequestArgs::ResidualData(process_residual_data) => process_residual_data
                .allocations
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            DeviceRequestArgs::Allocation(allocation) => {
                allocation.keys().copied().collect::<Vec<_>>()
            }
        };
        let gpu_free_space = data_manager.gpu_free_space(&in_gpu_list);
        match &into_gpu.1 {
            DeviceRequestArgs::ResidualData(process_residual_data) => process_residual_data
                .allocations
                .iter()
                .map(|(id, entries)| {
                    assert!(entries.iter().all(|e| !e.on_gpu));

                    let sum_size = entries.iter().map(|entry| entry.size).sum::<u64>();
                    let free_space = if with_free_memory {
                        match gpu_free_space.get(id).copied() {
                            Some(v) => v,
                            None => {
                                tracing::warn!(
                                    "Device {:?} not found when checking free space",
                                    id
                                );
                                0
                            }
                        }
                    } else {
                        0
                    }
                    // We reserve 2 blocks to avoid potential OOM due to API inaccuracy
                    .saturating_sub(2 * MAX_ALLOCATION_SIZE as u64);
                    let required_size = sum_size.saturating_sub(free_space);
                    (*id, required_size)
                })
                .collect::<HashMap<_, _>>(),
            DeviceRequestArgs::Allocation(allocation) => allocation.clone(),
        }
    };
    let mut shm_free_segments = data_manager.shm_free_segments();
    let mut host_mem_free_segments = data_manager.hostmem_free_segments();

    let mut hostmem_to_shm = Vec::new();
    let mut storage_to_shm = Vec::new();
    let mut shm_to_backend = HashMap::new();

    let mut shm_eviction_candidates = data_manager.shm_buffer_ids();
    shm_eviction_candidates.retain(|k, _| k.pid != into_gpu.0);

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
                            if let Some(alloc_size) = data_manager.shm_alloc_size(&buffer_id) {
                                shm_free_segments.push(alloc_size);
                                // in SHM
                                Some(MigrationSpecEntry {
                                    size: data_entry.size,
                                    handle_idx: data_entry.handle_idx,
                                    ready_for_pcie_xfer: true,
                                })
                            } else {
                                if data_manager.hostmem_contains(&buffer_id) {
                                    // in host mem
                                    host_mem_free_segments.push(MAX_ALLOCATION_SIZE as u64);
                                    hostmem_to_shm.push(buffer_id);
                                } else if data_manager.storage_contains(&buffer_id) {
                                    // in storage
                                    storage_to_shm.push(buffer_id);
                                } else {
                                    tracing::error!(
                                        "Data to migrate not found in SHM or host mem: {:?}",
                                        buffer_id
                                    );
                                    return None;
                                }
                                Some(MigrationSpecEntry {
                                    size: data_entry.size,
                                    handle_idx: data_entry.handle_idx,
                                    ready_for_pcie_xfer: false,
                                })
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
        tracing::debug!(
            "Device {:?} requires {} of data to migrate in",
            global_id,
            nihil_common::general::pretty_size(into_gpu_required_size)
        );
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
                        ready_for_pcie_xfer: true, // the buffer is on GPU
                    };
                    // Use SHM first, then host mem, then storage
                    // TODO: handle if segments have variable lengths
                    if let Some(seg) = shm_free_segments.pop() {
                        check_size!(seg, entry.size, "SHM free segment");
                    } else {
                        // pop one entry from shm_eviction_candidates
                        let evicted_buf_id = {
                            let res = shm_eviction_candidates
                                .iter()
                                .next()
                                .map(|(k, v)| (k.clone(), *v));
                            match res {
                                Some((k, v)) => {
                                    check_size!(v, entry.size, "SHM eviction candidate");
                                    shm_eviction_candidates.remove(&k);
                                    k
                                }
                                None => {
                                    panic!("No more SHM eviction candidates");
                                }
                            }
                        };
                        if let Some(seg) = host_mem_free_segments.pop() {
                            check_size!(seg, entry.size, "Host mem segment");
                            shm_to_backend.insert(evicted_buf_id, BufferLocation::HostMem);
                        } else {
                            shm_to_backend.insert(evicted_buf_id, BufferLocation::Storage);
                        }
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
            return None;
        }
    }

    let result = DataMigrationTask::new(
        out_of_gpu_list,
        Some((
            into_gpu.0,
            MigrationSpec {
                device_map: into_gpu_entries,
            },
            into_gpu.2,
            Arc::clone(&into_gpu.3),
        )),
        storage_to_shm,
        hostmem_to_shm,
        shm_to_backend,
        Vec::new(), // storage_to_hostmem
        Vec::new(), // hostmem_to_storage
        data_manager,
    );

    let elapsed = profiling_timestamp_start.elapsed();
    tracing::debug!(
        "Created migration task: {} for {} us",
        result.json_summary(),
        elapsed.as_micros()
    );
    Some(result)
}

/// Create a migration task that only organizes data out of GPU
pub(crate) fn local_prefetch_task<Client, Handle>(
    requests: Vec<(i32, Vec<MigrationRequirement>)>,
    data_manager: Handle,
) -> Option<DataMigrationTask<Client, Handle>>
where
    Client: Clone,
    Handle: AbstractDataHandle,
{
    let profiling_timestamp_start = std::time::Instant::now();
    let in_requests_pids: HashSet<_, std::hash::RandomState> =
        HashSet::from_iter(requests.iter().map(|(pid, _)| *pid));
    assert_eq!(in_requests_pids.len(), requests.len(), "duplicated pids");
    let mut shm_free_segments = data_manager.shm_free_segments();
    let mut host_mem_free_segments = data_manager.hostmem_free_segments();

    let mut hostmem_to_shm = Vec::new();
    let mut storage_to_shm = Vec::new();
    let mut shm_to_backend = HashMap::new();
    let mut hostmem_to_storage = Vec::new();
    let mut storage_to_hostmem = Vec::new();

    let mut shm_eviction_candidates = data_manager.shm_buffer_ids();
    let mut hostmem_eviction_candidates = data_manager.hostmem_buffer_ids();

    // decide which buffer to be moved based on requests
    for (pid, directions) in requests.into_iter() {
        for moving in directions.iter() {
            if matches!(moving.from, BufferLocation::Gpu(_))
                || matches!(moving.to, BufferLocation::Gpu(_))
            {
                tracing::error!("Cannot organize data from/to GPU");
                return None;
            }

            let (eviction_candiates, free_segments) = match moving.from {
                BufferLocation::Shm => (&mut shm_eviction_candidates, &mut shm_free_segments),
                BufferLocation::HostMem => (
                    &mut hostmem_eviction_candidates,
                    &mut host_mem_free_segments,
                ),
                BufferLocation::Storage => continue,
                BufferLocation::Gpu(_) => {
                    tracing::error!("Cannot organize data from GPU");
                    return None;
                }
            };

            // select eligible buffers to be moved
            let mut removed = HashMap::new();
            let mut accu_size = 0;
            for (buf_id, len) in eviction_candiates.iter().filter(|(k, _)| k.pid == pid) {
                if accu_size >= moving.size {
                    break;
                }
                removed.insert(buf_id.clone(), *len);
                accu_size += buf_id.size;
            }
            if accu_size < moving.size && !moving.allow_incomplete {
                tracing::warn!(
                    "Not enough data to organize for pid {}: required {}, but only {}",
                    pid,
                    moving.size,
                    accu_size
                );
                return None;
            }
            for (being_removed, _) in removed.iter() {
                eviction_candiates.remove(being_removed);
            }

            // add back the freed segments
            free_segments.extend(removed.values());

            // add to the corresponding move list
            let removed: Vec<BufferId> = removed.into_keys().collect();
            match (moving.from, moving.to) {
                (BufferLocation::Shm, dest) => {
                    for buf_id in removed.into_iter() {
                        shm_to_backend.insert(buf_id, dest);
                    }
                }
                (BufferLocation::HostMem, BufferLocation::Shm) => hostmem_to_shm.extend(removed),
                (BufferLocation::HostMem, BufferLocation::Storage) => {
                    hostmem_to_storage.extend(removed)
                }
                (BufferLocation::Storage, BufferLocation::Shm) => storage_to_shm.extend(removed),
                (BufferLocation::Storage, BufferLocation::HostMem) => {
                    storage_to_hostmem.extend(removed)
                }
                _ => {
                    tracing::warn!(
                        "Unsupported migration direction: {:?} -> {:?}",
                        moving.from,
                        moving.to
                    );
                    return None;
                }
            }
        }
        // remove all remaining entries for this pid
        shm_eviction_candidates.retain(|k, _| k.pid != pid);
        hostmem_eviction_candidates.retain(|k, _| k.pid != pid);
    }

    assert!(
        shm_eviction_candidates
            .keys()
            .all(|k| !in_requests_pids.contains(&k.pid))
    );
    assert!(
        hostmem_eviction_candidates
            .keys()
            .all(|k| !in_requests_pids.contains(&k.pid))
    );

    // decide if we can satisfy the requests with enough space
    // 1. we find out how much more space we need in SHM and host mem
    let mut shm_not_enough = Vec::new();
    let mut hostmem_not_enough = Vec::new();
    for entry in storage_to_shm.iter().chain(hostmem_to_shm.iter()) {
        if let Some(seg) = shm_free_segments.pop() {
            // TODO: support variable segment sizes
            check_size!(seg, entry.size, "SHM free segment");
        } else {
            shm_not_enough.push(entry.size);
        }
    }
    for entry in storage_to_hostmem
        .iter()
        .chain(shm_to_backend.iter().filter_map(|(b, loc)| {
            if *loc == BufferLocation::HostMem {
                Some(b)
            } else {
                None
            }
        }))
    {
        if let Some(seg) = host_mem_free_segments.pop() {
            // TODO: support variable segment sizes
            check_size!(seg, entry.size, "Host mem segment");
        } else {
            hostmem_not_enough.push(entry.size);
        }
    }
    // 2. we try to evict data out of SHM and host mem to make space
    if !(shm_not_enough.is_empty() && hostmem_not_enough.is_empty()) {
        for size in shm_not_enough.into_iter() {
            // move from shm to host mem or storage
            let Some((victim, victim_size)) = shm_eviction_candidates
                .iter()
                .next()
                .map(|(k, v)| (k.clone(), *v))
            else {
                tracing::warn!("Not enough SHM space to organize data");
                return None;
            };
            check_size!(victim_size, size, "SHM eviction candidate");
            shm_eviction_candidates.remove(&victim);
            if let Some(seg) = host_mem_free_segments.pop() {
                check_size!(seg, size, "Host mem segment");
                shm_to_backend.insert(victim, BufferLocation::HostMem);
            } else {
                shm_to_backend.insert(victim, BufferLocation::Storage);
            }
        }
        for size in hostmem_not_enough.into_iter() {
            // move from host mem to storage
            let Some((victim, victim_size)) = hostmem_eviction_candidates
                .iter()
                .next()
                .map(|(k, v)| (k.clone(), *v))
            else {
                tracing::warn!("Not enough host mem space to organize data");
                return None;
            };
            check_size!(victim_size, size, "Host mem eviction candidate");
            hostmem_eviction_candidates.remove(&victim);
            hostmem_to_storage.push(victim);
        }
    }

    let result = DataMigrationTask::new(
        Vec::new(),
        None,
        storage_to_shm,
        hostmem_to_shm,
        shm_to_backend,
        storage_to_hostmem,
        hostmem_to_storage,
        data_manager,
    );

    let elapsed = profiling_timestamp_start.elapsed();
    tracing::debug!(
        "Created organize task: {} with {} us",
        result.json_summary(),
        elapsed.as_micros()
    );
    Some(result)
}

// we don't use free gpu space here, since that could interfere with the running job
// should only involve GPU and SHM for simplicity
pub(crate) fn gpu_prefetch_task<Client, Handle>(
    current_pid: Option<i32>,
    into_gpu: (i32, ProcessResidualData, Client, Arc<DeviceOrdinalMapping>),
    out_from_gpu: &[(i32, ProcessResidualData, Client, Arc<DeviceOrdinalMapping>)],
    data_manager: Handle,
) -> Option<DataMigrationTask<Client, Handle>>
where
    Client: Clone,
    Handle: AbstractDataHandle,
{
    if let Some(pid) = current_pid {
        assert!(
            out_from_gpu
                .iter()
                .all(|(out_pid, _, _, _)| *out_pid != pid)
        );
        assert!(into_gpu.0 != pid);
    }
    assert!(
        into_gpu
            .1
            .allocations
            .values()
            .all(|entries| entries.iter().all(|e| !e.on_gpu))
    );
    // step 1: understand what data to be moved into
    // for each device, we first get the available size for moving in
    let out_of_gpu_available_size: HashMap<GlobalDeviceId, u64> = out_from_gpu
        .iter()
        .flat_map(|(_, residual_data, _, _)| residual_data.allocations.keys().copied())
        .collect::<HashSet<_>>()
        .into_iter()
        .map(|dev_id| {
            let total_size: u64 = out_from_gpu
                .iter()
                .filter_map(|(_, residual_data, _, _)| {
                    residual_data
                        .allocations
                        .get(&dev_id)
                        .map(|entries| entries.iter().map(|e| e.size).sum::<u64>())
                })
                .sum();
            (dev_id, total_size)
        })
        .collect();

    let into_gpu_entries = into_gpu
        .1
        .allocations
        .into_iter()
        .map(|(dev_id, entries)| {
            let available_size = out_of_gpu_available_size.get(&dev_id).copied().unwrap_or(0);
            let mut accu_size = 0;
            let mut migration_entries = Vec::new();
            for entry in entries {
                if accu_size + entry.size > available_size {
                    continue; // check other entries
                }
                accu_size += entry.size;
                migration_entries.push(entry);
            }
            (dev_id, migration_entries)
        })
        .collect::<HashMap<_, _>>();

    // step 2: create the migration task
    realtime_migrate_task(
        (
            into_gpu.0,
            DeviceRequestArgs::ResidualData(ProcessResidualData {
                pid: into_gpu.0,
                allocations: into_gpu_entries,
            }),
            into_gpu.2,
            Arc::clone(&into_gpu.3),
        ),
        out_from_gpu,
        data_manager,
        false,
    )
}

#[cfg(test)]
pub(super) mod tests {
    use colored::Colorize;
    use nihil_common::{ProcessLocalDeviceId, general::pretty_size};

    use super::*;
    use std::{
        collections::{BTreeMap, HashMap},
        num::NonZeroU32,
    };

    use crate::{control::PhysicalMemoryData, runtime::migration::BufferId};
    #[derive(Default)]
    struct MockProcessInput {
        pid: i32,
        gpu: u64,
        shm: u64,
        hostmem: u64,
        storage: u64,
    }

    struct MockProcessData {
        pid: i32,
        gpu_buffer_ids: Vec<BufferId>,
        shm_buffer_ids: Vec<BufferId>,
        hostmem_buffer_ids: Vec<BufferId>,
        storage_buffer_ids: Vec<BufferId>,
    }

    impl MockProcessData {
        fn from_input(input: &MockProcessInput) -> Self {
            let block_size = MAX_ALLOCATION_SIZE as u64;
            assert_eq!(input.gpu % block_size, 0);
            assert_eq!(input.shm % block_size, 0);
            assert_eq!(input.hostmem % block_size, 0);
            assert_eq!(input.storage % block_size, 0);
            Self {
                pid: input.pid,
                gpu_buffer_ids: (0..input.gpu / block_size)
                    .map(|i| BufferId {
                        pid: input.pid,
                        device_id: GlobalDeviceId(0),
                        block_id: NonZeroU32::new(i as u32 + 1).unwrap(),
                        size: block_size,
                    })
                    .collect(),
                shm_buffer_ids: (0..input.shm / block_size)
                    .map(|i| BufferId {
                        pid: input.pid,
                        device_id: GlobalDeviceId(0),
                        block_id: NonZeroU32::new(i as u32 + 1).unwrap(),
                        size: block_size,
                    })
                    .collect(),
                hostmem_buffer_ids: (0..input.hostmem / block_size)
                    .map(|i| BufferId {
                        pid: input.pid,
                        device_id: GlobalDeviceId(0),
                        block_id: NonZeroU32::new(i as u32 + 1).unwrap(),
                        size: block_size,
                    })
                    .collect(),
                storage_buffer_ids: (0..input.storage / block_size)
                    .map(|i| BufferId {
                        pid: input.pid,
                        device_id: GlobalDeviceId(0),
                        block_id: NonZeroU32::new(i as u32 + 1).unwrap(),
                        size: block_size,
                    })
                    .collect(),
            }
        }
    }

    #[derive(Clone)]
    struct MockManager {
        shm_map: HashMap<BufferId, AllocationCapacity>,
        hostmem_map: HashMap<BufferId, AllocationCapacity>,
        storage_map: HashMap<BufferId, AllocationCapacity>,
        gpu_free_space: HashMap<GlobalDeviceId, u64>,
        shm_capacity: u64,
        hostmem_capacity: u64,
    }

    impl AbstractDataHandle for MockManager {
        fn shm_alloc_size(&self, buf_id: &BufferId) -> Option<AllocationCapacity> {
            self.shm_map.get(buf_id).copied()
        }
        fn hostmem_contains(&self, buf_id: &BufferId) -> bool {
            self.hostmem_map.contains_key(buf_id)
        }
        fn storage_contains(&self, buf_id: &BufferId) -> bool {
            self.storage_map.contains_key(buf_id)
        }

        fn shm_buffer_ids(&self) -> HashMap<BufferId, AllocationCapacity> {
            self.shm_map.clone()
        }

        fn hostmem_buffer_ids(&self) -> HashMap<BufferId, AllocationCapacity> {
            self.hostmem_map.clone()
        }

        fn shm_free_segments(&self) -> Vec<AllocationCapacity> {
            let free_cnt = (self.shm_capacity - self.shm_map.values().sum::<u64>())
                / MAX_ALLOCATION_SIZE as u64;
            vec![MAX_ALLOCATION_SIZE as u64; free_cnt as usize]
        }
        fn hostmem_free_segments(&self) -> Vec<AllocationCapacity> {
            let free_cnt = (self.hostmem_capacity - self.hostmem_map.values().sum::<u64>())
                / MAX_ALLOCATION_SIZE as u64;
            vec![MAX_ALLOCATION_SIZE as u64; free_cnt as usize]
        }

        fn gpu_free_space(&self, devices: &[GlobalDeviceId]) -> HashMap<GlobalDeviceId, u64> {
            let mut result = HashMap::new();
            for dev in devices.iter() {
                if let Some(size) = self.gpu_free_space.get(dev) {
                    result.insert(*dev, *size);
                }
            }
            result
        }
    }

    fn create_mock_task(
        schedued_pid: i32,
        new_allocation: Option<u64>,
        processes: Vec<MockProcessInput>,
        gpu_capacity: u64,
        shm_capacity: u64,
        hostmem_capacity: u64,
    ) -> DataMigrationTask<(), MockManager> {
        assert_ne!(schedued_pid, 0); // we don't use pid = 0 in tests to avoid accidently use Default.
        assert!(
            new_allocation.is_none()
                || processes
                    .iter()
                    .any(|p| (p.pid == schedued_pid) && (p.hostmem + p.shm + p.storage == 0))
        );
        let dev_mapping = Arc::new(DeviceOrdinalMapping::from_real_to_visible_map(
            HashMap::from([(GlobalDeviceId(0), ProcessLocalDeviceId(0))]),
        ));
        let process_data_list: Vec<MockProcessData> =
            processes.iter().map(MockProcessData::from_input).collect();
        let mut shm_map = HashMap::new();
        let mut hostmem_map = HashMap::new();
        let mut storage_map = HashMap::new();
        for process_data in process_data_list.iter() {
            for buf_id in process_data.shm_buffer_ids.iter() {
                shm_map.insert(buf_id.clone(), buf_id.size);
            }
            for buf_id in process_data.hostmem_buffer_ids.iter() {
                hostmem_map.insert(buf_id.clone(), buf_id.size);
            }
            for buf_id in process_data.storage_buffer_ids.iter() {
                storage_map.insert(buf_id.clone(), buf_id.size);
            }
        }
        assert!(
            process_data_list
                .iter()
                .map(|p| p.gpu_buffer_ids.iter().map(|b| b.size).sum::<u64>())
                .sum::<u64>()
                <= gpu_capacity
        );
        assert!(shm_map.values().sum::<u64>() <= shm_capacity);
        assert!(hostmem_map.values().sum::<u64>() <= hostmem_capacity);
        let data_manager = MockManager {
            shm_map,
            hostmem_map,
            storage_map,
            gpu_free_space: HashMap::from([(GlobalDeviceId(0), gpu_capacity)]),
            shm_capacity,
            hostmem_capacity,
        };
        let task = realtime_migrate_task(
            (
                schedued_pid,
                if let Some(size) = new_allocation {
                    DeviceRequestArgs::Allocation(HashMap::from([(GlobalDeviceId(0), size)]))
                } else {
                    DeviceRequestArgs::ResidualData(ProcessResidualData {
                        pid: schedued_pid,
                        allocations: HashMap::from([(
                            GlobalDeviceId(0),
                            process_data_list
                                .iter()
                                .find(|p| p.pid == schedued_pid)
                                .unwrap()
                                .gpu_buffer_ids
                                .iter()
                                .map(|b| PhysicalMemoryData {
                                    on_gpu: true,
                                    handle_idx: b.block_id,
                                    size: b.size,
                                })
                                .collect(),
                        )]),
                    })
                },
                (),
                dev_mapping.clone(),
            ),
            &process_data_list
                .iter()
                .filter(|p| p.pid != schedued_pid && !p.gpu_buffer_ids.is_empty())
                .map(|p| {
                    (
                        p.pid,
                        ProcessResidualData {
                            pid: p.pid,
                            allocations: HashMap::from([(
                                GlobalDeviceId(0),
                                p.gpu_buffer_ids
                                    .iter()
                                    .map(|b| PhysicalMemoryData {
                                        on_gpu: true,
                                        handle_idx: b.block_id,
                                        size: b.size,
                                    })
                                    .collect(),
                            )]),
                        },
                        (),
                        dev_mapping.clone(),
                    )
                })
                .collect::<Vec<_>>(),
            data_manager,
            true,
        )
        .unwrap();
        task
    }

    fn size_to_bytes(size: &str) -> u64 {
        // support GB/G/gb/g etc.
        let size = size.trim().to_lowercase();
        let mapping = HashMap::from([
            ("g", 1024u64 * 1024 * 1024),
            ("gb", 1024u64 * 1024 * 1024),
            ("m", 1024u64 * 1024),
            ("mb", 1024u64 * 1024),
            ("k", 1024u64),
            ("kb", 1024u64),
        ]);
        for (unit, multiplier) in mapping.iter() {
            if size.ends_with(unit) {
                let num_part = size.trim_end_matches(unit).trim();
                if let Ok(num) = num_part.parse::<f64>() {
                    return (num * (*multiplier as f64)) as u64;
                }
            }
        }
        if let Ok(num) = size.parse::<u64>() {
            return num;
        }
        panic!("Invalid size format: {}", size);
    }

    #[test]
    fn test_migration_task() {
        let task = create_mock_task(
            1,
            Some(size_to_bytes("10GB")),
            vec![
                MockProcessInput {
                    pid: 1,
                    gpu: size_to_bytes("8GB"),
                    shm: size_to_bytes("0GB"),
                    ..Default::default()
                },
                MockProcessInput {
                    pid: 2,
                    gpu: size_to_bytes("24GB"),
                    shm: size_to_bytes("16GB"),
                    ..Default::default()
                },
                MockProcessInput {
                    pid: 3,
                    shm: size_to_bytes("20GB"),
                    ..Default::default()
                },
            ],
            size_to_bytes("32GB"),
            size_to_bytes("36GB"),
            size_to_bytes("32GB"),
        );
        println!(
            "{}",
            "================================ Output Start ================================="
                .bold()
        );
        println!("Migration Task: {}", task.json_summary());
        let shm_to_backend = task
            .shm_to_backend
            .iter()
            .map(|(k, v)| BTreeMap::from([(k.pid, HashMap::from([(*v, k.size)]))]))
            .reduce(|mut acc, m| {
                for (k, v) in m {
                    acc.entry(k)
                        .and_modify(|e| {
                            for (loc, size) in &v {
                                *e.entry(*loc).or_insert(0) += size;
                            }
                        })
                        .or_insert(v);
                }
                acc
            });
        if let Some(shm_to_backend) = shm_to_backend {
            println!(
                "SHM to backend distribution: [{}]",
                shm_to_backend
                    .iter()
                    .map(|(k, v)| format!(
                        "pid {} = ({})",
                        k,
                        v.iter()
                            .map(|(loc, size)| format!("SHM -> {:?} {}", loc, pretty_size(*size)))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        assert!(check_task_no_deadlock(&task));
        println!(
            "{}",
            "================================= Output End =================================="
                .bold()
        );
    }

    pub(super) fn check_task_no_deadlock<C, Handle: AbstractDataHandle>(
        task: &DataMigrationTask<C, Handle>,
    ) -> bool {
        if task.shm_to_backend.is_empty() {
            // no data to move out, no deadlock
            return true;
        }
        let shm_free_size: u64 = task.data_manager.shm_free_segments().iter().sum();
        // let hostmem_free_size: u64 = task.data_manager.hostmem_free_segments().iter().sum();
        let mut ready_shm_xfer = 0;
        // check if any shm to backend is already available in shm
        for (buf_id, _) in task.shm_to_backend.iter() {
            if task.data_manager.shm_contains(buf_id) {
                ready_shm_xfer += buf_id.size;
            }
        }
        println!(
            "SHM free size: {}, ready SHM to backend size: {}",
            pretty_size(shm_free_size),
            pretty_size(ready_shm_xfer)
        );
        if shm_free_size == 0 && ready_shm_xfer == 0 {
            println!("Deadlock detected: no SHM free space, and no SHM to backend can be done.");
            return false;
        }
        return true;
    }
}
