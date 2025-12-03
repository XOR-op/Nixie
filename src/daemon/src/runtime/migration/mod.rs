mod channel;
mod execution;
mod hostmem_buffer;
pub(super) mod migration_plan;
mod shm_buffer;
mod storage_buffer;

use std::{num::NonZeroU64, sync::Arc};

pub use hostmem_buffer::HostMemBufferManager;
use nihil_common::GlobalDeviceId;
use serde::{Deserialize, Serialize};
pub use shm_buffer::ShmBufferManager;
pub use storage_buffer::StorageBufferManager;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BufferId {
    pub pid: i32,
    pub device_id: GlobalDeviceId,
    pub block_id: NonZeroU64,
    pub size: u32,
}

impl BufferId {
    pub fn get_allocation_count(&self) -> AllocationCount {
        AllocationCount(self.size.div_ceil(nihil_common::MIN_ALLOCATION_SIZE as u32))
    }
}

#[derive(Debug, Clone)]
pub struct AllocationInfo {
    addr: u64,
    block_size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum BufferLocation {
    Gpu(GlobalDeviceId),
    Shm,
    HostMem,
    Storage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum ShmBufferRequest {
    FromGPU(AllocationCount),
    FromBackend(AllocationCount),
}

impl ShmBufferRequest {
    pub fn count(&self) -> AllocationCount {
        match self {
            ShmBufferRequest::FromGPU(count) => *count,
            ShmBufferRequest::FromBackend(count) => *count,
        }
    }
}

#[derive(Clone)]
pub struct DataManagerHandle {
    pub shm: Arc<ShmBufferManager>,
    pub hostmem: Arc<HostMemBufferManager>,
    pub storage: Arc<StorageBufferManager>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Offset(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AllocationCapacity(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AllocationCount(pub u32);

impl std::ops::Add for AllocationCount {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        AllocationCount(self.0 + rhs.0)
    }
}

impl std::ops::AddAssign for AllocationCount {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}
