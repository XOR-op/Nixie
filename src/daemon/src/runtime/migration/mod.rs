mod channel;
mod hostmem_buffer;
mod migration;
pub(super) mod migration_plan;
mod shm_buffer;
mod storage_buffer;

use std::{num::NonZeroU32, sync::Arc};

pub use hostmem_buffer::HostMemBufferManager;
use nihil_common::GlobalDeviceId;
pub use shm_buffer::ShmBufferManager;
pub use storage_buffer::StorageBufferManager;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BufferId {
    pub pid: i32,
    pub device_id: GlobalDeviceId,
    pub block_id: NonZeroU32,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct AllocationInfo {
    addr: u64,
    block_size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BufferLocation {
    HostMem,
    Storage,
}

#[derive(Clone)]
pub struct DataManagerHandle {
    pub shm: Arc<ShmBufferManager>,
    pub hostmem: Arc<HostMemBufferManager>,
    pub storage: Arc<StorageBufferManager>,
}
