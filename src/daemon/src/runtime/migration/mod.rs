mod channel;
mod hybrid_buffer;
mod migration;
pub(super) mod migration_plan;
mod shm_buffer;

use std::num::NonZeroU32;

pub use hybrid_buffer::HybridBufferManager;
use nihil_common::GlobalDeviceId;
pub use shm_buffer::ShmBufferManager;

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
