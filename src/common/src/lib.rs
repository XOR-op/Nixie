use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

mod constant;
pub mod general;
pub mod rpc;
pub mod shm;
pub mod sync;
pub use constant::*;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Handshake {
    pub pid: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitInfo {
    pub fd: Option<i32>,
    pub shm_path: String,
    pub visible_devices: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ActivityUpdate {
    RequestScheduling { memory_request: MemoryRequest },
    Idle,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MemoryUsage {
    pub on_gpu_bytes: u64,
    pub off_gpu_bytes: u64,
    pub alloc_count: u32,
}

// ------------------------------------------------------------

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MigrationArgs {
    pub size: u64,
    pub device: i32,
    pub handle_idx: NonZeroU32,
    pub host_buffer_idx: u32,
    pub host_to_device: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MigrationResponse {
    pub size: u64,
    pub device: i32,
    pub host_buffer_idx: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryRequest {
    pub mem_req: [Vec<u64>; MAX_GPUS],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchedulingArgs {
    Enable,
    Disable,
}
