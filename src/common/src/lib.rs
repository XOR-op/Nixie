use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

mod constant;
pub mod general;
pub mod rpc;
pub mod shm;
pub mod shm_buffer;
pub mod sync;
pub use constant::*;

// Device IDs for processes may be overridden by CUDA_VISIBLE_DEVICES.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProcessLocalDeviceId(pub i32);
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GlobalDeviceId(pub i32);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Handshake {
    pub pid: i32,
    pub shm_path: String,
    pub visible_devices: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeResponse {
    pub available_vram_sizes: Vec<(ProcessLocalDeviceId, u64)>,
    pub buffer_shm_path: String,
    pub buffer_length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityUpdate {
    pub message_id: u64,
    pub content: ActivityUpdateContent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ActivityUpdateContent {
    RequestScheduling,
    YieldThenRequestSchedulingAndMem { memory_request: Box<MemoryRequest> },
    Idle,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MemoryUsage {
    pub on_gpu_bytes: u64,
    pub off_gpu_bytes: u64,
    pub alloc_count: u32,
}

// ------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationArgs {
    // in different vectors for less padding; length must be the same
    pub host_buffer_offset: Vec<u64>,
    pub size: Vec<u32>,
    pub device: ProcessLocalDeviceId,
    pub handle_idx: NonZeroU32,
    pub host_to_device: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MigrationResponse {
    Success {
        handle_idx: NonZeroU32,
        device: ProcessLocalDeviceId,
        size: u64,
    },
    AlreadyFreed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryRequest {
    pub mem_req: [(ProcessLocalDeviceId, Vec<u64>); MAX_GPUS], // local device id
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchedulingArgs {
    Enable,
    Disable,
}
