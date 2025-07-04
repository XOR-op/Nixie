use serde::{Deserialize, Serialize};

pub mod general;
pub mod rpc;
pub mod shm;
pub mod sync;

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
    RequestScheduling {
        mem_usage_per_device: Vec<MemoryUsage>,
    },
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
pub enum S2AMessage {
    Migration(MigrationArgs),
    Scheduling(SchedulingArgs),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MigrationArgs {
    pub addr: u64,
    pub len: u64,
    pub to_gpu: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchedulingArgs {
    Enable,
    Disable,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SchedulingResults {
    pub ro_size_mb: u64,
    pub rw_size_mb: u64,
    pub ro_duration: std::time::Duration,
    pub rw_duration: std::time::Duration,
}
