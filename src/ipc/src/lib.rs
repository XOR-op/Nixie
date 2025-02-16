use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

pub mod rpc;
pub mod shm;
pub mod sync;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Handshake {
    pub pid: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitInfo {
    pub fd: i32,
    pub shm_path: String,
    pub visible_devices: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ActivityUpdate {}

// ------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum S2AMessage {
    SetAttr(AttrArgs),
    Prefetch(PrefetchArgs),
    Scheduling(SchedulingArgs),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AttrType {
    ReadDup,
    PrefLoc,
    AccessedBy,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AttrArgs {
    pub addr: Option<NonZeroU64>,
    pub len: u64,
    pub value: AttrType,
    pub will_set: bool,
    pub device: i32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PrefetchArgs {
    pub addr: u64,
    pub len: u64,
    pub to_gpu: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SchedulingArgs {
    Enable { prefetch: bool },
    Disable { swap_out_mb: Option<NonZeroU64> },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SchedulingResults {
    pub ro_size_mb: u64,
    pub rw_size_mb: u64,
    pub ro_duration: std::time::Duration,
    pub rw_duration: std::time::Duration,
}
