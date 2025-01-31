use std::time::SystemTime;

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
pub struct MemoryUsageUpdate {
    pub change: i64,
}

// ------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum S2CMessage {
    ReadDup(AttrArgs),
    Prefetch(PrefetchArgs),
    GrantRunningToken(GrantRunningTokenArgs),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AttrType {
    ReadDup,
    PrefLoc,
    AccessedBy,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AttrArgs {
    pub addr: Option<u64>,
    pub len: u64,
    pub value: AttrType,
    pub will_set: bool,
    pub device: i32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PrefetchArgs {
    pub addr: u64,
    pub len: u64,
    pub device: i32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct GrantRunningTokenArgs {
    pub time: SystemTime,
}
