use std::time::SystemTime;

use serde::{Deserialize, Serialize};

pub mod shm;
pub mod sync;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum C2SMessage {
    ClientHello(ClientHello),
    UvmFd(UvmFileDescriptor),
    ShmPath(ShmPath),
    MemoryUsage(MemoryUsageUpdate),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ClientHello {
    pub pid: i32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct UvmFileDescriptor {
    pub fd: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShmPath {
    pub path: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MemoryUsageUpdate {
    pub change: i64,
}

// ------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum S2CMessage {
    SetReadDup(SetReadDupArgs),
    GrantRunningToken(GrantRunningTokenArgs),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SetReadDupArgs {
    pub addr: u64,
    pub len: u64,
    pub device: i32,
    pub value: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct GrantRunningTokenArgs {
    pub time: SystemTime,
}
