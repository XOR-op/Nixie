pub mod client;

use serde::{Deserialize, Serialize};

pub static CONTROL_PATH: &str = "/tmp/nihilphase-ctl.sock";

#[tarpc::service]
pub(crate) trait Controllable {
    async fn list_processes() -> Vec<ProcessMetadata>;

    async fn read_dup(args: ReadDupMsg);

    async fn prefetch(args: PrefetchMsg);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReadDupMsg {
    pub pid: i32,
    pub size_low: Option<u64>,
    pub size_high: Option<u64>,
    pub set: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrefetchMsg {
    pub pid: i32,
    pub size_low: Option<u64>,
    pub size_high: Option<u64>,
    pub to_gpu: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProcessMetadata {
    pub pid: i32,
    pub allocations: Vec<AllocationData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AllocationData {
    pub size: u64,
    pub device: i32,
    pub read_only: bool, // TODO: Implement
}
