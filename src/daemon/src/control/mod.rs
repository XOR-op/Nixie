pub mod client;

use nihil_common::GlobalDeviceId;
use serde::{Deserialize, Serialize};

use crate::config::{Config, ConfigurableArgs};

pub static CONTROL_PATH: &str = "/tmp/nihilphase-ctl.sock";

#[tarpc::service]
pub(crate) trait Controllable {
    async fn list_pid() -> Vec<i32>;

    async fn list_processes() -> Vec<ProcessMetadata>;

    async fn prefetch(args: PrefetchMsg);

    async fn update_config(config: ConfigurableArgs);

    async fn get_config() -> Config;
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
    pub on_gpu_bytes: u64,
    pub off_gpu_bytes: u64,
    pub device: GlobalDeviceId,
}
