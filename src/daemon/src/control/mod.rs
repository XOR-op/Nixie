pub mod client;

use nihilipc::AttrType;
use serde::{Deserialize, Serialize};

use crate::config::{Config, ConfigurableArgs};

pub static CONTROL_PATH: &str = "/tmp/nihilphase-ctl.sock";

#[tarpc::service]
pub(crate) trait Controllable {
    async fn list_pid() -> Vec<i32>;

    async fn list_processes() -> Vec<ProcessMetadata>;

    async fn set_attr(args: AttrMsg);

    async fn prefetch(args: PrefetchMsg);

    async fn update_config(config: ConfigurableArgs);

    async fn get_config() -> Config;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AttrMsg {
    pub pid: i32,
    pub size_low: Option<u64>,
    pub size_high: Option<u64>,
    pub attr: AttrType,
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
    pub num_fault: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AllocationData {
    pub size: u64,
    pub device: i32,
    pub readonly: bool,
    pub move_reduced: bool,
}
