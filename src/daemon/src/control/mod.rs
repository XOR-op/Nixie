pub mod client;
pub(crate) mod parse;

use std::{collections::HashMap, time::SystemTime};

use nihil_common::{GlobalDeviceId, shm::PhysicalMemoryHandleId};
use serde::{Deserialize, Serialize};

use crate::{
    config::{Config, ConfigurableArgs},
    runtime::{ClientState, Priority, PriorityLevel, migration::BufferLocation},
};

pub static CONTROL_PATH: &str = "/tmp/nihilphase-ctl.sock";

#[tarpc::service]
pub(crate) trait Controllable {
    async fn list_pid() -> Vec<i32>;

    async fn list_processes() -> Vec<ProcessMetadata>;

    async fn data_details() -> DataManagerMetadata;

    async fn set_priority(args: SetPriorityArgs) -> Result<SetPriorityResponse, ()>;

    async fn prefetch(args: PrefetchArgs) -> Result<PrefetchResponse, ()>;

    async fn get_history(args: GetHistoryArgs) -> Result<GetHistoryResult, ()>;

    async fn update_config(config: ConfigurableArgs);

    async fn get_config() -> Config;
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrefetchArgs {
    pub list: Vec<PrefetchMsg>,
    pub rx_used: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrefetchResponse;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrefetchMsg {
    pub pid: i32,
    pub from: BufferLocation,
    pub to: BufferLocation,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum SetPriorityLevel {
    Set(Priority),
    FixToDynamic,
    UnsetToDefault,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SetPriorityArgs {
    pub pid: i32,
    pub level: SetPriorityLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum SetPriorityResponse {
    Success,
    FailureProcessNotExist,
    FailurePriorityNotFixed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GetHistoryArgs {
    pub pid: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GetHistoryResponse {
    pub entries: Vec<HistoryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum GetHistoryResult {
    Success(GetHistoryResponse),
    FailureProcessNotExist,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HistoryEntry {
    pub start: SystemTime,
    pub duration_ms: u128,
    pub start_priority: PriorityLevel,
    pub end_priority: PriorityLevel,
    pub stop_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProcessMetadata {
    pub pid: i32,
    pub state: Option<ClientState>,
    pub priority: Option<Priority>,
    pub allocations: Vec<(GlobalDeviceId, Vec<AllocationData>)>, // Global device ID
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProcessResidualRequest {
    pub pid: i32,
    pub on_gpu: bool,
    pub gpu_list: Vec<GlobalDeviceId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProcessResidualData {
    pub pid: i32,
    pub allocations: HashMap<GlobalDeviceId, Vec<PhysicalMemoryData>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AllocationData {
    pub on_gpu_bytes: u64,
    pub off_gpu_bytes: u64,
    pub physical: Vec<PhysicalMemoryData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PhysicalMemoryData {
    pub on_gpu: bool,
    pub handle_idx: PhysicalMemoryHandleId,
    pub size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DataManagerMetadata {
    pub shm: Vec<ProcessDataMeta>,
    pub hostmem: Vec<ProcessDataMeta>,
    pub storage: Vec<ProcessDataMeta>,
    pub shm_capacity: u64,
    pub hostmem_capacity: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProcessDataMeta {
    pub pid: i32,
    pub data_blocks: Vec<DataBlockMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DataBlockMeta {
    pub device_id: GlobalDeviceId,
    pub size: u64,
}
