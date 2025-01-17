pub mod client;

use serde::{Deserialize, Serialize};

pub static CONTROL_PATH: &str = "/tmp/nihilphase-ctl.sock";

#[tarpc::service]
pub(crate) trait Controllable {
    async fn list();

    async fn set_read_dup(args: SetReadDupMsg);

    async fn prefetch(args: PrefetchMsg);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SetReadDupMsg {
    pid: i32,
    size_low: Option<u64>,
    size_high: Option<u64>,
    set: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrefetchMsg {
    pid: i32,
    size_low: Option<u64>,
    size_high: Option<u64>,
    to_gpu: bool,
}
