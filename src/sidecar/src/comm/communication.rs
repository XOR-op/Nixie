use nihil_common::{
    ActivityUpdate, MigrationArgs, MigrationResponse, SchedulingArgs, general::CallParameter,
};
use tarpc::context::Context;

use super::init::{COMM, init_comm};
use super::msg::{A2SMessage, S2AMessage};

macro_rules! chan_send {
    ($result:expr) => {
        if $result.is_err() {
            eprintln!("Send error at {}:{}", file!(), line!());
        }
    };
}

pub(crate) fn update_activity(activity: ActivityUpdate) {
    let Some(chan) = COMM.get_or_init(init_comm) else {
        return;
    };
    chan_send!(chan.send(A2SMessage::NofityActivity(activity)));
}

#[derive(Clone)]
pub(crate) struct SidecarServer {
    pub(super) sender: flume::Sender<S2AMessage>,
}

impl nihil_common::rpc::Sidecar for SidecarServer {
    async fn migrate(self, _context: Context, params: MigrationArgs) -> MigrationResponse {
        let (params, fut) = CallParameter::new(params);
        chan_send!(self.sender.send(S2AMessage::MigrationRequest(params)));
        fut.await
            .unwrap_or_else(|| todo!("Handle migration request failure"))
    }

    async fn schedule(self, _context: Context, params: SchedulingArgs) {
        let (params, fut) = CallParameter::new(params);
        chan_send!(self.sender.send(S2AMessage::Scheduling(params)));
        fut.await.unwrap_or_default()
    }
}
