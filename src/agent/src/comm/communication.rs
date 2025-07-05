use nihil_common::{
    general::CallParameter, ActivityUpdate, MemoryRequest, MigrationArgs, MigrationResponse,
    SchedulingArgs,
};
use tarpc::context::Context;

use super::init::{init_comm, COMM};
use super::msg::{A2SMessage, S2AMessage};

macro_rules! chan_send {
    ($result:expr) => {
        if let Err(e) = $result {
            eprintln!("Error at {}:{}: {:?}", file!(), line!(), e);
        }
    };
}

pub(crate) fn update_activity(activity: ActivityUpdate) {
    let Some(chan) = COMM.get_or_init(init_comm) else {
        return;
    };
    chan_send!(chan.send(A2SMessage::NofityActivity(activity)));
}

pub(crate) fn request_memory(req: MemoryRequest) {
    let Some(chan) = COMM.get_or_init(init_comm) else {
        return;
    };
    chan_send!(chan.send(A2SMessage::MemoryRequest(req)));
}

pub(crate) fn migration_response_async(arg: Vec<MigrationResponse>) {
    let Some(chan) = COMM.get_or_init(init_comm) else {
        return;
    };
    chan_send!(chan.send(A2SMessage::MigrationResponse(arg)));
}

#[derive(Clone)]
pub(crate) struct SidecarServer {
    pub(super) sender: flume::Sender<S2AMessage>,
}

impl nihil_common::rpc::Sidecar for SidecarServer {
    async fn migrate_request_async(self, _context: Context, params: Vec<MigrationArgs>) {
        chan_send!(self.sender.send(S2AMessage::MigrationRequest(params)));
    }

    async fn schedule(self, _context: Context, params: SchedulingArgs) {
        let (params, fut) = CallParameter::new(params);
        chan_send!(self.sender.send(S2AMessage::Scheduling(params)));
        fut.await.unwrap_or_default()
    }
}
