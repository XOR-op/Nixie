use nihil_common::*;
use nihil_common::{general::CallParameter, MigrationArgs, SchedulingArgs};

#[derive(Debug, Clone)]
pub enum A2SMessage {
    Handshake(Handshake),
    NofityActivity(ActivityUpdate),
    MemoryRequest(MemoryRequest),
    MigrationResponse(Vec<MigrationResponse>),
}

pub enum S2AMessage {
    MigrationRequest(Vec<MigrationArgs>),
    Scheduling(CallParameter<SchedulingArgs, ()>),
}
