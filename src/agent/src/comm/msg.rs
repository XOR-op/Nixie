use nihil_common::*;
use nihil_common::{general::CallParameter, MigrationArgs, SchedulingArgs};

pub enum A2SMessage {
    Handshake(Handshake),
    NofityActivity(ActivityUpdate),
    MemoryRequest(CallParameter<MemoryRequest, ()>),
    MigrationResponse(Vec<MigrationResponse>),
}

pub enum S2AMessage {
    MigrationRequest(Vec<MigrationArgs>),
    Scheduling(CallParameter<SchedulingArgs, ()>),
}
