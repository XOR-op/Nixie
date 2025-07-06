use nihil_common::*;
use nihil_common::{general::CallParameter, MigrationArgs, SchedulingArgs};

pub enum A2SMessage {
    Handshake(Handshake),
    NofityActivity(ActivityUpdate),
    MemoryRequest(CallParameter<MemoryRequest, ()>),
}

pub enum S2AMessage {
    MigrationRequest(CallParameter<MigrationArgs, MigrationResponse>),
    Scheduling(CallParameter<SchedulingArgs, ()>),
}
