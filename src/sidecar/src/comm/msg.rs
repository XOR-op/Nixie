use nihil_common::*;
use nihil_common::{MigrationArgs, SchedulingArgs, general::CallParameter};

pub enum A2SMessage {
    Handshake(Handshake),
    NofityActivity(ActivityUpdate),
}

pub enum S2AMessage {
    MigrationRequest(CallParameter<MigrationArgs, MigrationResponse>),
    Scheduling(CallParameter<SchedulingArgs, ()>),
}
