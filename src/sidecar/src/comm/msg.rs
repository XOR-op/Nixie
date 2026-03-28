use nixie_common::*;
use nixie_common::{MigrationArgs, SchedulingArgs, general::CallParameter};

pub enum A2SMessage {
    Handshake(Handshake),
    ActivityUpdate(ActivityUpdate),
    GpuMemoryFreeUpdate(GpuMemoryFreeUpdate),
}

pub enum S2AMessage {
    MigrationRequest(CallParameter<MigrationArgs, MigrationResponse>),
    Scheduling(CallParameter<SchedulingArgs, ()>),
}
