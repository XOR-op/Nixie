use nihil_common::general::CallParameter;

use crate::{
    control::{PrefetchArgs, PrefetchResponse, SetPriorityArgs, SetPriorityResponse},
    runtime::{ClientState, Priority},
};

pub enum ScheduleControlReq {
    GetState(CallParameter<i32, GetStateResponse>),
    Prefetch(CallParameter<PrefetchArgs, PrefetchResponse>),
    SetPriority(CallParameter<SetPriorityArgs, SetPriorityResponse>),
}

#[derive(Debug, Clone)]
pub struct GetStateResponse {
    pub priority: Option<Priority>,
    pub state: Option<ClientState>,
}
