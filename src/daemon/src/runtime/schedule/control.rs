use nixie_common::general::CallParameter;

use crate::{
    control::{
        GetHistoryArgs, GetHistoryResult, PrefetchArgs, PrefetchResponse, SetPriorityArgs,
        SetPriorityResponse,
    },
    runtime::{ClientState, Priority},
};

pub enum ScheduleControlReq {
    GetState(CallParameter<i32, GetStateResponse>),
    Prefetch(CallParameter<PrefetchArgs, PrefetchResponse>),
    SetPriority(CallParameter<SetPriorityArgs, SetPriorityResponse>),
    GetHistory(CallParameter<GetHistoryArgs, GetHistoryResult>),
}

#[derive(Debug, Clone)]
pub struct GetStateResponse {
    pub priority: Option<Priority>,
    pub state: Option<ClientState>,
}
