use nihil_common::general::CallParameter;

use crate::runtime::{ClientState, Priority};

pub enum ScheduleControlReq {
    GetState(CallParameter<i32, GetStateResponse>),
}

#[derive(Debug, Clone)]
pub struct GetStateResponse {
    pub priority: Option<Priority>,
    pub state: Option<ClientState>,
}
