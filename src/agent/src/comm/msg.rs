use nihil_common::*;

#[derive(Debug, Clone)]
pub enum A2SMessage {
    Handshake(Handshake),
    InitInfo(InitInfo),
    NofityActivity(ActivityUpdate),
}
