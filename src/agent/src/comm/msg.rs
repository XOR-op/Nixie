use nihilipc::*;

#[derive(Debug, Clone)]
pub enum A2SMessage {
    Handshake(Handshake),
    InitInfo(InitInfo),
    NofityActivity(ActivityUpdate),
}
