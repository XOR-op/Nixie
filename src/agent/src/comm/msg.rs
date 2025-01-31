use nihilipc::*;

#[derive(Debug, Clone)]
pub enum C2SMessage {
    Handshake(Handshake),
    InitInfo(InitInfo),
    #[allow(dead_code)]
    MemoryUsage(MemoryUsageUpdate),
}
