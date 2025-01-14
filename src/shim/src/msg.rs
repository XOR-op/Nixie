use nihilipc::*;

#[derive(Debug, Clone)]
pub enum C2SMessage {
    InitClient(InitClient),
    UvmFd(UvmFd),
    ShmPath(ShmPath),
    MemoryUsage(MemoryUsageUpdate),
}
