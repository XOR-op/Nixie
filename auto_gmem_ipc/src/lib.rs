use serde::{Deserialize, Serialize};

pub mod shm;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    ClientHello(ClientHello),
    UvmFd(UvmFileDescriptor),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ClientHello {
    pub pid: i32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct UvmFileDescriptor {
    pub fd: i32,
}
