use nix::errno::Errno;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum NihilphaseError {
    #[error("Daemon: {0}")]
    Daemon(#[from] DaemonError),
    #[error("Uvm: {0}")]
    Uvm(#[from] UvmError),
    #[error("Client: {0}")]
    Client(#[from] ClientError),
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("{0}: RPC error {1}")]
    ClientRpc(&'static str, tarpc::client::RpcError),
    #[error("{0}: Invalid arguments")]
    Args(String),
    #[error("{0}: IO error {1}")]
    Io(&'static str, std::io::Error),
    #[error("{0}: NVML error {1:?}")]
    Nvml(&'static str, nvml_wrapper::error::NvmlError),
}

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error("{0}: IO error {1}")]
    Io(&'static str, std::io::Error),
    #[error("{0}: error with {1}")]
    Errno(&'static str, nix::errno::Errno),
    #[error("{0}: CUDA error {1:?}")]
    Cuda(&'static str, cudarc::driver::sys::cudaError_enum),
    #[error("{0}: NVML error {1}")]
    Nvml(&'static str, nvml_wrapper::error::NvmlError),
    #[error("{0}: Config error {1}")]
    Config(&'static str, toml::de::Error),
    #[error("HybridBuffer: {0}")]
    HybridBuffer(#[from] HybridBufferError),
}

#[derive(Debug, Error)]
pub enum HybridBufferError {
    #[error("Failed to allocate hybrid buffer")]
    MemoryExhausted,
    #[error("Invalid hybrid buffer")]
    InvalidInputBuffer,
    #[error("Invalid BufferId")]
    NoBufferId,
    #[error("{1} failed with error: {0}")]
    IoError(std::io::Error, String),
    #[error("Tokio task failed: {0}")]
    TaskError(#[from] tokio::task::JoinError),
}

#[derive(Debug, Error)]
pub enum UvmError {
    #[error("Assertion failed: {0}")]
    Assertion(&'static str),
    #[error("{0} failed with error: {1}")]
    LibError(&'static str, Errno),
    #[error("{0} failed with error: {1}, (version?: {2})")]
    DriverError(&'static str, i32, u32),
    #[error("{0} failed with IO error: {1}")]
    Io(&'static str, std::io::Error),
}

#[derive(Debug, Error)]
pub enum ScheduleError {
    #[error("Invalid process: {0}")]
    InvalidClient(i32),
    #[error("{0} Failed to send RPC to {1}: {2}")]
    RpcError(&'static str, i32, tarpc::client::RpcError),
    #[error("Invalid prefetch request: {0}")]
    InvalidPrefetchRequest(String),
    #[error("Unavailable resource: {0}")]
    Unavailable(String),
}
