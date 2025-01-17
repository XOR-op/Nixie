use thiserror::Error;

#[derive(Debug, Error)]
pub enum NihilphaseError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("Unix error: {0} for {1}")]
    Errno(nix::errno::Errno, &'static str),
    #[error("Invalid: {0}")]
    Invalid(&'static str),
    #[error("Invalid: {0}")]
    Invalid2(String),
    #[error("RPC Client error: {0}")]
    RpcClient(#[from] tarpc::client::RpcError),
}
