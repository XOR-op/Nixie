use thiserror::Error;

#[derive(Debug, Error)]
pub enum AutoGMemError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("Invalid message")]
    InvalidMessage,
}