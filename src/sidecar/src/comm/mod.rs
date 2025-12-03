mod communication;
mod controller;
pub(crate) mod init;
mod msg;
pub(crate) use communication::{update_activity, update_gpu_memory_free};
