use std::sync::OnceLock;

use crate::{comm::notify_init_info, warn_eprintln, GenericData};

pub(crate) static VALID_UVM_FD: OnceLock<i32> = OnceLock::new();
pub(crate) static UVM_FD_CANDIDATES: std::sync::Mutex<Vec<i32>> = std::sync::Mutex::new(Vec::new());

pub(crate) fn init_generic_data() -> GenericData {
    // CUDA libraries initialized; send UVM FD to daemon
    assert!(VALID_UVM_FD.get().is_none(), "UVM FD already set");
    let list = UVM_FD_CANDIDATES.lock().unwrap();
    let uvm_fd = if let Some(fd) = list.first().copied() {
        let _ = VALID_UVM_FD.set(fd);
        Some(fd)
    } else {
        warn_eprintln!("Failed to find valid UVM FD");
        None
    };
    // And create ptr mapping
    let uuid = uuid::Uuid::new_v4();
    let path = format!(
        "/nihilphase_ipc-{}-{}.shm",
        std::process::id(),
        uuid.to_string().split_at(8).0
    );
    let result = GenericData::new(&path);
    let cuda_visible_devices = std::env::var("CUDA_VISIBLE_DEVICES").unwrap_or_default();
    notify_init_info(uvm_fd, path, cuda_visible_devices);
    result
}
