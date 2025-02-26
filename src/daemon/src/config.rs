use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::error::DaemonError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub device_memory_mb: Vec<u64>,
    pub device_threshold: f64,
}

static CONFIG: OnceLock<Config> = OnceLock::new();

pub fn load_config() -> &'static Config {
    CONFIG.get().expect("Config not initialized")
}

pub fn init_config() -> Result<(), DaemonError> {
    let nvml = crate::staticly::get_nvml();
    let devices = nvml
        .device_count()
        .map_err(|e| DaemonError::Nvml("device_cound", e))?;
    let mut device_memory_mb = Vec::with_capacity(devices as usize);
    for i in 0..devices {
        let device = nvml
            .device_by_index(i)
            .map_err(|e| DaemonError::Nvml("device_by_index", e))?;
        let memory = device
            .memory_info()
            .map_err(|e| DaemonError::Nvml("memory_info", e))?;
        device_memory_mb.push(memory.total / 1024 / 1024);
    }
    CONFIG
        .set(Config {
            device_memory_mb,
            device_threshold: 0.95,
        })
        .expect("Config already initialized");
    Ok(())
}
