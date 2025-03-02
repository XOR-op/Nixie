use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::error::DaemonError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub device_memory_mb: Vec<u64>,
    pub device_threshold: f64,
    pub schedule_delay: Option<Duration>,
}

static CONFIG: RwLock<Option<Arc<Config>>> = RwLock::new(None);

pub fn load_config() -> Arc<Config> {
    CONFIG
        .read()
        .unwrap()
        .as_ref()
        .expect("config not initialized")
        .clone()
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
    let mut guard = CONFIG.write().unwrap();
    if guard.is_some() {
        panic!("config already initialized");
    }
    *guard = Some(Arc::new(Config {
        device_memory_mb,
        device_threshold: 0.95,
        schedule_delay: None,
    }));
    Ok(())
}

pub fn update_config(config: Config) {
    let mut guard = CONFIG.write().unwrap();
    *guard = Some(Arc::new(config));
    tracing::info!("config updated: {:?}", guard.as_ref().unwrap());
}
