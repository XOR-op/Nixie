use std::{
    path::PathBuf,
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
    pub preempt_delay: Option<Duration>,
    pub schedule_cooldown: Option<Duration>,
}

impl Config {
    pub fn to_configurable_args(&self) -> ConfigurableArgs {
        ConfigurableArgs {
            schedule_delay: self.schedule_delay,
            schedule_cooldown: self.schedule_cooldown,
            device_threshold: Some(self.device_threshold),
            preempt_delay: self.preempt_delay,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurableArgs {
    pub schedule_delay: Option<Duration>,
    pub schedule_cooldown: Option<Duration>,
    pub device_threshold: Option<f64>,
    pub preempt_delay: Option<Duration>,
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

pub fn init_config(config_path: Option<PathBuf>) -> Result<(), DaemonError> {
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
    // default value
    let mut config = Config {
        device_memory_mb,
        device_threshold: 0.95,
        schedule_delay: None,
        preempt_delay: Some(Duration::from_millis(50)),
        schedule_cooldown: None,
    };

    if let Some(config_path) = config_path {
        let config_content = std::fs::read_to_string(config_path)
            .map_err(|e| DaemonError::Io("read config file", e))?;
        let loaded_config: ConfigurableArgs =
            toml::from_str(&config_content).map_err(|e| DaemonError::Config("parse toml", e))?;
        update_config_from(&mut config, loaded_config);
    }

    let mut guard = CONFIG.write().unwrap();
    if guard.is_some() {
        panic!("config already initialized");
    }
    *guard = Some(Arc::new(config));
    Ok(())
}

pub fn update_config(config: ConfigurableArgs) {
    let mut guard = CONFIG.write().unwrap();
    let mut val = guard.as_ref().unwrap().as_ref().clone();
    update_config_from(&mut val, config);
    *guard = Some(Arc::new(val));
    tracing::info!("config updated: {:?}", guard.as_ref().unwrap());
}

fn update_config_from(config: &mut Config, args: ConfigurableArgs) {
    config.schedule_delay = args.schedule_delay;
    config.schedule_cooldown = args.schedule_cooldown;
    if let Some(device_threshold) = args.device_threshold {
        config.device_threshold = device_threshold;
    }
    config.preempt_delay = args.preempt_delay;
}
