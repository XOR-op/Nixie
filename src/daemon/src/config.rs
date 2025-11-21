use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::error::DaemonError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub shmem_size_mb: usize,
    pub hostmem_size_mb: usize,
    pub device_memory_mb: Vec<usize>,
    pub device_threshold: f64,
    pub schedule_cooldown: Option<Duration>,
    pub preallocate_hostmem: bool,
}

impl Config {
    pub fn to_configurable_args(&self) -> ConfigurableArgs {
        ConfigurableArgs {
            schedule_cooldown: self.schedule_cooldown,
            device_threshold: Some(self.device_threshold),
        }
    }

    pub fn merge_from(&mut self, other: InitConfig) {
        if let Some(shmem_size_mb) = other.shmem_size_mb {
            self.shmem_size_mb = shmem_size_mb;
        }
        if let Some(hostmem_size_mb) = other.hostmem_size_mb {
            self.hostmem_size_mb = hostmem_size_mb;
        }
        if let Some(device_memory_mb) = other.device_memory_mb {
            self.device_memory_mb = device_memory_mb;
        }
        if let Some(device_threshold) = other.device_threshold {
            self.device_threshold = device_threshold;
        }
        if let Some(schedule_cooldown) = other.schedule_cooldown {
            self.schedule_cooldown = Some(schedule_cooldown);
        }
        if let Some(preallocate_hostmem) = other.preallocate_hostmem {
            self.preallocate_hostmem = preallocate_hostmem;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitConfig {
    pub shmem_size_mb: Option<usize>,
    pub hostmem_size_mb: Option<usize>,
    pub device_memory_mb: Option<Vec<usize>>,
    pub device_threshold: Option<f64>,
    pub schedule_cooldown: Option<Duration>,
    pub preallocate_hostmem: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurableArgs {
    pub schedule_cooldown: Option<Duration>,
    pub device_threshold: Option<f64>,
}

/// CLI configuration options that override file config
#[derive(Debug, Clone, Default)]
pub struct CliConfig {
    pub shmem_size: Option<u64>,
    pub hostmem_size: Option<u64>,
    pub device_threshold: Option<f64>,
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

pub fn init_config(config_path: Option<PathBuf>, cli_config: CliConfig) -> Result<(), DaemonError> {
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
        device_memory_mb.push(memory.total as usize / 1024 / 1024);
    }
    // default value
    let mut config = Config {
        shmem_size_mb: 36 * 1024,
        hostmem_size_mb: 32 * 1024,
        device_memory_mb,
        device_threshold: 0.95,
        schedule_cooldown: None,
        preallocate_hostmem: false,
    };

    // Apply file config first
    if let Some(config_path) = config_path {
        let config_content = std::fs::read_to_string(config_path)
            .map_err(|e| DaemonError::Io("read config file", e))?;
        let loaded_config: InitConfig =
            toml::from_str(&config_content).map_err(|e| DaemonError::Config("parse toml", e))?;
        config.merge_from(loaded_config);
    }

    // Apply CLI config with higher priority (overrides file config)
    if let Some(shmem_size) = cli_config.shmem_size {
        config.shmem_size_mb = (shmem_size / (1024 * 1024)) as usize;
    }
    if let Some(hostmem_size) = cli_config.hostmem_size {
        config.hostmem_size_mb = (hostmem_size / (1024 * 1024)) as usize;
    }
    if let Some(device_threshold) = cli_config.device_threshold {
        config.device_threshold = device_threshold;
    }

    if !config.shmem_size_mb.is_multiple_of(2) || !config.hostmem_size_mb.is_multiple_of(2) {
        return Err(DaemonError::ConfigValue(
            "validate config",
            format!(
                "shmem_size_mb={} and hostmem_size_mb={} must be multiples of 2",
                config.shmem_size_mb, config.hostmem_size_mb
            ),
        ));
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
    config.schedule_cooldown = args.schedule_cooldown;
    if let Some(device_threshold) = args.device_threshold {
        config.device_threshold = device_threshold;
    }
}
