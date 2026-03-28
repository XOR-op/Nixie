use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

use nixie_common::GlobalDeviceId;
use serde::{Deserialize, Serialize};

use crate::error::DaemonError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeviceLimitEntry {
    Ratio(f64),    // 0.0..=1.0
    Absolute(u64), // bytes
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceLimit {
    pub global: DeviceLimitEntry,
    pub per_device: HashMap<GlobalDeviceId, DeviceLimitEntry>,
}

impl DeviceLimit {
    pub fn get_bytes(&self, dev_id: GlobalDeviceId, total_bytes: u64) -> u64 {
        let entry = self.per_device.get(&dev_id).unwrap_or(&self.global);
        match entry {
            DeviceLimitEntry::Ratio(r) => (total_bytes as f64 * r) as u64,
            DeviceLimitEntry::Absolute(b) => *b,
        }
    }
}

fn device_limit_to_string(limit: &DeviceLimit, device_memory_mb: &[usize]) -> String {
    let global = match &limit.global {
        DeviceLimitEntry::Ratio(r) => format!(
            "Global: {:.2}({})",
            r,
            device_memory_mb
                .iter()
                .map(|mb| nixie_common::general::pretty_size(
                    ((*mb as u64 * 1024 * 1024) as f64 * r) as u64
                ))
                .collect::<Vec<_>>()
                .join(",")
        ),
        DeviceLimitEntry::Absolute(b) => {
            format!("Global: {}", nixie_common::general::pretty_size(*b))
        }
    };
    let per_device = limit
        .per_device
        .iter()
        .map(|(dev_id, entry)| {
            let entry_str = match entry {
                DeviceLimitEntry::Ratio(r) => format!(
                    "{:.2}({})",
                    r,
                    device_memory_mb
                        .get(dev_id.0 as usize)
                        .map(|mb| nixie_common::general::pretty_size(
                            ((*mb as u64 * 1024 * 1024) as f64 * r) as u64
                        ))
                        .unwrap_or_else(|| "unknown".to_string())
                ),
                DeviceLimitEntry::Absolute(b) => nixie_common::general::pretty_size(*b),
            };
            format!("{}:{}", dev_id.0, entry_str)
        })
        .collect::<Vec<_>>()
        .join(",");
    if per_device.is_empty() {
        global
    } else {
        format!("{}; Per-device: {}", global, per_device)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub shmem_size_mb: usize,
    pub hostmem_size_mb: usize,
    pub device_memory_mb: Vec<usize>,
    pub device_limit: DeviceLimit,
    pub schedule_cooldown: Option<Duration>,
    pub automatic_prefetch: bool,
    pub preallocate_hostmem: bool,
}

impl Config {
    pub fn to_configurable_args(&self) -> ConfigurableArgs {
        ConfigurableArgs {
            schedule_cooldown: self.schedule_cooldown,
            device_limit: Some(self.device_limit.clone()),
            automatic_prefetch: Some(self.automatic_prefetch),
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
        if let Some(schedule_cooldown) = other.schedule_cooldown {
            self.schedule_cooldown = Some(schedule_cooldown);
        }
        if let Some(preallocate_hostmem) = other.preallocate_hostmem {
            self.preallocate_hostmem = preallocate_hostmem;
        }
        // device_limit string is intentionally not merged here; handled in init_config
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitConfig {
    pub shmem_size_mb: Option<usize>,
    pub hostmem_size_mb: Option<usize>,
    pub device_memory_mb: Option<Vec<usize>>,
    pub device_limit: Option<String>,
    pub schedule_cooldown: Option<Duration>,
    pub automatic_prefetch: Option<bool>,
    pub preallocate_hostmem: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurableArgs {
    pub schedule_cooldown: Option<Duration>,
    pub device_limit: Option<DeviceLimit>,
    pub automatic_prefetch: Option<bool>,
}

/// CLI configuration options that override file config
#[derive(Debug, Clone, Default)]
pub struct CliConfig {
    pub shmem_size: Option<u64>,
    pub hostmem_size: Option<u64>,
    pub device_limit: Option<String>,
    pub automatic_prefetch: Option<bool>,
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

pub fn infer_default_config() -> Config {
    let nvml = crate::staticly::get_nvml();
    let devices = nvml.device_count().unwrap_or(0);
    let mut device_memory_mb = Vec::with_capacity(devices as usize);
    for i in 0..devices {
        let device = nvml.device_by_index(i).unwrap();
        let memory = device.memory_info().unwrap();
        device_memory_mb.push(memory.total as usize / 1024 / 1024);
    }
    Config {
        shmem_size_mb: 32 * 1024,
        hostmem_size_mb: 32 * 1024,
        device_memory_mb,
        device_limit: DeviceLimit {
            global: DeviceLimitEntry::Ratio(0.95),
            per_device: HashMap::new(),
        },
        schedule_cooldown: None,
        automatic_prefetch: true,
        preallocate_hostmem: false,
    }
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

    let mut config = infer_default_config();
    // Collect device_limit spec string from file and CLI (CLI wins)
    let mut device_limit_spec: Option<String> = None;

    // Apply file config first
    if let Some(config_path) = config_path {
        let config_content = std::fs::read_to_string(config_path)
            .map_err(|e| DaemonError::Io("read config file", e))?;
        let loaded_config: InitConfig =
            toml::from_str(&config_content).map_err(|e| DaemonError::Config("parse toml", e))?;
        if let Some(spec) = loaded_config.device_limit.clone() {
            device_limit_spec = Some(spec);
        }
        config.merge_from(loaded_config);
    }

    // Apply CLI config with higher priority (overrides file config)
    if let Some(shmem_size) = cli_config.shmem_size {
        config.shmem_size_mb = (shmem_size / (1024 * 1024)) as usize;
    }
    if let Some(hostmem_size) = cli_config.hostmem_size {
        config.hostmem_size_mb = (hostmem_size / (1024 * 1024)) as usize;
    }
    if let Some(spec) = cli_config.device_limit {
        device_limit_spec = Some(spec);
    }
    if let Some(automatic_prefetch) = cli_config.automatic_prefetch {
        config.automatic_prefetch = automatic_prefetch;
    }

    // Parse and validate device_limit spec if provided
    if let Some(spec) = device_limit_spec {
        let limit = crate::control::parse::parse_device_limit(&spec)
            .map_err(|e| DaemonError::ConfigValue("parse device_limit", e))?;
        crate::control::parse::validate_device_limit(&limit, &config.device_memory_mb)
            .map_err(|e| DaemonError::ConfigValue("validate device_limit", e))?;
        config.device_limit = limit;
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
    tracing::info!(
        "Device limit: {}",
        device_limit_to_string(&config.device_limit, &config.device_memory_mb)
    );
    *guard = Some(Arc::new(config));
    Ok(())
}

pub fn update_config(config: ConfigurableArgs) {
    let mut guard = CONFIG.write().unwrap();
    let mut val = guard.as_ref().unwrap().as_ref().clone();
    update_config_from(&mut val, config);
    *guard = Some(Arc::new(val));
    tracing::info!("config updated: {:?}", guard.as_ref().unwrap());
    tracing::info!(
        "New device limit: {}",
        device_limit_to_string(
            &guard.as_ref().unwrap().device_limit,
            &guard.as_ref().unwrap().device_memory_mb
        )
    );
}

fn update_config_from(config: &mut Config, args: ConfigurableArgs) {
    config.schedule_cooldown = args.schedule_cooldown;
    if let Some(device_limit) = args.device_limit {
        config.device_limit = device_limit;
    }
    if let Some(automatic_prefetch) = args.automatic_prefetch {
        config.automatic_prefetch = automatic_prefetch;
    }
}
