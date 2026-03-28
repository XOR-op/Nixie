use std::collections::HashMap;

use nixie_common::GlobalDeviceId;

use crate::{
    ProcArgs,
    config::{DeviceLimit, DeviceLimitEntry},
    runtime::migration::BufferLocation,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveOperation {
    pub pid: ProcArgs,
    pub src: BufferLocation,
    pub dest: BufferLocation,
    pub size: u64,
}

pub(crate) fn parse_move_ops(s: &str) -> Result<Vec<MoveOperation>, String> {
    s.split(',')
        .map(|part| parse_move_op(part.trim()))
        .collect::<Result<Vec<_>, _>>()
        .map(|v| v.into_iter().flatten().collect())
}

pub(crate) fn parse_pid(s: &str) -> Result<ProcArgs, String> {
    let s = s.to_lowercase();
    match s.parse::<u32>() {
        Ok(pid) => Ok(ProcArgs {
            pid: Some(pid as i32),
            idx: None,
        }),
        Err(_) => {
            let new_pid_str = if let Some(idx) = s.strip_prefix("idx") {
                idx
            } else if let Some(idx) = s.strip_prefix('i') {
                idx
            } else {
                &s
            };
            Ok(ProcArgs {
                pid: None,
                idx: Some(
                    new_pid_str
                        .parse::<u32>()
                        .map_err(|_| format!("Invalid PID or index: '{}'", s))?,
                ),
            })
        }
    }
}

// Parser function with signature: fn(&str) -> Result<T, String>
fn parse_move_op(s: &str) -> Result<Vec<MoveOperation>, String> {
    let s = s.to_lowercase();
    // Split "1100:gpu->cpu=10g" into "1100" and "gpu->cpu=10g"
    let (pid_str, rest) = s
        .split_once(':')
        .ok_or(format!("Invalid format: missing ':' in '{}'", s))?;

    // Parse PID
    let pid = parse_pid(pid_str)?;

    // Split by comma for micro op
    rest.split(',')
        .map(|part| parse_move_op_microop(pid, part.trim()))
        .collect::<Result<Vec<_>, _>>()
}

fn parse_move_op_microop(pid: ProcArgs, s: &str) -> Result<MoveOperation, String> {
    let s = s.to_lowercase();
    // Split "gpu->cpu=10g" into "gpu->cpu" and "10g"
    let (path_str, size_str) = s
        .split_once('=')
        .ok_or(format!("Invalid format: missing '=' in '{}'", s))?;

    // Split "gpu->cpu" into "gpu" and "cpu"
    let (src, dest) = path_str
        .split_once("->")
        .ok_or(format!("Invalid format: missing '->' in '{}'", s))?;

    Ok(MoveOperation {
        pid,
        src: parse_buffer_location(src)?,
        dest: parse_buffer_location(dest)?,
        size: parse_size(size_str)?,
    })
}

fn parse_buffer_location(s: &str) -> Result<BufferLocation, String> {
    match s.to_lowercase().as_str() {
        "gpu" => Ok(BufferLocation::Gpu(GlobalDeviceId(0))),
        gpu if gpu.starts_with("gpu") => {
            let id_str = &gpu[3..];
            let id = id_str
                .parse::<i32>()
                .map_err(|_| format!("Invalid GPU ID: '{}'", id_str))?;
            if !(0..8).contains(&id) {
                return Err(format!("GPU ID out of range (0-7): '{}'", id));
            }
            Ok(BufferLocation::Gpu(GlobalDeviceId(id)))
        }
        "host" | "hostmem" => Ok(BufferLocation::HostMem),
        "shm" => Ok(BufferLocation::Shm),
        "storage" | "disk" => Ok(BufferLocation::Storage),
        _ => Err(format!("Invalid buffer location: '{}'", s)),
    }
}

// can parse sizes like "10g", "512m", "1024k", "10gb","100mb" or just numbers in bytes
pub fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("Size string is empty".to_string());
    }

    let (num_str, unit) = s.chars().partition::<String, _>(|c| c.is_ascii_digit());
    if num_str.is_empty() {
        return Err(format!("Invalid size number in '{}'", s));
    }
    let multiplier = match unit.to_lowercase().as_str() {
        "" => 1,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        "t" | "tb" => 1024 * 1024 * 1024 * 1024,
        _ => return Err(format!("Invalid size unit in '{}'", s)),
    };

    let num = num_str
        .parse::<u64>()
        .map_err(|_| format!("Invalid size number: '{}'", num_str))?;
    Ok(num * multiplier)
}

/// Parse a device limit spec string into a `DeviceLimit`.
///
/// Format: `g:<value>[/<n>:<value>]*`
/// - `g` or `global` (case-insensitive): mandatory global default
/// - `<n>`: non-negative integer device index override
/// - Value: float ratio in [0, 1] or size string (e.g. "24g", "24576m")
///
/// All entries are parsed before any error is returned (all-or-nothing).
pub fn parse_device_limit(s: &str) -> Result<DeviceLimit, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("device limit spec is empty".into());
    }

    let mut global: Option<DeviceLimitEntry> = None;
    let mut per_device: HashMap<GlobalDeviceId, DeviceLimitEntry> = HashMap::new();
    let mut errors: Vec<String> = Vec::new();

    for entry in s.split('/') {
        let entry = entry.trim();
        let (key, val) = match entry.split_once(':') {
            Some(pair) => pair,
            None => {
                errors.push(format!("missing ':' in '{}'", entry));
                continue;
            }
        };
        let key = key.trim();
        let val = val.trim();

        let limit = match parse_limit_value(val) {
            Ok(l) => l,
            Err(e) => {
                errors.push(format!("invalid value '{}': {}", val, e));
                continue;
            }
        };

        match key.to_lowercase().as_str() {
            "g" | "global" => {
                if global.is_some() {
                    errors.push("duplicate global 'g' entry".into());
                } else {
                    global = Some(limit);
                }
            }
            idx_str => match idx_str.parse::<i32>().map(GlobalDeviceId) {
                Ok(idx) => {
                    if let std::collections::hash_map::Entry::Vacant(e) = per_device.entry(idx) {
                        e.insert(limit);
                    } else {
                        errors.push(format!("duplicate device entry for index {}", idx.0));
                    }
                }
                Err(_) => {
                    errors.push(format!(
                        "invalid key '{}': must be 'g' or a device index",
                        key
                    ));
                }
            },
        }
    }

    if !errors.is_empty() {
        return Err(errors.join("; "));
    }

    let global = global.ok_or_else(|| "missing mandatory 'g:' (global) entry".to_string())?;

    Ok(DeviceLimit { global, per_device })
}

fn parse_limit_value(s: &str) -> Result<DeviceLimitEntry, String> {
    // Try ratio: plain float (digits + optional dot)
    if let Ok(ratio) = s.parse::<f64>() {
        if !(0.0..=1.0).contains(&ratio) {
            return Err(format!("ratio {} is out of range [0.0, 1.0]", ratio));
        }
        return Ok(DeviceLimitEntry::Ratio(ratio));
    }
    // Otherwise treat as size string
    let bytes = parse_size(s)?;
    Ok(DeviceLimitEntry::Absolute(bytes))
}

/// Validate a `DeviceLimit` against the known device memory sizes (in MB, sourced from NVML).
///
/// Fails if:
/// - A per-device index is out of range
/// - An absolute limit exceeds the device's total memory
/// - The global absolute limit exceeds any device's memory that it would apply to
pub fn validate_device_limit(
    limit: &DeviceLimit,
    device_memory_mb: &[usize],
) -> Result<(), String> {
    // Validate per-device entries
    for (&dev_idx, entry) in &limit.per_device {
        let idx = dev_idx.0 as usize;
        if idx >= device_memory_mb.len() {
            return Err(format!(
                "device index {} does not exist ({} device(s) available)",
                dev_idx.0,
                device_memory_mb.len()
            ));
        }
        if let DeviceLimitEntry::Absolute(bytes) = entry {
            let dev_bytes = device_memory_mb[idx] as u64 * 1024 * 1024;
            if *bytes > dev_bytes {
                return Err(format!(
                    "limit for device {} ({}MB) exceeds its total memory ({}MB)",
                    dev_idx.0,
                    bytes / 1024 / 1024,
                    device_memory_mb[idx]
                ));
            }
        }
    }

    // Validate global absolute limit against all devices it covers (those not in per_device)
    if let DeviceLimitEntry::Absolute(bytes) = &limit.global {
        for (idx, &mem_mb) in device_memory_mb.iter().enumerate() {
            if limit.per_device.contains_key(&GlobalDeviceId(idx as i32)) {
                continue;
            }
            let dev_bytes = mem_mb as u64 * 1024 * 1024;
            if *bytes > dev_bytes {
                return Err(format!(
                    "global limit ({}MB) exceeds device {} total memory ({}MB)",
                    bytes / 1024 / 1024,
                    idx,
                    mem_mb
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse_device_limit_ratio() {
        let dl = parse_device_limit("g:0.95").unwrap();
        assert!(matches!(dl.global, DeviceLimitEntry::Ratio(r) if (r - 0.95).abs() < 1e-9));
        assert!(dl.per_device.is_empty());
    }

    #[test]
    fn test_parse_device_limit_absolute() {
        let dl = parse_device_limit("g:31g").unwrap();
        assert!(matches!(dl.global, DeviceLimitEntry::Absolute(b) if b == 31 * 1024 * 1024 * 1024));
    }

    #[test]
    fn test_parse_device_limit_per_device() {
        let dl = parse_device_limit("g:31g/3:24g").unwrap();
        assert!(matches!(dl.global, DeviceLimitEntry::Absolute(b) if b == 31 * 1024 * 1024 * 1024));
        assert!(
            matches!(dl.per_device.get(&GlobalDeviceId(3)), Some(DeviceLimitEntry::Absolute(b)) if *b == 24 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn test_parse_device_limit_missing_global() {
        assert!(parse_device_limit("3:24g").is_err());
    }

    #[test]
    fn test_parse_device_limit_duplicate_global() {
        assert!(parse_device_limit("g:31g/g:24g").is_err());
    }

    #[test]
    fn test_parse_device_limit_ratio_out_of_range() {
        assert!(parse_device_limit("g:2.0").is_err());
        assert!(parse_device_limit("g:-0.1").is_err());
    }

    #[test]
    fn test_validate_device_limit_exceeds_memory() {
        let dl = parse_device_limit("g:999g").unwrap();
        // device with 24576 MB
        assert!(validate_device_limit(&dl, &[24576]).is_err());
    }

    #[test]
    fn test_validate_device_limit_index_out_of_range() {
        let dl = parse_device_limit("g:0.9/99:24g").unwrap();
        assert!(validate_device_limit(&dl, &[24576]).is_err());
    }

    #[test]
    fn test_validate_device_limit_ok() {
        let dl = parse_device_limit("g:0.9/0:24g").unwrap();
        assert!(validate_device_limit(&dl, &[40960, 40960]).is_ok());
    }

    #[test]
    fn test_parse_move_op() {
        let op = parse_move_op("1100:gpu->shm=10g")
            .unwrap()
            .first()
            .unwrap()
            .clone();
        assert_eq!(
            op,
            MoveOperation {
                pid: ProcArgs {
                    pid: Some(1100),
                    idx: None
                },
                src: BufferLocation::Gpu(GlobalDeviceId(0)),
                dest: BufferLocation::Shm,
                size: 10 * 1024 * 1024 * 1024
            }
        );
        let op = parse_move_op("idx2:shm->gpu1=512m")
            .unwrap()
            .first()
            .unwrap()
            .clone();
        assert_eq!(
            op,
            MoveOperation {
                pid: ProcArgs {
                    pid: None,
                    idx: Some(2)
                },
                src: BufferLocation::Shm,
                dest: BufferLocation::Gpu(GlobalDeviceId(1)),
                size: 512 * 1024 * 1024
            }
        );
        let op = parse_move_op("i0:storage->hostmem=1g")
            .unwrap()
            .first()
            .unwrap()
            .clone();
        assert_eq!(
            op,
            MoveOperation {
                pid: ProcArgs {
                    pid: None,
                    idx: Some(0)
                },
                src: BufferLocation::Storage,
                dest: BufferLocation::HostMem,
                size: 1 * 1024 * 1024 * 1024
            }
        );
        let ops = parse_move_op("1100:gpu->shm=10g,shm->gpu=5g").unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!(
            ops[0],
            MoveOperation {
                pid: ProcArgs {
                    pid: Some(1100),
                    idx: None
                },
                src: BufferLocation::Gpu(GlobalDeviceId(0)),
                dest: BufferLocation::Shm,
                size: 10 * 1024 * 1024 * 1024
            }
        );
        assert_eq!(
            ops[1],
            MoveOperation {
                pid: ProcArgs {
                    pid: Some(1100),
                    idx: None
                },
                src: BufferLocation::Shm,
                dest: BufferLocation::Gpu(GlobalDeviceId(0)),
                size: 5 * 1024 * 1024 * 1024
            }
        );
    }

    #[test]
    fn test_parse_move_ops() {
        let ops = parse_move_ops("1100:gpu->shm=10g,idx3:shm->gpu=5g").unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!(
            ops[0],
            MoveOperation {
                pid: ProcArgs {
                    pid: Some(1100),
                    idx: None
                },
                src: BufferLocation::Gpu(GlobalDeviceId(0)),
                dest: BufferLocation::Shm,
                size: 10 * 1024 * 1024 * 1024
            }
        );
        assert_eq!(
            ops[1],
            MoveOperation {
                pid: ProcArgs {
                    pid: None,
                    idx: Some(3)
                },
                src: BufferLocation::Shm,
                dest: BufferLocation::Gpu(GlobalDeviceId(0)),
                size: 5 * 1024 * 1024 * 1024
            }
        );
    }
}
