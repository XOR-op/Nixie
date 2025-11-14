use nihil_common::GlobalDeviceId;

use crate::{ProcArgs, runtime::migration::BufferLocation};

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
fn parse_size(s: &str) -> Result<u64, String> {
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

#[cfg(test)]
mod tests {
    use super::*;
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
