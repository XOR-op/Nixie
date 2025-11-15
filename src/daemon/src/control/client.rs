use std::{collections::HashMap, time::Duration};

use colored::Colorize;
use tarpc::tokio_util::codec::LengthDelimitedCodec;
use tokio_serde::formats::Cbor;

use crate::{
    ProcArgs, UpdateConfigArgs,
    control::{
        PrefetchResponse, ProcessMetadata, SetPriorityArgs, SetPriorityLevel, SetPriorityResponse,
    },
    error::ClientError,
    runtime::Priority,
};
use nihil_common::{GlobalDeviceId, general::pretty_size};

use super::ControllableClient;

pub(crate) struct ControlClient {
    client: ControllableClient,
}

fn get_pid_checked(args: ProcArgs, pid_list: &[i32]) -> Result<i32, ClientError> {
    if let Some(pid) = args.pid {
        if !pid_list.contains(&pid) {
            return Err(ClientError::Args(format!(
                "Process with pid {} not found",
                pid
            )));
        }
        Ok(pid)
    } else if let Some(idx) = args.idx {
        if (idx as usize) >= pid_list.len() {
            return Err(ClientError::Args(format!(
                "Process index {} out of range",
                idx
            )));
        }
        println!(
            "Process index {} mapped to pid {}",
            idx, pid_list[idx as usize]
        );
        Ok(pid_list[idx as usize])
    } else {
        Err(ClientError::Args(
            "Either pid or idx must be specified".to_string(),
        ))
    }
}

impl ControlClient {
    pub async fn new(path: &str) -> Result<Self, ClientError> {
        let conn = tokio::net::UnixStream::connect(path)
            .await
            .map_err(|e| ClientError::Io("Failed to connect to control socket", e))?;
        let conn = tarpc::serde_transport::new(
            LengthDelimitedCodec::builder().new_framed(conn),
            Cbor::default(),
        );
        let client = ControllableClient::new(Default::default(), conn).spawn();
        Ok(Self { client })
    }

    async fn get_pid_list(&self) -> Result<Vec<i32>, ClientError> {
        let r = self
            .client
            .list_pid(tarpc::context::current())
            .await
            .map_err(|e| ClientError::ClientRpc("list_pid", e))?;
        Ok(r)
    }

    pub async fn prefetch(
        &self,
        args: crate::PrefetchArgs,
    ) -> Result<PrefetchResponse, ClientError> {
        let pid_list = self.get_pid_list().await?;
        let processed_args = crate::control::PrefetchArgs {
            list: args
                .move_ops
                .into_iter()
                .map(|msg| {
                    let pid = get_pid_checked(msg.pid, &pid_list)?;
                    Ok(crate::control::PrefetchMsg {
                        pid,
                        from: msg.src,
                        to: msg.dest,
                        size: msg.size,
                    })
                })
                .collect::<Result<Vec<_>, ClientError>>()?,
        };
        let mut rpc_ctx = tarpc::context::current();
        rpc_ctx.deadline = std::time::Instant::now() + Duration::from_secs(120);
        if self
            .client
            .prefetch(rpc_ctx, processed_args)
            .await
            .map_err(|e| ClientError::ClientRpc("prefetch", e))?
            .is_err()
        {
            eprintln!("{}", "Prefetch request failed".red());
        }
        Ok(PrefetchResponse)
    }

    pub async fn list_processes(&self, verbose: bool) -> Result<(), ClientError> {
        let processes = filter_invalid_processes(
            self.client
                .list_processes(tarpc::context::current())
                .await
                .map_err(|e| ClientError::ClientRpc("list_processes", e))?,
        );
        // print info
        println!("Active processes: {}", processes.len());
        for (idx, process) in processes.into_iter().enumerate() {
            let process_name = std::fs::read_to_string(format!("/proc/{}/comm", process.pid))
                .map(|s| s.trim().to_string())
                .ok();
            let priority_str = match &process.priority {
                Some(p) => match *p {
                    Priority::Dynamic { level, weight } => {
                        format!("{:?}/{}", level, weight).purple()
                    }
                    Priority::Fixed(level) => format!("{:?}[F]", level).bright_yellow(),
                },
                None => "N/A".to_string().bright_black(),
            };
            println!(
                "{} [{}]{} <{}, {}>",
                format!("#{}", idx).magenta(),
                process.pid.to_string().yellow(),
                process_name
                    .map_or("".to_string(), |s| format!(" {}", s))
                    .green(),
                process
                    .state
                    .map_or("Unknown".to_string(), |s| format!("{:?}", s))
                    .blue(),
                priority_str
            );
            for (device, allocations) in process.allocations {
                // print aggregated per device info
                let alloc_size = allocations
                    .iter()
                    .map(|a| a.on_gpu_bytes + a.off_gpu_bytes)
                    .sum::<u64>();
                let on_gpu_size = allocations.iter().map(|a| a.on_gpu_bytes).sum::<u64>();
                println!(
                    "{} #alloc = {}, size = {}/{}",
                    format!("<Device {}>", device.0).cyan(),
                    format!("{}", allocations.len()).yellow(),
                    pretty_size(on_gpu_size).bright_blue(),
                    pretty_size(alloc_size).blue()
                );
                if verbose {
                    // print each allocation info
                    for (idx, a) in allocations.into_iter().enumerate() {
                        println!(
                            "\t{}: size = {}/{}",
                            format!("<Allocation {}>", idx).cyan(),
                            pretty_size(a.on_gpu_bytes).bright_blue(),
                            pretty_size(a.on_gpu_bytes + a.off_gpu_bytes).blue(),
                        )
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn data_details(
        &self,
        with_gpu_info: bool,
        verbose: bool,
    ) -> Result<(), ClientError> {
        let processes = if with_gpu_info {
            filter_invalid_processes(
                self.client
                    .list_processes(tarpc::context::current())
                    .await
                    .map_err(|e| ClientError::ClientRpc("list_processes", e))?,
            )
        } else {
            Vec::new()
        };
        let data_meta = self
            .client
            .data_details(tarpc::context::current())
            .await
            .map_err(|e| ClientError::ClientRpc("data_details", e))?;
        let shm_used = data_meta
            .shm
            .iter()
            .map(|p| p.data_blocks.iter().map(|b| b.size).sum::<u64>())
            .sum::<u64>();
        let hostmem_used = data_meta
            .hostmem
            .iter()
            .map(|p| p.data_blocks.iter().map(|b| b.size).sum::<u64>())
            .sum::<u64>();
        let storage_used = data_meta
            .storage
            .iter()
            .map(|p| p.data_blocks.iter().map(|b| b.size).sum::<u64>())
            .sum::<u64>();
        // use nvml to get GPU memory usage if possible
        if with_gpu_info {
            let nvml = crate::staticly::get_nvml();
            for dev in 0..(nvml
                .device_count()
                .map_err(|e| ClientError::Nvml("device_count", e))?)
            {
                let device = nvml
                    .device_by_index(dev)
                    .map_err(|e| ClientError::Nvml("device_by_index", e))?;
                let memory_info = device
                    .memory_info()
                    .map_err(|e| ClientError::Nvml("memory_info", e))?;
                let n_proc_on_dev = processes
                    .iter()
                    .filter(|p| {
                        p.allocations.iter().any(|(d, alloc)| {
                            *d == GlobalDeviceId(dev as i32)
                                && alloc.iter().any(|a| a.on_gpu_bytes > 0)
                        })
                    })
                    .count();
                println!(
                    "Device {}: {}/{} ({}, {} procs)",
                    dev,
                    pretty_size(memory_info.used).bright_blue(),
                    pretty_size(memory_info.total).blue(),
                    pretty_precentage(
                        (memory_info.used as f64) / (memory_info.total as f64) * 100.0
                    ),
                    n_proc_on_dev
                );
            }
        }
        println!(
            "SHM: {}/{} ({}, {} procs)\nHostMem: {}/{} ({}, {} procs)\nDisk: {} ({} procs)",
            pretty_size(shm_used).bright_blue(),
            pretty_size(data_meta.shm_capacity).blue(),
            pretty_precentage((shm_used as f64) / (data_meta.shm_capacity as f64) * 100.0),
            data_meta.shm.len(),
            pretty_size(hostmem_used).bright_blue(),
            pretty_size(data_meta.hostmem_capacity).blue(),
            pretty_precentage((hostmem_used as f64) / (data_meta.hostmem_capacity as f64) * 100.0,),
            data_meta.hostmem.len(),
            pretty_size(storage_used).bright_blue(),
            data_meta.storage.len()
        );

        if verbose {
            let mut proc_shm = HashMap::new();
            let mut proc_hostmem = HashMap::new();
            let mut proc_storage = HashMap::new();
            for p in data_meta.shm {
                let used = p.data_blocks.iter().map(|b| b.size).sum::<u64>();
                proc_shm.insert(p.pid, (used, p.data_blocks.len()));
            }
            for p in data_meta.hostmem {
                let used = p.data_blocks.iter().map(|b| b.size).sum::<u64>();
                proc_hostmem.insert(p.pid, (used, p.data_blocks.len()));
            }
            for p in data_meta.storage {
                let used = p.data_blocks.iter().map(|b| b.size).sum::<u64>();
                proc_storage.insert(p.pid, (used, p.data_blocks.len()));
            }
            let sorted_pids = proc_shm
                .keys()
                .chain(proc_hostmem.keys())
                .chain(proc_storage.keys())
                .chain(processes.iter().map(|p| &p.pid))
                .copied()
                .collect::<std::collections::BTreeSet<i32>>();
            if !sorted_pids.is_empty() {
                // empty line before details
                println!();
            }
            for pid in sorted_pids {
                let process_name = std::fs::read_to_string(format!("/proc/{}/comm", pid))
                    .map(|s| s.trim().to_string())
                    .ok()
                    .unwrap_or_else(|| "Unknown".to_string());
                let mut str = format!("[{}]{}; ", pid.to_string().yellow(), process_name.green());
                for dev in processes.iter().filter(|p| p.pid == pid) {
                    for (device, allocations) in &dev.allocations {
                        let on_gpu_size = allocations.iter().map(|a| a.on_gpu_bytes).sum::<u64>();
                        if on_gpu_size > 0 {
                            str.push_str(&format!(
                                "{} {}: {}; ",
                                "Device".bold(),
                                device.0.to_string().bold(),
                                pretty_size(on_gpu_size).bright_blue(),
                            ));
                        }
                    }
                }
                if let Some((used, blocks)) = proc_shm.get(&pid) {
                    str.push_str(&format!(
                        "{}: {} in {} blocks; ",
                        "SHM".bold(),
                        pretty_size(*used).bright_blue(),
                        blocks
                    ));
                }
                if let Some((used, blocks)) = proc_hostmem.get(&pid) {
                    str.push_str(&format!(
                        "{}: {} in {} blocks; ",
                        "HostMem".bold(),
                        pretty_size(*used).bright_blue(),
                        blocks
                    ));
                }
                if let Some((used, blocks)) = proc_storage.get(&pid) {
                    str.push_str(&format!(
                        "{}: {} in {} blocks; ",
                        "Storage".bold(),
                        pretty_size(*used).bright_blue(),
                        blocks
                    ));
                }
                println!("{}", str.trim_end_matches("; "));
            }
        }

        Ok(())
    }

    pub async fn set_priority(
        &self,
        pid: ProcArgs,
        level: SetPriorityLevel,
    ) -> Result<(), ClientError> {
        let pid_list = self.get_pid_list().await?;
        let pid = get_pid_checked(pid, &pid_list)?;
        let resp = self
            .client
            .set_priority(tarpc::context::current(), SetPriorityArgs { pid, level })
            .await
            .map_err(|e| ClientError::ClientRpc("set_priority", e))?;
        match resp {
            Ok(resp) => match resp {
                SetPriorityResponse::Success => {
                    println!("{}", "Priority set successfully".green());
                }
                SetPriorityResponse::FailureProcessNotExist => {
                    eprintln!("{}", "Failed to set priority: Process Not Exist".red());
                }
                SetPriorityResponse::FailurePriorityNotFixed => {
                    eprintln!("{}", "Failed to set priority: Priority Not Fixed".red());
                }
            },
            Err(_) => {
                eprintln!("{}", "Failed to set priority: Unknown Error".red());
            }
        }
        Ok(())
    }

    pub async fn show_config(&self) -> Result<(), ClientError> {
        let config = self
            .client
            .get_config(tarpc::context::current())
            .await
            .map_err(|e| ClientError::ClientRpc("get_config", e))?;
        println!("Config: {:?}", config);
        Ok(())
    }

    pub async fn update_config(&self, args: UpdateConfigArgs) -> Result<(), ClientError> {
        let mut config = self
            .client
            .get_config(tarpc::context::current())
            .await
            .map_err(|e| ClientError::ClientRpc("update_config, failed to get", e))?;
        if let Some(device_threshold) = args.device_threshold {
            if (0.0..=1.0).contains(&device_threshold) {
                config.device_threshold = device_threshold;
            } else {
                return Err(ClientError::Args(
                    "device_threshold must be in [0, 1]".to_string(),
                ));
            }
        }
        if let Some(schedule_cooldown) = args.schedule_cooldown {
            if schedule_cooldown == 0 {
                config.schedule_cooldown = None;
            } else {
                config.schedule_cooldown = Some(Duration::from_millis(schedule_cooldown as u64));
            }
        }
        self.client
            .update_config(tarpc::context::current(), config.to_configurable_args())
            .await
            .map_err(|e| ClientError::ClientRpc("update_config, failed to update", e))?;
        Ok(())
    }
}

fn colored_bool(b: bool) -> colored::ColoredString {
    if b {
        "T".bright_green()
    } else {
        "F".bright_red()
    }
}

fn pretty_precentage(p: f64) -> colored::ColoredString {
    if p >= 85.0 {
        format!("{:.2}%", p).red()
    } else if p >= 60.0 {
        format!("{:.2}%", p).yellow()
    } else {
        format!("{:.2}%", p).green()
    }
}

fn filter_invalid_processes(processes: Vec<ProcessMetadata>) -> Vec<ProcessMetadata> {
    processes
        .into_iter()
        .filter(|p| p.state.is_some())
        .collect()
}
