use std::{collections::HashMap, time::Duration};

use colored::Colorize;
use tarpc::tokio_util::codec::LengthDelimitedCodec;
use tokio_serde::formats::Cbor;

use crate::{UpdateConfigArgs, control::PrefetchResponse, error::ClientError};
use nihil_common::general::pretty_size;

use super::ControllableClient;

pub(crate) struct ControlClient {
    client: ControllableClient,
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
                    let pid = if let Some(pid) = msg.pid.pid {
                        if !pid_list.contains(&pid) {
                            return Err(ClientError::Args(format!(
                                "Process with pid {} not found",
                                pid
                            )));
                        }
                        pid
                    } else if let Some(idx) = msg.pid.idx {
                        if idx == 0 || (idx as usize) > pid_list.len() {
                            return Err(ClientError::Args(format!(
                                "Process index {} out of range",
                                idx
                            )));
                        }
                        pid_list[idx as usize - 1]
                    } else {
                        return Err(ClientError::Args(
                            "Either pid or idx must be specified".to_string(),
                        ));
                    };
                    Ok(crate::control::PrefetchMsg {
                        pid,
                        from: msg.src,
                        to: msg.dest,
                        size: msg.size,
                    })
                })
                .collect::<Result<Vec<_>, ClientError>>()?,
        };
        if self
            .client
            .prefetch(tarpc::context::current(), processed_args)
            .await
            .map_err(|e| ClientError::ClientRpc("prefetch", e))?
            .is_err()
        {
            eprintln!("{}", "Prefetch request failed".red());
        }
        Ok(PrefetchResponse)
    }

    pub async fn list_processes(&self, verbose: bool) -> Result<(), ClientError> {
        let processes = self
            .client
            .list_processes(tarpc::context::current())
            .await
            .map_err(|e| ClientError::ClientRpc("list_processes", e))?;
        // print info
        println!("Active processes: {}", processes.len());
        for process in processes {
            let process_name = std::fs::read_to_string(format!("/proc/{}/comm", process.pid))
                .map(|s| s.trim().to_string())
                .ok();
            println!(
                "[{}]{} <{}, {}>",
                process.pid.to_string().yellow(),
                process_name
                    .map_or("".to_string(), |s| format!(" {}", s))
                    .green(),
                process
                    .state
                    .map_or("Unknown".to_string(), |s| format!("{:?}", s))
                    .blue(),
                process
                    .priority
                    .map_or("N/A".to_string(), |p| format!(
                        "{:?}{}",
                        p.level(),
                        p.weight().map(|w| format!("/{}", w)).unwrap_or_default()
                    ))
                    .purple()
            );
            for (device, allocations) in process.allocations {
                // print aggregated per device info
                let alloc_size = allocations
                    .iter()
                    .map(|a| a.on_gpu_bytes + a.off_gpu_bytes)
                    .sum::<u64>();
                println!(
                    "{} #alloc = {}, size = {}",
                    format!("<Device {}>", device.0).cyan(),
                    format!("{}", allocations.len()).yellow(),
                    pretty_size(alloc_size).blue()
                );
                if verbose {
                    // print each allocation info
                    for (idx, a) in allocations.into_iter().enumerate() {
                        println!(
                            "\t{}: size = {}, on_gpu_size = {}",
                            format!("<Allocation {}>", idx).cyan(),
                            pretty_size(a.on_gpu_bytes + a.off_gpu_bytes).blue(),
                            pretty_size(a.on_gpu_bytes).blue()
                        )
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn data_details(&self, verbose: bool) -> Result<(), ClientError> {
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
        println!(
            "SHM: {}/{} ({}, {} procs), HostMem: {}/{} ({}, {} procs), Disk: {} ({} procs)",
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
                .copied()
                .collect::<std::collections::BTreeSet<i32>>();
            for pid in sorted_pids {
                let process_name = std::fs::read_to_string(format!("/proc/{}/comm", pid))
                    .map(|s| s.trim().to_string())
                    .ok()
                    .unwrap_or_else(|| "Unknown".to_string());
                let mut str = format!("[{}]{}; ", pid.to_string().yellow(), process_name.green());
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
        if let Some(schedule_delay) = args.schedule_delay {
            if schedule_delay == 0 {
                config.schedule_delay = None;
            } else {
                config.schedule_delay = Some(Duration::from_millis(schedule_delay as u64));
            }
        }
        if let Some(schedule_cooldown) = args.schedule_cooldown {
            if schedule_cooldown == 0 {
                config.schedule_cooldown = None;
            } else {
                config.schedule_cooldown = Some(Duration::from_millis(schedule_cooldown as u64));
            }
        }
        if let Some(preempt_delay) = args.preempt_delay {
            if preempt_delay == 0 {
                config.preempt_delay = None;
            } else {
                config.preempt_delay = Some(Duration::from_millis(preempt_delay as u64));
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
