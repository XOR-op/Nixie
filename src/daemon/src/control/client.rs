use std::time::Duration;

use colored::Colorize;
use tarpc::tokio_util::codec::LengthDelimitedCodec;
use tokio_serde::formats::Cbor;

use crate::{ProcArgs, UpdateConfigArgs, error::DaemonError};
use nihil_common::general::pretty_size;

use super::{ControllableClient, PrefetchMsg};

pub(crate) struct ControlClient {
    client: ControllableClient,
    pid: i32,
}

impl ControlClient {
    pub async fn new(path: &str, pid: ProcArgs) -> Result<Self, DaemonError> {
        let conn = tokio::net::UnixStream::connect(path)
            .await
            .map_err(|e| DaemonError::Io("Failed to connect to control socket", e))?;
        let conn = tarpc::serde_transport::new(
            LengthDelimitedCodec::builder().new_framed(conn),
            Cbor::default(),
        );
        let client = ControllableClient::new(Default::default(), conn).spawn();
        match pid {
            ProcArgs {
                pid: Some(pid),
                idx: None,
            } => Ok(Self { client, pid }),
            ProcArgs {
                pid: None,
                idx: Some(idx),
            } => {
                let r = client
                    .list_pid(tarpc::context::current())
                    .await
                    .map_err(|e| DaemonError::ClientRpc("list_pid", e))?;
                if let Some(pid) = r.get(idx as usize) {
                    Ok(Self { client, pid: *pid })
                } else {
                    Err(DaemonError::Errno(
                        "Invalid process index",
                        nix::errno::Errno::EINVAL,
                    ))
                }
            }
            _ => Err(DaemonError::Errno(
                "-p(--pid) or -i(--idx) must be specified amd cannot be used together",
                nix::errno::Errno::EINVAL,
            )),
        }
    }

    pub async fn prefetch(&self, to_gpu: bool) -> Result<(), DaemonError> {
        self.client
            .prefetch(
                tarpc::context::current(),
                PrefetchMsg {
                    pid: self.pid,
                    to_gpu,
                },
            )
            .await
            .map_err(|e| DaemonError::ClientRpc("prefetch", e))?;
        Ok(())
    }

    pub async fn list_processes(&self, verbose: bool) -> Result<(), DaemonError> {
        let processes = self
            .client
            .list_processes(tarpc::context::current())
            .await
            .map_err(|e| DaemonError::ClientRpc("list_processes", e))?;
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

    pub async fn show_config(&self) -> Result<(), DaemonError> {
        let config = self
            .client
            .get_config(tarpc::context::current())
            .await
            .map_err(|e| DaemonError::ClientRpc("get_config", e))?;
        println!("Config: {:?}", config);
        Ok(())
    }

    pub async fn update_config(&self, args: UpdateConfigArgs) -> Result<(), DaemonError> {
        let mut config = self
            .client
            .get_config(tarpc::context::current())
            .await
            .map_err(|e| DaemonError::ClientRpc("update_config, failed to get", e))?;
        if let Some(device_threshold) = args.device_threshold {
            if (0.0..=1.0).contains(&device_threshold) {
                config.device_threshold = device_threshold;
            } else {
                return Err(DaemonError::Errno(
                    "device_threshold must be in [0, 1]",
                    nix::errno::Errno::EINVAL,
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
            .map_err(|e| DaemonError::ClientRpc("update_config, failed to update", e))?;
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
