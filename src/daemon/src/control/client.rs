use colored::Colorize;
use tarpc::tokio_util::codec::LengthDelimitedCodec;
use tokio_serde::formats::Cbor;

use crate::{error::DaemonError, general::pretty_size, ProcArgs};

use super::{AttrMsg, ControllableClient, PrefetchMsg};

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

    pub async fn read_dup(&self, size_low: Option<u64>, set: bool) -> Result<(), DaemonError> {
        self.client
            .set_attr(
                tarpc::context::current(),
                AttrMsg {
                    pid: self.pid,
                    size_low,
                    size_high: None,
                    attr: nihilipc::AttrType::ReadDup,
                    set,
                },
            )
            .await
            .map_err(|e| DaemonError::ClientRpc("read_dup", e))?;
        Ok(())
    }

    pub async fn reduce_move(
        &self,
        size_low: Option<u64>,
        size_high: Option<u64>,
        set: bool,
    ) -> Result<(), DaemonError> {
        self.client
            .set_attr(
                tarpc::context::current(),
                AttrMsg {
                    pid: self.pid,
                    size_low,
                    size_high,
                    attr: nihilipc::AttrType::AccessedBy,
                    set,
                },
            )
            .await
            .map_err(|e| DaemonError::ClientRpc("reduce_move", e))?;
        Ok(())
    }

    pub async fn prefetch(&self, to_gpu: bool, size_low: Option<u64>) -> Result<(), DaemonError> {
        self.client
            .prefetch(
                tarpc::context::current(),
                PrefetchMsg {
                    pid: self.pid,
                    size_low,
                    size_high: None,
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
            let group_by_device =
                process
                    .allocations
                    .iter()
                    .fold(std::collections::BTreeMap::new(), |mut acc, a| {
                        acc.entry(a.device).or_insert(Vec::new()).push(a.clone());
                        acc
                    });
            let process_name = std::fs::read_to_string(format!("/proc/{}/comm", process.pid))
                .map(|s| s.trim().to_string())
                .ok();
            println!(
                "[{}]{}: num_fault = {}",
                process.pid.to_string().yellow(),
                process_name
                    .map_or("".to_string(), |s| format!(" {}", s))
                    .green(),
                process.num_fault.to_string().blue()
            );
            for (device, allocations) in group_by_device {
                // print aggregated per device info
                let alloc_size = allocations.iter().map(|a| a.size).sum::<u64>();
                println!(
                    "{} #alloc = {}, size = {}",
                    format!("<Device {}>", device).cyan(),
                    format!("{}", allocations.len()).yellow(),
                    pretty_size(alloc_size).blue()
                );
                if verbose {
                    // print each allocation info
                    for (idx, a) in allocations.into_iter().enumerate() {
                        println!(
                            "\t{}: size = {}, readonly = {}, move_reduced = {}",
                            format!("<Allocation {}>", idx).cyan(),
                            pretty_size(a.size).blue(),
                            colored_bool(a.readonly),
                            colored_bool(a.move_reduced)
                        )
                    }
                }
            }
        }
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
