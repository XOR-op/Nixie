use colored::Colorize;
use tarpc::tokio_util::codec::LengthDelimitedCodec;
use tokio_serde::formats::Cbor;

use crate::{error::DaemonError, general::pretty_size};

use super::{ControllableClient, PrefetchMsg, ReadDupMsg};

pub(crate) struct ControlClient {
    client: ControllableClient,
    pid: i32,
}

impl ControlClient {
    pub async fn new(path: &str, pid: i32) -> Result<Self, DaemonError> {
        let conn = tokio::net::UnixStream::connect(path)
            .await
            .map_err(|e| DaemonError::Io("Failed to connect to control socket", e))?;
        let conn = tarpc::serde_transport::new(
            LengthDelimitedCodec::builder().new_framed(conn),
            Cbor::default(),
        );
        let client = ControllableClient::new(Default::default(), conn).spawn();
        Ok(Self { client, pid })
    }

    pub async fn read_dup(&self, size_low: Option<u64>, set: bool) -> Result<(), DaemonError> {
        self.client
            .read_dup(
                tarpc::context::current(),
                ReadDupMsg {
                    pid: self.pid,
                    size_low,
                    size_high: None,
                    set,
                },
            )
            .await
            .map_err(|e| DaemonError::ClientRpc("read_dup", e))?;
        Ok(())
    }

    pub async fn prefetch(&self, size_low: Option<u64>) -> Result<(), DaemonError> {
        self.client
            .prefetch(
                tarpc::context::current(),
                PrefetchMsg {
                    pid: self.pid,
                    size_low,
                    size_high: None,
                    to_gpu: true,
                },
            )
            .await
            .map_err(|e| DaemonError::ClientRpc("prefetch", e))?;
        Ok(())
    }

    pub async fn list_processes(&self) -> Result<(), DaemonError> {
        let processes = self
            .client
            .list_processes(tarpc::context::current())
            .await
            .map_err(|e| DaemonError::ClientRpc("list_processes", e))?;
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
                "[{}]{}:",
                process.pid.to_string().yellow(),
                process_name
                    .map_or("".to_string(), |s| format!(" {}", s))
                    .green()
            );
            for (device, allocations) in group_by_device {
                let alloc_size = allocations.iter().map(|a| a.size).sum::<u64>();
                println!(
                    "{} #alloc = {}, size = {}",
                    format!("<Device {}>", device).cyan(),
                    format!("{}", allocations.len()).yellow(),
                    pretty_size(alloc_size).blue()
                )
            }
        }
        Ok(())
    }
}
