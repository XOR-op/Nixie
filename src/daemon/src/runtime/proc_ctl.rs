#![allow(non_upper_case_globals)]
use std::os::fd::OwnedFd;

use nihil_common::{rpc::SidecarClient, shm::ShmGuard, MAX_GPUS};
use tokio::{io::unix::AsyncFd, sync::mpsc};

use crate::{
    control::{AllocationData, ProcessMetadata},
    error::NihilphaseError,
};

use super::{daemon_server::DeviceOrdinalMapping, ProcCtlReq};

pub(crate) struct ProcessControl {
    peer_pid: i32,
    pid_fd: AsyncFd<OwnedFd>,
    shm: ShmGuard,
    dev_mapping: DeviceOrdinalMapping,
    rpc_sender: SidecarClient,
    inst_rx: mpsc::UnboundedReceiver<ProcCtlReq>,
    exit_tx: mpsc::UnboundedSender<i32>,
}

impl ProcessControl {
    pub fn new(
        peer_pid: i32,
        pid_fd: AsyncFd<OwnedFd>,
        shm: ShmGuard,
        dev_mapping: DeviceOrdinalMapping,
        rpc_sender: SidecarClient,
        inst_rx: mpsc::UnboundedReceiver<ProcCtlReq>,
        exit_tx: mpsc::UnboundedSender<i32>,
    ) -> Self {
        Self {
            peer_pid,
            pid_fd,
            shm,
            dev_mapping,
            rpc_sender,
            inst_rx,
            exit_tx,
        }
    }

    pub async fn run(self) {
        let peer_pid = self.peer_pid;
        if let Err(e) = self.run_inner().await {
            tracing::error!("ProcessControl [pid={}] failed: {:?}", peer_pid, e);
        }
    }

    async fn run_inner(mut self) -> Result<(), NihilphaseError> {
        tracing::info!("Listen events from process [pid={}]", self.peer_pid);
        loop {
            tokio::select! {
                Some(inst) = self.inst_rx.recv() => {
                    self.handle_inst(inst);
                }
                _ = self.pid_fd.readable() => {
                    break;
                }
            }
        }

        tracing::info!("ProcessControl [pid={}] finished", self.peer_pid);
        let _ = self.exit_tx.send(self.peer_pid);
        Ok(())
    }

    fn handle_inst(&mut self, inst: ProcCtlReq) {
        match inst {
            ProcCtlReq::List(param) => {
                let mut allocations = Vec::new();
                for device in 0..MAX_GPUS {
                    let mapping = self.shm.inner.alloc_tables[device].lock();
                    for entry in mapping.entry.iter() {
                        let (on_gpu, off_gpu) = mapping.handle_list.memory_usage(entry.handle_idx);
                        allocations.push(AllocationData {
                            device: self
                                .dev_mapping
                                .visible_to_real(device as i32)
                                .unwrap_or_default(),
                            on_gpu_bytes: on_gpu as u64,
                            off_gpu_bytes: off_gpu as u64,
                        });
                    }
                }
                param.ret(ProcessMetadata {
                    pid: self.peer_pid,
                    allocations,
                });
            }
        }
    }
}
