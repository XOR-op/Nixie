#![allow(non_upper_case_globals)]
use std::{collections::HashMap, os::fd::OwnedFd};

use nihil_common::{rpc::SidecarClient, shm::ShmGuard, ProcessLocalDeviceId, MAX_GPUS};
use tokio::{io::unix::AsyncFd, sync::mpsc};

use crate::{
    control::{AllocationData, PhysicalMemoryData, ProcessMetadata, ProcessResidualData},
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
    pub(super) fn new(
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
                let mut allocations = std::array::from_fn(|_| Vec::new());
                for device in 0..MAX_GPUS {
                    if let Some(global_device_id) = self
                        .dev_mapping
                        .visible_to_real(ProcessLocalDeviceId(device as i32))
                    {
                        let mapping = self.shm.inner.alloc_tables[device].lock();
                        for entry in mapping.entry.iter() {
                            let mut physical = Vec::new();
                            let mut cur_handle = Some(entry.handle_idx);
                            let mut on_gpu_bytes = 0;
                            let mut off_gpu_bytes = 0;
                            while let Some(handle_idx) = cur_handle {
                                let handle = mapping.handle_list.get_handle(handle_idx).unwrap();
                                physical.push(PhysicalMemoryData {
                                    on_gpu: handle.on_gpu,
                                    handle_idx,
                                    size: handle.size as u64,
                                });
                                cur_handle = handle.next_handle_idx;
                                if handle.on_gpu {
                                    on_gpu_bytes += handle.size as u64;
                                } else {
                                    off_gpu_bytes += handle.size as u64;
                                }
                            }
                            allocations[global_device_id.0 as usize].push(AllocationData {
                                on_gpu_bytes,
                                off_gpu_bytes,
                                physical,
                            });
                        }
                    }
                }

                param.ret(ProcessMetadata {
                    pid: self.peer_pid,
                    allocations,
                });
            }
            ProcCtlReq::ListProcessResidual(call_parameter) => {
                let (param, ret_chan) = call_parameter.into_parts();
                let mut result = HashMap::new();
                for device in param.gpu_list {
                    if let Some(proc_local_id) = self.dev_mapping.real_to_visible(device) {
                        let mut mem_list = Vec::new();
                        let mapping = self.shm.inner.alloc_tables[proc_local_id.0 as usize].lock();
                        for entry in mapping.entry.iter() {
                            let mut cur_handle = Some(entry.handle_idx);
                            while let Some(handle_idx) = cur_handle {
                                let handle = mapping.handle_list.get_handle(handle_idx).unwrap();
                                if handle.on_gpu == param.on_gpu {
                                    mem_list.push(PhysicalMemoryData {
                                        on_gpu: handle.on_gpu,
                                        handle_idx,
                                        size: handle.size as u64,
                                    });
                                }
                                cur_handle = handle.next_handle_idx;
                            }
                        }
                        result.insert(device, mem_list);
                    }
                }
                ret_chan.ret(ProcessResidualData {
                    pid: self.peer_pid,
                    allocations: result,
                });
            }
        }
    }
}
