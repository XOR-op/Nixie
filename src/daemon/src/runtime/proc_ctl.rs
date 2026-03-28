#![allow(non_upper_case_globals)]
use std::{collections::HashMap, os::fd::OwnedFd};

use nixie_common::{
    MAX_GPUS, ProcessLocalDeviceId,
    general::CallParameter,
    rpc::SidecarClient,
    shm::{PhysicalMemoryHandleId, ShmGuard},
};
use tokio::{io::unix::AsyncFd, sync::mpsc};

use crate::{
    control::{AllocationData, PhysicalMemoryData, ProcessMetadata, ProcessResidualData},
    error::NixieError,
    runtime::schedule::control::ScheduleControlReq,
};

use super::{ProcCtlReq, daemon_server::DeviceOrdinalMapping};

pub(crate) struct ProcessControl {
    peer_pid: i32,
    pid_fd: AsyncFd<OwnedFd>,
    shm: ShmGuard,
    dev_mapping: DeviceOrdinalMapping,
    rpc_sender: SidecarClient,
    inst_rx: mpsc::UnboundedReceiver<ProcCtlReq>,
    sched_req_tx: mpsc::UnboundedSender<ScheduleControlReq>,
    exit_tx: mpsc::UnboundedSender<i32>,
}

impl ProcessControl {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        peer_pid: i32,
        pid_fd: AsyncFd<OwnedFd>,
        shm: ShmGuard,
        dev_mapping: DeviceOrdinalMapping,
        rpc_sender: SidecarClient,
        inst_rx: mpsc::UnboundedReceiver<ProcCtlReq>,
        sched_req_tx: mpsc::UnboundedSender<ScheduleControlReq>,
        exit_tx: mpsc::UnboundedSender<i32>,
    ) -> Self {
        Self {
            peer_pid,
            pid_fd,
            shm,
            dev_mapping,
            rpc_sender,
            inst_rx,
            sched_req_tx,
            exit_tx,
        }
    }

    pub async fn run(self) {
        let peer_pid = self.peer_pid;
        if let Err(e) = self.run_inner().await {
            tracing::error!("ProcessControl [pid={}] failed: {:?}", peer_pid, e);
        }
    }

    async fn run_inner(mut self) -> Result<(), NixieError> {
        tracing::info!("Listen events from process [pid={}]", self.peer_pid);
        loop {
            tokio::select! {
                Some(inst) = self.inst_rx.recv() => {
                    self.handle_inst(inst).await;
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

    async fn handle_inst(&mut self, inst: ProcCtlReq) {
        match inst {
            ProcCtlReq::List(param) => {
                let mut allocations = Vec::new();
                for device in 0..MAX_GPUS {
                    if let Some(global_device_id) = self
                        .dev_mapping
                        .visible_to_real(ProcessLocalDeviceId(device as i32))
                    {
                        let mapping = self.shm.inner.alloc_tables[device].lock();
                        let mut list = Vec::new();
                        for entry in mapping.entry.iter() {
                            let mut physical = Vec::new();
                            let mut cur_handle = Some(entry.handle_idx.idx);
                            let mut on_gpu_bytes = 0;
                            let mut off_gpu_bytes = 0;
                            while let Some(handle_idx) = cur_handle {
                                let handle = mapping
                                    .handle_list
                                    .get_handle_by_raw_idx(handle_idx)
                                    .unwrap();
                                physical.push(PhysicalMemoryData {
                                    on_gpu: handle.on_gpu,
                                    handle_idx: PhysicalMemoryHandleId::new(
                                        handle.alloc_generation,
                                        handle_idx,
                                    ),
                                    size: handle.size as u32,
                                });
                                cur_handle = handle.next_handle_idx;
                                if handle.on_gpu {
                                    on_gpu_bytes += handle.size as u64;
                                } else {
                                    off_gpu_bytes += handle.size as u64;
                                }
                            }
                            list.push(AllocationData {
                                on_gpu_bytes,
                                off_gpu_bytes,
                                physical,
                            });
                        }
                        if !list.is_empty() {
                            allocations.push((global_device_id, list));
                        }
                    }
                }
                allocations.sort_by_key(|(device_id, _)| *device_id);
                let (state_para, call_fut) = CallParameter::new(self.peer_pid);
                self.sched_req_tx
                    .send(ScheduleControlReq::GetState(state_para))
                    .unwrap();
                let (state, priority) = match call_fut.await {
                    Some(x) => (x.state, x.priority),
                    None => (None, None),
                };
                param.ret(ProcessMetadata {
                    pid: self.peer_pid,
                    state,
                    priority,
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
                            let mut cur_handle = Some(entry.handle_idx.idx);
                            while let Some(handle_idx) = cur_handle {
                                let handle = mapping
                                    .handle_list
                                    .get_handle_by_raw_idx(handle_idx)
                                    .unwrap();
                                if handle.on_gpu == param.on_gpu {
                                    mem_list.push(PhysicalMemoryData {
                                        on_gpu: handle.on_gpu,
                                        handle_idx: PhysicalMemoryHandleId::new(
                                            handle.alloc_generation,
                                            handle_idx,
                                        ),
                                        size: handle.size as u32,
                                    });
                                }
                                cur_handle = handle.next_handle_idx;
                            }
                        }
                        result.insert(device, mem_list);
                    }
                }
                if ret_chan
                    .ret(ProcessResidualData {
                        pid: self.peer_pid,
                        allocations: result,
                    })
                    .is_err()
                {
                    tracing::warn!(
                        "Failed to send ListProcessResidual response to pid {}",
                        self.peer_pid
                    );
                }
            }
        }
    }
}
