#![allow(non_upper_case_globals)]
use std::{collections::BTreeSet, num::NonZeroU64, os::fd::OwnedFd};

use nihilipc::{
    rpc::SidecarClient,
    shm::{AllocationEntry, ShmGuard},
};
use tarpc::context::Context;
use tokio::{io::unix::AsyncFd, sync::mpsc};

use crate::{
    control::AllocationData,
    error::NihilphaseError,
    uvm::{event_queue::EventQueue, uvm_binding::UvmEventType_UvmEventTypeGpuFault},
};

use super::{daemon_server::DeviceOrdinalMapping, ProcCtlReq, ProcessMetadata};

pub(crate) struct ProcessControl {
    peer_pid: i32,
    pid_fd: AsyncFd<OwnedFd>,
    event_queue: Option<EventQueue>,
    shm: ShmGuard,
    dev_mapping: DeviceOrdinalMapping,
    rpc_sender: SidecarClient,
    inst_rx: mpsc::UnboundedReceiver<ProcCtlReq>,
    exit_tx: mpsc::UnboundedSender<i32>,
    num_fault: u64,
}

impl ProcessControl {
    pub async fn run(self) {
        let peer_pid = self.peer_pid;
        if let Err(e) = self.run_inner().await {
            tracing::error!("ProcessControl [pid={}] failed: {:?}", peer_pid, e);
        }
    }

    async fn run_inner(mut self) -> Result<(), NihilphaseError> {
        if let Some(mut event_queue) = self.event_queue.take() {
            tracing::info!("Listen events from process [pid={}]", self.peer_pid);
            event_queue.enable_event(UvmEventType_UvmEventTypeGpuFault)?;
            loop {
                tokio::select! {
                    // _ = self.event_queue.ready() => {
                    _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => {
                        self.process_event(&mut event_queue).await?;
                    }
                    Some(inst) = self.inst_rx.recv() => {
                        self.handle_inst(inst).await;
                    }
                    _ = self.pid_fd.readable() => {
                        break;
                    }
                }
            }
        } else {
            tracing::info!(
                "Listen events from process [pid={}] <UVM Disabled>",
                self.peer_pid
            );
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
        }

        tracing::info!("ProcessControl [pid={}] finished", self.peer_pid);
        let _ = self.exit_tx.send(self.peer_pid);
        Ok(())
    }

    async fn process_event(
        &mut self,
        event_queue: &mut EventQueue,
    ) -> Result<u32, NihilphaseError> {
        let mut fault_tree = BTreeSet::new();
        let (n_completed, num_fault) = event_queue.read_events(|event| {
            let event_type = unsafe { event.__bindgen_anon_1.eventData.eventType };
            match event_type as u32 {
                UvmEventType_UvmEventTypeGpuFault => {
                    let event_ref = unsafe { &event.__bindgen_anon_1.eventData.gpuFault };
                    const UVM_FAULT_TYPE_WRITE: u8 = 3;
                    #[allow(clippy::single_match)]
                    match event_ref.faultType {
                        UVM_FAULT_TYPE_WRITE => {
                            fault_tree.insert(event_ref.address);
                        }
                        _ => {}
                    }
                    true
                }
                _ => {
                    tracing::warn!("Unknown event type: {}", event_type);
                    false
                }
            }
        });
        self.num_fault += num_fault as u64;
        let mut disabled = BTreeSet::new();
        // disable read duplication
        if !fault_tree.is_empty() {
            let mapping = self.shm.inner.ptr_mapping.lock();
            for entry in mapping.iter() {
                let start = entry.addr;
                let end = start + entry.len as u64;
                if fault_tree.range(start..end).next().is_some() {
                    disabled.insert(*entry);
                }
            }
            drop(mapping);
            self.batched_read_dup(disabled.iter(), false).await;
        }

        if !fault_tree.is_empty() {
            tracing::trace!(
                "[pid={}] Received {} events: write_fault={}, num_disable={}",
                self.peer_pid,
                n_completed,
                fault_tree.len(),
                disabled.len()
            );
        }
        Ok(n_completed)
    }

    async fn handle_inst(&mut self, inst: ProcCtlReq) {
        match inst {
            ProcCtlReq::SetAttr(inst) => {
                if let Err(e) = self
                    .rpc_sender
                    .set_attr(
                        Context::current(),
                        nihilipc::AttrArgs {
                            addr: None,
                            len: inst.size_low.unwrap_or(0),
                            value: inst.attr,
                            will_set: inst.set,
                            device: 0, // TODO: real device
                        },
                    )
                    .await
                {
                    tracing::warn!("Failed to set read duplication: {:?}", e);
                }
            }
            ProcCtlReq::List(param) => {
                let mut allocations = Vec::new();
                let mapping = self.shm.inner.ptr_mapping.lock();
                for entry in mapping.iter() {
                    allocations.push(AllocationData {
                        size: entry.len as u64,
                        device: self
                            .dev_mapping
                            .visible_to_real(entry.device)
                            .unwrap_or_default(),
                        readonly: entry.is_readonly,
                        move_reduced: entry.is_move_reduced,
                        likely_on_gpu: entry.likely_on_gpu,
                    });
                }
                drop(mapping);
                let _ = param
                    .ret_tx
                    .send(ProcessMetadata {
                        pid: self.peer_pid,
                        allocations,
                        num_fault: self.num_fault,
                    })
                    .await;
            }
        }
    }

    async fn batched_read_dup<'a, I>(&self, iter: I, set: bool)
    where
        I: Iterator<Item = &'a AllocationEntry>,
    {
        for entry in iter {
            if let Err(e) = self
                .rpc_sender
                .set_attr(
                    Context::current(),
                    nihilipc::AttrArgs {
                        addr: NonZeroU64::new(entry.addr),
                        len: entry.len as u64,
                        value: nihilipc::AttrType::ReadDup,
                        will_set: set,
                        device: entry.device,
                    },
                )
                .await
            {
                tracing::warn!("Failed to set read duplication: {:?}", e);
            }
        }
    }
}

pub(super) struct ProcessControlBuilder {
    pid: Option<i32>,
    pid_fd: Option<AsyncFd<OwnedFd>>,
    event_queue: Option<Option<EventQueue>>,
    shm: Option<ShmGuard>,
    dev_mapping: Option<DeviceOrdinalMapping>,
    msg_sender: Option<SidecarClient>,
    inst_rx: Option<mpsc::UnboundedReceiver<ProcCtlReq>>,
    exit_tx: Option<mpsc::UnboundedSender<i32>>,
}

impl ProcessControlBuilder {
    pub fn new(
        msg_sender: SidecarClient,
        inst_rx: mpsc::UnboundedReceiver<ProcCtlReq>,
        exit_tx: mpsc::UnboundedSender<i32>,
    ) -> Self {
        Self {
            pid: None,
            pid_fd: None,
            event_queue: None,
            shm: None,
            dev_mapping: None,
            msg_sender: Some(msg_sender),
            inst_rx: Some(inst_rx),
            exit_tx: Some(exit_tx),
        }
    }

    pub fn with_pid(&mut self, pid: i32) -> &mut Self {
        if self.pid.is_some() {
            tracing::warn!("pid is already set");
        }
        self.pid = Some(pid);
        self
    }

    pub fn with_pid_fd(&mut self, pid_fd: AsyncFd<OwnedFd>) -> &mut Self {
        if self.pid_fd.is_some() {
            tracing::warn!("pid_fd is already set");
        }
        self.pid_fd = Some(pid_fd);
        self
    }

    pub fn with_event_queue(&mut self, event_queue: Option<EventQueue>) -> &mut Self {
        if self.event_queue.is_some() {
            tracing::warn!("event_queue is already set");
        }
        self.event_queue = Some(event_queue);
        self
    }

    pub fn with_shm(&mut self, shm: ShmGuard) -> &mut Self {
        if self.shm.is_some() {
            tracing::warn!("shm is already set");
        }
        self.shm = Some(shm);
        self
    }

    pub fn with_dev_mapping(&mut self, dev_mapping: DeviceOrdinalMapping) -> &mut Self {
        if self.dev_mapping.is_some() {
            tracing::warn!("dev_mapping is already set");
        }
        self.dev_mapping = Some(dev_mapping);
        self
    }

    pub fn ready(&self) -> bool {
        self.pid.is_some()
            && self.pid_fd.is_some()
            && self.event_queue.is_some()
            && self.shm.is_some()
            && self.dev_mapping.is_some()
            && self.msg_sender.is_some()
            && self.inst_rx.is_some()
            && self.exit_tx.is_some()
    }

    // use mutable reference to self to allow failed try
    pub fn build(&mut self) -> Option<ProcessControl> {
        if !self.ready() {
            return None;
        }
        Some(ProcessControl {
            peer_pid: self.pid.take().unwrap(),
            pid_fd: self.pid_fd.take().unwrap(),
            event_queue: self.event_queue.take().unwrap(),
            shm: self.shm.take().unwrap(),
            dev_mapping: self.dev_mapping.take().unwrap(),
            rpc_sender: self.msg_sender.take().unwrap(),
            inst_rx: self.inst_rx.take().unwrap(),
            exit_tx: self.exit_tx.take().unwrap(),
            num_fault: 0,
        })
    }
}

fn serialize_msg(msg: nihilipc::S2AMessage) -> Vec<u8> {
    let buf = bincode::serialize(&msg).unwrap();
    let length = buf.len() as u32;
    let length_buf = length.to_le_bytes();
    let mut coalesced_buf = Vec::with_capacity(4 + buf.len());
    coalesced_buf.extend_from_slice(&length_buf);
    coalesced_buf.extend_from_slice(&buf);
    coalesced_buf
}
