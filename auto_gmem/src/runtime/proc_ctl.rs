#![allow(non_upper_case_globals)]
use std::{collections::BTreeSet, os::fd::OwnedFd};

use auto_gmem_ipc::shm::ShmGuard;
use tokio::io::unix::AsyncFd;

use crate::{
    error::AutoGMemError,
    inject_wrapper,
    uvm::{event_queue::EventQueue, uvm_binding::UvmEventType_UvmEventTypeGpuFault},
};

pub(crate) struct ProcessControl {
    peer_pid: i32,
    pid_fd: AsyncFd<OwnedFd>,
    event_queue: EventQueue,
    shm: ShmGuard,
    dylib_path: String,
}

impl ProcessControl {
    pub async fn run(self) {
        let peer_pid = self.peer_pid;
        if let Err(e) = self.run_inner().await {
            tracing::error!("ProcessControl [pid={}] failed: {:?}", peer_pid, e);
        }
    }

    async fn run_inner(mut self) -> Result<(), AutoGMemError> {
        self.event_queue
            .enable_event(UvmEventType_UvmEventTypeGpuFault)
            .map_err(|e| AutoGMemError::from(e))?;

        tracing::info!("Listen events from process [pid={}]", self.peer_pid);
        loop {
            tokio::select! {
                // _ = self.event_queue.ready() => {
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {
                    let n = self.process_event();
                    // if n > 0 {
                    //     tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    // }
                }
                _ = self.pid_fd.readable() => {
                    break;
                }
            }
        }
        tracing::info!("ProcessControl [pid={}] finished", self.peer_pid);
        Ok(())
    }

    fn process_event(&mut self) -> u32 {
        let mut fault_tree = BTreeSet::new();
        let n_completed = self.event_queue.read_events(|event| {
            let event_type = unsafe { event.__bindgen_anon_1.eventData.eventType };
            match event_type as u32 {
                UvmEventType_UvmEventTypeGpuFault => {
                    let event_ref = unsafe { &event.__bindgen_anon_1.eventData.gpuFault };
                    const UVM_FAULT_TYPE_WRITE: u8 = 3;
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
        // disable read duplication
        if !fault_tree.is_empty() {
            let mapping = self.shm.inner.ptr_mapping.lock();
            let mut disabled = BTreeSet::new();
            for entry in mapping.iter() {
                let start = entry.0;
                let end = entry.0 + entry.1 as u64;
                if fault_tree.range(start..end).next().is_some() {
                    disabled.insert(*entry);
                }
            }
            drop(mapping);
            tracing::debug!(
                "[pid={}] Disable read duplication for {:?}",
                self.peer_pid,
                disabled
            );
            for entry in disabled {
                inject_wrapper(
                    self.peer_pid,
                    self.dylib_path.clone(),
                    "_auto_gmem_advise_read_mostly_for",
                    false as u64,
                    entry.0,
                    0,
                );
            }
        }

        if n_completed > 0 {
            tracing::info!(
                "[pid={}] Received {} events: write_fault={}",
                self.peer_pid,
                n_completed,
                fault_tree.len()
            );
        }
        n_completed
    }
}

pub(crate) struct ProcessControlBuilder {
    pid: Option<i32>,
    pid_fd: Option<AsyncFd<OwnedFd>>,
    event_queue: Option<EventQueue>,
    shm: Option<ShmGuard>,
    dylib_path: String,
}

impl ProcessControlBuilder {
    pub fn new(dylib_path: String) -> Self {
        Self {
            pid: None,
            pid_fd: None,
            event_queue: None,
            shm: None,
            dylib_path,
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

    pub fn with_event_queue(&mut self, event_queue: EventQueue) -> &mut Self {
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

    pub fn ready(&self) -> bool {
        self.pid.is_some()
            && self.pid_fd.is_some()
            && self.event_queue.is_some()
            && self.shm.is_some()
    }

    pub fn build(&mut self) -> Option<ProcessControl> {
        if !self.ready() {
            return None;
        }
        Some(ProcessControl {
            peer_pid: self.pid.take().unwrap(),
            pid_fd: self.pid_fd.take().unwrap(),
            event_queue: self.event_queue.take().unwrap(),
            shm: self.shm.take().unwrap(),
            dylib_path: self.dylib_path.clone(),
        })
    }
}
