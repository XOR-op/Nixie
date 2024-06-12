#![allow(non_upper_case_globals)]
use std::os::fd::OwnedFd;

use auto_gmem_ipc::shm::ShmGuard;

use crate::{
    error::AutoGMemError,
    uvm::{event_queue::EventQueue, uvm_binding::UvmEventType_UvmEventTypeGpuFault},
};

pub(crate) struct ProcessControl {
    peer_pid: i32,
    pid_fd: OwnedFd,
    event_queue: EventQueue,
    shm: ShmGuard,
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
            let _ = tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let mut write_cnt = 0;
            let mut first = true;
            let n_completed = self.event_queue.read_events(|event| {
                let event_type = unsafe { event.__bindgen_anon_1.eventData.eventType };
                match event_type as u32 {
                    UvmEventType_UvmEventTypeGpuFault => {
                        let event_ref = unsafe { &event.__bindgen_anon_1.eventData.gpuFault };
                        const UVM_FAULT_TYPE_WRITE: u8 = 3;
                        match event_ref.faultType {
                            UVM_FAULT_TYPE_WRITE => write_cnt += 1,
                            _ => {}
                        }
                        if first && event_ref.faultType == UVM_FAULT_TYPE_WRITE {
                            tracing::info!(
                                "fault: addr={:#018x}, fault_type={}",
                                event_ref.address,
                                event_ref.faultType
                            );
                            first = false;
                        }
                        true
                    }
                    _ => {
                        tracing::warn!("Unknown event type: {}", event_type);
                        false
                    }
                }
            });
            if n_completed > 0 {
                tracing::info!(
                    "[pid={}] Received {} events: write={}",
                    self.peer_pid,
                    n_completed,
                    write_cnt
                );
            }
        }
    }
}

pub(crate) struct ProcessControlBuilder {
    pid: Option<i32>,
    pid_fd: Option<OwnedFd>,
    event_queue: Option<EventQueue>,
    shm: Option<ShmGuard>,
}

impl ProcessControlBuilder {
    pub fn new() -> Self {
        Self {
            pid: None,
            pid_fd: None,
            event_queue: None,
            shm: None,
        }
    }

    pub fn with_pid(&mut self, pid: i32) -> &mut Self {
        if self.pid.is_some() {
            tracing::warn!("pid is already set");
        }
        self.pid = Some(pid);
        self
    }

    pub fn with_pid_fd(&mut self, pid_fd: OwnedFd) -> &mut Self {
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
        })
    }
}
