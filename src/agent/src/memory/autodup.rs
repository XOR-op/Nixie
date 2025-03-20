#![allow(unused_imports)]
#![allow(dead_code)]

use nihilipc::{shm::AllocationEntry, AttrType};
use std::{
    num::NonZeroU64,
    sync::Mutex,
    time::{Duration, Instant},
};

use crate::{
    env_config::agent_config,
    info_eprintln,
    intercept_launch::is_during_capture,
    utils::{set_device, CudaContextGuard},
    FusedPtrMapping, GENERIC_DATA,
};

#[derive(Debug, Clone)]
struct MallocRecord {
    idx: usize, // index in the allocation table
    addr: NonZeroU64,
    len: usize,
    device: i32,
    timestamp: Instant, // time of the allocation
}

impl MallocRecord {
    pub fn matches(&self, alloc_entry: &AllocationEntry) -> bool {
        self.addr.get() == alloc_entry.addr
            && self.len == alloc_entry.len
            && self.device == alloc_entry.device
    }
}

// Lock order: allocation table first, then dup daemon
pub struct DupDaemon {
    candidates: Vec<MallocRecord>,
}

impl DupDaemon {
    pub fn new() -> Self {
        Self {
            candidates: Vec::new(),
        }
    }

    pub fn record(&mut self, idx: usize, entry: &AllocationEntry) {
        if agent_config().auto_dup {
            self.candidates.push(MallocRecord {
                idx,
                addr: NonZeroU64::new(entry.addr).unwrap(),
                len: entry.len,
                device: entry.device,
                timestamp: Instant::now(),
            });
        }
    }

    pub fn mark_as_dup(&mut self, mut table_handle: FusedPtrMapping<'_>) {
        let _guard = CudaContextGuard::new();
        let dup_threshold = Duration::from_secs(agent_config().auto_dup_delay);
        // split by time reaching the threshold
        let now = Instant::now();
        let candidates = match self
            .candidates
            .iter()
            .position(|record| now.duration_since(record.timestamp) < dup_threshold)
        {
            Some(idx) => {
                let mut candidates = std::mem::take(&mut self.candidates);
                // all items after idx have not reached the time threshold
                self.candidates = candidates.split_off(idx);
                candidates
            }
            None => {
                // all candidates need to be processed
                std::mem::take(&mut self.candidates)
            }
        };

        for record in candidates {
            // check if the allocation is still valid
            if table_handle
                .shm
                .at(record.idx)
                .is_some_and(|entry| record.matches(entry))
            {
                set_device(record.device);
                super::attribute::set_attribute_single(
                    &mut table_handle,
                    AttrType::ReadDup,
                    true,
                    record.addr,
                    record.len as u64,
                    record.device,
                );
            }
        }
    }

    pub fn spawn_daemon(daemon: &'static Mutex<Self>) {
        if agent_config().auto_dup {
            std::thread::spawn(|| loop {
                std::thread::sleep(std::time::Duration::from_secs(3));
                if !is_during_capture() {
                    if let Some(data) = GENERIC_DATA.get() {
                        let table_handle = data.lock_ptr_mapping();
                        let mut daemon_handle = daemon.lock().unwrap();
                        daemon_handle.mark_as_dup(table_handle);
                    }
                }
            });
            info_eprintln!("Auto Duplication enabled");
        }
    }
}
