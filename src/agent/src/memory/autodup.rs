use nihilipc::{shm::AllocationEntry, AttrType};
use std::{
    num::NonZeroU64,
    sync::Mutex,
    time::{Duration, Instant},
};

use crate::{utils::set_device, FusedPtrMapping, GENERIC_DATA};

const DUP_THRESHOLD: Duration = Duration::from_secs(5);

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
        self.addr == alloc_entry.addr
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
        self.candidates.push(MallocRecord {
            idx,
            addr: entry.addr,
            len: entry.len,
            device: entry.device,
            timestamp: Instant::now(),
        });
    }

    pub fn mark_as_dup<'a>(&mut self, mut table_handle: FusedPtrMapping<'a>) {
        // split by time reaching the threshold
        let now = Instant::now();
        let candidates = match self
            .candidates
            .iter()
            .position(|record| now.duration_since(record.timestamp) < DUP_THRESHOLD)
        {
            Some(idx) => {
                let mut candidates = std::mem::replace(&mut self.candidates, Vec::new());
                // all items after idx have not reached the time threshold
                self.candidates = candidates.split_off(idx);
                candidates
            }
            None => {
                // all candidates need to be processed
                std::mem::replace(&mut self.candidates, Vec::new())
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
        std::thread::spawn(|| loop {
            std::thread::sleep(std::time::Duration::from_secs(3));
            if let Some(data) = GENERIC_DATA.get() {
                let table_handle = data.lock_ptr_mapping();
                let mut daemon_handle = daemon.lock().unwrap();
                daemon_handle.mark_as_dup(table_handle);
            }
        });
    }
}
