use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
};

use nixie_common::ProcessLocalDeviceId;

use crate::{debug_eprintln, warn_eprintln};

#[derive(Debug, Clone)]
pub struct AllocRecord {
    pub dev_ptr: u64,
    pub size: u64,
    pub alloc_size: u64,
    pub device: ProcessLocalDeviceId,
}

pub struct AllocTracker {
    tracker: Mutex<BTreeMap<u64, AllocRecord>>,
}

impl AllocTracker {
    pub fn new() -> Self {
        Self {
            tracker: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn insert(&self, dev_ptr: u64, size: u64, alloc_size: u64, device: ProcessLocalDeviceId) {
        let mut tracker = self.tracker.lock().unwrap();
        // first check for overlaps in left neighbor
        if let Some((&left_ptr, left_record)) = tracker.range(..=dev_ptr).next_back()
            && left_ptr + left_record.size > dev_ptr
        {
            debug_eprintln!(
                "Warning: Overlapping device pointer detected: new ({:#x}, size {}) overlaps with existing ({:#x}, size {})",
                dev_ptr,
                size,
                left_ptr,
                left_record.size
            );
        }
        tracker.insert(
            dev_ptr,
            AllocRecord {
                dev_ptr,
                size,
                alloc_size,
                device,
            },
        );
    }

    pub fn remove(&self, dev_ptr: u64) {
        let mut tracker = self.tracker.lock().unwrap();
        if tracker.remove(&dev_ptr).is_none() {
            warn_eprintln!(
                "Warning: Attempting to remove non-existent device pointer: {:#x}",
                dev_ptr
            );
        }
    }

    pub fn find_exact(&self, dev_ptr: u64) -> Option<AllocRecord> {
        let tracker = self.tracker.lock().unwrap();
        tracker.get(&dev_ptr).cloned()
    }

    #[allow(unused)]
    pub fn find(&self, dev_ptr: u64) -> Option<AllocRecord> {
        let tracker = self.tracker.lock().unwrap();
        tracker
            .range(..=dev_ptr)
            .next_back()
            .map(|r| r.1)
            .filter(|record| record.dev_ptr <= dev_ptr && dev_ptr < record.dev_ptr + record.size)
            .cloned()
    }

    #[allow(unused)]
    pub fn find_and<F, R>(&self, dev_ptr: u64, f: F) -> Option<R>
    where
        F: FnOnce(&mut AllocRecord) -> R,
    {
        let mut tracker = self.tracker.lock().unwrap();
        tracker
            .range_mut(..=dev_ptr)
            .next_back()
            .map(|r| r.1)
            .filter(|record| record.dev_ptr <= dev_ptr && dev_ptr < record.dev_ptr + record.size)
            .map(f)
    }
}

pub fn global_tracker() -> &'static AllocTracker {
    static TRACKER: OnceLock<AllocTracker> = OnceLock::new();
    TRACKER.get_or_init(AllocTracker::new)
}
