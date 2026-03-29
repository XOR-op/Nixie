use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

use cudarc::driver::sys::{CUevent, cudaError_enum};
use nixie_common::{MAX_GPUS, ProcessLocalDeviceId};

use crate::cu_api;

pub(crate) struct CachedBlock {
    pub ptr: u64,
    pub actual_size: usize,
    pub event: CUevent,
}

// SAFETY: CUevent is a raw pointer to a CUDA event object. CUDA events are thread-safe
// and can be used from any thread (cuEventQuery, cuEventSynchronize, cuEventDestroy are all
// thread-safe per CUDA documentation).
unsafe impl Send for CachedBlock {}

struct DevicePool {
    /// Free blocks indexed by actual_size. VecDeque so oldest (most likely completed) are checked first.
    blocks: BTreeMap<usize, VecDeque<CachedBlock>>,
    /// Reverse index: ptr → actual_size, for removal by pointer
    cached_ptr_to_size: HashMap<u64, usize>,
    /// Total cached bytes on this device
    cached_bytes: usize,
}

impl DevicePool {
    fn new() -> Self {
        Self {
            blocks: BTreeMap::new(),
            cached_ptr_to_size: HashMap::new(),
            cached_bytes: 0,
        }
    }
}

pub(crate) struct AsyncPool {
    devices: [DevicePool; MAX_GPUS],
}

impl AsyncPool {
    fn new() -> Self {
        Self {
            devices: std::array::from_fn(|_| DevicePool::new()),
        }
    }

    /// Try to find a completed (event done) cached block of suitable size.
    /// Returns `None` if no completed block in [effective_size, effective_size*2] range.
    pub fn try_alloc(
        &mut self,
        effective_size: usize,
        device_id: ProcessLocalDeviceId,
    ) -> Option<CachedBlock> {
        let pool = &mut self.devices[device_id.0 as usize];
        let max_size = effective_size.saturating_mul(2);

        // Search size buckets in ascending order for smallest fit
        // Collect matching keys first to avoid borrow issues
        let candidate_keys: Vec<usize> = pool
            .blocks
            .range(effective_size..=max_size)
            .map(|(&k, _)| k)
            .collect();

        for key in candidate_keys {
            let deque = match pool.blocks.get_mut(&key) {
                Some(d) => d,
                None => continue,
            };

            // Check front of deque first (oldest = most likely completed)
            let mut found_idx = None;
            for (idx, block) in deque.iter().enumerate() {
                let query_result = unsafe { cu_api::cuEventQuery(block.event) };
                if query_result == cudaError_enum::CUDA_SUCCESS {
                    found_idx = Some(idx);
                    break;
                }
            }

            if let Some(idx) = found_idx {
                let block = deque.remove(idx).unwrap();
                if deque.is_empty() {
                    pool.blocks.remove(&key);
                }
                pool.cached_ptr_to_size.remove(&block.ptr);
                pool.cached_bytes -= block.actual_size;
                return Some(block);
            }
        }

        None
    }

    /// Cache a freed block in the pool.
    pub fn cache_free(&mut self, block: CachedBlock, device_id: ProcessLocalDeviceId) {
        let pool = &mut self.devices[device_id.0 as usize];
        pool.cached_ptr_to_size.insert(block.ptr, block.actual_size);
        pool.cached_bytes += block.actual_size;
        pool.blocks
            .entry(block.actual_size)
            .or_default()
            .push_back(block);
    }

    /// Remove a specific block by pointer. Used when `cudaFree` is called on a pooled block.
    pub fn remove_by_ptr(
        &mut self,
        ptr: u64,
        device_id: ProcessLocalDeviceId,
    ) -> Option<CachedBlock> {
        let pool = &mut self.devices[device_id.0 as usize];
        let size = pool.cached_ptr_to_size.remove(&ptr)?;
        let deque = pool.blocks.get_mut(&size)?;
        let idx = deque.iter().position(|b| b.ptr == ptr)?;
        let block = deque.remove(idx).unwrap();
        if deque.is_empty() {
            pool.blocks.remove(&size);
        }
        pool.cached_bytes -= block.actual_size;
        Some(block)
    }

    /// Drain all cached blocks for a device. Used for OOM retry.
    pub fn release_cached(&mut self, device_id: i32) -> Vec<CachedBlock> {
        let pool = &mut self.devices[device_id as usize];
        let mut released = Vec::new();
        for (_, deque) in std::mem::take(&mut pool.blocks) {
            released.extend(deque);
        }
        pool.cached_ptr_to_size.clear();
        pool.cached_bytes = 0;
        released
    }
}

pub(crate) fn async_pool() -> &'static Mutex<AsyncPool> {
    static ASYNC_POOL: OnceLock<Mutex<AsyncPool>> = OnceLock::new();
    ASYNC_POOL.get_or_init(|| Mutex::new(AsyncPool::new()))
}
