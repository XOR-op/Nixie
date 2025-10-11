use std::collections::HashMap;

use bytes::BytesMut;
use nihil_common::MAX_ALLOCATION_SIZE;

use crate::{
    error::HybridBufferError,
    runtime::migration::{AllocationCapacity, BufferLocation},
};

use super::BufferId;

pub struct HostMemBufferManager {
    inner: std::sync::Mutex<HostMemBufferInner>,
}

pub(crate) struct BlockMemBuffer(pub BytesMut);

impl BlockMemBuffer {
    pub fn new() -> Self {
        Self(BytesMut::with_capacity(MAX_ALLOCATION_SIZE))
    }
}

impl HostMemBufferManager {
    pub fn new(in_mem_size: usize, extra_burst_size: usize) -> Self {
        let inner = HostMemBufferInner {
            mem_bookkeeping: HashMap::new(),
            free_mem_buffers: Vec::new(),
            max_mem_buffer_count: in_mem_size / MAX_ALLOCATION_SIZE,
            extra_burst_mem_buffer_count: extra_burst_size / MAX_ALLOCATION_SIZE,
            borrowed_count: 0,
        };
        Self {
            inner: std::sync::Mutex::new(inner),
        }
    }

    pub fn store(&self, buffer_id: &BufferId, data: &[u8]) -> Result<(), HybridBufferError> {
        let mut inner = self.inner.lock().unwrap();
        if data.len() > MAX_ALLOCATION_SIZE {
            return Err(HybridBufferError::InvalidInputBuffer);
        }
        if let Some(mut block_buffer) = inner.alloc_buffer() {
            block_buffer.0.clear();
            block_buffer.0.extend_from_slice(data);
            inner
                .mem_bookkeeping
                .insert(buffer_id.clone(), block_buffer);
            Ok(())
        } else {
            Err(HybridBufferError::MemoryExhausted)
        }
    }

    pub fn load_to(
        &self,
        buffer_id: &BufferId,
        data: &mut [u8],
    ) -> Result<BufferLocation, HybridBufferError> {
        let mut inner = self.inner.lock().unwrap();
        if buffer_id.size > (MAX_ALLOCATION_SIZE as u64) || (data.len() as u64) < buffer_id.size {
            return Err(HybridBufferError::InvalidInputBuffer);
        }
        if let Some(block_buffer) = inner.mem_bookkeeping.remove(buffer_id) {
            if data.len() < block_buffer.0.len() {
                return Err(HybridBufferError::InvalidInputBuffer);
            }
            data[..block_buffer.0.len()].copy_from_slice(&block_buffer.0);
            inner.put_back_mem(block_buffer);
            Ok(BufferLocation::HostMem)
        } else {
            Err(HybridBufferError::NoBufferId)
        }
    }

    pub fn pop_buffer(&self, buffer_id: &BufferId) -> Option<BlockMemBuffer> {
        let mut inner = self.inner.lock().unwrap();
        let res = inner.mem_bookkeeping.remove(buffer_id)?;
        inner.borrowed_count += 1;
        Some(res)
    }

    pub fn allocate_empty_buffer(&self) -> Option<BlockMemBuffer> {
        let mut inner = self.inner.lock().unwrap();
        let mut res = inner.alloc_buffer()?;
        if res.0.capacity() < MAX_ALLOCATION_SIZE {
            res.0.resize(MAX_ALLOCATION_SIZE, 0);
        }
        // Safety: We just resized the buffer to MAX_ALLOCATION_SIZE, so it's safe to set the length.
        unsafe {
            res.0.set_len(MAX_ALLOCATION_SIZE);
        }
        inner.borrowed_count += 1;
        Some(res)
    }

    pub fn return_associated_buffer(&self, buffer_id: BufferId, buffer: BlockMemBuffer) {
        let mut inner = self.inner.lock().unwrap();
        assert!(inner.borrowed_count > 0);
        assert_eq!(buffer.0.len(), buffer_id.size as usize);
        inner.borrowed_count -= 1;
        inner.mem_bookkeeping.insert(buffer_id, buffer);
    }

    pub fn put_back_mem(&self, buffer: BlockMemBuffer) {
        let inner = &mut *self.inner.lock().unwrap();
        assert!(inner.borrowed_count > 0);
        inner.borrowed_count -= 1;
        inner.put_back_mem(buffer);
    }

    pub fn release_process_residual(&self, pid: i32) {
        let inner = &mut *self.inner.lock().unwrap();
        for (id, mem) in std::mem::take(&mut inner.mem_bookkeeping) {
            if id.pid == pid {
                inner.put_back_mem(mem);
            } else {
                inner.mem_bookkeeping.insert(id, mem);
            }
        }
    }

    pub fn contains(&self, buffer_id: &BufferId) -> bool {
        self.inner
            .lock()
            .unwrap()
            .mem_bookkeeping
            .contains_key(buffer_id)
    }

    /// Returns: a list of length of free memory segments in bytes.
    pub fn free_mem_segments(&self) -> Vec<AllocationCapacity> {
        let inner = self.inner.lock().unwrap();
        let free_count = (inner.max_mem_buffer_count + inner.extra_burst_mem_buffer_count)
            .saturating_sub(inner.mem_bookkeeping.len() + inner.borrowed_count);

        (0..free_count)
            .map(|_| MAX_ALLOCATION_SIZE as u64)
            .collect()
    }

    pub fn dump_buffers(&self) -> HashMap<BufferId, AllocationCapacity> {
        self.inner
            .lock()
            .unwrap()
            .mem_bookkeeping
            .iter()
            .map(|(k, v)| (k.clone(), v.0.len() as u64))
            .collect()
    }

    pub fn capacity(&self) -> usize {
        self.inner.lock().unwrap().max_mem_buffer_count * MAX_ALLOCATION_SIZE
    }
}

struct HostMemBufferInner {
    mem_bookkeeping: std::collections::HashMap<BufferId, BlockMemBuffer>,
    free_mem_buffers: Vec<BlockMemBuffer>,
    max_mem_buffer_count: usize,
    extra_burst_mem_buffer_count: usize,

    borrowed_count: usize,
}

impl HostMemBufferInner {
    fn alloc_buffer(&mut self) -> Option<BlockMemBuffer> {
        if let Some(buffer) = self.free_mem_buffers.pop() {
            Some(buffer)
        } else if self.mem_bookkeeping.len()
            < self.max_mem_buffer_count + self.extra_burst_mem_buffer_count + self.borrowed_count
        {
            let new_buffer = BlockMemBuffer::new();
            Some(new_buffer)
        } else {
            None
        }
    }

    fn put_back_mem(&mut self, buffer: BlockMemBuffer) {
        if self.free_mem_buffers.len() < self.max_mem_buffer_count {
            self.free_mem_buffers.push(buffer);
        }
    }
}
