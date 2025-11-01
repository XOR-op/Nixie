use std::collections::HashMap;

use bytes::BytesMut;
use nihil_common::{MAX_ALLOCATION_SIZE, MIN_ALLOCATION_SIZE};

use crate::{
    error::HybridBufferError,
    runtime::migration::{AllocationCapacity, AllocationCount, BufferLocation},
};

use super::BufferId;

pub struct HostMemBufferManager {
    inner: std::sync::Mutex<HostMemBufferInner>,
}

pub(crate) struct BlockMemBuffer(pub BytesMut);

impl BlockMemBuffer {
    pub fn new() -> Self {
        Self(BytesMut::with_capacity(MIN_ALLOCATION_SIZE))
    }
}

impl HostMemBufferManager {
    pub fn new(in_mem_size: usize, extra_burst_size: usize, preallocate: bool) -> Self {
        let max_buffer_count = in_mem_size / MIN_ALLOCATION_SIZE;
        let free_buffers = if preallocate {
            let mut bufs = Vec::with_capacity(max_buffer_count);
            for _ in 0..max_buffer_count {
                let mut buf = BlockMemBuffer::new();
                // write random data to the buffer to actually allocate the memory
                buf.0.resize(MIN_ALLOCATION_SIZE, 1);
                buf.0.clear();
                bufs.push(buf);
            }
            bufs
        } else {
            Vec::new()
        };
        let inner = HostMemBufferInner {
            mem_bookkeeping: HashMap::new(),
            free_mem_buffers: free_buffers,
            max_mem_buffer_count: max_buffer_count,
            extra_burst_mem_buffer_count: extra_burst_size / MIN_ALLOCATION_SIZE,
            borrowed_count: 0,
        };
        Self {
            inner: std::sync::Mutex::new(inner),
        }
    }

    fn store_preparation(
        inner: &mut std::sync::MutexGuard<'_, HostMemBufferInner>,
        data_len: usize,
    ) -> Result<Vec<BlockMemBuffer>, HybridBufferError> {
        if data_len > MAX_ALLOCATION_SIZE {
            tracing::warn!(
                "Invalid store request: data length = {}",
                nihil_common::general::pretty_size(data_len as u64)
            );
            return Err(HybridBufferError::InvalidInputBuffer);
        }
        if let Some(block_buffers) = inner.alloc_n_buffers(data_len.div_ceil(MIN_ALLOCATION_SIZE)) {
            Ok(block_buffers)
        } else {
            Err(HybridBufferError::MemoryExhausted)
        }
    }

    pub fn store(&self, buffer_id: &BufferId, data: &[u8]) -> Result<(), HybridBufferError> {
        let mut inner = self.inner.lock().unwrap();
        let mut block_buffers = Self::store_preparation(&mut inner, data.len())?;
        let mut offset = 0;
        for block_buffer in &mut block_buffers {
            let copy_size = std::cmp::min(MIN_ALLOCATION_SIZE, data.len() - offset);
            block_buffer.0.clear();
            block_buffer
                .0
                .extend_from_slice(&data[offset..offset + copy_size]);
            offset += copy_size;
        }
        inner
            .mem_bookkeeping
            .insert(buffer_id.clone(), block_buffers);
        Ok(())
    }

    pub fn store_vectored(
        &self,
        buffer_id: &BufferId,
        data: &[std::io::IoSlice<'_>],
    ) -> Result<(), HybridBufferError> {
        let mut inner = self.inner.lock().unwrap();
        let total_size: usize = data.iter().map(|b| b.len()).sum();
        let mut block_buffers = Self::store_preparation(&mut inner, total_size)?;
        assert_eq!(block_buffers.len(), data.len());
        for (src, dst) in data.iter().zip(block_buffers.iter_mut()) {
            dst.0.clear();
            dst.0.extend_from_slice(&src);
        }
        inner
            .mem_bookkeeping
            .insert(buffer_id.clone(), block_buffers);
        Ok(())
    }

    fn load_preparation(
        inner: &mut std::sync::MutexGuard<'_, HostMemBufferInner>,
        buffer_id: &BufferId,
        data_len: usize,
    ) -> Result<Vec<BlockMemBuffer>, HybridBufferError> {
        if buffer_id.size as usize > MAX_ALLOCATION_SIZE || data_len < buffer_id.size as usize {
            tracing::warn!(
                "Invalid load request: buffer size = {}, data length = {}",
                nihil_common::general::pretty_size(buffer_id.size as u64),
                nihil_common::general::pretty_size(data_len as u64)
            );
            return Err(HybridBufferError::InvalidInputBuffer);
        }
        if let Some(block_buffers) = inner.mem_bookkeeping.remove(buffer_id) {
            Ok(block_buffers)
        } else {
            Err(HybridBufferError::NoBufferId)
        }
    }

    pub fn load_to(
        &self,
        buffer_id: &BufferId,
        data: &mut [u8],
    ) -> Result<BufferLocation, HybridBufferError> {
        let mut inner = self.inner.lock().unwrap();
        let block_buffers = Self::load_preparation(&mut inner, buffer_id, data.len())?;
        let mut offset = 0;
        for block_buffer in &block_buffers {
            let copy_size = block_buffer.0.len();
            data[offset..offset + copy_size].copy_from_slice(&block_buffer.0);
            offset += copy_size;
        }
        Ok(BufferLocation::HostMem)
    }

    pub fn load_to_vectored(
        &self,
        buffer_id: &BufferId,
        data: &mut [std::io::IoSliceMut<'_>],
    ) -> Result<BufferLocation, HybridBufferError> {
        let mut inner = self.inner.lock().unwrap();
        let total_size: usize = data.iter().map(|b| b.len()).sum();
        let block_buffers = Self::load_preparation(&mut inner, buffer_id, total_size)?;
        assert_eq!(block_buffers.len(), data.len());
        for (src, dst) in block_buffers.iter().zip(data.iter_mut()) {
            let copy_size = src.0.len();
            dst[..copy_size].copy_from_slice(&src.0);
        }
        Ok(BufferLocation::HostMem)
    }

    pub fn pop_buffer(&self, buffer_id: &BufferId) -> Option<Vec<BlockMemBuffer>> {
        let mut inner = self.inner.lock().unwrap();
        let res = inner.mem_bookkeeping.remove(buffer_id)?;
        inner.borrowed_count += res.len();
        Some(res)
    }

    pub fn allocate_empty_buffer(&self, bytes: usize) -> Option<Vec<BlockMemBuffer>> {
        let mut inner = self.inner.lock().unwrap();
        if bytes > MAX_ALLOCATION_SIZE {
            tracing::warn!(
                "Requested allocation size {} exceeds MAX_ALLOCATION_SIZE {}",
                bytes,
                MAX_ALLOCATION_SIZE
            );
            return None;
        }
        let n = bytes.div_ceil(MIN_ALLOCATION_SIZE);
        let mut res = inner.alloc_n_buffers(n)?;
        let mut accumulated_size = 0;
        for buf in &mut res {
            buf.0.clear();
            if buf.0.capacity() < MIN_ALLOCATION_SIZE {
                buf.0.resize(MIN_ALLOCATION_SIZE, 0);
            }
            let to_resize =
                std::cmp::min(MIN_ALLOCATION_SIZE, bytes.saturating_sub(accumulated_size));
            // Safety: We just resized the buffer to MIN_ALLOCATION_SIZE, so it's safe to set the length.
            unsafe {
                buf.0.set_len(accumulated_size);
            }
            accumulated_size += to_resize;
        }
        inner.borrowed_count += n;
        Some(res)
    }

    pub fn return_associated_buffer(&self, buffer_id: BufferId, buffer: Vec<BlockMemBuffer>) {
        let mut inner = self.inner.lock().unwrap();
        assert!(inner.borrowed_count > 0);
        assert_eq!(
            buffer.iter().map(|b| b.0.len()).sum::<usize>(),
            buffer_id.size as usize
        );
        inner.borrowed_count -= 1;
        inner.mem_bookkeeping.insert(buffer_id, buffer);
    }

    pub fn put_back_mem(&self, buffer: Vec<BlockMemBuffer>) {
        let inner = &mut *self.inner.lock().unwrap();
        assert!(inner.borrowed_count > buffer.len());
        inner.borrowed_count -= buffer.len();
        for buf in buffer {
            inner.put_back_mem(buf);
        }
    }

    pub fn release_process_residual(&self, pid: i32) {
        let inner = &mut *self.inner.lock().unwrap();
        for (id, mems) in std::mem::take(&mut inner.mem_bookkeeping) {
            if id.pid == pid {
                for mem in mems {
                    inner.put_back_mem(mem);
                }
            } else {
                inner.mem_bookkeeping.insert(id, mems);
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

    pub fn get_block_count(&self, buffer_id: &BufferId) -> Option<AllocationCount> {
        let inner = self.inner.lock().unwrap();
        let buffers = inner.mem_bookkeeping.get(buffer_id)?;
        Some(AllocationCount(buffers.len() as u32))
    }

    /// Returns: a list of length of free memory segments in bytes.
    pub fn free_blocks_count(&self) -> AllocationCount {
        let inner = self.inner.lock().unwrap();
        let free_count = (inner.max_mem_buffer_count + inner.extra_burst_mem_buffer_count)
            .saturating_sub(inner.mem_bookkeeping.len() + inner.borrowed_count);
        AllocationCount(free_count as u32)
    }

    pub fn dump_buffers(&self) -> HashMap<BufferId, AllocationCapacity> {
        self.inner
            .lock()
            .unwrap()
            .mem_bookkeeping
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    AllocationCapacity((v.len() * MIN_ALLOCATION_SIZE) as u32),
                )
            })
            .collect()
    }

    pub fn capacity(&self) -> usize {
        self.inner.lock().unwrap().max_mem_buffer_count * MIN_ALLOCATION_SIZE
    }
}

struct HostMemBufferInner {
    mem_bookkeeping: std::collections::HashMap<BufferId, Vec<BlockMemBuffer>>,
    free_mem_buffers: Vec<BlockMemBuffer>,
    max_mem_buffer_count: usize,
    extra_burst_mem_buffer_count: usize,

    borrowed_count: usize,
}

impl HostMemBufferInner {
    fn alloc_buffer(&mut self) -> Option<BlockMemBuffer> {
        if let Some(buffer) = self.free_mem_buffers.pop() {
            Some(buffer)
        } else if self.mem_bookkeeping.len() + self.borrowed_count
            < self.max_mem_buffer_count + self.extra_burst_mem_buffer_count
        {
            let new_buffer = BlockMemBuffer::new();
            Some(new_buffer)
        } else {
            None
        }
    }

    fn alloc_n_buffers(&mut self, n: usize) -> Option<Vec<BlockMemBuffer>> {
        let allowed_extra_buffer_cnt = self.max_mem_buffer_count
            + self.extra_burst_mem_buffer_count
            - (self.mem_bookkeeping.len() + self.borrowed_count);
        if self.free_mem_buffers.len() + allowed_extra_buffer_cnt < n {
            return None;
        }
        let mut buffers = Vec::with_capacity(n);
        for _ in 0..n {
            if let Some(buf) = self.free_mem_buffers.pop() {
                buffers.push(buf);
            } else {
                buffers.push(BlockMemBuffer::new());
            }
        }
        Some(buffers)
    }

    fn put_back_mem(&mut self, buffer: BlockMemBuffer) {
        if self.free_mem_buffers.len() < self.max_mem_buffer_count {
            self.free_mem_buffers.push(buffer);
        }
    }
}
