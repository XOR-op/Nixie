use std::io::{Read, Seek, Write};

use bytes::BytesMut;
use nihil_common::MAX_ALLOCATION_SIZE;

use super::{AllocationInfo, BufferId};

pub struct HybridBufferManager {
    inner: std::sync::Mutex<HybridBufferInner>,
}

struct BlockMemBuffer(BytesMut);

impl BlockMemBuffer {
    pub fn new() -> Self {
        Self(BytesMut::with_capacity(MAX_ALLOCATION_SIZE))
    }
}

#[derive(Debug)]
pub enum HybridBufferError {
    MemoryExhausted,
    InvalidInputBuffer,
    NoBufferId,
    IoError(std::io::Error, String),
}

impl HybridBufferManager {
    pub fn store(&self, buffer_id: &BufferId, data: &[u8]) -> Result<(), HybridBufferError> {
        let mut inner = self.inner.lock().unwrap();
        if data.len() > MAX_ALLOCATION_SIZE as usize {
            return Err(HybridBufferError::InvalidInputBuffer);
        }
        if let Some(mut block_buffer) = inner.alloc_buffer() {
            block_buffer.0.clear();
            block_buffer.0.extend_from_slice(data);
            inner
                .mem_bookkeeping
                .insert(buffer_id.clone(), block_buffer);
        } else {
            let alloc_info = inner.save_to_disk(data)?;
            inner.disk_bookkeeping.insert(buffer_id.clone(), alloc_info);
        }
        Ok(())
    }

    pub fn load_to(&self, buffer_id: &BufferId, data: &mut [u8]) -> Result<(), HybridBufferError> {
        let mut inner = self.inner.lock().unwrap();
        if buffer_id.size > (MAX_ALLOCATION_SIZE as u64) || (data.len() as u64) < buffer_id.size {
            return Err(HybridBufferError::InvalidInputBuffer);
        }
        if let Some(block_buffer) = inner.mem_bookkeeping.remove(buffer_id) {
            if data.len() < block_buffer.0.len() {
                return Err(HybridBufferError::InvalidInputBuffer);
            }
            data[..block_buffer.0.len()].copy_from_slice(&block_buffer.0);
            inner.put_back(block_buffer);
            Ok(())
        } else if let Some(info) = inner.disk_bookkeeping.remove(buffer_id) {
            inner.load_from_disk(info.addr, buffer_id.size, data)
        } else {
            Err(HybridBufferError::NoBufferId)
        }
    }
}

struct HybridBufferInner {
    mem_bookkeeping: std::collections::HashMap<BufferId, BlockMemBuffer>,
    free_mem_buffers: Vec<BlockMemBuffer>,
    max_mem_buffer_count: usize,

    file: std::fs::File,
    disk_bookkeeping: std::collections::HashMap<BufferId, AllocationInfo>,
    free_disk_buffers: Vec<AllocationInfo>,
    file_size: u64,
}

impl HybridBufferInner {
    fn alloc_buffer(&mut self) -> Option<BlockMemBuffer> {
        if let Some(buffer) = self.free_mem_buffers.pop() {
            Some(buffer)
        } else if self.mem_bookkeeping.len() < self.max_mem_buffer_count {
            let new_buffer = BlockMemBuffer::new();
            Some(new_buffer)
        } else {
            None
        }
    }

    fn put_back(&mut self, buffer: BlockMemBuffer) {
        if self.free_mem_buffers.len() < self.max_mem_buffer_count {
            self.free_mem_buffers.push(buffer);
        }
    }

    fn save_to_disk(&mut self, buf: &[u8]) -> Result<AllocationInfo, HybridBufferError> {
        let offset = if let Some(alloc_info) = self.free_disk_buffers.pop() {
            alloc_info.addr
        } else {
            let offset = self.file_size;
            self.file_size += MAX_ALLOCATION_SIZE as u64;
            self.file.set_len(self.file_size).map_err(|e| {
                HybridBufferError::IoError(e, "Failed to set file length".to_string())
            })?;
            offset
        };
        self.file
            .seek(std::io::SeekFrom::Start(offset))
            .map_err(|e| HybridBufferError::IoError(e, "Failed to seek in file".to_string()))?;
        self.file
            .write_all(buf)
            .map_err(|e| HybridBufferError::IoError(e, "Failed to write to file".to_string()))?;
        Ok(AllocationInfo {
            addr: offset,
            block_size: MAX_ALLOCATION_SIZE as u64,
        })
    }

    fn load_from_disk(
        &mut self,
        offset: u64,
        read_length: u64,
        data: &mut [u8],
    ) -> Result<(), HybridBufferError> {
        self.file
            .seek(std::io::SeekFrom::Start(offset))
            .map_err(|e| HybridBufferError::IoError(e, "Failed to seek in file".to_string()))?;
        self.file
            .read_exact(&mut data[..read_length as usize])
            .map_err(|e| HybridBufferError::IoError(e, "Failed to read from file".to_string()))?;
        Ok(())
    }
}
