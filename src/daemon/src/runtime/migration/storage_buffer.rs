use std::{
    collections::HashMap,
    io::{Read, Seek, Write},
    path::Path,
};

use nihil_common::MAX_ALLOCATION_SIZE;

use crate::{
    error::HybridBufferError,
    runtime::migration::{AllocationCapacity, AllocationInfo, BufferId},
};

pub struct StorageBufferManager {
    inner: std::sync::Mutex<StorageBufferInner>,
}

impl StorageBufferManager {
    pub fn new(disk_path: &Path) -> Result<Self, HybridBufferError> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(disk_path)
            .map_err(|e| HybridBufferError::IoError(e, "Failed to open disk file".to_string()))?;
        let inner = StorageBufferInner {
            file,
            disk_bookkeeping: HashMap::new(),
            free_disk_buffers: Vec::new(),
            file_size: 0,
        };
        Ok(Self {
            inner: std::sync::Mutex::new(inner),
        })
    }

    pub fn store(&self, buffer_id: &BufferId, data: &[u8]) -> Result<(), HybridBufferError> {
        let mut inner = self.inner.lock().unwrap();
        if data.len() > MAX_ALLOCATION_SIZE  {
            return Err(HybridBufferError::InvalidInputBuffer);
        }

        let alloc_info = inner.save_to_disk(data)?;
        inner.disk_bookkeeping.insert(buffer_id.clone(), alloc_info);
        Ok(())
    }

    pub fn load_to(&self, buffer_id: &BufferId, data: &mut [u8]) -> Result<(), HybridBufferError> {
        let mut inner = self.inner.lock().unwrap();
        if buffer_id.size > (MAX_ALLOCATION_SIZE as u64) || (data.len() as u64) < buffer_id.size {
            return Err(HybridBufferError::InvalidInputBuffer);
        }
        if let Some(info) = inner.disk_bookkeeping.remove(buffer_id) {
            inner.load_from_disk(info.addr, buffer_id.size, data)?;
            inner.put_back_disk(info);
            Ok(())
        } else {
            Err(HybridBufferError::NoBufferId)
        }
    }

    pub fn release_process_residual(&self, pid: i32) {
        let inner = &mut *self.inner.lock().unwrap();
        for (id, alloc_info) in std::mem::take(&mut inner.disk_bookkeeping) {
            if id.pid == pid {
                inner.free_disk_buffers.push(alloc_info);
            } else {
                inner.disk_bookkeeping.insert(id, alloc_info);
            }
        }
    }

    pub fn contains(&self, buffer_id: &BufferId) -> bool {
        self.inner
            .lock()
            .unwrap()
            .disk_bookkeeping
            .contains_key(buffer_id)
    }

    pub fn dump_buffers(&self) -> HashMap<BufferId, AllocationCapacity> {
        self.inner
            .lock()
            .unwrap()
            .disk_bookkeeping
            .iter()
            .map(|(k, v)| (k.clone(), v.block_size))
            .collect()
    }
}

struct StorageBufferInner {
    file: std::fs::File,
    disk_bookkeeping: std::collections::HashMap<BufferId, AllocationInfo>,
    free_disk_buffers: Vec<AllocationInfo>,
    file_size: u64,
}

impl StorageBufferInner {
    fn put_back_disk(&mut self, alloc_info: AllocationInfo) {
        self.free_disk_buffers.push(alloc_info);
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
