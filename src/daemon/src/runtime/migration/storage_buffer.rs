use std::{collections::HashMap, path::Path};

use nihil_common::MAX_ALLOCATION_SIZE;

use crate::{
    error::HybridBufferError,
    runtime::migration::{AllocationInfo, BufferId, BufferLocation},
};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
pub struct StorageBufferManager {
    inner: tokio::sync::Mutex<StorageBufferInner>,
}

impl StorageBufferManager {
    pub fn new(disk_path: &Path) -> Result<Self, HybridBufferError> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(disk_path)
            .map_err(|e| HybridBufferError::IoError(e, "Failed to open disk file".to_string()))?;
        let inner = StorageBufferInner {
            file: tokio::fs::File::from_std(file),
            disk_bookkeeping: HashMap::new(),
            free_disk_buffers: Vec::new(),
            file_size: 0,
        };
        Ok(Self {
            inner: tokio::sync::Mutex::new(inner),
        })
    }

    pub async fn store(
        &self,
        buffer_id: &BufferId,
        data: &[u8],
    ) -> Result<BufferLocation, HybridBufferError> {
        let mut inner = self.inner.lock().await;
        if data.len() > MAX_ALLOCATION_SIZE as usize {
            return Err(HybridBufferError::InvalidInputBuffer);
        }

        let alloc_info = inner.save_to_disk(data).await?;
        inner.disk_bookkeeping.insert(buffer_id.clone(), alloc_info);
        Ok(BufferLocation::Storage)
    }

    pub async fn load_to(
        &self,
        buffer_id: &BufferId,
        data: &mut [u8],
    ) -> Result<BufferLocation, HybridBufferError> {
        let mut inner = self.inner.lock().await;
        if buffer_id.size > (MAX_ALLOCATION_SIZE as u64) || (data.len() as u64) < buffer_id.size {
            return Err(HybridBufferError::InvalidInputBuffer);
        }
        if let Some(info) = inner.disk_bookkeeping.remove(buffer_id) {
            inner
                .load_from_disk(info.addr, buffer_id.size, data)
                .await?;
            inner.put_back_disk(info);
            Ok(BufferLocation::Storage)
        } else {
            Err(HybridBufferError::NoBufferId)
        }
    }

    pub fn release_process_residual(&self, pid: i32) {
        // use wrapper; otherwise blocking_lock in async context will panic
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(move || self._release_process_residual_inner(pid))
        } else {
            self._release_process_residual_inner(pid)
        }
    }

    fn _release_process_residual_inner(&self, pid: i32) {
        let inner = &mut *self.inner.blocking_lock();
        for (id, alloc_info) in std::mem::take(&mut inner.disk_bookkeeping) {
            if id.pid == pid {
                inner.free_disk_buffers.push(alloc_info);
            } else {
                inner.disk_bookkeeping.insert(id, alloc_info);
            }
        }
    }

    pub fn contains(&self, buffer_id: &BufferId) -> bool {
        // use wrapper; otherwise blocking_lock in async context will panic
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(move || self._contains_inner(buffer_id))
        } else {
            self._contains_inner(buffer_id)
        }
    }

    fn _contains_inner(&self, buffer_id: &BufferId) -> bool {
        self.inner
            .blocking_lock()
            .disk_bookkeeping
            .contains_key(buffer_id)
    }
}

struct StorageBufferInner {
    file: tokio::fs::File,
    disk_bookkeeping: std::collections::HashMap<BufferId, AllocationInfo>,
    free_disk_buffers: Vec<AllocationInfo>,
    file_size: u64,
}

impl StorageBufferInner {
    fn put_back_disk(&mut self, alloc_info: AllocationInfo) {
        self.free_disk_buffers.push(alloc_info);
    }

    async fn save_to_disk(&mut self, buf: &[u8]) -> Result<AllocationInfo, HybridBufferError> {
        let offset = if let Some(alloc_info) = self.free_disk_buffers.pop() {
            alloc_info.addr
        } else {
            let offset = self.file_size;
            self.file_size += MAX_ALLOCATION_SIZE as u64;
            self.file.set_len(self.file_size).await.map_err(|e| {
                HybridBufferError::IoError(e, "Failed to set file length".to_string())
            })?;
            offset
        };
        self.file
            .seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|e| HybridBufferError::IoError(e, "Failed to seek in file".to_string()))?;
        self.file
            .write_all(buf)
            .await
            .map_err(|e| HybridBufferError::IoError(e, "Failed to write to file".to_string()))?;
        Ok(AllocationInfo {
            addr: offset,
            block_size: MAX_ALLOCATION_SIZE as u64,
        })
    }

    async fn load_from_disk(
        &mut self,
        offset: u64,
        read_length: u64,
        data: &mut [u8],
    ) -> Result<(), HybridBufferError> {
        self.file
            .seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|e| HybridBufferError::IoError(e, "Failed to seek in file".to_string()))?;
        self.file
            .read_exact(&mut data[..read_length as usize])
            .await
            .map_err(|e| HybridBufferError::IoError(e, "Failed to read from file".to_string()))?;
        Ok(())
    }
}
