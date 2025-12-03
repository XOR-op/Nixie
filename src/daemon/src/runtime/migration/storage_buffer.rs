use std::{
    collections::HashMap,
    io::{IoSlice, IoSliceMut, Read, Seek, Write},
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

    pub fn store_vectored(
        &self,
        buffer_id: &BufferId,
        data: &[IoSlice<'_>],
    ) -> Result<(), HybridBufferError> {
        let mut inner = self.inner.lock().unwrap();
        if data.iter().map(|b| b.len()).sum::<usize>() > MAX_ALLOCATION_SIZE {
            return Err(HybridBufferError::InvalidInputBuffer);
        }

        let alloc_info = inner.save_to_disk_vectored(data)?;
        inner.disk_bookkeeping.insert(buffer_id.clone(), alloc_info);
        Ok(())
    }

    pub fn load_to_vectored(
        &self,
        buffer_id: &BufferId,
        data: &mut [IoSliceMut<'_>],
    ) -> Result<(), HybridBufferError> {
        let mut inner = self.inner.lock().unwrap();
        if buffer_id.size > (MAX_ALLOCATION_SIZE as u32) || (data.len() < buffer_id.size as usize) {
            return Err(HybridBufferError::InvalidInputBuffer);
        }
        if let Some(info) = inner.disk_bookkeeping.remove(buffer_id) {
            inner.load_from_disk_vectored(info.addr, buffer_id.size, data)?;
            inner.put_back_disk(info);
            Ok(())
        } else {
            Err(HybridBufferError::NoBufferId)
        }
    }

    pub fn batch_release(&self, buffer_ids: &[BufferId]) -> usize {
        let inner = &mut *self.inner.lock().unwrap();
        let mut released_count = 0;
        for buffer_id in buffer_ids {
            if let Some(info) = inner.disk_bookkeeping.remove(buffer_id) {
                inner.put_back_disk(info);
                released_count += 1;
            }
        }
        released_count
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
            .map(|(k, v)| (k.clone(), AllocationCapacity(v.block_size as u32)))
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
        assert_eq!(alloc_info.block_size, MAX_ALLOCATION_SIZE as u64);
        self.free_disk_buffers.push(alloc_info);
    }

    fn save_to_disk_vectored(
        &mut self,
        bufs: &[IoSlice<'_>],
    ) -> Result<AllocationInfo, HybridBufferError> {
        let expected_size: usize = bufs.iter().map(|b| b.len()).sum();
        let offset = if let Some(alloc_info) = self.free_disk_buffers.pop() {
            assert_eq!(alloc_info.block_size, MAX_ALLOCATION_SIZE as u64);
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
        let write_len = self
            .file
            .write_vectored(bufs)
            .map_err(|e| HybridBufferError::IoError(e, "Failed to write to file".to_string()))?;
        if write_len != expected_size {
            return Err(HybridBufferError::IoError(
                std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "Failed to write enough data to file",
                ),
                "Failed to write enough data to file".to_string(),
            ));
        }
        Ok(AllocationInfo {
            addr: offset,
            block_size: MAX_ALLOCATION_SIZE as u64,
        })
    }

    fn load_from_disk_vectored(
        &mut self,
        offset: u64,
        read_length: u32,
        data: &mut [IoSliceMut<'_>],
    ) -> Result<(), HybridBufferError> {
        self.file
            .seek(std::io::SeekFrom::Start(offset))
            .map_err(|e| HybridBufferError::IoError(e, "Failed to seek in file".to_string()))?;
        assert_eq!(
            data.iter().map(|d| d.len()).sum::<usize>(),
            read_length as usize
        );
        let read_len = self.file.read_vectored(data).map_err(|e| {
            HybridBufferError::IoError(e, "Failed to read vectored from file".to_string())
        })?;
        if read_len != read_length as usize {
            return Err(HybridBufferError::IoError(
                std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "Failed to read enough data from file",
                ),
                "Failed to read enough data from file".to_string(),
            ));
        }
        Ok(())
    }
}
