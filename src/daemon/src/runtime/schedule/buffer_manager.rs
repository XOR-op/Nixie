use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroU32,
    sync::Mutex,
};

use nihil_common::{shm_buffer::ShmBuffer, GlobalDeviceId, MAX_ALLOCATION_SIZE};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BufferId {
    pub pid: i32,
    pub device_id: GlobalDeviceId,
    pub block_id: NonZeroU32,
    pub size: u64,
}

pub struct ShmBufferManager {
    shm_buffer: ShmBuffer,
    inner: Mutex<ShmBufferInner>,
}

struct ShmBufferInner {
    bookkeeping: HashMap<BufferId, u64>,
    avail_addrs: BTreeMap<u64, u64>,
}

impl ShmBufferManager {
    pub fn new(shm_path: &str, shm_size: usize) -> Result<Self, std::io::Error> {
        assert!(
            shm_size % MAX_ALLOCATION_SIZE == 0,
            "Shared memory size must be a multiple of MAX_ALLOCATION_SIZE"
        );
        let shm_buffer = ShmBuffer::new(shm_path, shm_size, true)?;
        let mut avail_addrs = BTreeMap::new();
        let mut offset = 0;
        while offset < shm_size as u64 {
            let size = MAX_ALLOCATION_SIZE as u64;
            avail_addrs.insert(offset, size);
            offset += size;
        }
        Ok(Self {
            shm_buffer,
            inner: Mutex::new(ShmBufferInner {
                bookkeeping: HashMap::new(),
                avail_addrs,
            }),
        })
    }

    pub fn reserve(&self, buf_id: &BufferId) -> Option<u64> {
        let mut inner = self.inner.lock().unwrap();
        let addr = *(inner
            .avail_addrs
            .iter()
            .find(|(_, size)| **size as u64 >= buf_id.size)?
            .0);
        let len = inner.avail_addrs.remove(&addr);
        debug_assert!(len.is_some());
        inner.bookkeeping.insert(buf_id.clone(), addr);
        Some(addr)
    }

    pub fn release(&self, buf_id: &BufferId) -> Result<(), ()> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(addr) = inner.bookkeeping.remove(buf_id) {
            inner.avail_addrs.insert(addr, MAX_ALLOCATION_SIZE as u64);
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn get_buffer(&self, buf_id: &BufferId) -> Option<u64> {
        self.inner.lock().unwrap().bookkeeping.get(buf_id).copied()
    }

    pub fn release_process_residual(&self, pid: i32) {
        let inner = &mut *self.inner.lock().unwrap();
        inner.bookkeeping.retain(|buf_id, _| {
            let will_keep = buf_id.pid != pid;
            if !will_keep {
                inner.avail_addrs.insert(buf_id.size, buf_id.size);
            }
            will_keep
        });
    }
}
