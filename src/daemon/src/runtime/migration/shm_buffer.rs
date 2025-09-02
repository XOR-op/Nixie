use std::{
    collections::{BTreeMap, HashMap},
    sync::Mutex,
};

use nihil_common::{MAX_ALLOCATION_SIZE, shm_buffer::ShmBuffer};
use tokio::sync::oneshot;

use super::{AllocationInfo, BufferId};

pub struct ShmBufferManager {
    shm_buffer: ShmBuffer,
    inner: Mutex<ShmBufferInner>,
}

type Offset = u64;
type AllocationCapacity = u64;

struct ShmBufferInner {
    bookkeeping: HashMap<BufferId, AllocationInfo>,
    avail_addrs: BTreeMap<Offset, AllocationCapacity>,
    pending_reservations: Vec<oneshot::Sender<()>>,
}

impl ShmBufferInner {
    fn reserve_inner(
        inner: &mut std::sync::MutexGuard<'_, Self>,
        buf_id: &BufferId,
    ) -> Option<u64> {
        let r = inner
            .avail_addrs
            .iter()
            .find(|(_, size)| **size as u64 >= buf_id.size)?;
        let (addr, block_size) = (*r.0, *r.1);
        let len = inner.avail_addrs.remove(&addr);
        debug_assert!(len.is_some());
        inner
            .bookkeeping
            .insert(buf_id.clone(), AllocationInfo { addr, block_size });
        Some(addr)
    }

    fn notify_reservation(inner: &mut std::sync::MutexGuard<'_, Self>, mut cnt: usize) {
        // TODO: better scheduling strategy
        while cnt > 0
            && let Some(tx) = inner.pending_reservations.pop()
        {
            let _ = tx.send(());
            cnt -= 1;
        }
    }
}

// Basic operations
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
                pending_reservations: Vec::new(),
            }),
        })
    }

    pub fn get_buffer(&self, buf_id: &BufferId) -> Option<AllocationInfo> {
        self.inner.lock().unwrap().bookkeeping.get(buf_id).cloned()
    }

    /// Returns: a list of lengths of free segments
    pub fn free_segments(&self) -> Vec<u64> {
        self.inner
            .lock()
            .unwrap()
            .avail_addrs
            .values()
            .cloned()
            .collect()
    }

    pub unsafe fn at_offset(&self, offset: u64, size: usize) -> Option<*mut u8> {
        unsafe { self.shm_buffer.at_offset(offset, size) }
    }
}

// Allocation and release logic
impl ShmBufferManager {
    pub fn try_reserve(&self, buf_id: &BufferId) -> Option<u64> {
        let mut inner = self.inner.lock().unwrap();
        ShmBufferInner::reserve_inner(&mut inner, buf_id)
    }

    pub async fn reserve(&self, buf_id: &BufferId) -> u64 {
        loop {
            let mut inner = self.inner.lock().unwrap();
            if let Some(res) = ShmBufferInner::reserve_inner(&mut inner, buf_id) {
                return res;
            }
            let (tx, rx) = oneshot::channel();
            inner.pending_reservations.push(tx);
            drop(inner);
            let _ = rx.await;
        }
    }

    pub fn release(&self, buf_id: &BufferId) -> Result<(), ()> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(info) = inner.bookkeeping.remove(buf_id) {
            inner.avail_addrs.insert(info.addr, info.block_size);
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn release_process_residual(&self, pid: i32) {
        let inner = &mut *self.inner.lock().unwrap();
        inner.bookkeeping.retain(|buf_id, info| {
            let will_keep = buf_id.pid != pid;
            if !will_keep {
                inner.avail_addrs.insert(info.addr, info.block_size);
            }
            will_keep
        });
    }
}
