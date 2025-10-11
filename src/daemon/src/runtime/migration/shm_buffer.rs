use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::Mutex,
    time::Duration,
};

use nihil_common::{MAX_ALLOCATION_SIZE, shm_buffer::ShmBuffer};
use tokio::sync::oneshot;

use crate::runtime::migration::{AllocationCapacity, Offset};

use super::{AllocationInfo, BufferId};

pub struct ShmBufferManager {
    shm_buffer: ShmBuffer,
    inner: Mutex<ShmBufferInner>,
}

struct ShmBufferInner {
    bookkeeping: HashMap<BufferId, AllocationInfo>,
    avail_addrs: BTreeMap<Offset, AllocationCapacity>,
    pending_reservations: VecDeque<oneshot::Sender<()>>,
}

impl ShmBufferInner {
    fn reserve_inner(
        inner: &mut std::sync::MutexGuard<'_, Self>,
        buf_id: &BufferId,
    ) -> Option<u64> {
        let r = inner
            .avail_addrs
            .iter()
            .find(|(_, size)| **size >= buf_id.size)?;
        tracing::trace!("ShmBuffer: available length = {}", inner.avail_addrs.len());
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
            && let Some(tx) = inner.pending_reservations.pop_front()
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
            shm_size.is_multiple_of(MAX_ALLOCATION_SIZE),
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
                pending_reservations: VecDeque::new(),
            }),
        })
    }

    pub fn get_buffer(&self, buf_id: &BufferId) -> Option<AllocationInfo> {
        self.inner.lock().unwrap().bookkeeping.get(buf_id).cloned()
    }

    /// Returns: a list of lengths of free segments
    pub fn free_segments(&self) -> Vec<AllocationCapacity> {
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

    pub fn dump_buffers(&self) -> HashMap<BufferId, AllocationCapacity> {
        self.inner
            .lock()
            .unwrap()
            .bookkeeping
            .iter()
            .map(|(k, v)| (k.clone(), v.block_size))
            .collect()
    }

    pub fn capacity(&self) -> usize {
        self.shm_buffer.size()
    }
}

// Allocation and release logic
impl ShmBufferManager {
    pub fn try_reserve(&self, buf_id: &BufferId) -> Option<u64> {
        let mut inner = self.inner.lock().unwrap();
        ShmBufferInner::reserve_inner(&mut inner, buf_id)
    }

    async fn handle_timeout(
        &self,
        rx: oneshot::Receiver<()>,
        buf_id: &BufferId,
        timeout: Option<Duration>,
    ) -> Result<(), ()> {
        // wait for notification or timeout
        if let Some(timeout) = timeout {
            tokio::select! {
                _ = rx => Ok(()),
                _ = tokio::time::sleep(timeout) => {
                    let inner = self.inner.lock().unwrap();
                    let pending_len = inner.pending_reservations.len();
                    tracing::warn!(
                        "Reservation timeout for buffer {:?}, pending reservations: {}, free size = {}",
                        buf_id,
                        pending_len,
                        inner.avail_addrs.values().sum::<u64>()
                    );
                    Err(())
                }
            }
        } else {
            let _ = rx.await;
            Ok(())
        }
    }

    // Returns Err if the number of pending requests exceeds max_pending
    pub async fn reserve_with_max_pending(
        &self,
        buf_id: &BufferId,
        max_pending: usize,
        timeout: Option<Duration>,
    ) -> Result<u64, ()> {
        loop {
            let rx = {
                let (tx, rx) = oneshot::channel();
                let mut inner = self.inner.lock().unwrap();
                if let Some(res) = ShmBufferInner::reserve_inner(&mut inner, buf_id) {
                    return Ok(res);
                }
                if inner.pending_reservations.len() > max_pending {
                    return Err(());
                }
                inner.pending_reservations.push_back(tx);
                rx
            };
            self.handle_timeout(rx, buf_id, timeout).await?;
        }
    }

    pub async fn reserve_with_timeout(
        &self,
        buf_id: &BufferId,
        timeout: Option<Duration>,
    ) -> Result<u64, ()> {
        loop {
            let rx = {
                let (tx, rx) = oneshot::channel();
                let mut inner = self.inner.lock().unwrap();
                if let Some(res) = ShmBufferInner::reserve_inner(&mut inner, buf_id) {
                    return Ok(res);
                }
                inner.pending_reservations.push_back(tx);
                rx
            };
            self.handle_timeout(rx, buf_id, timeout).await?;
        }
    }

    pub async fn reserve(&self, buf_id: &BufferId) -> u64 {
        self.reserve_with_timeout(buf_id, None).await.unwrap()
    }

    pub fn find<F>(&self, func: F) -> Option<(BufferId, AllocationInfo)>
    where
        F: Fn(&BufferId, &AllocationInfo) -> bool,
    {
        let inner = self.inner.lock().unwrap();
        inner
            .bookkeeping
            .iter()
            .find(|(buf_id, info)| func(buf_id, info))
            .map(|(buf_id, info)| (buf_id.clone(), info.clone()))
    }

    pub fn release(&self, buf_id: &BufferId) -> Result<(), ()> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(info) = inner.bookkeeping.remove(buf_id) {
            inner.avail_addrs.insert(info.addr, info.block_size);
            ShmBufferInner::notify_reservation(&mut inner, 1);
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn release_process_residual(&self, pid: i32) {
        let mut inner = self.inner.lock().unwrap();
        let mut cnt = 0;
        {
            let inner_ref = &mut *inner;
            inner_ref.bookkeeping.retain(|buf_id, info| {
                let will_keep = buf_id.pid != pid;
                if !will_keep {
                    inner_ref.avail_addrs.insert(info.addr, info.block_size);
                    cnt += 1;
                }
                will_keep
            });
        }
        ShmBufferInner::notify_reservation(&mut inner, cnt);
    }
}
