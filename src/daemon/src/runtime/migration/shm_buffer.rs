use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use nihil_common::{MAX_ALLOCATION_SIZE, MIN_ALLOCATION_SIZE, shm_buffer::ShmBuffer};
use tokio::sync::oneshot;

use crate::runtime::migration::{AllocationCapacity, AllocationCount, Offset};

use super::BufferId;

pub struct ShmBufferManager {
    shm_buffer: ShmBuffer,
    inner: Mutex<ShmBufferInner>,
}

#[derive(Clone, Debug)]
pub struct ShmBlock {
    pub offset: Offset,
    pub data_size: u32,
}

struct ShmBufferInner {
    bookkeeping: HashMap<BufferId, Arc<[ShmBlock]>>,
    avail_addrs: Vec<Offset>,
    pending_reservations: VecDeque<oneshot::Sender<()>>,
}

impl ShmBufferInner {
    fn reserve_inner(
        inner: &mut std::sync::MutexGuard<'_, Self>,
        buf_id: &BufferId,
    ) -> Option<Arc<[ShmBlock]>> {
        let required_len = buf_id.size.div_ceil(MIN_ALLOCATION_SIZE as u32) as usize;
        if inner.avail_addrs.len() < required_len {
            return None;
        }
        tracing::trace!("ShmBuffer: available length = {}", inner.avail_addrs.len());
        let blocks: Arc<[ShmBlock]> = {
            let mut accumulated_size = 0;
            let mut blocks = Box::new_uninit_slice(required_len);
            for idx in 0..required_len {
                let offset = inner.avail_addrs.pop().unwrap();
                let block_size = if accumulated_size + MIN_ALLOCATION_SIZE as u32 > buf_id.size {
                    buf_id.size - accumulated_size
                } else {
                    MIN_ALLOCATION_SIZE as u32
                };
                accumulated_size += block_size;
                blocks[idx].write(ShmBlock {
                    offset,
                    data_size: block_size,
                });
            }
            let inited = unsafe { blocks.assume_init() };
            Arc::from(inited)
        };
        inner.bookkeeping.insert(buf_id.clone(), blocks.clone());
        Some(blocks)
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
        let mut avail_addrs = Vec::with_capacity(shm_size / MIN_ALLOCATION_SIZE + 1);
        let mut offset = 0;
        while offset < shm_size as u64 {
            let size = MIN_ALLOCATION_SIZE as u64;
            avail_addrs.push(Offset(offset));
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

    pub fn get_buffer(&self, buf_id: &BufferId) -> Option<Arc<[ShmBlock]>> {
        self.inner.lock().unwrap().bookkeeping.get(buf_id).cloned()
    }

    /// Returns: length of free segments
    pub fn free_blocks_count(&self) -> AllocationCount {
        AllocationCount(self.inner.lock().unwrap().avail_addrs.len() as u32)
    }

    pub unsafe fn at_offset(&self, offset: u64, size: usize) -> Option<*mut u8> {
        unsafe { self.shm_buffer.at_offset(offset, size) }
    }

    // buffer_id -> buffer allocation length in bytes
    pub fn dump_buffers(&self) -> HashMap<BufferId, AllocationCapacity> {
        self.inner
            .lock()
            .unwrap()
            .bookkeeping
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
        self.shm_buffer.size()
    }

    pub fn is_full(&self) -> bool {
        self.inner.lock().unwrap().avail_addrs.is_empty()
    }
}

// Allocation and release logic
impl ShmBufferManager {
    pub fn try_reserve(&self, buf_id: &BufferId) -> Option<Arc<[ShmBlock]>> {
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
                        "Reservation timeout ({:?}) for buffer {:?}, pending reservations: {}, free chunk num = {}",
                        timeout,
                        buf_id,
                        pending_len,
                        inner.avail_addrs.len()
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
    ) -> Result<Arc<[ShmBlock]>, ()> {
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
    ) -> Result<Arc<[ShmBlock]>, ()> {
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

    pub async fn reserve(&self, buf_id: &BufferId) -> Arc<[ShmBlock]> {
        self.reserve_with_timeout(buf_id, None).await.unwrap()
    }

    pub fn find<F>(&self, func: F) -> Option<(BufferId, Arc<[ShmBlock]>)>
    where
        F: Fn(&BufferId, &[ShmBlock]) -> bool,
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
        if let Some(blocks) = inner.bookkeeping.remove(buf_id) {
            for blk in blocks.iter() {
                inner.avail_addrs.push(blk.offset);
            }
            ShmBufferInner::notify_reservation(&mut inner, todo!());
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
            inner_ref.bookkeeping.retain(|buf_id, blocks| {
                let will_keep = buf_id.pid != pid;
                if !will_keep {
                    for blk in blocks.iter() {
                        inner_ref.avail_addrs.push(blk.offset);
                        cnt += 1;
                    }
                }
                will_keep
            });
        }
        ShmBufferInner::notify_reservation(&mut inner, todo!());
    }
}
