use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, atomic::AtomicBool},
};

use nihil_common::{MAX_ALLOCATION_SIZE, MIN_ALLOCATION_SIZE, shm_buffer::ShmBuffer};
use tokio::sync::oneshot;

use crate::runtime::migration::{AllocationCapacity, AllocationCount, Offset};

use super::BufferId;

/*
 * Warning: This is not safe; FIXME
 */
pub struct ShmBufferManager {
    shm_buffer: ShmBuffer,
    inner: Mutex<ShmBufferInner>,
}

#[derive(Clone, Debug)]
pub struct ShmBlock {
    pub offset: Offset,
    pub data_size: u32,
}

struct BookKeepingEntry {
    blocks: Arc<[ShmBlock]>,
    incomplete: Arc<AtomicBool>,
}

struct ShmBufferInner {
    bookkeeping: HashMap<BufferId, BookKeepingEntry>,
    pid_counter: HashMap<i32, usize>,
    avail_addrs: Vec<Offset>,
    pending_reservations: VecDeque<oneshot::Sender<()>>, // TODO: deprecate
}

impl ShmBufferInner {
    fn reserve_inner(
        inner: &mut std::sync::MutexGuard<'_, Self>,
        buf_id: &BufferId,
        incomplete: bool,
    ) -> Option<(Arc<[ShmBlock]>, Arc<AtomicBool>)> {
        let required_len = buf_id.get_allocation_count().0 as usize;
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
        let in_transfer_flag = Arc::new(AtomicBool::new(incomplete));
        inner.bookkeeping.insert(
            buf_id.clone(),
            BookKeepingEntry {
                blocks: blocks.clone(),
                incomplete: in_transfer_flag.clone(),
            },
        );
        inner
            .pid_counter
            .entry(buf_id.pid)
            .and_modify(|c| *c += required_len)
            .or_insert(required_len);
        Some((blocks, in_transfer_flag))
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
                pid_counter: HashMap::new(),
                avail_addrs,
                pending_reservations: VecDeque::new(),
            }),
        })
    }

    pub fn get_buffer(&self, buf_id: &BufferId) -> Option<Arc<[ShmBlock]>> {
        self.inner
            .lock()
            .unwrap()
            .bookkeeping
            .get(buf_id)
            .map(|entry| entry.blocks.clone())
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
                    AllocationCapacity((v.blocks.len() * MIN_ALLOCATION_SIZE) as u32),
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
    pub fn try_reserve(&'_ self, buf_id: &BufferId) -> Option<ShmBufferTransferGuard<'_>> {
        let mut inner = self.inner.lock().unwrap();
        // we reserve 2*128MB space for migration && no single process should occupy all shm space
        if let Some(count) = inner.pid_counter.get(&buf_id.pid)
            && (*count + buf_id.get_allocation_count().0 as usize) * MIN_ALLOCATION_SIZE
                + 2 * MAX_ALLOCATION_SIZE
                >= self.shm_buffer.size()
        {
            return None;
        }

        let val = ShmBufferInner::reserve_inner(&mut inner, buf_id, true);
        val.map(|(blocks, incomplete_flag)| ShmBufferTransferGuard {
            shm_buffer: self,
            blocks,
            buffer_id: buf_id.clone(),
            incomplete_flag: Some(incomplete_flag),
        })
    }

    pub fn find<F>(&self, func: F) -> Option<(BufferId, Arc<[ShmBlock]>)>
    where
        F: Fn(&BufferId, &[ShmBlock], &AtomicBool) -> bool,
    {
        let inner = self.inner.lock().unwrap();
        inner
            .bookkeeping
            .iter()
            .find(|(buf_id, info)| func(buf_id, &info.blocks, &info.incomplete))
            .map(|(buf_id, info)| (buf_id.clone(), info.blocks.clone()))
    }

    pub fn release(&self, buf_id: &BufferId) -> Result<(), ()> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(blocks) = inner.bookkeeping.remove(buf_id) {
            let count = inner.pid_counter.get_mut(&buf_id.pid).unwrap();
            *count = count.saturating_sub(buf_id.get_allocation_count().0 as usize);
            if *count == 0 {
                inner.pid_counter.remove(&buf_id.pid);
            }

            for blk in blocks.blocks.iter() {
                inner.avail_addrs.push(blk.offset);
            }
            ShmBufferInner::notify_reservation(&mut inner, blocks.blocks.len());
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn batch_release(&self, buf_ids: &[BufferId]) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let mut release_count = 0;
        let mut cnt = 0;
        for buf_id in buf_ids.iter() {
            if let Some(blocks) = inner.bookkeeping.remove(buf_id) {
                let count = inner.pid_counter.get_mut(&buf_id.pid).unwrap();
                *count = count.saturating_sub(buf_id.get_allocation_count().0 as usize);
                if *count == 0 {
                    inner.pid_counter.remove(&buf_id.pid);
                }

                for blk in blocks.blocks.iter() {
                    inner.avail_addrs.push(blk.offset);
                }
                release_count += 1;
                cnt += blocks.blocks.len();
            }
        }
        ShmBufferInner::notify_reservation(&mut inner, cnt);
        release_count
    }

    pub fn release_process_residual(&self, pid: i32) {
        let mut inner = self.inner.lock().unwrap();
        let mut cnt = 0;
        {
            let inner_ref = &mut *inner;
            inner_ref.bookkeeping.retain(|buf_id, blocks| {
                let will_keep = buf_id.pid != pid;
                if !will_keep {
                    for blk in blocks.blocks.iter() {
                        inner_ref.avail_addrs.push(blk.offset);
                        cnt += 1;
                    }
                }
                will_keep
            });
            inner_ref.pid_counter.remove(&pid);
        }
        ShmBufferInner::notify_reservation(&mut inner, cnt);
    }
}

pub struct ShmBufferTransferGuard<'a> {
    shm_buffer: &'a ShmBufferManager,
    blocks: Arc<[ShmBlock]>,
    buffer_id: BufferId,
    incomplete_flag: Option<Arc<AtomicBool>>,
}

impl<'a> ShmBufferTransferGuard<'a> {
    pub fn blocks(&self) -> &Arc<[ShmBlock]> {
        &self.blocks
    }

    pub fn buffer_id(&self) -> &BufferId {
        &self.buffer_id
    }
}

impl Drop for ShmBufferTransferGuard<'_> {
    fn drop(&mut self) {
        if let Some(flag) = &self.incomplete_flag {
            flag.store(false, std::sync::atomic::Ordering::Relaxed);
        }
    }
}
