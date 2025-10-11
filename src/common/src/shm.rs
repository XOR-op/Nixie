use core::{ffi::c_int, pin::Pin};
use std::num::NonZeroU32;

use nix::libc;

use crate::{HANDLE_NUM, MAX_GPUS, sync::IpcMutex};

/// There should be no side effects of the drop.
pub(crate) trait ReInitializable {
    unsafe fn reinit_from_uninited(&mut self);
}

pub struct AllocationTable {
    // usize
    pub entry: ShmVec<AllocationEntry, 8192>,
    pub handle_list: HandleList,
}

pub struct HandleList {
    // NonZeroU32
    handles: [PhysicalMemoryHandle; HANDLE_NUM],
    freelist_head: Option<NonZeroU32>,
}

impl AllocationTable {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            entry: ShmVec::new(),
            handle_list: HandleList::new(),
        }
    }
}

impl ReInitializable for AllocationTable {
    unsafe fn reinit_from_uninited(&mut self) {
        unsafe {
            self.entry.reinit();
            self.handle_list.reinit_from_uninited();
        }
    }
}
impl HandleList {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let mut handles = [PhysicalMemoryHandle {
            addr: 0,
            size: 0,
            cu_handle: None,
            next_handle_idx: None,
            on_gpu: false,
            valid: false,
        }; HANDLE_NUM];
        #[allow(clippy::needless_range_loop)]
        for i in 1..HANDLE_NUM {
            handles[i].next_handle_idx = NonZeroU32::new(i as u32 + 1);
        }
        Self {
            handles,
            freelist_head: NonZeroU32::new(1),
        }
    }

    pub fn allocate_handle(&mut self, addr: u64, size: usize) -> Option<NonZeroU32> {
        if let Some(idx) = self.freelist_head {
            let handle = &mut self.handles[idx.get() as usize];
            self.freelist_head = handle.next_handle_idx;
            handle.next_handle_idx = None;
            handle.addr = addr;
            handle.size = size;
            handle.on_gpu = false;
            handle.valid = true;
            Some(idx)
        } else {
            None
        }
    }

    pub fn free_handle(&mut self, idx: NonZeroU32) {
        let handle = &mut self.handles[idx.get() as usize];
        handle.addr = 0;
        handle.size = 0;
        handle.next_handle_idx = self.freelist_head;
        handle.on_gpu = false;
        handle.valid = false;
        self.freelist_head = Some(idx);
    }

    pub fn get_handle(&self, idx: NonZeroU32) -> Option<&PhysicalMemoryHandle> {
        if idx.get() as usize >= HANDLE_NUM {
            return None;
        }
        Some(&self.handles[idx.get() as usize])
    }

    pub fn get_handle_mut(&mut self, idx: NonZeroU32) -> Option<&mut PhysicalMemoryHandle> {
        if idx.get() as usize >= HANDLE_NUM {
            return None;
        }
        Some(&mut self.handles[idx.get() as usize])
    }

    // return (on_gpu, not_on_gpu)
    pub fn memory_usage(&self, handle_idx: NonZeroU32) -> (usize, usize) {
        let mut on_gpu = 0;
        let mut not_on_gpu = 0;
        let mut cur_index = Some(handle_idx);
        while let Some(index) = cur_index {
            let handle = self.get_handle(index).unwrap();
            if handle.on_gpu {
                on_gpu += handle.size;
            } else {
                not_on_gpu += handle.size;
            }
            cur_index = handle.next_handle_idx;
        }
        (on_gpu, not_on_gpu)
    }
}

impl ReInitializable for HandleList {
    unsafe fn reinit_from_uninited(&mut self) {
        for handle in self.handles.iter_mut() {
            handle.addr = 0;
            handle.size = 0;
            handle.cu_handle = None;
            handle.next_handle_idx = None;
            handle.on_gpu = false;
            handle.valid = false;
        }
        for i in 1..HANDLE_NUM {
            self.handles[i].next_handle_idx = NonZeroU32::new(i as u32 + 1);
        }
        self.freelist_head = NonZeroU32::new(1);
    }
}

pub struct Shm {
    all_len: u32,
    pub alloc_tables: [IpcMutex<AllocationTable>; MAX_GPUS], // Process local device ID
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PhysicalMemoryHandle {
    pub addr: u64,
    pub size: usize,
    pub cu_handle: Option<cudarc::driver::sys::CUmemGenericAllocationHandle>,
    pub next_handle_idx: Option<NonZeroU32>,
    pub on_gpu: bool,
    pub valid: bool,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AllocationEntry {
    pub addr: u64,
    pub len: usize,
    pub handle_idx: NonZeroU32,
}

impl Default for AllocationEntry {
    fn default() -> Self {
        Self {
            addr: 0,
            len: 0,
            handle_idx: NonZeroU32::new(u32::MAX).unwrap(),
        }
    }
}

impl Shm {
    pub const SHM_STRUCT_SIZE: u32 = core::mem::size_of::<Self>() as u32;

    pub fn init_at(shm_fd: i32, len: u32) -> Result<Pin<&'static mut Self>, c_int> {
        if len < core::mem::size_of::<Self>() as u32 {
            return Err(libc::EINVAL);
        }
        unsafe {
            // extend shmem
            let errno = libc::ftruncate(shm_fd, len as i64);
            if errno != 0 {
                return Err(errno);
            }
            // create mmap
            let ptr = libc::mmap(
                core::ptr::null_mut(),
                len as usize,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                shm_fd,
                0,
            );
            if ptr == libc::MAP_FAILED {
                return Err(nix::errno::Errno::last_raw());
            }
            let typed_ptr = ptr as *mut Self;
            let mut_ref = &mut *typed_ptr;
            mut_ref.all_len = len;
            for table in mut_ref.alloc_tables.iter_mut() {
                table.reinit_from_uninited();
            }
            Ok(Pin::new(mut_ref))
        }
    }

    /// # Safety
    ///
    /// This involves mmap
    pub unsafe fn open_copy_at(shm_fd: i32, len: u32) -> Result<Pin<&'static mut Self>, c_int> {
        if len < core::mem::size_of::<Self>() as u32 {
            return Err(libc::EINVAL);
        }
        // create mmap
        let ptr = unsafe {
            libc::mmap(
                core::ptr::null_mut(),
                len as usize,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                shm_fd,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            Err(nix::errno::Errno::last_raw())
        } else {
            let r = unsafe { Pin::new(&mut *(ptr as *mut Self)) };
            for alloc_table in r.alloc_tables.iter() {
                // Increase ref count for each allocation table
                alloc_table.increase_ref_count();
            }
            Ok(r)
        }
    }

    unsafe fn close(&mut self) {
        unsafe {
            for alloc_table in self.alloc_tables.iter_mut() {
                // Decrease ref count for each allocation table
                alloc_table.close();
            }
            libc::munmap(
                self as *const Self as *mut libc::c_void,
                self.all_len as usize,
            );
        }
    }
}

pub struct ShmGuard {
    pub inner: Pin<&'static mut Shm>,
}

impl ShmGuard {
    pub fn new(shm: Pin<&'static mut Shm>) -> Self {
        Self { inner: shm }
    }
}

impl Drop for ShmGuard {
    fn drop(&mut self) {
        unsafe {
            self.inner.close();
        }
    }
}

pub struct ShmVec<T: Default, const N: usize> {
    len: u32,
    data: [T; N],
}

impl<T: Default, const N: usize> ShmVec<T, N> {
    pub fn new() -> Self {
        Self {
            len: 0,
            data: unsafe { core::mem::zeroed() },
        }
    }

    pub unsafe fn reinit(&mut self) {
        self.len = 0;
        self.data = unsafe { core::mem::zeroed() };
    }

    #[allow(clippy::result_unit_err)]
    pub fn push(&mut self, val: T) -> Result<usize, ()> {
        if self.len as usize >= N {
            return Err(());
        }
        let new_idx = self.len as usize;
        self.data[new_idx] = val;
        self.len += 1;
        Ok(new_idx)
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        Some(core::mem::take(&mut self.data[self.len as usize]))
    }

    pub fn remove(&mut self, idx: usize) -> T {
        if idx >= self.len as usize {
            panic!("index out of bounds")
        }
        let val = core::mem::take(&mut self.data[idx]);
        self.len -= 1;
        for i in idx..self.len as usize {
            self.data[i] = core::mem::take(&mut self.data[i + 1]);
        }
        val
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub const fn capacity(&self) -> usize {
        N
    }

    pub fn as_slice(&self) -> &[T] {
        &self.data[..self.len as usize]
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.data[..self.len as usize]
    }

    pub fn at(&self, idx: usize) -> Option<&T> {
        self.as_slice().get(idx)
    }

    pub fn at_mut(&mut self, idx: usize) -> Option<&mut T> {
        self.as_mut_slice().get_mut(idx)
    }

    pub fn iter(&self) -> core::slice::Iter<'_, T> {
        self.as_slice().iter()
    }

    pub fn iter_mut(&mut self) -> core::slice::IterMut<'_, T> {
        self.as_mut_slice().iter_mut()
    }
}

impl<T: Default, const N: usize> Default for ShmVec<T, N> {
    fn default() -> Self {
        Self::new()
    }
}
