use cudarc::driver::sys::CUstream;
use nihilipc::{
    shm::{AllocationEntry, Shm, ShmGuard, ShmVec},
    sync::IpcMutexGuard,
};
use nix::libc;
use std::{
    ffi::CString,
    sync::{mpsc, Mutex, MutexGuard, OnceLock},
};

mod comm;
mod intercept;
mod intercept_launch;
mod memory;
mod schedule;
mod utils;

struct CuStreamWrapper(CUstream);
unsafe impl Send for CuStreamWrapper {}
unsafe impl Sync for CuStreamWrapper {}

/// For some reasons, cudaMemPrefetchAsync exhibits blocking behavior.
/// Use a separate thread to prefetch.
pub(crate) static PREFETCH_REQ_QUEUE: OnceLock<mpsc::Sender<u64>> = OnceLock::new();

/// All streams used for prefetching
pub(crate) static STREAM_VEC: OnceLock<Vec<CuStreamWrapper>> = OnceLock::new();

pub(crate) static GENERIC_DATA: OnceLock<GenericData> = OnceLock::new();

pub(crate) struct GenericData {
    shm: ShmGuard,
    overflowed_ptr_mapping: Mutex<Vec<AllocationEntry>>,
}

impl GenericData {
    /// Global mapping of device pointers and their sizes
    pub fn lock_ptr_mapping<'a>(&'a self) -> FusedPtrMapping<'a> {
        // We always lock shared memory first
        let shm_guard = self.shm.inner.ptr_mapping.lock();
        let ptr_mapping_guard = self.overflowed_ptr_mapping.lock().unwrap();
        FusedPtrMapping {
            shm: shm_guard,
            overflowed: ptr_mapping_guard,
        }
    }

    pub fn new(path: &str) -> Self {
        let cpath = CString::new(path).unwrap();
        info_eprintln!("Creating shared memory at {}", path);
        let shm_fd = unsafe {
            libc::shm_open(
                cpath.as_ptr(),
                libc::O_RDWR | libc::O_CREAT,
                libc::S_IRUSR | libc::S_IWUSR,
            )
        };
        if shm_fd < 0 {
            panic!(
                "Failed to open shared memory: {}",
                nix::errno::Errno::last()
            );
        }
        // create mmap
        let shm = ShmGuard::new(
            Shm::init_at(shm_fd, nihilipc::shm::Shm::SHM_STRUCT_SIZE)
                .expect("Failed to init shared memory"),
        );
        // close fd but not unlink; daemon will be responsible for unlinking
        unsafe {
            libc::close(shm_fd);
        }

        let overflowed_ptr_mapping = Mutex::new(Vec::new());
        Self {
            shm,
            overflowed_ptr_mapping,
        }
    }
}

pub(crate) struct FusedPtrMapping<'a> {
    shm: IpcMutexGuard<'a, ShmVec<AllocationEntry, 4096>>,
    overflowed: MutexGuard<'a, Vec<AllocationEntry>>,
}

impl<'a> FusedPtrMapping<'a> {
    pub fn push(&mut self, ptr: AllocationEntry) -> Option<usize> {
        if self.shm.len() < self.shm.capacity() {
            self.shm.push(ptr).ok()
        } else {
            self.overflowed.push(ptr);
            None
        }
    }

    pub fn remove(&mut self, idx: usize) -> AllocationEntry {
        if idx < self.shm.len() {
            self.shm.remove(idx)
        } else {
            self.overflowed.remove(idx - self.shm.len())
        }
    }

    pub fn len(&self) -> usize {
        self.shm.len() + self.overflowed.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &AllocationEntry> {
        self.shm.as_slice().iter().chain(self.overflowed.iter())
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut AllocationEntry> {
        self.shm
            .as_mut_slice()
            .iter_mut()
            .chain(self.overflowed.iter_mut())
    }
}
