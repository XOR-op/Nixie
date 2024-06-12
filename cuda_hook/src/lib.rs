use auto_gmem_ipc::{
    shm::{Shm, ShmGuard, ShmVec},
    sync::IpcMutexGuard,
};
use comm::nofity_shm;
use cudarc::driver::sys::CUstream;
use nix::libc;
use std::{
    ffi::CString,
    sync::{mpsc, Mutex, MutexGuard, OnceLock},
};

mod comm;
mod intercept;
mod snippet;
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
    overflowed_ptr_mapping: Mutex<Vec<(u64, usize)>>,
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

    pub fn new() -> Self {
        let uuid = uuid::Uuid::new_v4();
        let path = format!(
            "/auto_gmem_ipc-{}-{}.shm",
            std::process::id(),
            uuid.to_string().split_at(8).0
        );
        let cpath = CString::new(path.clone()).unwrap();
        eprintln!("Creating shared memory at {}", path);
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
            Shm::init_at(shm_fd, auto_gmem_ipc::shm::Shm::SHM_STRUCT_SIZE)
                .expect("Failed to init shared memory"),
        );
        // close fd but not unlink; daemon will be responsible for unlinking
        unsafe {
            libc::close(shm_fd);
        }
        nofity_shm(path);

        let overflowed_ptr_mapping = Mutex::new(Vec::new());
        Self {
            shm,
            overflowed_ptr_mapping,
        }
    }
}

pub(crate) struct FusedPtrMapping<'a> {
    shm: IpcMutexGuard<'a, ShmVec<(u64, usize), 4096>>,
    overflowed: MutexGuard<'a, Vec<(u64, usize)>>,
}

impl<'a> FusedPtrMapping<'a> {
    pub fn push(&mut self, ptr: (u64, usize)) {
        if self.shm.len() < self.shm.capacity() {
            let _ = self.shm.push(ptr);
        } else {
            self.overflowed.push(ptr);
        }
    }

    pub fn remove(&mut self, idx: usize) -> (u64, usize) {
        if idx < self.shm.len() {
            self.shm.remove(idx)
        } else {
            self.overflowed.remove(idx - self.shm.len())
        }
    }

    pub fn len(&self) -> usize {
        self.shm.len() + self.overflowed.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &(u64, usize)> {
        self.shm.as_slice().iter().chain(self.overflowed.iter())
    }
}
