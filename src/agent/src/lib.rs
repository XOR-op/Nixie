use cudarc::driver::sys::{cudaError_enum, lib as cuda_lib, CUstream};
use nihilipc::{
    shm::{AllocationEntry, Shm, ShmGuard, ShmVec},
    sync::IpcMutexGuard,
};
use nix::libc;
use std::{
    ffi::CString,
    sync::{mpsc, Mutex, MutexGuard, OnceLock},
};
use utils::set_device;

mod comm;
mod env_config;
mod intercept;
mod intercept_launch;
mod memory;
mod schedule;
mod utils;

/*
 * Expected workflow:
 * 1. Attached process opened /dev/nvidia-uvm, we record the fd
 *  * 1.1. Some applications may open and close /dev/nvidia-uvm multiple times; we delay to use it until they truly invoke CUDA APIs
 * 2. Process called cudaMalloc, we use the latest uvmfd
 */

struct CuStreamWrapper(CUstream);
unsafe impl Send for CuStreamWrapper {}
unsafe impl Sync for CuStreamWrapper {}

/// For some reasons, cudaMemPrefetchAsync exhibits blocking behavior.
/// Use a separate thread to prefetch.
pub(crate) static PREFETCH_REQ_QUEUE: OnceLock<mpsc::Sender<u64>> = OnceLock::new();

/// All streams used for prefetching

pub(crate) fn stream_get_or_init() -> &'static Vec<CuStreamWrapper> {
    static STREAM_VEC: OnceLock<Vec<CuStreamWrapper>> = OnceLock::new();
    STREAM_VEC.get_or_init(|| {
        // TODO: fix this buggy implementation;
        set_device(0);
        let mut vec = Vec::new();
        for _ in 0..8 {
            let mut stream = std::ptr::null_mut();
            let res = unsafe {
                cuda_lib().cuStreamCreate(
                    &mut stream,
                    cudarc::driver::sys::CUstream_flags_enum::CU_STREAM_NON_BLOCKING as u32,
                )
            };
            if res != cudaError_enum::CUDA_SUCCESS {
                panic!("Failed to create stream: {:?}", res);
            }
            vec.push(CuStreamWrapper(stream));
        }
        vec
    })
}

pub(crate) static GENERIC_DATA: OnceLock<GenericData> = OnceLock::new();

pub(crate) struct GenericData {
    shm: ShmGuard,
    overflowed_ptr_mapping: Mutex<Vec<AllocationEntry>>,
}

impl GenericData {
    /// Global mapping of device pointers and their sizes
    pub fn lock_ptr_mapping(&self) -> FusedPtrMapping<'_> {
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

impl FusedPtrMapping<'_> {
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
