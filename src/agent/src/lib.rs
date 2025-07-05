use cudarc::driver::sys::{cudaError_enum, lib as cuda_lib, CUstream};
use nihil_common::{
    shm::{AllocationTable, Shm, ShmGuard},
    sync::IpcMutexGuard,
};
use nix::libc;
use std::{ffi::CString, sync::OnceLock};
use utils::set_device;

mod comm;
mod env_config;
mod init;
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

impl CuStreamWrapper {
    pub fn new(device: i32) -> Self {
        set_device(device);
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
        Self(stream)
    }
}

/// All streams used for prefetching
pub(crate) fn stream_get_or_init() -> &'static Vec<CuStreamWrapper> {
    static STREAM_VEC: OnceLock<Vec<CuStreamWrapper>> = OnceLock::new();
    STREAM_VEC.get_or_init(|| {
        // TODO: fix this buggy implementation;
        set_device(0);
        let mut vec = Vec::new();
        for _ in 0..8 {
            let stream = CuStreamWrapper::new(0);
            vec.push(stream);
        }
        vec
    })
}

pub(crate) static GENERIC_DATA: OnceLock<GenericData> = OnceLock::new();

pub(crate) struct GenericData {
    shm: ShmGuard,
}

impl GenericData {
    /// Global mapping of device pointers and their sizes
    pub fn lock(&self, nth_table: usize) -> IpcMutexGuard<'_, AllocationTable> {
        // We always lock shared memory first
        self.shm.inner.alloc_tables[nth_table].lock()
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
            Shm::init_at(shm_fd, nihil_common::shm::Shm::SHM_STRUCT_SIZE)
                .expect("Failed to init shared memory"),
        );
        // close fd but not unlink; daemon will be responsible for unlinking
        crate::intercept::real_libc_close(shm_fd);

        Self { shm }
    }
}
