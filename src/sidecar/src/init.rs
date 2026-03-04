use cudarc::driver::sys::cudaError_enum;
use nihil_common::ProcessLocalDeviceId;
use nihil_common::shm_buffer::ShmBuffer;

use crate::comm::init::{COMM, init_comm};
use crate::memory::{MEMORY_MIGRATION_CTL, init_memory_migration_ctl};
use crate::{GenericData, check_cu_err, set_device, shm_buf, warn_eprintln};

pub(crate) fn should_have_initialized() -> GenericData {
    panic!("GENERIC_DATA should already be initialized by init_comm");
}

// should be called only once, before any other code that uses SHM_BUFFER
pub(crate) fn init_shm_buffer(path: &str, size: usize) {
    if shm_buf::SHM_BUFFER
        .set(ShmBuffer::new(path, size, false).expect("Failed to create SHM buffer"))
        .is_err()
    {
        panic!("SHM_BUFFER is already initialized");
    }
}

pub(crate) fn init_cuda_env() {
    static FIRST_TIME: std::sync::Mutex<bool> = std::sync::Mutex::new(false);
    let mut guard = FIRST_TIME.lock().unwrap();
    if *guard {
        let mut cur_device = std::ptr::null_mut();
        let error = unsafe { cudarc::driver::sys::cuCtxGetCurrent(&mut cur_device) };
        if error == cudaError_enum::CUDA_SUCCESS {
            if cur_device.is_null() {
                set_device(0);
            }
        } else {
            warn_eprintln!("CUDA was partially initialized before, but no context is current");
        }
        return; // already initialized
    }
    *guard = true;
    let mut dev_cnt = 0;
    let res = unsafe { cudarc::driver::sys::cuDeviceGetCount(&mut dev_cnt) };
    if res == cudaError_enum::CUDA_ERROR_NOT_INITIALIZED {
        check_cu_err!(unsafe { cudarc::driver::sys::cuInit(0) }, "initialize CUDA");
        set_device(0);
        crate::debug_eprintln!("CUDA initialized successfully");
    } else if res == cudaError_enum::CUDA_SUCCESS {
        crate::debug_eprintln!("CUDA already initialized");
    } else {
        check_cu_err!(res, "CUDA initialization test failed");
    }
    // we init migration control in advance, to avoid OOM in stream create
    // when we need to migrate memory back to CPU
    MEMORY_MIGRATION_CTL.get_or_init(init_memory_migration_ctl);
    init_mapped_gpu_memory();
}

pub(crate) fn init_all_entrypoint() {
    static FIRST_TIME: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    if FIRST_TIME.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
        COMM.get_or_init(init_comm);
    }
}

pub(crate) fn init_generic_data() -> (GenericData, String) {
    // create ptr mapping
    let uuid = uuid::Uuid::new_v4();
    let shm_path = format!(
        "/nihilphase_ipc-{}-{}.shm",
        std::process::id(),
        uuid.to_string().split_at(8).0
    );
    let result = GenericData::new(&shm_path);
    (result, shm_path)
}

pub(crate) fn init_max_available_vram_size(sizes: &[(ProcessLocalDeviceId, u64)]) {
    crate::memory::set_max_allocation_size(
        sizes
            .iter()
            .map(|(dev_id, size)| (dev_id.0, *size))
            .collect(),
    );
}

fn init_mapped_gpu_memory() {
    set_device(0);
    unsafe {
        let global_buf = crate::global_shm_buffer();
        let shm_buf_ptr = global_buf.at_offset(0, 1).unwrap();
        let size = global_buf.size();
        check_cu_err!(
            cudarc::driver::sys::cuMemHostRegister_v2(
                shm_buf_ptr as *mut nix::libc::c_void,
                size,
                cudarc::driver::sys::CU_MEMHOSTALLOC_PORTABLE,
            ),
            "Failed to register SHM buffer with CUDA"
        );
    }
}
