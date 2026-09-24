use cudarc::driver::sys::cudaError_enum;
use nixie_common::ProcessLocalDeviceId;
use nixie_common::shm_buffer::ShmBuffer;

use crate::comm::init::{COMM, init_comm};
use crate::memory::{MEMORY_MIGRATION_CTL, init_memory_migration_ctl};
use crate::utils::{CudaContextGuard, get_device};
use crate::{GenericData, check_cu_err, cu_api, set_device, shm_buf, warn_eprintln};

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
        let error = unsafe { cu_api::cuCtxGetCurrent(&mut cur_device) };
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
    let res = unsafe { cu_api::cuDeviceGetCount(&mut dev_cnt) };
    if res == cudaError_enum::CUDA_ERROR_NOT_INITIALIZED {
        check_cu_err!(unsafe { cu_api::cuInit(0) }, "initialize CUDA");
        set_device(0);
        crate::debug_eprintln!("CUDA initialized successfully");
    } else if res == cudaError_enum::CUDA_SUCCESS {
        crate::debug_eprintln!("CUDA already initialized");
        // Establish a context before temporarily switching devices to register
        // the shared buffer below.
        let mut cur_device = std::ptr::null_mut();
        check_cu_err!(
            unsafe { cu_api::cuCtxGetCurrent(&mut cur_device) },
            "get current CUDA context"
        );
        if cur_device.is_null() {
            set_device(0);
        }
    } else {
        check_cu_err!(res, "CUDA initialization test failed");
    }
    let _guard = CudaContextGuard::new();
    set_device(0);
    init_mapped_gpu_memory();
}

pub(crate) fn init_memory_for_device(device_id: i32) {
    let _guard = CudaContextGuard::new();
    // Reserve streams before allocating migratable memory, while there is still
    // space for them. Memory queries must not create these resources: applications
    // can reset an unused device after probing it, invalidating its streams.
    MEMORY_MIGRATION_CTL
        .get_or_init(init_memory_migration_ctl)
        .init_device(device_id);

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
        "/nixie_ipc-{}-{}.shm",
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

// A portable registration is shared by all devices, but belongs to the context
// that registered it. Store the context address as an identity, not a live handle.
static MAPPED_GPU_MEMORY: std::sync::Mutex<Option<usize>> = std::sync::Mutex::new(None);

pub(crate) fn init_mapped_gpu_memory() {
    let mut registered_on = MAPPED_GPU_MEMORY.lock().unwrap();
    if registered_on.is_some() {
        return;
    }
    let device_id = get_device();
    let mut context = std::ptr::null_mut();
    check_cu_err!(
        unsafe { cu_api::cuCtxGetCurrent(&mut context) },
        "get host registration context"
    );
    unsafe {
        let global_buf = crate::global_shm_buffer();
        let shm_buf_ptr = global_buf.at_offset(0, 1).unwrap();
        let size = global_buf.size();
        let res = cu_api::cuMemHostRegister_v2(
            shm_buf_ptr as *mut nix::libc::c_void,
            size,
            cudarc::driver::sys::CU_MEMHOSTALLOC_PORTABLE,
        );
        check_cu_err!(res, "Failed to register SHM buffer with CUDA");
        if res == cudaError_enum::CUDA_SUCCESS {
            *registered_on = Some(context as usize);
            crate::debug_eprintln!("Registered shared host buffer on device {}", device_id);
        }
    }
}

pub(crate) fn reset_cuda_device(reset: impl FnOnce() -> cudaError_enum) -> cudaError_enum {
    let reset = || {
        let mut registered_on = MAPPED_GPU_MEMORY.lock().unwrap();
        let mut context = std::ptr::null_mut();
        let context_result = unsafe { cu_api::cuCtxGetCurrent(&mut context) };
        let resets_registration = context_result == cudaError_enum::CUDA_SUCCESS
            && !context.is_null()
            && *registered_on == Some(context as usize);
        let res = reset();
        if res == cudaError_enum::CUDA_SUCCESS && resets_registration {
            *registered_on = None;
            crate::debug_eprintln!("Device reset invalidated the shared host registration");
        }
        res
    };
    if let Some(control) = MEMORY_MIGRATION_CTL.get() {
        control.with_device_reset(reset)
    } else {
        reset()
    }
}
