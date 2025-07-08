use cudarc::driver::sys::lib as cuda_lib;
use nihil_common::shm_buffer::ShmBuffer;

use crate::comm::init::{init_comm, COMM};
use crate::{check_cu_err, set_device, shm_buf, GenericData};

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
    static FIRST_TIME: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    if FIRST_TIME.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
        let lib = unsafe { cuda_lib() };
        let mut dev_cnt = 0;
        let res = unsafe { lib.cuDeviceGetCount(&mut dev_cnt) };
        if res == cudarc::driver::sys::cudaError_enum::CUDA_ERROR_NOT_INITIALIZED {
            check_cu_err!(unsafe { lib.cuInit(0) }, "initialize CUDA");
            set_device(0);
        }
    }
}

// should only
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
