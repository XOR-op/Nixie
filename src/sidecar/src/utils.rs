use cudarc::driver::sys::{CUdevice, cudaError_enum};
use nixie_common::{CUDA_PROCESS_RESERVATION_SIZE, MemoryRequest, ProcessLocalDeviceId};

use crate::{
    cu_api,
    env_config::sidecar_config,
    schedule::{LaunchType, SCHED_CTL},
};

#[inline(always)]
pub(crate) fn should_log(level: u8) -> bool {
    sidecar_config().log_level >= level
}

#[macro_export]
macro_rules! debug_eprintln {
    ($($arg:tt)*) => {
        if $crate::utils::should_log(3) {
            eprintln!("{} {}", colored::Colorize::blue("NIXIE-DEBUG"), format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! info_eprintln {
    ($($arg:tt)*) => {
        if $crate::utils::should_log(2) {
            eprintln!("{} {}", colored::Colorize::green("NIXIE-INFO"), format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! warn_eprintln {
    ($($arg:tt)*) => {
        if $crate::utils::should_log(1) {
            eprintln!("{} {}", colored::Colorize::yellow("NIXIE-WARN"), format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! check_cu_err {
    ($res:expr, $msg:literal) => {
        if $res != cudarc::driver::sys::cudaError_enum::CUDA_SUCCESS {
            $crate::warn_eprintln!("CUDA error from {}: {:?}", $msg, $res);
        }
    };
}

pub(crate) fn set_device(dev: i32) {
    let mut cu_ctx = std::ptr::null_mut();
    let mut res = unsafe { cu_api::cuDevicePrimaryCtxRetain(&mut cu_ctx, dev) };
    if res == cudaError_enum::CUDA_ERROR_OUT_OF_MEMORY {
        crate::debug_eprintln!("Allocating memory for CUDA context");
        SCHED_CTL.pause_then_require_memory(
            LaunchType::Malloc,
            Box::new(MemoryRequest {
                mem_req: std::array::from_fn(|ith_dev| {
                    if ith_dev == dev as usize {
                        (
                            ProcessLocalDeviceId(dev),
                            vec![CUDA_PROCESS_RESERVATION_SIZE as u64],
                        )
                    } else {
                        (ProcessLocalDeviceId(0), Vec::new())
                    }
                }),
            }),
        );
        res = unsafe { cu_api::cuDevicePrimaryCtxRetain(&mut cu_ctx, dev) };
    }
    check_cu_err!(res, "cuCtxGetCurrent");
    assert!(!cu_ctx.is_null());
    let res = unsafe { cu_api::cuCtxSetCurrent(cu_ctx) };
    check_cu_err!(res, "cuCtxSetCurrent");
}

pub(crate) fn get_device() -> i32 {
    let mut device_id = CUdevice::default();
    let res = unsafe { cu_api::cuCtxGetDevice(&mut device_id as *mut _) };
    if res != cudaError_enum::CUDA_SUCCESS {
        panic!("Failed to get device id: {:?}", res);
    }
    device_id
}

// restore the context when dropped
#[allow(unused)]
pub(crate) struct CudaContextGuard {
    ctx_ptr: cudarc::driver::sys::CUcontext,
    // mark as !Send
    _marker: std::marker::PhantomData<std::cell::Cell<()>>,
}

impl CudaContextGuard {
    #[allow(unused)]
    pub fn new() -> Self {
        let mut cu_ctx = std::ptr::null_mut();
        let res = unsafe { cu_api::cuCtxGetCurrent(&mut cu_ctx) };
        check_cu_err!(res, "cuCtxGetCurrent");
        assert!(!cu_ctx.is_null());
        Self {
            ctx_ptr: cu_ctx,
            _marker: std::marker::PhantomData,
        }
    }
}

impl Drop for CudaContextGuard {
    fn drop(&mut self) {
        let res = unsafe { cu_api::cuCtxSetCurrent(self.ctx_ptr) };
        check_cu_err!(res, "cuCtxSetCurrent");
    }
}
