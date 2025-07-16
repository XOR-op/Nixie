use cudarc::driver::sys::{cudaError_enum, lib as cuda_lib};
use nihil_common::{MemoryRequest, CUDA_PROCESS_RESERVATION_SIZE};

use crate::{
    env_config::agent_config,
    schedule::{LaunchType, SCHED_CTL},
};

#[inline(always)]
pub(crate) fn should_log(level: u8) -> bool {
    agent_config().log_level >= level
}

#[macro_export]
macro_rules! debug_eprintln {
    ($($arg:tt)*) => {
        if $crate::utils::should_log(3) {
            eprintln!("{} {}", colored::Colorize::blue("NIHIL-DEBUG"), format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! info_eprintln {
    ($($arg:tt)*) => {
        if $crate::utils::should_log(2) {
            eprintln!("{} {}", colored::Colorize::green("NIHIL-INFO"), format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! warn_eprintln {
    ($($arg:tt)*) => {
        if $crate::utils::should_log(1) {
            eprintln!("{} {}", colored::Colorize::yellow("NIHIL-WARN"), format!($($arg)*));
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
    let mut res = unsafe { cuda_lib().cuDevicePrimaryCtxRetain(&mut cu_ctx, dev) };
    if res == cudaError_enum::CUDA_ERROR_OUT_OF_MEMORY {
        crate::debug_eprintln!("Allocating memory for CUDA context");
        SCHED_CTL.pause_then_require_memory(
            LaunchType::Malloc,
            MemoryRequest {
                mem_req: std::array::from_fn(|ith_dev| {
                    if ith_dev == dev as usize {
                        vec![CUDA_PROCESS_RESERVATION_SIZE as u64]
                    } else {
                        Vec::new()
                    }
                }),
            },
        );
        res = unsafe { cuda_lib().cuDevicePrimaryCtxRetain(&mut cu_ctx, dev) };
    }
    check_cu_err!(res, "cuCtxGetCurrent");
    assert!(!cu_ctx.is_null());
    let res = unsafe { cuda_lib().cuCtxSetCurrent(cu_ctx) };
    check_cu_err!(res, "cuCtxSetCurrent");
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
        let res = unsafe { cuda_lib().cuCtxGetCurrent(&mut cu_ctx) };
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
        let res = unsafe { cuda_lib().cuCtxSetCurrent(self.ctx_ptr) };
        check_cu_err!(res, "cuCtxSetCurrent");
    }
}
