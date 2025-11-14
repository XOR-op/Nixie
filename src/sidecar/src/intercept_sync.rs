use nix::libc::{RTLD_NEXT, dlsym};
use std::sync::OnceLock;

use cudarc::driver::sys::cudaError_enum;

use crate::{generate_init_fn, generate_init_fn_as, schedule::SCHED_CTL};

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn cudaDeviceSynchronize() -> cudaError_enum {
    type CudaDeviceSynchronizeType = extern "C" fn() -> cudaError_enum;
    static DEVICE_SYNCHRONIZE_FN: OnceLock<CudaDeviceSynchronizeType> = OnceLock::new();
    generate_init_fn!(CudaDeviceSynchronizeType, cr"cudaDeviceSynchronize");
    let device_synchronize_func = DEVICE_SYNCHRONIZE_FN.get_or_init(init_fn);
    SCHED_CTL.record_sync_start();
    let res = device_synchronize_func();
    SCHED_CTL.record_sync_end();
    res
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn cudaStreamSynchronize(stream: cudarc::driver::sys::CUstream) -> cudaError_enum {
    type CudaStreamSynchronizeType = extern "C" fn(cudarc::driver::sys::CUstream) -> cudaError_enum;
    static STREAM_SYNCHRONIZE_FN: OnceLock<CudaStreamSynchronizeType> = OnceLock::new();
    generate_init_fn!(CudaStreamSynchronizeType, cr"cudaStreamSynchronize");
    let stream_synchronize_func = STREAM_SYNCHRONIZE_FN.get_or_init(init_fn);
    SCHED_CTL.record_sync_start();
    let res = stream_synchronize_func(stream);
    SCHED_CTL.record_sync_end();
    res
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn cudaEventSynchronize(event: cudarc::driver::sys::CUevent) -> cudaError_enum {
    type CudaEventSynchronizeType = extern "C" fn(cudarc::driver::sys::CUevent) -> cudaError_enum;
    static EVENT_SYNCHRONIZE_FN: OnceLock<CudaEventSynchronizeType> = OnceLock::new();
    generate_init_fn!(CudaEventSynchronizeType, cr"cudaEventSynchronize");
    let event_synchronize_func = EVENT_SYNCHRONIZE_FN.get_or_init(init_fn);
    SCHED_CTL.record_sync_start();
    let res = event_synchronize_func(event);
    SCHED_CTL.record_sync_end();
    res
}
