use std::{
    ffi::c_void,
    sync::{OnceLock, atomic::AtomicBool},
};

use cudarc::driver::sys::{CUgraphExec, CUstream, cudaError_enum};
use nihil_common::CUDA_CONTROL_PLANE_RESERVATION_SIZE;
use nix::libc::{self, RTLD_NEXT, dlsym};

use crate::{
    generate_init_fn, generate_init_fn_as,
    schedule::{LaunchType, SCHED_CTL, require_reserved_memory},
    utils::get_device,
};
#[repr(C)]
pub struct CudaDim3 {
    x: u32,
    y: u32,
    z: u32,
}

static IS_DURING_CAPTURE: AtomicBool = AtomicBool::new(false);

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn cudaLaunchKernel(
    func: *const libc::c_void,
    gridDim: CudaDim3,
    blockDim: CudaDim3,
    args: *mut *mut libc::c_void,
    sharedMem: usize,
    stream: CUstream,
) -> cudaError_enum {
    type CudaLaunchKernelType = extern "C" fn(
        *const libc::c_void,
        CudaDim3,
        CudaDim3,
        *mut *mut libc::c_void,
        usize,
        CUstream,
    ) -> cudaError_enum;
    static LAUNCH_KERNEL_FN: OnceLock<CudaLaunchKernelType> = OnceLock::new();
    generate_init_fn!(CudaLaunchKernelType, cr"cudaLaunchKernel");
    let launch_kernel_func = LAUNCH_KERNEL_FN.get_or_init(init_fn);
    SCHED_CTL.launch_allowed(LaunchType::Kernel);
    launch_kernel_func(func, gridDim, blockDim, args, sharedMem, stream)
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn cudaGraphLaunch(graph: CUgraphExec, stream: CUstream) -> cudaError_enum {
    type CudaGraphLaunchType = extern "C" fn(CUgraphExec, CUstream) -> cudaError_enum; // we use CU here since they are actually opaque pointers; can be fixed later
    static GRAPH_LAUNCH_FN: OnceLock<CudaGraphLaunchType> = OnceLock::new();
    generate_init_fn!(CudaGraphLaunchType, cr"cudaGraphLaunch");
    let graph_launch_func = GRAPH_LAUNCH_FN.get_or_init(init_fn);
    SCHED_CTL.launch_allowed(LaunchType::Graph);
    graph_launch_func(graph, stream)
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn cudaStreamCaptureBegin(stream: CUstream, mode: i32) -> cudaError_enum {
    type CudaStreamBeginCaptureType = extern "C" fn(CUstream, i32) -> cudaError_enum;
    static STREAM_CAPTURE_BEGIN_FN: OnceLock<CudaStreamBeginCaptureType> = OnceLock::new();
    generate_init_fn!(CudaStreamBeginCaptureType, cr"cudaStreamBeginCapture");
    let stream_capture_begin_func = STREAM_CAPTURE_BEGIN_FN.get_or_init(init_fn);
    SCHED_CTL.launch_allowed(LaunchType::Graph);
    IS_DURING_CAPTURE.store(true, std::sync::atomic::Ordering::Relaxed);
    stream_capture_begin_func(stream, mode)
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn cudaStreamEndCapture(stream: CUstream, pGraph: *mut c_void) -> cudaError_enum {
    type CudaStreamEndCaptureType = extern "C" fn(CUstream, *mut c_void) -> cudaError_enum;
    static STREAM_END_CAPTURE_FN: OnceLock<CudaStreamEndCaptureType> = OnceLock::new();
    generate_init_fn!(CudaStreamEndCaptureType, cr"cudaStreamEndCapture");
    let stream_end_capture_func = STREAM_END_CAPTURE_FN.get_or_init(init_fn);
    let res = stream_end_capture_func(stream, pGraph);
    IS_DURING_CAPTURE.store(false, std::sync::atomic::Ordering::Relaxed);
    res
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn cudaGraphInstantiate(
    pGraphExec: *mut CUgraphExec,
    graph: *mut c_void,
    flag: usize,
) -> cudaError_enum {
    type CudaGraphInstantiateType =
        extern "C" fn(*mut CUgraphExec, *mut c_void, usize) -> cudaError_enum;
    static GRAPH_INSTANTIATE_FN: OnceLock<CudaGraphInstantiateType> = OnceLock::new();
    generate_init_fn!(CudaGraphInstantiateType, cr"cudaGraphInstantiate");
    let graph_instantiate_func = GRAPH_INSTANTIATE_FN.get_or_init(init_fn);
    let device_id = get_device();
    require_reserved_memory(CUDA_CONTROL_PLANE_RESERVATION_SIZE, device_id);
    graph_instantiate_func(pGraphExec, graph, flag)
}

#[allow(unused)]
pub(crate) fn is_during_capture() -> bool {
    IS_DURING_CAPTURE.load(std::sync::atomic::Ordering::Relaxed)
}
