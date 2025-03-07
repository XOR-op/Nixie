use std::{
    ffi::c_void,
    sync::{atomic::AtomicBool, OnceLock},
};

use cudarc::driver::sys::{cudaError_enum, CUgraphExec, CUstream};
use nix::libc::{self, dlsym, RTLD_NEXT};

use crate::schedule::{LaunchType, SCHED_CTL};
#[repr(C)]
pub struct CudaDim3 {
    x: u32,
    y: u32,
    z: u32,
}

type CudaLaunchKernelType = extern "C" fn(
    *const libc::c_void,
    CudaDim3,
    CudaDim3,
    *mut *mut libc::c_void,
    usize,
    CUstream,
) -> cudaError_enum;
type CudaGraphLaunchType = extern "C" fn(CUgraphExec, CUstream) -> cudaError_enum; // we use CU here since they are actually opaque pointers; can be fixed later
type CudaStreamBeginCaptureType = extern "C" fn(CUstream, i32) -> cudaError_enum;
type CudaStreamEndCaptureType = extern "C" fn(CUstream, *mut c_void) -> cudaError_enum;

static LAUNCH_KERNEL_FN: OnceLock<CudaLaunchKernelType> = OnceLock::new();
static GRAPH_LAUNCH_FN: OnceLock<CudaGraphLaunchType> = OnceLock::new();
static STREAM_CAPTURE_BEGIN_FN: OnceLock<CudaStreamBeginCaptureType> = OnceLock::new();
static STREAM_END_CAPTURE_FN: OnceLock<CudaStreamEndCaptureType> = OnceLock::new();

static IS_DURING_CAPTURE: AtomicBool = AtomicBool::new(false);

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaLaunchKernel(
    func: *const libc::c_void,
    gridDim: CudaDim3,
    blockDim: CudaDim3,
    args: *mut *mut libc::c_void,
    sharedMem: usize,
    stream: CUstream,
) -> cudaError_enum {
    let launch_kernel_func = LAUNCH_KERNEL_FN.get_or_init(|| unsafe {
        let func = dlsym(RTLD_NEXT, cr"cudaLaunchKernel".as_ptr()) as *mut CudaLaunchKernelType;
        if func.is_null() {
            panic!("Failed to get original cudaLaunchKernel function");
        }
        std::mem::transmute(func)
    });
    SCHED_CTL.launch_allowed(LaunchType::Kernel);
    launch_kernel_func(func, gridDim, blockDim, args, sharedMem, stream)
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaGraphLaunch(graph: CUgraphExec, stream: CUstream) -> cudaError_enum {
    let graph_launch_func = GRAPH_LAUNCH_FN.get_or_init(|| unsafe {
        let func = dlsym(RTLD_NEXT, cr"cudaGraphLaunch".as_ptr()) as *mut CudaGraphLaunchType;
        if func.is_null() {
            panic!("Failed to get original cudaGraphLaunch function");
        }
        std::mem::transmute(func)
    });
    SCHED_CTL.launch_allowed(LaunchType::Graph);
    graph_launch_func(graph, stream)
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaStreamCaptureBegin(stream: CUstream, mode: i32) -> cudaError_enum {
    let stream_capture_begin_func = STREAM_CAPTURE_BEGIN_FN.get_or_init(|| unsafe {
        let func = dlsym(RTLD_NEXT, cr"cudaStreamBeginCapture".as_ptr())
            as *mut CudaStreamBeginCaptureType;
        if func.is_null() {
            panic!("Failed to get original cudaStreamCaptureBegin function");
        }
        std::mem::transmute(func)
    });
    IS_DURING_CAPTURE.store(true, std::sync::atomic::Ordering::Relaxed);
    stream_capture_begin_func(stream, mode)
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaStreamEndCapture(stream: CUstream, pGraph: *mut c_void) -> cudaError_enum {
    let stream_end_capture_func = STREAM_END_CAPTURE_FN.get_or_init(|| unsafe {
        let func =
            dlsym(RTLD_NEXT, cr"cudaStreamEndCapture".as_ptr()) as *mut CudaStreamEndCaptureType;
        if func.is_null() {
            panic!("Failed to get original cudaStreamEndCapture function");
        }
        std::mem::transmute(func)
    });
    IS_DURING_CAPTURE.store(false, std::sync::atomic::Ordering::Relaxed);
    stream_end_capture_func(stream, pGraph)
}

pub(crate) fn is_during_capture() -> bool {
    IS_DURING_CAPTURE.load(std::sync::atomic::Ordering::Relaxed)
}
