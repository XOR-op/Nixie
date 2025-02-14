use std::sync::OnceLock;

use cudarc::driver::sys::{cudaError_enum, CUgraphExec, CUstream};
use nix::libc::{self, dlsym, RTLD_NEXT};

use crate::schedule::SCHED_CTL;
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

static LAUNCH_KERNEL_FN: OnceLock<CudaLaunchKernelType> = OnceLock::new();
static GRAPH_LAUNCH_FN: OnceLock<CudaGraphLaunchType> = OnceLock::new();

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
    SCHED_CTL.launch_allowed();
    return launch_kernel_func(func, gridDim, blockDim, args, sharedMem, stream);
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
    SCHED_CTL.launch_allowed();
    return graph_launch_func(graph, stream);
}
