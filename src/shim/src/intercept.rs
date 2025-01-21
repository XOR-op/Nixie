use colored::Colorize;
use cudarc::driver::sys::{cudaError_enum, CUdevice, CUstream};
use nihilipc::shm::AllocationEntry;
use nix::libc::{self, c_char, c_int, dlsym, RTLD_NEXT};
use nix::sys::stat::mode_t;
use std::sync::OnceLock;

use crate::comm::notify_fd;
use crate::utils::{should_log, size_to_string};
use crate::{info_eprintln, warn_eprintln, GenericData, GENERIC_DATA};

#[repr(C)]
pub struct CudaDim3 {
    x: u32,
    y: u32,
    z: u32,
}

type CudaMallocType = extern "C" fn(*mut *mut libc::c_void, usize, u32) -> cudaError_enum;
type CudaFreeType = extern "C" fn(*mut libc::c_void) -> cudaError_enum;
type CudaLaunchKernelType = extern "C" fn(
    *const libc::c_void,
    CudaDim3,
    CudaDim3,
    *mut *mut libc::c_void,
    usize,
    CUstream,
) -> cudaError_enum;
type OpenType = extern "C" fn(*const c_char, c_int, mode_t) -> c_int;
type IoCtlType = extern "C" fn(c_int, c_int, *mut libc::c_void) -> c_int;

static MALLOC_FN: OnceLock<CudaMallocType> = OnceLock::new();
static FREE_FN: OnceLock<CudaFreeType> = OnceLock::new();
static LAUNCH_KERNEL_FN: OnceLock<CudaLaunchKernelType> = OnceLock::new();
static OPEN_FN: OnceLock<OpenType> = OnceLock::new();
static IOCTL_FN: OnceLock<IoCtlType> = OnceLock::new();

static UVM_FD: OnceLock<i32> = OnceLock::new();

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaMalloc(dev_ptr: *mut *mut libc::c_void, size: usize) -> cudaError_enum {
    let malloc_func = MALLOC_FN.get_or_init(|| unsafe {
        let func = dlsym(RTLD_NEXT, cr"cudaMallocManaged".as_ptr()) as *mut CudaMallocType;
        if func.is_null() {
            panic!("Failed to get original cudaMalloc function");
        }
        std::mem::transmute(func)
    });
    warn_eprintln!("cudaMalloc: {} Entering", size);
    let res = malloc_func(dev_ptr, size, 0x01);
    if res == cudaError_enum::CUDA_SUCCESS {
        let device_id = {
            let mut device_id = CUdevice::default();
            let res = unsafe { cudarc::driver::sys::cuCtxGetDevice(&mut device_id as *mut _) };
            if res != cudaError_enum::CUDA_SUCCESS {
                panic!("Failed to get device id: {:?}", res);
            }
            device_id
        };
        // set read mostly
        // let res = unsafe {
        //     cudarc::driver::sys::cuMemAdvise(
        //         *dev_ptr as u64,
        //         size,
        //         cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_SET_READ_MOSTLY,
        //         device_id,
        //     )
        // };
        // if res != cudaError_enum::CUDA_SUCCESS {
        //     eprintln!("Failed to set read mostly: {:?}", res);
        // }
        // record ptr mapping
        let mut ptr_mapping = GENERIC_DATA
            .get_or_init(|| GenericData::new())
            .lock_ptr_mapping();
        ptr_mapping.push(AllocationEntry {
            addr: unsafe { *dev_ptr as u64 },
            len: size,
            device: device_id,
        });
        let total_size = ptr_mapping.iter().map(|pr| pr.len).sum();
        info_eprintln!(
            "{} {}: at={}, size={}, total_size={}, count={}",
            "[libcuda_hook]".bold(),
            "cudaMalloc".green(),
            format!("{:#018x}", unsafe { *dev_ptr as u64 }).blue(),
            size_to_string(size),
            size_to_string(total_size),
            ptr_mapping.len()
        );
    }
    return res;
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaFree(dev_ptr: *mut libc::c_void) -> cudaError_enum {
    let free_func = FREE_FN.get_or_init(|| unsafe {
        let func = dlsym(RTLD_NEXT, cr"cudaFree".as_ptr()) as *mut CudaFreeType;
        if func.is_null() {
            panic!("Failed to get original cudaFree function");
        }
        std::mem::transmute(func)
    });
    let mut mapping = GENERIC_DATA
        .get_or_init(|| GenericData::new())
        .lock_ptr_mapping();
    let idx = mapping.iter().position(|pr| pr.addr == dev_ptr as u64);
    if let Some(idx) = idx {
        mapping.remove(idx);
    } else {
        warn_eprintln!("Failed to find ptr mapping for {}", dev_ptr as u64);
    }
    info_eprintln!(
        "{} {}: at={:#018x}",
        "[libcuda_hook]".bold(),
        "cudaFree".green(),
        dev_ptr as u64
    );
    return free_func(dev_ptr);
}

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
    const ANSI_BOLD: &'static str = "\x1b[1m";
    const ANSI_GREEN: &'static str = "\x1b[0;32m";
    const ANSI_RESET: &'static str = "\x1b[0m";
    info_eprintln!(
        "{}[libcudahook] ({}) {}cudaLaunchKernel{}: gridDim=({},{},{}), blockDim=({},{},{}), sharedMem={}",
        ANSI_BOLD,
        chrono::Local::now().format("%H:%M:%S%.6f"),
        ANSI_GREEN,
        ANSI_RESET,
        gridDim.x,
        gridDim.y,
        gridDim.z,
        blockDim.x,
        blockDim.y,
        blockDim.z,
        sharedMem
    );
    return launch_kernel_func(func, gridDim, blockDim, args, sharedMem, stream);
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "C" fn open(path: *const c_char, oflag: c_int, mode: mode_t) -> c_int {
    let open_func = OPEN_FN.get_or_init(|| {
        let func = dlsym(RTLD_NEXT, cr"open".as_ptr()) as *mut OpenType;
        if func.is_null() {
            panic!("Failed to get original open function");
        }
        // print address
        std::mem::transmute(func)
    });
    let res = open_func(path, oflag, mode);
    if UVM_FD.get().is_none()
        && std::ffi::CStr::from_ptr(path)
            .to_str()
            .is_ok_and(|s| s == "/dev/nvidia-uvm")
    {
        let _ = UVM_FD.set(res);
        notify_fd(res);
    }
    return res;
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "C" fn ioctl(fd: c_int, request: c_int, arg: *mut libc::c_void) -> c_int {
    let ioctl_func = IOCTL_FN.get_or_init(|| {
        let func = dlsym(RTLD_NEXT, cr"ioctl".as_ptr()) as *mut IoCtlType;
        if func.is_null() {
            panic!("Failed to get original ioctl function");
        }
        std::mem::transmute(func)
    });
    let res = ioctl_func(fd, request, arg);
    return res;
}
