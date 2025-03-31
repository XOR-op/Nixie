use colored::Colorize;
use cudarc::driver::sys::{cudaError_enum, lib as cuda_lib, CUdevice};
use nihilipc::shm::AllocationEntry;
use nix::libc::{self, c_char, c_int, dlsym, RTLD_NEXT};
use nix::sys::stat::mode_t;
use std::sync::OnceLock;

use crate::init::{init_generic_data, UVM_FD_CANDIDATES, VALID_UVM_FD};
use crate::memory::get_dup_daemon;
use crate::utils::size_to_string;
use crate::{debug_eprintln, warn_eprintln, GENERIC_DATA};

type CudaMallocManagedType = extern "C" fn(*mut *mut libc::c_void, usize, u32) -> cudaError_enum;
type CudaFreeType = extern "C" fn(*mut libc::c_void) -> cudaError_enum;

type OpenType = extern "C" fn(*const c_char, c_int, mode_t) -> c_int;
type CloseType = extern "C" fn(c_int) -> c_int;
type IoCtlType = extern "C" fn(c_int, c_int, *mut libc::c_void) -> c_int;

static MALLOC_FN: OnceLock<CudaMallocManagedType> = OnceLock::new();
static FREE_FN: OnceLock<CudaFreeType> = OnceLock::new();
static OPEN_FN: OnceLock<OpenType> = OnceLock::new();
static CLOSE_FN: OnceLock<CloseType> = OnceLock::new();
static IOCTL_FN: OnceLock<IoCtlType> = OnceLock::new();

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaMalloc(dev_ptr: *mut *mut libc::c_void, size: usize) -> cudaError_enum {
    cuda_malloc_inner(dev_ptr, size)
}

// #[allow(non_snake_case)]
// #[no_mangle]
// pub extern "C" fn cudaMallocAsync(
//     dev_ptr: *mut *mut libc::c_void,
//     size: usize,
//     // _mem_poll_handler: *mut libc::c_void,
//     _h_stream: *mut libc::c_void,
// ) -> cudaError_enum {
//     let res = cuda_malloc_inner(dev_ptr, size);
//     if res != cudaError_enum::CUDA_SUCCESS {
//         warn_eprintln!(
//             "{} {}: at={:#018x}, size={}, error={:?}",
//             "[libcuda_hook]".bold(),
//             "cudaMallocAsync".green(),
//             unsafe { *dev_ptr as u64 },
//             size,
//             res
//         );
//     }
//     res
// }

fn cuda_malloc_inner(dev_ptr: *mut *mut libc::c_void, size: usize) -> cudaError_enum {
    let malloc_func = MALLOC_FN.get_or_init(|| unsafe {
        let func = dlsym(RTLD_NEXT, cr"cudaMallocManaged".as_ptr()) as *mut CudaMallocManagedType;
        if func.is_null() {
            panic!("Failed to get original cudaMalloc function");
        }
        std::mem::transmute(func)
    });
    let res = malloc_func(dev_ptr, size, 0x01);
    if res == cudaError_enum::CUDA_SUCCESS {
        let device_id = {
            let mut device_id = CUdevice::default();
            let res = unsafe { cuda_lib().cuCtxGetDevice(&mut device_id as *mut _) };
            if res != cudaError_enum::CUDA_SUCCESS {
                panic!("Failed to get device id: {:?}", res);
            }
            device_id
        };

        let mut ptr_mapping = GENERIC_DATA
            .get_or_init(init_generic_data)
            .lock_ptr_mapping();
        let mut dup_daemon = get_dup_daemon().lock().unwrap();
        let alloc_entry = AllocationEntry {
            addr: unsafe { *dev_ptr } as u64,
            len: size,
            device: device_id,
            is_readonly: false,
            is_move_reduced: false,
            likely_on_gpu: true,
        };
        // if the allocation is stored successfully, record it
        if let Some(idx) = ptr_mapping.push(alloc_entry) {
            dup_daemon.record(idx, &alloc_entry);
        }
        let total_size = ptr_mapping.iter().map(|pr| pr.len).sum();
        debug_eprintln!(
            "{} {}: at={}, size={}, total_size={}, count={}",
            "[libcuda_hook]".bold(),
            "cudaMallocManaged".green(),
            format!("{:#018x}", unsafe { *dev_ptr as u64 }).blue(),
            size_to_string(size),
            size_to_string(total_size),
            ptr_mapping.len()
        );
    }
    res
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
    if let Some(mapping) = GENERIC_DATA.get() {
        let mut mapping = mapping.lock_ptr_mapping();
        let idx = mapping.iter().position(|pr| pr.addr == dev_ptr as u64);
        if let Some(idx) = idx {
            mapping.remove(idx);
        } else {
            warn_eprintln!("Failed to find ptr mapping for {}", dev_ptr as u64);
        }
    }

    debug_eprintln!(
        "{} {}: at={:#018x}",
        "[libcuda_hook]".bold(),
        "cudaFree".green(),
        dev_ptr as u64
    );
    free_func(dev_ptr)
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
    debug_eprintln!(
        "{} {}: path={}, fd={}",
        "[libcuda_hook]".bold(),
        "open".green(),
        std::ffi::CStr::from_ptr(path).to_str().unwrap_or(""),
        res
    );

    if std::ffi::CStr::from_ptr(path)
        .to_str()
        .is_ok_and(|s| s == "/dev/nvidia-uvm")
    {
        // store potential UVM FDs used by CUDA libraries
        if VALID_UVM_FD.get().is_none() {
            let mut guard = UVM_FD_CANDIDATES.lock().unwrap();
            if !guard.iter().any(|&fd| fd == res) {
                guard.push(res);
            }
        }
    }
    res
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "C" fn close(fd: c_int) -> c_int {
    let close_func = CLOSE_FN.get_or_init(init_close_fn);
    let res = close_func(fd);
    if VALID_UVM_FD.get().is_none() {
        let mut guard = UVM_FD_CANDIDATES.lock().unwrap();
        if let Some(idx) = guard.iter().position(|&x| x == fd) {
            guard.remove(idx);
            debug_eprintln!(
                "!!! UVM FD closed: {} from pid={}: {:?}",
                fd,
                std::process::id(),
                *guard
            );
        }
    }
    res
}

pub(crate) fn real_libc_close(fd: c_int) -> c_int {
    let close_func = CLOSE_FN.get_or_init(init_close_fn);
    close_func(fd)
}

fn init_close_fn() -> CloseType {
    unsafe {
        let func = dlsym(RTLD_NEXT, cr"close".as_ptr()) as *mut CloseType;
        if func.is_null() {
            panic!("Failed to get original close function");
        }
        std::mem::transmute(func)
    }
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
    ioctl_func(fd, request, arg)
}
