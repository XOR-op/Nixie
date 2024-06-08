use cudarc::driver::sys::cudaError_enum;
use nix::libc::{self, c_char, c_int, dlsym, RTLD_NEXT};
use nix::sys::stat::mode_t;
use once_cell::sync::OnceCell;
use std::sync::Mutex;

use crate::{utils::size_to_string, PTR_MAPPING};

type CudaMallocType = extern "C" fn(*mut *mut libc::c_void, usize, u32) -> cudaError_enum;
type CudaFreeType = extern "C" fn(*mut libc::c_void) -> cudaError_enum;
type OpenType = extern "C" fn(*const c_char, c_int, mode_t) -> c_int;
type IoCtlType = extern "C" fn(c_int, c_int, *mut libc::c_void) -> c_int;

static MALLOC_FN: OnceCell<CudaMallocType> = OnceCell::new();
static FREE_FN: OnceCell<CudaFreeType> = OnceCell::new();
static OPEN_FN: OnceCell<OpenType> = OnceCell::new();
static IOCTL_FN: OnceCell<IoCtlType> = OnceCell::new();

static UVM_FD: OnceCell<i32> = OnceCell::new();

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
    let res = malloc_func(dev_ptr, size, 0x01);
    if res == cudaError_enum::CUDA_SUCCESS {
        PTR_MAPPING
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .unwrap()
            .push((unsafe { *dev_ptr as u64 }, size));
        let total_size = PTR_MAPPING
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .iter()
            .map(|pr| pr.1)
            .sum();
        println!(
            "cudaMalloc: size={}, total_size={}, count={}",
            size_to_string(size),
            size_to_string(total_size),
            PTR_MAPPING.get().unwrap().lock().unwrap().len()
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
    let mut mapping = PTR_MAPPING
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    if let Some(idx) = mapping.iter().position(|pr| pr.0 == dev_ptr as u64) {
        mapping.remove(idx);
    }
    return free_func(dev_ptr);
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
    }
    println!(
        "open({:?}, {:08X}) -> {}",
        std::ffi::CStr::from_ptr(path),
        oflag,
        res
    );
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
    // if let Some(uvm_fd) = UVM_FD.get() {
    //     if *uvm_fd == fd {
    //         println!("ioctl({}, {}, ...) -> {}", fd, request, res);
    //     }
    // }
    return res;
}
