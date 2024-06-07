use cudarc::driver::sys::cudaError_enum;
use nix::libc::{self, c_char, c_int, dlsym, RTLD_NEXT};
use nix::sys::stat::mode_t;
use once_cell::sync::OnceCell;
use std::sync::Mutex;

use crate::{utils::size_to_string, PTR_MAPPING};

type CudaMallocType = extern "C" fn(*mut *mut libc::c_void, usize, u32) -> cudaError_enum;
type CudaFreeType = extern "C" fn(*mut libc::c_void) -> cudaError_enum;
type OpenType = extern "C" fn(*const c_char, c_int, mode_t) -> c_int;

static MALLOC_FN: OnceCell<CudaMallocType> = OnceCell::new();
static FREE_FN: OnceCell<CudaFreeType> = OnceCell::new();
static OPEN_FN: OnceCell<OpenType> = OnceCell::new();

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
    println!(
        "open({:?}, {:08X}) -> {}",
        std::ffi::CStr::from_ptr(path),
        oflag,
        res
    );
    return res;
}
