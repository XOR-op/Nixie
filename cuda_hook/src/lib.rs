use cudarc::driver::sys::cudaError_enum;
use nix;
use nix::libc::{self, dlsym, RTLD_NEXT};
use once_cell::sync::OnceCell;

type CudaMallocType = extern "C" fn(*mut *mut libc::c_void, usize, u32) -> cudaError_enum;

static FN: OnceCell<CudaMallocType> = OnceCell::new();

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaMalloc(dev_ptr: *mut *mut libc::c_void, size: usize) -> cudaError_enum {
    let malloc_func = FN.get_or_init(|| unsafe {
        let func = dlsym(RTLD_NEXT, cr"cudaMallocManaged".as_ptr()) as *mut CudaMallocType;
        if func.is_null() {
            panic!("Failed to get original cudaMalloc function");
        }
        std::mem::transmute(func)
    });
    return malloc_func(dev_ptr, size, 0x01);
}

#[no_mangle]
pub extern "C" fn _auto_gmem_prefetch() {
    println!("Hello from _auto_gmem_prefetch")
}
