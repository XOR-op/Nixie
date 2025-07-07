use cudarc::driver::sys::{cudaError_enum, lib as cuda_lib, CUdevice};
use nihil_common::shm::AllocationEntry;
use nihil_common::{MAX_ALLOCATION_SIZE, MAX_GPUS, MIN_ALLOCATION_SIZE};
use nix::libc::{self, c_char, c_int, dlsym, RTLD_NEXT};
use nix::sys::stat::mode_t;
use std::sync::OnceLock;

use crate::comm::init::init_comm_entrypoint;
use crate::init_generic_data;
use crate::memory::{deallocate_list, populate_entry};
use crate::{warn_eprintln, GENERIC_DATA};

#[macro_export]
macro_rules! generate_init_fn_as {
    ($func_type:ty, $func_name:expr, $init_func_name:ident) => {
        fn $init_func_name() -> $func_type {
            unsafe {
                let func = dlsym(RTLD_NEXT, $func_name.as_ptr());
                if func.is_null() {
                    panic!("Failed to get original {:?} function", $func_name);
                }
                std::mem::transmute(func)
            }
        }
    };
}

#[macro_export]
macro_rules! generate_init_fn {
    ($func_type:ty, $func_name:expr) => {
        generate_init_fn_as!($func_type, $func_name, init_fn);
    };
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaMalloc(dev_ptr: *mut *mut libc::c_void, size: usize) -> cudaError_enum {
    type CudaMallocType = extern "C" fn(*mut *mut libc::c_void, usize) -> cudaError_enum;
    static MALLOC_FN: OnceLock<CudaMallocType> = OnceLock::new();
    generate_init_fn!(CudaMallocType, cr"cudaMalloc");
    let malloc_func = MALLOC_FN.get_or_init(init_fn);
    if size < MIN_ALLOCATION_SIZE {
        return malloc_func(dev_ptr, size);
    }

    // round up the size to the nearest multiple of MIN_ALLOCATION_SIZE
    let rounded_up_size = (size + MIN_ALLOCATION_SIZE - 1) & !(MIN_ALLOCATION_SIZE - 1);
    let res = unsafe {
        cuda_lib().cuMemAddressReserve(
            dev_ptr as *mut _,
            rounded_up_size,
            MIN_ALLOCATION_SIZE,
            0,
            0,
        )
    };
    if res == cudaError_enum::CUDA_SUCCESS {
        let device_id = {
            let mut device_id = CUdevice::default();
            let res = unsafe { cuda_lib().cuCtxGetDevice(&mut device_id as *mut _) };
            if res != cudaError_enum::CUDA_SUCCESS {
                panic!("Failed to get device id: {:?}", res);
            }
            device_id
        };

        let mut table = GENERIC_DATA
            .get_or_init(init_generic_data)
            .lock(device_id as usize);

        let mut remaining_size = rounded_up_size;
        let mut cur_addr = unsafe { *dev_ptr } as u64;
        let mut handle_idx = None;
        // Allocate bookkeeping structures
        while remaining_size > 0 {
            let alloc_size = remaining_size.min(MAX_ALLOCATION_SIZE);
            if let Some(idx) = table.handle_list.allocate_handle(cur_addr, alloc_size) {
                handle_idx = Some(idx);
                cur_addr += alloc_size as u64;
                remaining_size -= alloc_size;
            } else {
                warn_eprintln!(
                    "Failed to allocate bookkeeping for {} bytes at address {:x}",
                    alloc_size,
                    cur_addr
                );
                return cudaError_enum::CUDA_ERROR_OUT_OF_MEMORY;
            }
        }

        let alloc_entry = AllocationEntry {
            addr: unsafe { *dev_ptr } as u64,
            len: rounded_up_size,
            handle_idx: handle_idx.expect("Failed to allocate handle"),
        };

        if !populate_entry(&alloc_entry, device_id, &mut table) {
            return cudaError_enum::CUDA_ERROR_OUT_OF_MEMORY;
        }
    }
    res
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaFree(dev_ptr: *mut libc::c_void) -> cudaError_enum {
    type CudaFreeType = extern "C" fn(*mut libc::c_void) -> cudaError_enum;
    static FREE_FN: OnceLock<CudaFreeType> = OnceLock::new();
    generate_init_fn!(CudaFreeType, cr"cudaFree");
    let free_func = FREE_FN.get_or_init(init_fn);

    // first check if is non-managed allocation
    if dev_ptr as usize % MIN_ALLOCATION_SIZE != 0 {
        // if not, just call the original function
        return free_func(dev_ptr);
    }

    for possible_dev in 0..MAX_GPUS {
        let mut table_guard = GENERIC_DATA
            .get_or_init(init_generic_data)
            .lock(possible_dev);
        let table = &mut *table_guard;
        // find the allocation entry
        let mut entry_idx = None;
        for entry in table.entry.iter() {
            if entry.addr == dev_ptr as u64 {
                entry_idx = Some(entry.handle_idx);
                break;
            }
        }
        // on this device, we found the entry
        if let Some(idx) = entry_idx {
            let entry = table.entry.at(idx.get() as usize).unwrap();
            let handle_idx = entry.handle_idx;
            let mut cur_index = Some(entry.handle_idx);
            deallocate_list(handle_idx, &mut table.handle_list);
            while let Some(index) = cur_index {
                let handle = table.handle_list.get_handle(index).unwrap();
                cur_index = handle.next_handle_idx;
                table.handle_list.free_handle(index);
            }
            return cudaError_enum::CUDA_SUCCESS;
        }
    }
    cudaError_enum::CUDA_ERROR_INVALID_VALUE
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "C" fn open(path: *const c_char, oflag: c_int, mode: mode_t) -> c_int {
    type OpenType = extern "C" fn(*const c_char, c_int, mode_t) -> c_int;
    static OPEN_FN: OnceLock<OpenType> = OnceLock::new();
    static FIRST_TIME: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    generate_init_fn!(OpenType, cr"open");
    let open_func = OPEN_FN.get_or_init(init_fn);
    let res = open_func(path, oflag, mode);
    if FIRST_TIME.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
        init_comm_entrypoint();
    }
    res
}
