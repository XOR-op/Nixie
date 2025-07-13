use cudarc::driver::sys::{cudaError_enum, lib as cuda_lib, CUdevice, CUstream};
use nihil_common::shm::AllocationEntry;
use nihil_common::{MAX_ALLOCATION_SIZE, MAX_GPUS, MIN_ALLOCATION_SIZE};
use nix::libc::{self, c_char, c_int, dlsym, RTLD_NEXT};
use nix::sys::stat::mode_t;
use std::collections::BTreeSet;
use std::sync::{Mutex, OnceLock};

use crate::init::{init_all_entrypoint, init_cuda_env, should_have_initialized};
use crate::memory::{deallocate_list, populate_entry};
use crate::schedule::{LaunchType, SCHED_CTL};
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

static SMALL_ALLOCATION: Mutex<BTreeSet<u64>> = Mutex::new(BTreeSet::new());

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaMalloc(dev_ptr: *mut *mut libc::c_void, size: usize) -> cudaError_enum {
    type CudaMallocType = extern "C" fn(*mut *mut libc::c_void, usize) -> cudaError_enum;
    static MALLOC_FN: OnceLock<CudaMallocType> = OnceLock::new();
    generate_init_fn!(CudaMallocType, cr"cudaMalloc");
    let malloc_func = MALLOC_FN.get_or_init(init_fn);
    init_cuda_env();
    SCHED_CTL.launch_allowed(LaunchType::Malloc);
    if size < MIN_ALLOCATION_SIZE {
        let res = malloc_func(dev_ptr, size);
        if res == cudaError_enum::CUDA_SUCCESS {
            SMALL_ALLOCATION
                .lock()
                .unwrap()
                .insert(unsafe { *dev_ptr } as u64);
        }
        return res;
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
            .get_or_init(should_have_initialized)
            .lock(device_id as usize);

        let mut remaining_size = rounded_up_size;
        let mut cur_addr = unsafe { *dev_ptr } as u64;
        let mut handle_idx = None;
        // Allocate bookkeeping structures
        while remaining_size > 0 {
            let alloc_size = remaining_size.min(MAX_ALLOCATION_SIZE);
            if let Some(new_idx) = table.handle_list.allocate_handle(cur_addr, alloc_size) {
                table
                    .handle_list
                    .get_handle_mut(new_idx)
                    .unwrap()
                    .next_handle_idx = handle_idx;
                handle_idx = Some(new_idx);
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
            // deallocate all handles
            while let Some(idx) = handle_idx {
                let handle = table.handle_list.get_handle(idx).unwrap();
                handle_idx = handle.next_handle_idx;
                table.handle_list.free_handle(idx);
            }
            return cudaError_enum::CUDA_ERROR_OUT_OF_MEMORY;
        }

        // add the entry to the table
        if table.entry.push(alloc_entry).is_err() {
            warn_eprintln!(
                "Exceeded maximum number of allocations for device {}",
                device_id
            );
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
    if SMALL_ALLOCATION.lock().unwrap().remove(&(dev_ptr as u64)) {
        return free_func(dev_ptr);
    }

    for possible_dev in 0..MAX_GPUS {
        let mut table_guard = GENERIC_DATA
            .get_or_init(should_have_initialized)
            .lock(possible_dev);
        let table = &mut *table_guard;
        // find the allocation entry
        let mut possible_entry_indx = None;
        for (entry_idx, entry) in table.entry.iter().enumerate() {
            if entry.addr == dev_ptr as u64 {
                possible_entry_indx = Some(entry_idx);
                break;
            }
        }
        // on this device, we found the entry
        if let Some(entry_idx) = possible_entry_indx {
            let entry = table.entry.at(entry_idx).unwrap();
            let handle_idx = entry.handle_idx;
            let mut cur_index = Some(entry.handle_idx);
            deallocate_list(handle_idx, &mut table.handle_list);
            while let Some(index) = cur_index {
                let handle = table.handle_list.get_handle(index).unwrap();
                cur_index = handle.next_handle_idx;
                table.handle_list.free_handle(index);
            }
            table.entry.remove(entry_idx);
            return cudaError_enum::CUDA_SUCCESS;
        }
    }
    cudaError_enum::CUDA_ERROR_INVALID_VALUE
}

#[allow(unused)]
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CudaMemcpyKind {
    HostToHost = 0,
    HostToDevice = 1,
    DeviceToHost = 2,
    DeviceToDevice = 3,
    Default = 4,
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaMemcpy(
    dst: *mut libc::c_void,
    src: *const libc::c_void,
    size: usize,
    kind: CudaMemcpyKind,
) -> cudaError_enum {
    type CudaMemcpyType = extern "C" fn(
        *mut libc::c_void,
        *const libc::c_void,
        usize,
        CudaMemcpyKind,
    ) -> cudaError_enum;
    static MEMCPY_FN: OnceLock<CudaMemcpyType> = OnceLock::new();
    generate_init_fn!(CudaMemcpyType, cr"cudaMemcpy");
    let memcpy_func = MEMCPY_FN.get_or_init(init_fn);
    SCHED_CTL.launch_allowed(LaunchType::Transfer);
    memcpy_func(dst, src, size, kind)
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaMemcpyAsync(
    dst: *mut libc::c_void,
    src: *const libc::c_void,
    size: usize,
    kind: CudaMemcpyKind,
    stream: CUstream,
) -> cudaError_enum {
    type CudaMemcpyAsyncType = extern "C" fn(
        *mut libc::c_void,
        *const libc::c_void,
        usize,
        CudaMemcpyKind,
        CUstream,
    ) -> cudaError_enum;
    static MEMCPY_ASYNC_FN: OnceLock<CudaMemcpyAsyncType> = OnceLock::new();
    generate_init_fn!(CudaMemcpyAsyncType, cr"cudaMemcpyAsync");
    let memcpy_async_func = MEMCPY_ASYNC_FN.get_or_init(init_fn);
    SCHED_CTL.launch_allowed(LaunchType::Transfer);
    memcpy_async_func(dst, src, size, kind, stream)
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaMemset(
    dev_ptr: *mut libc::c_void,
    value: i32,
    size: usize,
) -> cudaError_enum {
    type CudaMemsetType = extern "C" fn(*mut libc::c_void, i32, usize) -> cudaError_enum;
    static MEMSET_FN: OnceLock<CudaMemsetType> = OnceLock::new();
    generate_init_fn!(CudaMemsetType, cr"cudaMemset");
    let memset_func = MEMSET_FN.get_or_init(init_fn);
    SCHED_CTL.launch_allowed(LaunchType::Transfer);
    memset_func(dev_ptr, value, size)
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "C" fn open(path: *const c_char, oflag: c_int, mode: mode_t) -> c_int {
    type OpenType = extern "C" fn(*const c_char, c_int, mode_t) -> c_int;
    static OPEN_FN: OnceLock<OpenType> = OnceLock::new();
    generate_init_fn!(OpenType, cr"open");
    let open_func = OPEN_FN.get_or_init(init_fn);
    let res = open_func(path, oflag, mode);
    init_all_entrypoint();
    res
}
