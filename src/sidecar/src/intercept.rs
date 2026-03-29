use cudarc::driver::sys::{CUevent_flags_enum, CUstream, cudaError_enum};
use nix::libc::{self, RTLD_NEXT, c_char, c_int, dlsym};
use nix::sys::stat::mode_t;
use nixie_common::shm::{AllocationEntry, PhysicalMemoryHandleId};
use nixie_common::{
    CUDA_CONTROL_PLANE_RESERVATION_SIZE, GpuMemoryFreeUpdate, MAX_ALLOCATION_SIZE, MAX_GPUS,
    MIN_ALLOCATION_SIZE, ProcessLocalDeviceId,
};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Mutex, OnceLock};

use crate::comm::update_gpu_memory_free;
use crate::init::{init_all_entrypoint, init_cuda_env, should_have_initialized};
use crate::memory::{
    CachedBlock, async_pool, deallocate_list, get_max_allocation_size, global_tracker,
    populate_entry,
};
use crate::schedule::{LaunchType, SCHED_CTL, require_reserved_memory};
use crate::utils::get_device;
use crate::{GENERIC_DATA, check_cu_err, cu_api, warn_eprintln};

#[macro_export]
macro_rules! generate_init_fn_as {
    ($func_type:ty, $func_name:expr, $init_func_name:ident) => {
        fn $init_func_name() -> $func_type {
            #[allow(clippy::macro_metavars_in_unsafe)]
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

type CudaMemGetInfoType = extern "C" fn(*mut usize, *mut usize) -> cudaError_enum;
fn get_func_ptr_cuda_mem_get_info() -> &'static CudaMemGetInfoType {
    static MEM_GET_INFO_FN: OnceLock<CudaMemGetInfoType> = OnceLock::new();
    generate_init_fn!(CudaMemGetInfoType, cr"cudaMemGetInfo");
    MEM_GET_INFO_FN.get_or_init(init_fn)
}

pub(crate) fn cuda_mem_get_info_impl() -> (usize, usize) {
    let mut avail = 0;
    let mut total = 0;
    check_cu_err!(
        get_func_ptr_cuda_mem_get_info()(&mut avail, &mut total),
        "GET_MEM_INFO"
    );
    (avail, total)
}

static SMALL_ALLOCATION: Mutex<BTreeMap<u64, u64>> = Mutex::new(BTreeMap::new());
static CURRENT_ALLOCATION_SIZE: AtomicU64 = AtomicU64::new(0);

/// Synchronize and free a list of cached blocks released from the pool.
fn free_cached_blocks(blocks: Vec<CachedBlock>) {
    for block in blocks {
        unsafe {
            cu_api::cuEventSynchronize(block.event);
            cu_api::cuEventDestroy_v2(block.event);
        }
        cudaFree(block.ptr as *mut libc::c_void);
    }
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn cudaMalloc(dev_ptr: *mut *mut libc::c_void, size: usize) -> cudaError_enum {
    type CudaMallocType = extern "C" fn(*mut *mut libc::c_void, usize) -> cudaError_enum;
    static MALLOC_FN: OnceLock<CudaMallocType> = OnceLock::new();
    generate_init_fn!(CudaMallocType, cr"cudaMalloc");
    let malloc_func = MALLOC_FN.get_or_init(init_fn);
    init_cuda_env();

    // check against size limit
    let device_id = get_device();
    if CURRENT_ALLOCATION_SIZE.load(std::sync::atomic::Ordering::Relaxed) + size as u64
        > get_max_allocation_size(device_id)
    {
        return cudaError_enum::CUDA_ERROR_OUT_OF_MEMORY;
    }

    SCHED_CTL.launch_allowed(LaunchType::Malloc);
    // small allocation
    if size < MIN_ALLOCATION_SIZE {
        let res = malloc_func(dev_ptr, size);
        if res == cudaError_enum::CUDA_SUCCESS {
            SMALL_ALLOCATION
                .lock()
                .unwrap()
                .insert(unsafe { *dev_ptr } as u64, size as u64);
            CURRENT_ALLOCATION_SIZE.fetch_add(size as u64, std::sync::atomic::Ordering::Relaxed);
            global_tracker().insert(
                unsafe { *dev_ptr } as u64,
                size as u64,
                size as u64,
                ProcessLocalDeviceId(device_id),
            );
        }
        return res;
    }

    // round up the size to the nearest multiple of MIN_ALLOCATION_SIZE
    let rounded_up_size = (size + MIN_ALLOCATION_SIZE - 1) & !(MIN_ALLOCATION_SIZE - 1);
    let res = unsafe {
        cu_api::cuMemAddressReserve(
            dev_ptr as *mut _,
            rounded_up_size,
            MIN_ALLOCATION_SIZE,
            0,
            0,
        )
    };
    if res == cudaError_enum::CUDA_SUCCESS {
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
                handle_idx = Some(new_idx.idx);
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
            handle_idx: PhysicalMemoryHandleId {
                alloc_generation: table
                    .handle_list
                    .get_handle_by_raw_idx(handle_idx.expect("Failed to allocate handle"))
                    .unwrap()
                    .alloc_generation,
                idx: handle_idx.expect("Failed to allocate handle"),
            },
        };
        if !populate_entry(&alloc_entry, device_id, &mut table) {
            // deallocate all handles
            while let Some(idx) = handle_idx {
                let handle = table.handle_list.get_handle_by_raw_idx(idx).unwrap();
                handle_idx = handle.next_handle_idx;
                table.handle_list.free_handle_by_raw_idx(idx);
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
        CURRENT_ALLOCATION_SIZE
            .fetch_add(rounded_up_size as u64, std::sync::atomic::Ordering::Relaxed);
        global_tracker().insert(
            unsafe { *dev_ptr } as u64,
            size as u64,
            rounded_up_size as u64,
            ProcessLocalDeviceId(device_id),
        );
        require_reserved_memory(CUDA_CONTROL_PLANE_RESERVATION_SIZE, device_id);
    }
    res
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn cudaFree(dev_ptr: *mut libc::c_void) -> cudaError_enum {
    type CudaFreeType = extern "C" fn(*mut libc::c_void) -> cudaError_enum;
    static FREE_FN: OnceLock<CudaFreeType> = OnceLock::new();
    generate_init_fn!(CudaFreeType, cr"cudaFree");
    let free_func = FREE_FN.get_or_init(init_fn);

    // Check if this pointer is cached in the async pool
    {
        let block = {
            let mut pool = async_pool().lock().unwrap();
            // Search all devices since we don't know which device this ptr belongs to
            let mut found = None;
            for dev in 0..MAX_GPUS {
                if let Some(block) =
                    pool.remove_by_ptr(dev_ptr as u64, ProcessLocalDeviceId(dev as i32))
                {
                    found = Some(block);
                    break;
                }
            }
            found
        };
        if let Some(block) = block {
            // Sync the event to ensure GPU is done, then destroy it
            unsafe {
                cu_api::cuEventSynchronize(block.event);
                cu_api::cuEventDestroy_v2(block.event);
            }
            // Fall through to normal cudaFree logic below — entries are still in tables
        }
    }

    // first check if is non-managed allocation
    if let Some(size) = SMALL_ALLOCATION.lock().unwrap().remove(&(dev_ptr as u64)) {
        CURRENT_ALLOCATION_SIZE.fetch_sub(size, std::sync::atomic::Ordering::Relaxed);
        return free_func(dev_ptr);
    }

    let running_allowed = SCHED_CTL.get_running_is_allowed();

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
            let handle_idx = entry.handle_idx.idx;
            let mut cur_index = Some(entry.handle_idx.idx);
            deallocate_list(handle_idx, &mut table.handle_list);
            let mut released_handles = Vec::new();
            while let Some(index) = cur_index {
                let handle = table.handle_list.get_handle_by_raw_idx(index).unwrap();
                // when is not running, these data may on CPU; daemon should be informed to release them
                if !running_allowed {
                    released_handles.push((
                        ProcessLocalDeviceId(possible_dev as i32),
                        PhysicalMemoryHandleId {
                            alloc_generation: handle.alloc_generation,
                            idx: index,
                        },
                        handle.size,
                    ));
                }
                cur_index = handle.next_handle_idx;
                table.handle_list.free_handle_by_raw_idx(index);
            }
            CURRENT_ALLOCATION_SIZE
                .fetch_sub(entry.len as u64, std::sync::atomic::Ordering::Relaxed);
            table.entry.remove(entry_idx);
            if !released_handles.is_empty() {
                update_gpu_memory_free(GpuMemoryFreeUpdate {
                    freed_memory: released_handles,
                });
            }
            global_tracker().remove(dev_ptr as u64);
            return cudaError_enum::CUDA_SUCCESS;
        }
    }
    cudaError_enum::CUDA_ERROR_INVALID_VALUE
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn cudaMallocAsync(
    dev_ptr: *mut *mut libc::c_void,
    size: usize,
    stream: CUstream,
) -> cudaError_enum {
    init_cuda_env();
    let device_id = get_device();

    SCHED_CTL.launch_allowed(LaunchType::Malloc);

    // Compute effective size (same rounding as cudaMalloc)
    let effective_size = if size >= MIN_ALLOCATION_SIZE {
        (size + MIN_ALLOCATION_SIZE - 1) & !(MIN_ALLOCATION_SIZE - 1)
    } else {
        size
    };

    // Try the async pool first — only returns blocks whose events have completed
    {
        let mut pool = async_pool().lock().unwrap();
        if let Some(block) = pool.try_alloc(effective_size, ProcessLocalDeviceId(device_id)) {
            // Event is already completed (cuEventQuery succeeded in try_alloc), just destroy it
            unsafe { cu_api::cuEventDestroy_v2(block.event) };
            unsafe { *dev_ptr = block.ptr as *mut libc::c_void };
            return cudaError_enum::CUDA_SUCCESS;
        }
    }

    // No cached block available — fall through to cudaMalloc after synchronization
    let res = unsafe { cu_api::cuStreamSynchronize(stream) };
    if res != cudaError_enum::CUDA_SUCCESS {
        return res;
    }
    let result = cudaMalloc(dev_ptr, size);

    // On OOM: release cached blocks and retry
    if result == cudaError_enum::CUDA_ERROR_OUT_OF_MEMORY {
        let blocks = {
            let mut pool = async_pool().lock().unwrap();
            pool.release_cached(device_id)
        };
        if !blocks.is_empty() {
            free_cached_blocks(blocks);
            return cudaMalloc(dev_ptr, size);
        }
    }

    result
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn cudaFreeAsync(dev_ptr: *mut libc::c_void, stream: CUstream) -> cudaError_enum {
    const MAX_ASYNC_CACHE_SIZE: u64 = 256 * 1024 * 1024;
    if dev_ptr.is_null() {
        return cudaError_enum::CUDA_SUCCESS;
    }

    let ptr = dev_ptr as u64;

    // Look up the actual allocation size
    let record = match global_tracker().find_exact(dev_ptr as u64) {
        Some(info) => info,
        None => return cudaError_enum::CUDA_ERROR_INVALID_VALUE,
    };

    // For allocation that is too large, free directly without caching
    if record.alloc_size > MAX_ASYNC_CACHE_SIZE {
        unsafe { cu_api::cuStreamSynchronize(stream) };
        return cudaFree(dev_ptr);
    }

    // Create and record an event on the stream to track when prior work completes
    let mut event: cudarc::driver::sys::CUevent = std::ptr::null_mut();
    let err = unsafe {
        cu_api::cuEventCreate(
            &mut event,
            CUevent_flags_enum::CU_EVENT_DISABLE_TIMING as u32,
        )
    };
    if err != cudaError_enum::CUDA_SUCCESS {
        // Fallback: synchronize stream and do regular free
        unsafe { cu_api::cuStreamSynchronize(stream) };
        return cudaFree(dev_ptr);
    }

    let err = unsafe { cu_api::cuEventRecord(event, stream) };
    if err != cudaError_enum::CUDA_SUCCESS {
        unsafe { cu_api::cuEventDestroy_v2(event) };
        unsafe { cu_api::cuStreamSynchronize(stream) };
        return cudaFree(dev_ptr);
    }

    // Cache the block in the pool — no stats changes, block stays tracked as allocated
    {
        let mut pool = async_pool().lock().unwrap();
        pool.cache_free(
            CachedBlock {
                ptr,
                actual_size: record.alloc_size as usize,
                event,
            },
            record.device,
        );
    }

    cudaError_enum::CUDA_SUCCESS
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
#[unsafe(no_mangle)]
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
    SCHED_CTL.launch_allowed(LaunchType::Transfer(size));
    SCHED_CTL.record_blocking_transfer_start();
    let res = memcpy_func(dst, src, size, kind);
    SCHED_CTL.record_blocking_transfer_end();
    res
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
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
    SCHED_CTL.launch_allowed(LaunchType::Transfer(size));
    // when cudaMemcpyAsync is HostToDevice or DeviceToHost, it may block the host
    match kind {
        CudaMemcpyKind::HostToDevice | CudaMemcpyKind::DeviceToHost => {
            SCHED_CTL.record_blocking_transfer_start();
        }
        _ => {}
    }
    let res = memcpy_async_func(dst, src, size, kind, stream);
    match kind {
        CudaMemcpyKind::HostToDevice | CudaMemcpyKind::DeviceToHost => {
            SCHED_CTL.record_blocking_transfer_end();
        }
        _ => {}
    }
    res
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn cudaMemset(
    dev_ptr: *mut libc::c_void,
    value: i32,
    size: usize,
) -> cudaError_enum {
    type CudaMemsetType = extern "C" fn(*mut libc::c_void, i32, usize) -> cudaError_enum;
    static MEMSET_FN: OnceLock<CudaMemsetType> = OnceLock::new();
    generate_init_fn!(CudaMemsetType, cr"cudaMemset");
    let memset_func = MEMSET_FN.get_or_init(init_fn);
    SCHED_CTL.launch_allowed(LaunchType::Transfer(size));
    memset_func(dev_ptr, value, size)
}

// cudaMemGetInfo
#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn cudaMemGetInfo(free: *mut usize, total: *mut usize) -> cudaError_enum {
    let mem_get_info_func = get_func_ptr_cuda_mem_get_info();
    init_cuda_env();
    let mut device_id = 0;
    check_cu_err!(
        unsafe { cu_api::cuCtxGetDevice(&mut device_id as *mut _) },
        "get device"
    );
    let res = mem_get_info_func(free, total);
    if res != cudaError_enum::CUDA_SUCCESS {
        return res;
    }
    // override free size
    let available_size = get_max_allocation_size(device_id)
        .saturating_sub(CURRENT_ALLOCATION_SIZE.load(std::sync::atomic::Ordering::Relaxed));
    unsafe {
        *free = available_size as usize;
    }
    res
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn open(path: *const c_char, oflag: c_int, mode: mode_t) -> c_int {
    type OpenType = extern "C" fn(*const c_char, c_int, mode_t) -> c_int;
    static OPEN_FN: OnceLock<OpenType> = OnceLock::new();
    generate_init_fn!(OpenType, cr"open");
    let open_func = OPEN_FN.get_or_init(init_fn);
    let res = open_func(path, oflag, mode);
    init_all_entrypoint();
    res
}
