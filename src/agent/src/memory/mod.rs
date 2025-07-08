mod streaming;
use std::num::NonZeroU32;

use cudarc::driver::sys::{
    cudaError_enum, lib as cuda_lib, CUmemAccessDesc, CUmemAllocationHandleType,
    CUmemAllocationProp, CUmemAllocationType, CUmemLocation, CUmemLocationType,
};
use nihil_common::{
    general::CallParameter,
    shm::{AllocationEntry, AllocationTable, HandleList, PhysicalMemoryHandle},
    MemoryRequest,
};

use crate::{check_cu_err, comm::request_memory};
pub use streaming::{init_memory_migration_ctl, MEMORY_MIGRATION_CTL};

pub(super) fn default_alloc_prop(device: i32) -> CUmemAllocationProp {
    CUmemAllocationProp {
        type_: CUmemAllocationType::CU_MEM_ALLOCATION_TYPE_PINNED,
        requestedHandleTypes: CUmemAllocationHandleType::CU_MEM_HANDLE_TYPE_NONE,
        location: CUmemLocation {
            type_: CUmemLocationType::CU_MEM_LOCATION_TYPE_DEVICE,
            id: device,
        },
        win32HandleMetaData: std::ptr::null_mut(),
        allocFlags: Default::default(),
    }
}

pub(super) fn default_access_desc(device: i32) -> CUmemAccessDesc {
    CUmemAccessDesc {
        location: CUmemLocation {
            type_: CUmemLocationType::CU_MEM_LOCATION_TYPE_DEVICE,
            id: device,
        },
        flags: cudarc::driver::sys::CUmemAccess_flags::CU_MEM_ACCESS_FLAGS_PROT_READWRITE,
    }
}

pub(crate) fn populate_entry(
    entry: &AllocationEntry,
    device_id: i32,
    table: &mut AllocationTable,
) -> bool {
    // Populate memory
    let mut has_requested_reservation = false;
    let mut remaining_size = entry.len;
    let mut cur_index = Some(entry.handle_idx);
    let alloc_prop = default_alloc_prop(device_id);
    while let Some(index) = cur_index {
        let handle = table.handle_list.get_handle_mut(index).unwrap();
        let res = alloc_for_mem_handle(handle, &alloc_prop);
        if let Err(mut res) = res {
            if res == cudaError_enum::CUDA_ERROR_OUT_OF_MEMORY && !has_requested_reservation {
                has_requested_reservation = true;
                reserve_memory_blocking(nihil_common::MemoryRequest {
                    mem_req: std::array::from_fn(|idx| {
                        if idx == device_id as usize {
                            vec![handle.size as u64]
                        } else {
                            Vec::new()
                        }
                    }),
                });
                if let Err(res2) = alloc_for_mem_handle(handle, &alloc_prop) {
                    res = res2;
                }
            }
            if res != cudaError_enum::CUDA_SUCCESS {
                // deallocate all previously allocated handles
                deallocate_list(entry.handle_idx, &mut table.handle_list);
                return false;
            }
        }
        remaining_size -= handle.size;
        cur_index = handle.next_handle_idx;
    }
    assert_eq!(remaining_size, 0);
    let access_desc = default_access_desc(device_id);
    cur_index = Some(entry.handle_idx);
    while let Some(index) = cur_index {
        let handle = table.handle_list.get_handle_mut(index).unwrap();
        if handle.on_gpu {
            // Map memory to the device
            map_mem_handle(handle, &access_desc);
        }
        cur_index = handle.next_handle_idx;
    }
    true
}

pub(super) fn alloc_for_mem_handle(
    handle: &mut PhysicalMemoryHandle,
    alloc_prop: &CUmemAllocationProp,
) -> Result<(), cudaError_enum> {
    let mut cu_handle = 0u64;
    let res = unsafe {
        cuda_lib().cuMemCreate(
            &mut cu_handle as *mut _,
            handle.size,
            alloc_prop as *const _,
            0,
        )
    };
    if res != cudaError_enum::CUDA_SUCCESS {
        return Err(res);
    }
    handle.cu_handle = Some(cu_handle);
    handle.on_gpu = true;
    Ok(())
}

pub(super) fn map_mem_handle(handle: &PhysicalMemoryHandle, access_desc: &CUmemAccessDesc) {
    check_cu_err!(
        unsafe { cuda_lib().cuMemMap(handle.addr, handle.size, 0, handle.cu_handle.unwrap(), 0) },
        "Failed to map memory"
    );

    check_cu_err!(
        unsafe { cuda_lib().cuMemSetAccess(handle.addr, handle.size, access_desc as *const _, 1) },
        "Failed to set memory access"
    );
}

pub(super) fn unmap_and_release_mem_handle(handle: &PhysicalMemoryHandle) {
    // unmap first, where the access will be invalidated automatically
    check_cu_err!(
        unsafe { cuda_lib().cuMemUnmap(handle.cu_handle.unwrap(), handle.size) },
        "Failed to unmap memory"
    );

    // then release physical allocation
    check_cu_err!(
        unsafe { cuda_lib().cuMemRelease(handle.cu_handle.unwrap()) },
        "Failed to release memory"
    );
}

pub(crate) fn deallocate_list(start_idx: NonZeroU32, handle_list: &mut HandleList) {
    let mut cur_index = Some(start_idx);
    while let Some(index) = cur_index {
        let handle = handle_list.get_handle_mut(index).unwrap();
        if handle.on_gpu {
            // unmap first, where the access will be invalidated automatically
            check_cu_err!(
                unsafe { cuda_lib().cuMemUnmap(handle.addr, handle.size) },
                "Failed to unmap memory"
            );

            // then release physical allocation
            check_cu_err!(
                unsafe { cuda_lib().cuMemRelease(handle.cu_handle.unwrap()) },
                "Failed to release memory"
            );
            handle.cu_handle = None;
        }
        handle.on_gpu = false;
        cur_index = handle.next_handle_idx;
    }
}

pub(crate) fn reserve_memory_blocking(req: MemoryRequest) {
    let (parameter, rx) = CallParameter::new(req);
    request_memory(parameter);
    rx.wait_blocking();
}
