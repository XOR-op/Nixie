use std::num::NonZeroU32;

use cudarc::driver::sys::{
    cudaError_enum, lib as cuda_lib, CUmemAllocationHandleType, CUmemAllocationType, CUmemLocation,
    CUmemLocationType,
};
use nihil_common::shm::{AllocationEntry, AllocationTable};

use crate::check_cu_err;

pub(crate) fn populate_entry(
    entry: &AllocationEntry,
    device_id: i32,
    table: &mut AllocationTable,
) -> bool {
    // Populate memory
    let mut has_requested_reservation = false;
    let mut remaining_size = entry.len;
    let mut cur_index = Some(entry.handle_idx);
    let alloc_prop = cudarc::driver::sys::CUmemAllocationProp {
        type_: CUmemAllocationType::CU_MEM_ALLOCATION_TYPE_PINNED,
        requestedHandleTypes: CUmemAllocationHandleType::CU_MEM_HANDLE_TYPE_NONE,
        location: CUmemLocation {
            type_: CUmemLocationType::CU_MEM_LOCATION_TYPE_DEVICE,
            id: device_id,
        },
        win32HandleMetaData: std::ptr::null_mut(),
        allocFlags: Default::default(),
    };
    while let Some(index) = cur_index {
        let handle = table.get_handle_mut(index).unwrap();
        let mut cu_handle = 0u64;
        let mut res = unsafe {
            cuda_lib().cuMemCreate(
                &mut cu_handle as *mut _,
                handle.size,
                &alloc_prop as *const _,
                0,
            )
        };
        if res == cudaError_enum::CUDA_ERROR_OUT_OF_MEMORY && !has_requested_reservation {
            has_requested_reservation = true;
            reserve_memory(remaining_size);
            res = unsafe {
                cuda_lib().cuMemCreate(
                    &mut cu_handle as *mut _,
                    handle.size,
                    &alloc_prop as *const _,
                    0,
                )
            };
        }
        if res != cudaError_enum::CUDA_SUCCESS {
            // deallocate all previously allocated handles
            deallocate_list(entry.handle_idx, table);
            return false;
        }
        handle.cu_handle = Some(cu_handle);
        handle.on_gpu = true;
        remaining_size -= handle.size;
        cur_index = handle.next_handle_idx;
    }
    assert_eq!(remaining_size, 0);
    let access_desc = cudarc::driver::sys::CUmemAccessDesc {
        location: CUmemLocation {
            type_: CUmemLocationType::CU_MEM_LOCATION_TYPE_DEVICE,
            id: device_id,
        },
        flags: cudarc::driver::sys::CUmemAccess_flags::CU_MEM_ACCESS_FLAGS_PROT_READWRITE,
    };
    cur_index = Some(entry.handle_idx);
    while let Some(index) = cur_index {
        let handle = table.get_handle_mut(index).unwrap();
        if handle.on_gpu {
            // Map memory to the device
            check_cu_err!(
                unsafe {
                    cuda_lib().cuMemMap(handle.addr, handle.size, 0, handle.cu_handle.unwrap(), 0)
                },
                "Failed to map memory"
            );
            // then set access
            check_cu_err!(
                unsafe {
                    cuda_lib().cuMemSetAccess(handle.addr, handle.size, &access_desc as *const _, 1)
                },
                "Failed to set memory access"
            )
        }
        cur_index = handle.next_handle_idx;
    }
    true
}

pub(crate) fn deallocate_list(start_idx: NonZeroU32, table: &mut AllocationTable) {
    let mut cur_index = Some(start_idx);
    while let Some(index) = cur_index {
        let handle = table.get_handle_mut(index).unwrap();
        if handle.on_gpu {
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
            handle.cu_handle = None;
        }
        handle.on_gpu = false;
        cur_index = handle.next_handle_idx;
    }
}

pub(crate) fn reserve_memory(size: usize) {
    todo!()
}
