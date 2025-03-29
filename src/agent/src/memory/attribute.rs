use std::num::NonZeroU64;

use crate::{debug_eprintln, warn_eprintln, FusedPtrMapping};
use cudarc::driver::sys::{cudaError_enum, lib as cuda_lib};
use nihilipc::AttrType;

pub(crate) fn set_attribute(
    ptr_mapping: &mut FusedPtrMapping<'_>,
    attr_val: AttrType,
    will_set: bool,
    size_mb: u64,
) {
    for entry in ptr_mapping.iter_mut() {
        let ptr = entry.addr;
        let size = entry.len;
        if size >= 1024 * 1024 * size_mb as usize {
            let res = unsafe {
                cuda_lib().cuMemAdvise(
                    ptr,
                    size,
                    compute_cu_advise(attr_val, will_set),
                    entry.device,
                )
            };
            if res != cudaError_enum::CUDA_SUCCESS {
                warn_eprintln!("Failed to set read mostly: {:?}", res);
            }
            match attr_val {
                AttrType::ReadDup => {
                    entry.is_readonly = will_set;
                }
                AttrType::PrefLoc => {}
                AttrType::AccessedBy => {
                    entry.is_move_reduced = will_set;
                }
            }
            debug_eprintln!(
                "Set {:?}: address={:#018x}, size={}, value={}",
                attr_val,
                ptr,
                size,
                will_set
            );
        }
    }
}

// TODO: check address in client side; should read allocation record before calling
pub(crate) fn set_attribute_single(
    ptr_mapping: &mut FusedPtrMapping<'_>,
    attr_val: AttrType,
    will_set: bool,
    address: NonZeroU64,
    length: u64,
    device: i32,
) {
    let address = address.get();
    let entry = ptr_mapping.iter_mut().find(|entry| {
        entry.addr <= address
            && entry.addr + entry.len as u64 >= address + length
            && entry.device == device
    });
    if let Some(entry) = entry {
        let res = unsafe {
            cuda_lib().cuMemAdvise(
                address,
                length as usize,
                compute_cu_advise(attr_val, will_set),
                device,
            )
        };
        if res != cudaError_enum::CUDA_SUCCESS {
            warn_eprintln!("Failed to set read mostly: {:?}", res);
        }
        match attr_val {
            AttrType::ReadDup => {
                entry.is_readonly = will_set;
            }
            AttrType::PrefLoc => {}
            AttrType::AccessedBy => {
                entry.is_move_reduced = will_set;
            }
        };
        debug_eprintln!(
            "Set {:?}: address={:#018x}, size={}, value={}",
            attr_val,
            address,
            length,
            will_set
        );
    } else {
        warn_eprintln!(
            "Failed to find entry: address={:#018x}, size={}, device={}",
            address,
            length,
            device
        );
    }
}

fn compute_cu_advise(attr_val: AttrType, will_set: bool) -> cudarc::driver::sys::CUmem_advise_enum {
    match attr_val {
        AttrType::ReadDup => {
            if will_set {
                cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_SET_READ_MOSTLY
            } else {
                cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_UNSET_READ_MOSTLY
            }
        }
        AttrType::PrefLoc => {
            if will_set {
                cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_SET_PREFERRED_LOCATION
            } else {
                cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_UNSET_PREFERRED_LOCATION
            }
        }
        AttrType::AccessedBy => {
            if will_set {
                cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_SET_ACCESSED_BY
            } else {
                cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_UNSET_ACCESSED_BY
            }
        }
    }
}
