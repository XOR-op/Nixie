use std::{collections::HashMap, num::NonZeroU64};

use cudarc::driver::sys::{cudaError_enum, lib as cuda_lib, CUmem_advise};
use nihilipc::shm::AllocationEntry;

use crate::{
    intercept::VALID_UVM_FD,
    memory::{prefetch::prefetch_call, CUDA_CPU_DEVICE_ID},
    schedule::uvm_api::{self, UvmSetReadDuplicationParams, UvmUnsetPreferredLocationParams},
    utils::{set_device, size_to_string},
    warn_eprintln, GENERIC_DATA, STREAM_VEC,
};

use super::uvm_api::NV_PROCESSOR_UUID_CPU_DEFAULT;

// release most `size_mb` MB of memory
pub(crate) fn release_gpu_mem(size_mb: Vec<Option<NonZeroU64>>, blocking: bool) {
    let streams = STREAM_VEC.get().unwrap();
    let stream_idx = 0;
    let mut remaining_bytes = size_mb
        .iter()
        .enumerate()
        .filter_map(|(dev_idx, size_mb)| {
            size_mb.map(|size_mb| (dev_idx, 1024 * 1024 * size_mb.get() as usize))
        })
        .collect::<HashMap<_, _>>();
    let mut ptr_mapping = GENERIC_DATA.get().unwrap().lock_ptr_mapping();
    let mut cur_cuda_device = -1;
    for (alloc_idx, entry) in ptr_mapping.iter_mut().enumerate() {
        let device_idx = entry.device as usize;
        if remaining_bytes.is_empty() {
            // all memory released
            break;
        }
        let Some(mut evict_bytes) = remaining_bytes.get(&device_idx).cloned() else {
            continue;
        };
        if evict_bytes > entry.len {
            evict_bytes = entry.len;
        } else {
            // all required memories on this device have been released
            remaining_bytes.remove(&device_idx);
        }

        let start = std::time::Instant::now();
        if entry.is_readonly {
            move_readonly_mem(entry, evict_bytes, &mut cur_cuda_device);
            warn_eprintln!(
                "Release #{}: size={}, time={:?}",
                alloc_idx,
                size_to_string(evict_bytes),
                start.elapsed()
            );
        } else {
            prefetch_call(entry, Some(evict_bytes), false, &streams[stream_idx]);
        }
    }
    if blocking {
        let res = unsafe { cuda_lib().cuStreamSynchronize(streams[stream_idx].0) };
        if res != cudaError_enum::CUDA_SUCCESS {
            warn_eprintln!("Failed to synchronize stream: {:?}", res);
        }
    }
}

fn checked_error(res: cudaError_enum, error_msg: &str) {
    if res != cudaError_enum::CUDA_SUCCESS {
        warn_eprintln!("CUDA error: {:?} from {}", res, error_msg);
    }
}

#[allow(dead_code)]
fn mem_advise_lite(addr: u64, size: u64, advice: CUmem_advise, device: i32) {
    let uvm_fd = *VALID_UVM_FD.get().expect("UVM FD not set");
    let (ioctl_res, rm_status) = match advice {
        CUmem_advise::CU_MEM_ADVISE_SET_READ_MOSTLY => {
            let mut param = UvmSetReadDuplicationParams {
                requested_base: addr,
                length: size,
                rm_status: 0,
            };
            let ioctl_res = unsafe {
                uvm_api::uvm_enable_read_duplication(
                    uvm_fd,
                    &mut param as *mut uvm_api::UvmSetReadDuplicationParams,
                )
            };
            (ioctl_res, param.rm_status)
        }
        CUmem_advise::CU_MEM_ADVISE_UNSET_READ_MOSTLY => {
            let mut param = UvmSetReadDuplicationParams {
                requested_base: addr,
                length: size,
                rm_status: 0,
            };
            let ioctl_res = unsafe {
                uvm_api::uvm_disable_read_duplication(
                    uvm_fd,
                    &mut param as *mut uvm_api::UvmSetReadDuplicationParams,
                )
            };
            (ioctl_res, param.rm_status)
        }
        CUmem_advise::CU_MEM_ADVISE_SET_PREFERRED_LOCATION => {
            if device != CUDA_CPU_DEVICE_ID {
                warn_eprintln!("GPU set_preferred_location is not supported by mem_advise_lite, use mem_advise instead.");
                return;
            }
            let mut param = uvm_api::UvmSetPreferredLocationParams {
                requested_base: addr,
                length: size,
                processor: NV_PROCESSOR_UUID_CPU_DEFAULT,
                preferred_cpu_numa_node: 0,
                rm_status: 0,
            };
            let ioctl_res = unsafe {
                uvm_api::uvm_set_preferred_location(
                    uvm_fd,
                    &mut param as *mut uvm_api::UvmSetPreferredLocationParams,
                )
            };
            (ioctl_res, param.rm_status)
        }
        CUmem_advise::CU_MEM_ADVISE_UNSET_PREFERRED_LOCATION => {
            let mut param = UvmUnsetPreferredLocationParams {
                requested_base: addr,
                length: size,
                rm_status: 0,
            };
            let ioctl_res = unsafe {
                uvm_api::uvm_unset_preferred_location(
                    uvm_fd,
                    &mut param as *mut UvmUnsetPreferredLocationParams,
                )
            };
            (ioctl_res, param.rm_status)
        }
        variant @ _ => unreachable!("{:?} is not supported", variant),
    };
    if rm_status != 0 || ioctl_res.is_err() {
        warn_eprintln!(
            "Fail to set {:?}: ioctl={:?}, rm_status={}",
            advice,
            ioctl_res,
            rm_status
        );
    }
}

fn move_readonly_mem(entry: &AllocationEntry, size_bytes: usize, cur_cuda_device: &mut i32) {
    /*
     * 1. set prefered location to CPU
     * 2. unset read mostly to invalidate pages on GPU
     * 3. unset prefered location
     * 4. reset read duplication
     */
    if entry.device != *cur_cuda_device {
        set_device(entry.device);
        *cur_cuda_device = entry.device;
    }
    let start = std::time::Instant::now();
    checked_error(
        unsafe {
            cuda_lib().cuMemAdvise(
                entry.addr,
                size_bytes,
                CUmem_advise::CU_MEM_ADVISE_SET_PREFERRED_LOCATION,
                CUDA_CPU_DEVICE_ID,
            )
        },
        "set prefered location to CPU",
    );
    checked_error(
        unsafe {
            cuda_lib().cuMemAdvise(
                entry.addr,
                size_bytes,
                CUmem_advise::CU_MEM_ADVISE_UNSET_READ_MOSTLY,
                entry.device,
            )
        },
        "unset read mostly",
    );
    warn_eprintln!("Evict costs: {:?}", start.elapsed());
    checked_error(
        unsafe {
            cuda_lib().cuMemAdvise(
                entry.addr,
                size_bytes,
                CUmem_advise::CU_MEM_ADVISE_UNSET_PREFERRED_LOCATION,
                entry.device, // this is ignored
            )
        },
        "unset read mostly",
    );
    checked_error(
        unsafe {
            cuda_lib().cuMemAdvise(
                entry.addr,
                size_bytes,
                CUmem_advise::CU_MEM_ADVISE_SET_READ_MOSTLY,
                entry.device,
            )
        },
        "unset read mostly",
    );
}
