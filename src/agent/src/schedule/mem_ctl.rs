use cudarc::driver::sys::{cudaError_enum, lib as cuda_lib};

use crate::{
    memory::{prefetch::prefetch_call, CUDA_CPU_DEVICE_ID},
    utils::{set_device, size_to_string},
    warn_eprintln, GENERIC_DATA, STREAM_VEC,
};

// release most `size_mb` MB of memory
pub(crate) fn release_gpu_mem(size_mb: u64, blocking: bool) {
    let size_mb = size_mb as usize;
    let streams = STREAM_VEC.get().unwrap();
    let stream_idx = 0;
    let mut accu_bytes = 0;
    let mut ptr_mapping = GENERIC_DATA.get().unwrap().lock_ptr_mapping();
    let mut cur_cuda_device = -1;
    for (alloc_idx, entry) in ptr_mapping.iter_mut().enumerate() {
        if accu_bytes >= 1024 * 1024 * size_mb {
            break;
        }
        let size_bytes = std::cmp::min(1024 * 1024 * size_mb - accu_bytes, entry.len);
        if entry.is_readonly {
            /*
             * 1. set prefered location to CPU
             * 2. unset read mostly to invalidate pages on GPU
             * 3. unset prefered location
             * 4. reset read duplication
             */
            if entry.device != cur_cuda_device {
                set_device(entry.device);
                cur_cuda_device = entry.device;
            }
            let start = std::time::Instant::now();
            checked_error(
                unsafe {
                    cuda_lib().cuMemAdvise(
                    entry.addr.get(),
                    size_bytes,
                    cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_SET_PREFERRED_LOCATION,
                    CUDA_CPU_DEVICE_ID,
                )
                },
                "set prefered location to CPU",
            );
            checked_error(
                unsafe {
                    cuda_lib().cuMemAdvise(
                        entry.addr.get(),
                        size_bytes,
                        cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_UNSET_READ_MOSTLY,
                        entry.device,
                    )
                },
                "unset read mostly",
            );
            warn_eprintln!("Evict costs: {:?}", start.elapsed());
            checked_error(
                unsafe {
                    cuda_lib().cuMemAdvise(
                    entry.addr.get(),
                    size_bytes,
                    cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_UNSET_PREFERRED_LOCATION,
                    entry.device, // this is ignored
                )
                },
                "unset read mostly",
            );
            checked_error(
                unsafe {
                    cuda_lib().cuMemAdvise(
                        entry.addr.get(),
                        size_bytes,
                        cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_SET_READ_MOSTLY,
                        entry.device,
                    )
                },
                "unset read mostly",
            );
            warn_eprintln!(
                "Release #{}: size={}, time={:?}",
                alloc_idx,
                size_to_string(size_bytes),
                start.elapsed()
            );
        } else {
            prefetch_call(entry, Some(size_bytes), false, &streams[stream_idx]);
        }
        accu_bytes += size_bytes;
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
