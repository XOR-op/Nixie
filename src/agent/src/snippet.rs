use colored::Colorize;
use cudarc::driver::sys::{cudaError_enum, lib as cuda_lib, CUdevice};
use std::sync::mpsc;

use crate::{
    info_eprintln, utils::should_log, utils::size_to_string, warn_eprintln, CuStreamWrapper,
    GENERIC_DATA, PREFETCH_REQ_QUEUE, STREAM_VEC,
};

fn prefetch_impl(size_mb: u64) {
    let streams = STREAM_VEC.get().unwrap();
    let mut prefetch_cnt = 0;
    let mut stream_idx = 0;
    let ptr_mapping = GENERIC_DATA.get().unwrap().lock_ptr_mapping();
    for pair in ptr_mapping.iter() {
        if prefetch_cnt > streams.len() * 40 {
            break;
        }
        let ptr = pair.addr;
        let size = pair.len;
        if size >= 1024 * 1024 * size_mb as usize {
            let start = std::time::Instant::now();
            let res = unsafe {
                cuda_lib().cuMemPrefetchAsync(
                    ptr,
                    size,
                    CUdevice::from(pair.device),
                    streams[stream_idx].0,
                )
            };
            if res != cudaError_enum::CUDA_SUCCESS {
                warn_eprintln!("Failed to prefetch memory: {:?}", res);
            }
            prefetch_cnt += 1;
            stream_idx = (stream_idx + 1) % streams.len();
            info_eprintln!(
                "Prefetch: size={}, time={:?}",
                size_to_string(size),
                start.elapsed()
            )
        }
    }
}

#[no_mangle]
pub extern "C" fn _nihilphase_prefetch(size_mb: u64) -> u64 {
    let _ = STREAM_VEC.get_or_init(|| {
        let mut vec = Vec::new();
        for _ in 0..8 {
            let mut stream = std::ptr::null_mut();
            let res = unsafe {
                cuda_lib().cuStreamCreate(
                    &mut stream,
                    cudarc::driver::sys::CUstream_flags_enum::CU_STREAM_NON_BLOCKING as u32,
                )
            };
            if res != cudaError_enum::CUDA_SUCCESS {
                panic!("Failed to create stream: {:?}", res);
            }
            vec.push(CuStreamWrapper(stream));
        }
        vec
    });
    let sender = PREFETCH_REQ_QUEUE.get_or_init(|| {
        let (sender, receiver) = mpsc::channel();
        std::thread::spawn(move || {
            while let Ok(mut len) = receiver.recv() {
                // exhaust the queue
                while let Ok(l) = receiver.try_recv() {
                    len = l;
                }
                prefetch_impl(len);
            }
            warn_eprintln!("WARN: Prefetch thread exited");
        });
        sender
    });
    info_eprintln!(
        "{} {}: size={}MB",
        "[libcuda_hook]".bold(),
        "_nihilphase_prefetch".blue(),
        size_mb
    );
    dbg!(sender.send(size_mb).ok());
    0
}

#[no_mangle]
pub extern "C" fn _nihilphase_advise_read_mostly(read_mostly: bool, size_threshold_mb: u64) -> u64 {
    info_eprintln!(
        "{} {}: read_mostly={}, size_threshold={}MB",
        "[libcuda_hook]".bold(),
        "_nihilphase_advise_read_mostly".blue(),
        format!("{}", read_mostly).yellow(),
        size_threshold_mb
    );
    let mapping = GENERIC_DATA.get().unwrap().lock_ptr_mapping();
    let mut cadidate_cnt = 0;
    unsafe {
        for entry in mapping.iter() {
            if entry.len < (size_threshold_mb as usize * 1024 * 1024) {
                continue;
            }
            cadidate_cnt += 1;
            let res = cuda_lib().cuMemAdvise(
                entry.addr,
                entry.len,
                if read_mostly {
                    cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_SET_READ_MOSTLY
                } else {
                    cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_UNSET_READ_MOSTLY
                },
                entry.device,
            );
            if res != cudaError_enum::CUDA_SUCCESS {
                warn_eprintln!("Failed to set read mostly: {:?}", res);
            }
        }
    }
    cadidate_cnt
}

#[no_mangle]
pub extern "C" fn _nihilphase_disable_read_duplication(
    address: u64,
    length: u64,
    device: u64,
) -> u64 {
    info_eprintln!(
        "{} {}: address={:#018x}, length={}, device={}",
        "[libcuda_hook]".bold(),
        "_nihilphase_disable_read_duplication".blue(),
        address,
        length,
        device
    );
    let res = unsafe {
        cuda_lib().cuMemAdvise(
            address,
            length as usize,
            cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_UNSET_READ_MOSTLY,
            device as i32,
        )
    };
    if res != cudaError_enum::CUDA_SUCCESS {
        warn_eprintln!("Failed to unset read mostly: {:?}", res);
        return 1;
    }
    0
}
