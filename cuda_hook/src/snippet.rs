use colored::Colorize;
use cudarc::driver::sys::{cuMemPrefetchAsync, cuStreamCreate, cudaError_enum, CUdevice};
use std::sync::mpsc;

use crate::{utils::size_to_string, CuStreamWrapper, GENERIC_DATA, PREFETCH_REQ_QUEUE, STREAM_VEC};

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
            let res =
                unsafe { cuMemPrefetchAsync(ptr, size, CUdevice::from(0), streams[stream_idx].0) };
            if res != cudaError_enum::CUDA_SUCCESS {
                eprintln!("Failed to prefetch memory: {:?}", res);
            }
            prefetch_cnt += 1;
            stream_idx = (stream_idx + 1) % streams.len();
            eprintln!(
                "Prefetch: size={}, time={:?}",
                size_to_string(size),
                start.elapsed()
            )
        }
    }
}

#[no_mangle]
pub extern "C" fn _auto_gmem_prefetch(size_mb: u64) -> u64 {
    let _ = STREAM_VEC.get_or_init(|| {
        let mut vec = Vec::new();
        for _ in 0..8 {
            let mut stream = std::ptr::null_mut();
            let res = unsafe {
                cuStreamCreate(
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
            eprintln!("WARN: Prefetch thread exited");
        });
        sender
    });
    eprintln!(
        "{} {}: size={}MB",
        "[libcuda_hook]".bold(),
        "_auto_gmem_prefetch".green(),
        size_mb
    );
    dbg!(sender.send(size_mb).ok());
    0
}

#[no_mangle]
pub extern "C" fn _auto_gmem_advise_read_mostly(read_mostly: bool, size_threshold_mb: u64) -> u64 {
    eprintln!(
        "{} {}: read_mostly={}, size_threshold={}MB",
        "[libcuda_hook]".bold(),
        "_auto_gmem_advise_read_mostly".green(),
        format!("{}", read_mostly).blue(),
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
            let res = cudarc::driver::sys::cuMemAdvise(
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
                eprintln!("Failed to set read mostly: {:?}", res);
            }
        }
    }
    cadidate_cnt
}

#[no_mangle]
pub extern "C" fn _auto_gmem_advise_read_mostly_for(
    read_mostly: bool,
    address: u64,
    length: u64,
    device: i32,
) -> u64 {
    eprintln!(
        "{} {}: read_mostly={}, address={:#018x}",
        "[libcuda_hook]".bold(),
        "_auto_gmem_advise_read_mostly_for".green(),
        format!("{}", read_mostly).blue(),
        address
    );
    unsafe {
        let res = cudarc::driver::sys::cuMemAdvise(
            address,
            length as usize,
            if read_mostly {
                cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_SET_READ_MOSTLY
            } else {
                cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_UNSET_READ_MOSTLY
            },
            device,
        );
        if res != cudaError_enum::CUDA_SUCCESS {
            eprintln!("Failed to set read mostly: {:?}", res);
            return 1;
        }
        0
    }
}
