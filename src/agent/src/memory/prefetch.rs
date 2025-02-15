use colored::Colorize;
use cudarc::driver::sys::{cudaError_enum, lib as cuda_lib, CUdevice};
use std::sync::mpsc;

const CUDA_CPU_DEVICE_ID: CUdevice = -1;

use crate::{
    info_eprintln, utils::size_to_string, warn_eprintln, CuStreamWrapper, GENERIC_DATA,
    PREFETCH_REQ_QUEUE, STREAM_VEC,
};

fn prefetch_impl(size_mb: u64, to_gpu: bool) {
    let streams = STREAM_VEC.get().unwrap();
    let mut prefetch_cnt = 0;
    let stream_idx = 0;
    let mut ptr_mapping = GENERIC_DATA.get().unwrap().lock_ptr_mapping();
    for pair in ptr_mapping.iter_mut() {
        if prefetch_cnt > streams.len() * 40 {
            break;
        }
        let ptr = pair.addr;
        let size = pair.len;
        if size >= 1024 * 1024 * size_mb as usize {
            let start = std::time::Instant::now();
            let res = unsafe {
                cuda_lib().cuMemPrefetchAsync(
                    ptr.get(),
                    size,
                    if to_gpu {
                        CUdevice::from(pair.device)
                    } else {
                        CUDA_CPU_DEVICE_ID
                    },
                    streams[stream_idx].0,
                )
            };
            pair.likely_on_gpu = to_gpu;
            if res != cudaError_enum::CUDA_SUCCESS {
                warn_eprintln!("Failed to prefetch memory: {:?}", res);
            }
            prefetch_cnt += 1;
            // stream_idx = (stream_idx + 1) % streams.len();
            warn_eprintln!(
                "Prefetch: size={}, time={:?} to {}",
                size_to_string(size),
                start.elapsed(),
                if to_gpu { "GPU" } else { "CPU" }
            )
        }
    }
}

pub fn filtered_prefetch(size_mb: u64, to_gpu: bool) -> u64 {
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
                prefetch_impl(len, to_gpu);
            }
            warn_eprintln!("WARN: Prefetch thread exited");
        });
        sender
    });
    info_eprintln!(
        "{} {}: size={}MB to {}",
        "[libcuda_hook]".bold(),
        "_nihilphase_prefetch".blue(),
        size_mb,
        if to_gpu { "GPU" } else { "CPU" }
    );
    dbg!(sender.send(size_mb).ok());
    0
}
