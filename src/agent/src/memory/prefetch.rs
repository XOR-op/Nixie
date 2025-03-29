use colored::Colorize;
use cudarc::driver::sys::{cudaError_enum, lib as cuda_lib, CUdevice};
use nihilipc::shm::AllocationEntry;
use std::sync::mpsc;

use crate::{
    debug_eprintln, info_eprintln, memory::CUDA_CPU_DEVICE_ID, stream_get_or_init,
    utils::size_to_string, warn_eprintln, CuStreamWrapper, GENERIC_DATA, PREFETCH_REQ_QUEUE,
};

pub(crate) fn filtered_prefetch_impl(size_mb: u64, to_gpu: bool, blocking: bool) {
    let streams = stream_get_or_init();
    let stream_idx = 0;
    let mut ptr_mapping = GENERIC_DATA.get().unwrap().lock_ptr_mapping();
    for entry in ptr_mapping.iter_mut() {
        if entry.len >= 1024 * 1024 * size_mb as usize {
            prefetch_call(entry, None, to_gpu, &streams[stream_idx]);
            // stream_idx = (stream_idx + 1) % streams.len();
        }
    }

    if blocking {
        let res = unsafe { cuda_lib().cuStreamSynchronize(streams[stream_idx].0) };
        if res != cudaError_enum::CUDA_SUCCESS {
            warn_eprintln!("Failed to synchronize stream: {:?}", res);
        }
    }
}

pub(crate) fn prefetch_call(
    entry: &mut AllocationEntry,
    size_bytes: Option<usize>,
    to_gpu: bool,
    stream: &CuStreamWrapper,
) {
    use chrono::Local;
    debug_eprintln!("{}: prefetch_call", Local::now().format("%H:%M:%S%.6f"));
    let start = std::time::Instant::now();
    let ptr = entry.addr;
    let size = size_bytes.unwrap_or(entry.len);
    let res = unsafe {
        cuda_lib().cuMemPrefetchAsync(
            ptr,
            size,
            if to_gpu {
                CUdevice::from(entry.device)
            } else {
                CUDA_CPU_DEVICE_ID
            },
            stream.0,
        )
    };
    entry.likely_on_gpu = to_gpu;
    if res != cudaError_enum::CUDA_SUCCESS {
        warn_eprintln!("Failed to prefetch memory: {:?}", res);
    }
    info_eprintln!(
        "Prefetch: size={}, time={:?} to {}",
        size_to_string(size),
        start.elapsed(),
        if to_gpu { "GPU" } else { "CPU" }
    )
}

pub fn filtered_prefetch_non_blocking(size_mb: u64, to_gpu: bool) -> u64 {
    let sender = PREFETCH_REQ_QUEUE.get_or_init(|| {
        let (sender, receiver) = mpsc::channel();
        std::thread::spawn(move || {
            while let Ok(mut len) = receiver.recv() {
                // exhaust the queue
                while let Ok(l) = receiver.try_recv() {
                    len = l;
                }
                filtered_prefetch_impl(len, to_gpu, false);
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
