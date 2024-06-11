use cudarc::driver::sys::{cuMemPrefetchAsync, cuStreamCreate, cudaError_enum, CUdevice};
use std::sync::{mpsc, Mutex};

use crate::{utils::size_to_string, CuStreamWrapper, PREFETCH_REQ_QUEUE, PTR_MAPPING, STREAM_VEC};

fn prefetch_impl(size_mb: u64) {
    println!("Hello from _auto_gmem_prefetch >={size_mb}MB");
    let streams = STREAM_VEC.get().unwrap();
    let mut prefetch_cnt = 0;
    let mut stream_idx = 0;
    for pair in PTR_MAPPING.get().unwrap().lock().unwrap().iter() {
        if prefetch_cnt > streams.len() * 40 {
            break;
        }
        let ptr = pair.0;
        let size = pair.1;
        if size >= 1024 * 1024 * size_mb as usize {
            let start = std::time::Instant::now();
            let res =
                unsafe { cuMemPrefetchAsync(ptr, size, CUdevice::from(0), streams[stream_idx].0) };
            if res != cudaError_enum::CUDA_SUCCESS {
                println!("Failed to prefetch memory: {:?}", res);
            }
            prefetch_cnt += 1;
            stream_idx = (stream_idx + 1) % streams.len();
            println!(
                "Prefetch:  size={}, time={:?}",
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
            println!("WARN: Prefetch thread exited");
        });
        sender
    });
    dbg!(sender.send(size_mb).ok());
    0
}

#[no_mangle]
pub extern "C" fn _auto_gmem_advise_read_mostly(read_mostly: bool, size_threshold_mb: u64) -> u64 {
    println!(
        "Hello from _auto_gmem_advise_read_mostly: read_mostly={}, size_threshold={}MB",
        read_mostly, size_threshold_mb
    );
    let mapping = PTR_MAPPING
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    let mut cadidate_cnt = 0;
    unsafe {
        let mut curr_dev = 0;
        let res = cudarc::driver::sys::cuCtxGetDevice(&mut curr_dev as *mut _);
        if res != cudaError_enum::CUDA_SUCCESS {
            println!("Failed to get current device: {:?}", res);
            return 0;
        }
        for (dev_ptr, size) in mapping.iter() {
            if *size < (size_threshold_mb as usize * 1024 * 1024) {
                continue;
            }
            cadidate_cnt += 1;
            let res = cudarc::driver::sys::cuMemAdvise(
                *dev_ptr as u64,
                *size,
                if read_mostly {
                    cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_SET_READ_MOSTLY
                } else {
                    cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_UNSET_READ_MOSTLY
                },
                curr_dev,
            );
            if res != cudaError_enum::CUDA_SUCCESS {
                println!("Failed to set read mostly: {:?}", res);
            }
        }
    }
    cadidate_cnt
}
