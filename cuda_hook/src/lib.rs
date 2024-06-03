use std::sync::{mpsc, Mutex};

use cudarc::driver::sys::{cuMemPrefetchAsync, cuStreamCreate, cudaError_enum, CUdevice, CUstream};
use nix;
use nix::libc::{self, dlsym, RTLD_NEXT};
use once_cell::sync::OnceCell;

type CudaMallocType = extern "C" fn(*mut *mut libc::c_void, usize, u32) -> cudaError_enum;
type CudaFreeType = extern "C" fn(*mut libc::c_void) -> cudaError_enum;

struct CuStreamWrapper(CUstream);
unsafe impl Send for CuStreamWrapper {}
unsafe impl Sync for CuStreamWrapper {}

static MALLOC_FN: OnceCell<CudaMallocType> = OnceCell::new();
static FREE_FN: OnceCell<CudaFreeType> = OnceCell::new();

/// All streams used for prefetching
static STREAM_VEC: OnceCell<Vec<CuStreamWrapper>> = OnceCell::new();

/// Global mapping of device pointers and their sizes
static PTR_MAPPING: OnceCell<Mutex<Vec<(u64, usize)>>> = OnceCell::new();

/// For some reasons, cudaMemPrefetchAsync exhibits blocking behavior.
/// Use a separate thread to prefetch.
static PREFETCH_REQ_QUEUE: OnceCell<mpsc::Sender<u64>> = OnceCell::new();

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaMalloc(dev_ptr: *mut *mut libc::c_void, size: usize) -> cudaError_enum {
    let malloc_func = MALLOC_FN.get_or_init(|| unsafe {
        let func = dlsym(RTLD_NEXT, cr"cudaMallocManaged".as_ptr()) as *mut CudaMallocType;
        if func.is_null() {
            panic!("Failed to get original cudaMalloc function");
        }
        std::mem::transmute(func)
    });
    let res = malloc_func(dev_ptr, size, 0x01);
    if res == cudaError_enum::CUDA_SUCCESS {
        PTR_MAPPING
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .unwrap()
            .push((unsafe { *dev_ptr as u64 }, size));
        let total_size = PTR_MAPPING
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .iter()
            .map(|pr| pr.1)
            .sum();
        println!(
            "cudaMalloc: size={}, total_size={}, count={}",
            size_to_string(size),
            size_to_string(total_size),
            PTR_MAPPING.get().unwrap().lock().unwrap().len()
        );
    }
    return res;
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn cudaFree(dev_ptr: *mut libc::c_void) -> cudaError_enum {
    let free_func = FREE_FN.get_or_init(|| unsafe {
        let func = dlsym(RTLD_NEXT, cr"cudaFree".as_ptr()) as *mut CudaFreeType;
        if func.is_null() {
            panic!("Failed to get original cudaFree function");
        }
        std::mem::transmute(func)
    });
    let mut mapping = PTR_MAPPING
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    if let Some(idx) = mapping.iter().position(|pr| pr.0 == dev_ptr as u64) {
        mapping.remove(idx);
    }
    return free_func(dev_ptr);
}

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
pub extern "C" fn _auto_gmem_prefetch(size_mb: u64) {
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
}

#[no_mangle]
pub extern "C" fn _auto_gmem_advise_read_mostly(read_mostly: bool) {
    println!("Hello from _auto_gmem_advise_read_mostly: read_mostly={read_mostly}");
    let mapping = PTR_MAPPING
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    unsafe {
        let mut curr_dev = 0;
        let res = cudarc::driver::sys::cuCtxGetDevice(&mut curr_dev as *mut _);
        if res != cudaError_enum::CUDA_SUCCESS {
            println!("Failed to get current device: {:?}", res);
            return;
        }
        for (dev_ptr, size) in mapping.iter() {
            if false && *size < 1024 * 1024 * 512 {
                continue;
            }
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
}

fn size_to_string(size: usize) -> String {
    if size < 1024 {
        return format!("{}B", size);
    }
    let kb = size as f64 / 1024.0;
    if kb < 1024.0 {
        return format!("{:.2}KB", kb);
    }
    let mb = kb / 1024.0;
    if mb < 1024.0 {
        return format!("{:.2}MB", mb);
    }
    let gb = mb / 1024.0;
    return format!("{:.2}GB", gb);
}
