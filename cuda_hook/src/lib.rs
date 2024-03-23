use cudarc::driver::sys::{cuMemPrefetchAsync, cuStreamCreate, cudaError_enum, CUdevice, CUstream};
use dashmap::DashMap;
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

static STREAM_VEC: OnceCell<Vec<CuStreamWrapper>> = OnceCell::new();

static PTR_MAPPING: OnceCell<DashMap<u64, usize>> = OnceCell::new();

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
            .get_or_init(|| DashMap::new())
            .insert(unsafe { *dev_ptr as u64 }, size);
        let total_size = PTR_MAPPING
            .get()
            .unwrap()
            .iter()
            .map(|re| *re.value())
            .sum();
        println!(
            "cudaMalloc: size={}, total_size={}, count={}",
            size_to_string(size),
            size_to_string(total_size),
            PTR_MAPPING.get().unwrap().len()
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
    PTR_MAPPING
        .get_or_init(|| DashMap::new())
        .remove(&(dev_ptr as u64));
    return free_func(dev_ptr);
}

#[no_mangle]
pub extern "C" fn _auto_gmem_prefetch(size_mb: u64) {
    println!("Hello from _auto_gmem_prefetch >={size_mb}MB");
    let streams = STREAM_VEC.get_or_init(|| {
        let mut vec = Vec::new();
        for _ in 0..8 {
            let mut stream = std::ptr::null_mut();
            let res = unsafe { cuStreamCreate(&mut stream, 0) };
            if res != cudaError_enum::CUDA_SUCCESS {
                panic!("Failed to create stream: {:?}", res);
            }
            vec.push(CuStreamWrapper(stream));
        }
        vec
    });
    let mut prefetch_cnt = 0;
    let mut stream_idx = 0;
    for re in PTR_MAPPING.get().unwrap().iter() {
        if prefetch_cnt > streams.len() * 40 {
            break;
        }
        let ptr = *re.key();
        let size = *re.value();
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
