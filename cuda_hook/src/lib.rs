use cudarc::driver::sys::CUstream;
use std::sync::{mpsc, Mutex, OnceLock};

mod comm;
mod intercept;
mod snippet;
mod utils;

struct CuStreamWrapper(CUstream);
unsafe impl Send for CuStreamWrapper {}
unsafe impl Sync for CuStreamWrapper {}

/// For some reasons, cudaMemPrefetchAsync exhibits blocking behavior.
/// Use a separate thread to prefetch.
pub(crate) static PREFETCH_REQ_QUEUE: OnceLock<mpsc::Sender<u64>> = OnceLock::new();

/// All streams used for prefetching
pub(crate) static STREAM_VEC: OnceLock<Vec<CuStreamWrapper>> = OnceLock::new();

/// Global mapping of device pointers and their sizes
pub(crate) static PTR_MAPPING: OnceLock<Mutex<Vec<(u64, usize)>>> = OnceLock::new();
