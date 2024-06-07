use cudarc::driver::sys::CUstream;
use once_cell::sync::OnceCell;
use std::sync::{mpsc, Mutex};

mod intercept;
mod snippet;
mod utils;

struct CuStreamWrapper(CUstream);
unsafe impl Send for CuStreamWrapper {}
unsafe impl Sync for CuStreamWrapper {}

/// For some reasons, cudaMemPrefetchAsync exhibits blocking behavior.
/// Use a separate thread to prefetch.
pub(crate) static PREFETCH_REQ_QUEUE: OnceCell<mpsc::Sender<u64>> = OnceCell::new();

/// All streams used for prefetching
pub(crate) static STREAM_VEC: OnceCell<Vec<CuStreamWrapper>> = OnceCell::new();

/// Global mapping of device pointers and their sizes
pub(crate) static PTR_MAPPING: OnceCell<Mutex<Vec<(u64, usize)>>> = OnceCell::new();
