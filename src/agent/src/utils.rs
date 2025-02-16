use std::sync::OnceLock;

use cudarc::driver::sys::lib as cuda_lib;

pub(crate) fn size_to_string(size: usize) -> String {
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

static LOG_LEVEL: OnceLock<u8> = OnceLock::new();
pub(crate) fn should_log(level: u8) -> bool {
    let expect = LOG_LEVEL.get_or_init(|| {
        let level = std::env::var("NIHIL_LOG").unwrap_or("1".to_string());
        match level.to_lowercase().as_str() {
            "0" | "none" => 0,
            "1" | "warn" | "error" => 1,
            "2" | "info" => 2,
            _ => 0,
        }
    });
    *expect >= level
}

#[macro_export]
macro_rules! info_eprintln {
    ($($arg:tt)*) => {
        if crate::utils::should_log(2) {
            eprintln!("{} {}", colored::Colorize::green("NIHIL-INFO"), format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! warn_eprintln {
    ($($arg:tt)*) => {
        if crate::utils::should_log(1) {
            eprintln!("{} {}", colored::Colorize::yellow("NIHIL-WARN"), format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! check_cu_err {
    ($res:expr, $msg:literal) => {
        if $res != cudarc::driver::sys::cudaError_enum::CUDA_SUCCESS {
            crate::warn_eprintln!("CUDA error from {}: {:?}", $msg, $res);
        }
    };
}

pub(crate) fn set_device(dev: i32) {
    let mut cu_ctx = std::ptr::null_mut();
    let res = unsafe { cuda_lib().cuDevicePrimaryCtxRetain(&mut cu_ctx, dev) };
    check_cu_err!(res, "cuCtxGetCurrent");
    assert!(!cu_ctx.is_null());
    let res = unsafe { cuda_lib().cuCtxSetCurrent(cu_ctx) };
    check_cu_err!(res, "cuCtxSetCurrent");
}
