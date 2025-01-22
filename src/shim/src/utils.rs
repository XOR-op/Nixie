use std::sync::OnceLock;

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
        if should_log(2) {
            println!($($arg)*);
        }
    };
}

#[macro_export]
macro_rules! warn_eprintln {
    ($($arg:tt)*) => {
        if should_log(1) {
            println!($($arg)*);
        }
    };
}
