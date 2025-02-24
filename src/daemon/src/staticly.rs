use nvml_wrapper::Nvml;
use std::{ffi::OsStr, sync::OnceLock};

static NVML: OnceLock<Nvml> = OnceLock::new();

pub fn get_nvml() -> &'static Nvml {
    NVML.get_or_init(|| {
        Nvml::builder()
            .lib_path(OsStr::new("/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.1"))
            .init()
            .expect("Failed to initialize NVML")
    })
}
