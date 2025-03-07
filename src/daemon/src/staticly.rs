use nvml_wrapper::Nvml;
use std::{ffi::OsStr, sync::OnceLock};

static NVML: OnceLock<Nvml> = OnceLock::new();

pub fn get_nvml() -> &'static Nvml {
    NVML.get_or_init(|| {
        let mut detected_path = None;
        let paths = &[
            "/usr/lib/x86_64-linux-gnu/libnvidia-ml.so",
            "/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.1",
            "/usr/lib/libnvidia-ml.so",
            "/usr/lib/libnvidia-ml.so.1",
            "/usr/local/cuda/targets/x86_64-linux/lib/stubs/libnvidia-ml.so",
        ];
        for path in paths {
            if std::path::Path::new(path).exists() {
                detected_path = Some(path);
                break;
            }
        }
        Nvml::builder()
            .lib_path(OsStr::new(
                detected_path.expect("Failed to find libnvidia-ml"),
            ))
            .init()
            .expect("Failed to initialize NVML")
    })
}
