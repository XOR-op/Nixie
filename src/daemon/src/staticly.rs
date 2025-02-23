use nvml_wrapper::Nvml;
use std::sync::OnceLock;

static NVML: OnceLock<Nvml> = OnceLock::new();

pub fn get_nvml() -> &'static Nvml {
    NVML.get_or_init(|| Nvml::init().expect("Failed to initialize NVML"))
}
