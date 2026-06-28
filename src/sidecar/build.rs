use std::process::Command;

struct CudaCfgThreshold {
    cfg: &'static str,
    major: usize,
    minor: usize,
}

const CUDA_CFG_THRESHOLDS: &[CudaCfgThreshold] = &[CudaCfgThreshold {
    cfg: "nixie_cuda_geq_13_2",
    major: 13,
    minor: 2,
}];

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    for threshold in CUDA_CFG_THRESHOLDS {
        println!("cargo:rustc-check-cfg=cfg({})", threshold.cfg);
    }

    if !feature_enabled("cuda-system") {
        return;
    }

    println!("cargo:rerun-if-env-changed=CUDARC_CUDA_VERSION");
    println!("cargo:rerun-if-env-changed=PATH");

    let cuda_version = detect_cuda_system_version();

    let (major, minor) = cuda_version;
    println!("cargo:rustc-env=NIXIE_CUDA_MAJOR_VERSION={major}");
    println!("cargo:rustc-env=NIXIE_CUDA_MINOR_VERSION={minor}");

    for threshold in CUDA_CFG_THRESHOLDS {
        if cuda_version_at_least(cuda_version, (threshold.major, threshold.minor)) {
            println!("cargo:rustc-cfg={}", threshold.cfg);
        }
    }
}

fn feature_enabled(feature: &str) -> bool {
    std::env::var_os(format!(
        "CARGO_FEATURE_{}",
        feature.replace('-', "_").to_ascii_uppercase()
    ))
    .is_some()
}

fn cuda_version_at_least(version: (usize, usize), threshold: (usize, usize)) -> bool {
    version.0 > threshold.0 || (version.0 == threshold.0 && version.1 >= threshold.1)
}

fn detect_cuda_system_version() -> (usize, usize) {
    if let Some(version) = std::env::var_os("CUDARC_CUDA_VERSION") {
        let version = version.to_string_lossy();
        return parse_cudarc_cuda_version(&version).unwrap_or_else(|| {
            panic!("unsupported CUDARC_CUDA_VERSION `{version}`; expected a value like `13020`")
        });
    }

    let output = Command::new("nvcc")
        .arg("--version")
        .output()
        .unwrap_or_else(|err| panic!("failed to run `nvcc --version`: {err}"));

    if !output.status.success() {
        panic!(
            "`nvcc --version` failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    parse_nvcc_version(&String::from_utf8_lossy(&output.stdout)).unwrap_or_else(|| {
        panic!(
            "failed to parse CUDA version from `nvcc --version` output:\n{}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

fn parse_cudarc_cuda_version(version: &str) -> Option<(usize, usize)> {
    let version = version.trim();
    if version.len() != 5 || !version.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    let major = version[0..2].parse().ok()?;
    let minor = version[3..4].parse().ok()?;
    Some((major, minor))
}

fn parse_nvcc_version(stdout: &str) -> Option<(usize, usize)> {
    let release = stdout.split("release ").nth(1)?;
    let version = release.split([',', ' ']).next()?;
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    Some((major, minor))
}
