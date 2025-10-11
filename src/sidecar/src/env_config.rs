use std::sync::OnceLock;

#[derive(Debug, Clone)]
pub(crate) struct SidecarConfig {
    pub log_level: u8, // 0=none, 1=warn, 2=info, 3=debug
    pub auto_dup: bool,
    pub auto_dup_delay: u64,
    pub auto_idle: bool,
}

const DEFAULT_LOG_LEVEL: u8 = 1;
const DEFAULT_AUTO_DUP: bool = false;
const DEFAULT_AUTO_DUP_DELAY: u64 = 5;
const DEFAULT_AUTO_IDLE: bool = true;

impl Default for SidecarConfig {
    fn default() -> Self {
        Self {
            log_level: DEFAULT_LOG_LEVEL,
            auto_dup: DEFAULT_AUTO_DUP,
            auto_dup_delay: DEFAULT_AUTO_DUP_DELAY,
            auto_idle: DEFAULT_AUTO_IDLE,
        }
    }
}

static SIDECAR_CFG: OnceLock<SidecarConfig> = OnceLock::new();

pub(crate) fn sidecar_config() -> &'static SidecarConfig {
    SIDECAR_CFG.get_or_init(|| {
        let mut cfg = SidecarConfig::default();
        if let Ok(content) = std::env::var("NIHIL_CFG") {
            for pair in content.split("/") {
                let mut iter = pair.split(":");
                if let Some(key) = iter.next()
                    && let Some(val) = iter.next() {
                        // valid key-value pair
                        match key.to_lowercase().as_str() {
                            "log_level" | "log" => {
                                cfg.log_level = match val.to_lowercase().as_str() {
                                    "0" | "none" => 0,
                                    "1" | "warn" | "error" => 1,
                                    "2" | "info" => 2,
                                    "3" | "debug" => 3,
                                    _ => DEFAULT_LOG_LEVEL,
                                }
                            }
                            "auto_dup" | "autodup" | "dup" => {
                                cfg.auto_dup = match val.to_lowercase().as_str() {
                                    "true" | "1" | "yes" => true,
                                    "false" | "0" | "no" => false,
                                    _ => DEFAULT_AUTO_DUP,
                                }
                            }
                            "auto_dup_delay" | "autodup_delay" | "delay" => {
                                cfg.auto_dup_delay = match val.parse() {
                                    Ok(num) => num,
                                    _ => DEFAULT_AUTO_DUP_DELAY,
                                }
                            }
                            "auto_idle" | "autoidle" | "idle" => {
                                cfg.auto_idle = match val.to_lowercase().as_str() {
                                    "true" | "1" | "yes" => true,
                                    "false" | "0" | "no" => false,
                                    _ => DEFAULT_AUTO_IDLE,
                                }
                            }
                            _ => {}
                        }
                    }
            }
            if cfg.log_level >= 2 {
                eprintln!(
                    "{} Sidecar inited with {:?}",
                    colored::Colorize::green("NIHIL-INFO"),
                    cfg
                );
            }
        }
        cfg
    })
}
