use std::sync::OnceLock;

#[derive(Debug, Clone)]
pub(crate) struct AgentConfig {
    pub log_level: u8, // 0=none, 1=warn, 2=info
    pub auto_dup: bool,
    pub auto_dup_delay: u64,
}

const DEFAULT_LOG_LEVEL: u8 = 1;
const DEFAULT_AUTO_DUP: bool = false;
const DEFAULT_AUTO_DUP_DELAY: u64 = 5;

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            log_level: DEFAULT_LOG_LEVEL,
            auto_dup: DEFAULT_AUTO_DUP,
            auto_dup_delay: DEFAULT_AUTO_DUP_DELAY,
        }
    }
}

static AGENT_CFG: OnceLock<AgentConfig> = OnceLock::new();

pub(crate) fn agent_config() -> &'static AgentConfig {
    AGENT_CFG.get_or_init(|| {
        let mut cfg = AgentConfig::default();
        if let Ok(content) = std::env::var("NIHIL_CFG") {
            for pair in content.split("/") {
                let mut iter = pair.split(":");
                if let Some(key) = iter.next() {
                    if let Some(val) = iter.next() {
                        // valid key-value pair
                        match key.to_lowercase().as_str() {
                            "log_level" | "log" => {
                                cfg.log_level = match val.to_lowercase().as_str() {
                                    "0" | "none" => 0,
                                    "1" | "warn" | "error" => 1,
                                    "2" | "info" => 2,
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
                            _ => {}
                        }
                    }
                }
            }
        }
        cfg
    })
}
