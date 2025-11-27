#![allow(dead_code)]

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use colored::Colorize;
use control::{client::ControlClient, parse::parse_move_ops};

use crate::{
    config::{CliConfig, init_config},
    control::parse::{MoveOperation, parse_pid, parse_size},
    runtime::{Priority, PriorityLevel},
};

mod config;
mod control;
mod error;
mod logging;
mod runtime;
mod staticly;

macro_rules! check_error {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => {
                eprintln!("{}: {}", "Error".red(), e);
                std::process::exit(1);
            }
        }
    };
}

#[derive(Debug, Parser)]
struct PrefetchArgs {
    #[arg(value_parser = parse_move_ops)]
    pub move_ops: std::vec::Vec<MoveOperation>, // qualify as std::vec::Vec to make clap happy; see https://github.com/clap-rs/clap/issues/4808
}

#[derive(Debug, Parser)]
struct ListArgs {
    /// Show detailed information
    #[arg(short, long, default_value = "false")]
    pub verbose: bool,
    /// Show in JSON format
    #[arg(long, default_value = "false")]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
enum ConfigArgs {
    /// Show configuration
    Show,
    /// Update configuration
    Update(UpdateConfigArgs),
}

#[derive(Debug, Parser)]
struct UpdateConfigArgs {
    /// Set schedule cooldown in ms
    #[arg(short = 'c', long)]
    pub schedule_cooldown: Option<u32>,
    /// Set device threshold
    #[arg(short = 't', long)]
    pub device_threshold: Option<f64>,
}

#[derive(Debug, Parser)]
struct DaemonArgs {
    #[arg(short, long)]
    pub config_path: Option<PathBuf>,
    /// Set shared memory size (e.g., "32g", "1024m")
    #[arg(long, value_parser = parse_size, visible_aliases = ["shm"])]
    pub shmem: Option<u64>,
    /// Set host memory size (e.g., "32g", "1024m")
    #[arg(long, value_parser = parse_size, visible_aliases = ["host", "ram","paged"])]
    pub hostmem: Option<u64>,
    /// Set device memory usage ratio (0.0 - 1.0)
    #[arg(long)]
    pub device_ratio: Option<f64>,
}

#[derive(Debug, Parser)]
struct UsageArgs {
    /// Show detailed information
    #[arg(short, long, default_value = "false")]
    pub verbose: bool,
}

#[derive(Clone, Copy, Debug, Subcommand)]
enum SetPriorityLevel {
    Interactive,
    LowInteractive,
    Batch,
    Background,
}

impl SetPriorityLevel {
    fn to_fixed(self) -> Priority {
        match self {
            SetPriorityLevel::Interactive => Priority::Fixed(PriorityLevel::Interactive),
            SetPriorityLevel::LowInteractive => Priority::Fixed(PriorityLevel::LowInteractive),
            SetPriorityLevel::Batch => Priority::Fixed(PriorityLevel::Batch),
            SetPriorityLevel::Background => Priority::Fixed(PriorityLevel::Background),
        }
    }
}

#[derive(Debug, Subcommand)]
enum SetPriorityOption {
    /// Unset priority to dynamic
    Unset,
    /// Set priority to fixed level
    #[clap(subcommand)]
    Set(SetPriorityLevel),
}

#[derive(Debug, Parser)]
struct SetPriorityArgs {
    #[arg(short, long)]
    pid: String,
    #[clap(subcommand)]
    option: SetPriorityOption,
}

#[derive(Debug, Parser)]
struct ShowHistoryArgs {
    /// Process ID to show history for
    #[arg(short, long)]
    pid: String,
}

#[derive(Debug, Parser)]
#[clap(name = "nihilphase", about = "", version = env!("CARGO_PKG_VERSION"))]
enum Args {
    Daemon(DaemonArgs),
    Prefetch(PrefetchArgs),
    List(ListArgs),
    Usage(UsageArgs),
    SetPriority(SetPriorityArgs),
    ShowHistory(ShowHistoryArgs),
    #[clap(subcommand)]
    Config(ConfigArgs),
}

#[derive(Debug, Parser, Clone, Copy, PartialEq, Eq)]
struct ProcArgs {
    #[arg(short, long, conflicts_with = "idx")]
    pub pid: Option<i32>,
    #[arg(short, long, conflicts_with = "pid")]
    pub idx: Option<u32>,
}

impl ProcArgs {
    fn empty() -> Self {
        Self {
            pid: Some(0),
            idx: None,
        }
    }
}

fn main() {
    let args: Args = Args::parse();
    if let Args::Daemon(args) = args {
        crate::logging::init_tracing();
        tracing::info!("Starting daemon...");
        if unsafe { cudarc::driver::sys::cuInit(0) }
            != cudarc::driver::sys::cudaError_enum::CUDA_SUCCESS
        {
            tracing::error!("Failed to initialize CUDA");
            return;
        }
        let cli_config = CliConfig {
            shmem_size: args.shmem,
            hostmem_size: args.hostmem,
            device_threshold: args.device_ratio,
        };
        if let Err(e) = init_config(args.config_path, cli_config) {
            tracing::error!("Failed to init config: {}", e);
            return;
        }
        let config = crate::config::load_config();
        let runtime = runtime::Daemon::new(
            config.shmem_size_mb * 1024 * 1024,
            config.hostmem_size_mb * 1024 * 1024,
        );
        runtime.run();
        std::process::exit(0);
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        match args {
            Args::Prefetch(args) => {
                let client = check_error!(ControlClient::new(control::CONTROL_PATH).await);
                client.prefetch(args).await.unwrap();
            }
            Args::List(args) => {
                let client = check_error!(ControlClient::new(control::CONTROL_PATH).await);
                if args.json {
                    if args.verbose {
                        eprintln!(
                            "{}",
                            "Error: JSON output does not support verbose mode".red()
                        );
                        std::process::exit(1);
                    }
                    client.list_processes_json().await.unwrap();
                } else {
                    client.list_processes(args.verbose).await.unwrap();
                }
            }
            Args::Config(args) => {
                let client = check_error!(ControlClient::new(control::CONTROL_PATH).await);
                match args {
                    ConfigArgs::Show => {
                        client.show_config().await.unwrap();
                    }
                    ConfigArgs::Update(args) => {
                        client.update_config(args).await.unwrap();
                    }
                }
            }
            Args::SetPriority(args) => {
                let client = check_error!(ControlClient::new(control::CONTROL_PATH).await);
                let pid = parse_pid(&args.pid)
                    .map_err(|e| {
                        eprintln!("{}: {}", "Error".red(), e);
                        std::process::exit(1);
                    })
                    .unwrap();
                match args.option {
                    SetPriorityOption::Unset => {
                        client
                            .set_priority(pid, control::SetPriorityLevel::FixToDynamic)
                            .await
                            .unwrap();
                    }
                    SetPriorityOption::Set(level) => {
                        client
                            .set_priority(pid, control::SetPriorityLevel::Set(level.to_fixed()))
                            .await
                            .unwrap();
                    }
                }
            }
            Args::ShowHistory(args) => {
                let client = check_error!(ControlClient::new(control::CONTROL_PATH).await);
                let pid = parse_pid(&args.pid)
                    .map_err(|e| {
                        eprintln!("{}: {}", "Error".red(), e);
                        std::process::exit(1);
                    })
                    .unwrap();
                client.show_history(pid).await.unwrap();
            }
            Args::Usage(args) => {
                let client = check_error!(ControlClient::new(control::CONTROL_PATH).await);
                client.data_details(true, args.verbose).await.unwrap();
            }
            Args::Daemon(_) => unreachable!(),
        };
    });
}

fn is_set(set: bool, unset: bool) -> bool {
    if set ^ unset {
        set
    } else {
        eprintln!("{}: set or unset must be specified", "Error".red());
        std::process::exit(1);
    }
}
