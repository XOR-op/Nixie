#![allow(dead_code)]

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use colored::Colorize;
use control::client::ControlClient;

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

#[derive(clap::ValueEnum, Debug, Parser, Clone, Copy, Default)]
enum DeviceArgs {
    Cpu,
    #[default]
    Gpu,
}

#[derive(Debug, Parser)]
struct PrefetchArgs {
    #[arg(short, long)]
    pub dest: DeviceArgs,
    #[command(flatten)]
    pub proc: ProcArgs,
}

#[derive(Debug, Parser)]
struct ListArgs {
    /// Show detailed information
    #[arg(short, long, default_value = "false")]
    pub verbose: bool,
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
    /// Set schedule delay in ms
    #[arg(short = 'd', long)]
    pub schedule_delay: Option<u32>,
    /// Set schedule cooldown in ms
    #[arg(short = 'c', long)]
    pub schedule_cooldown: Option<u32>,
    /// Set device threshold
    #[arg(short = 't', long)]
    pub device_threshold: Option<f64>,
    /// Set preempt delay in ms
    #[arg(short = 'p', long)]
    pub preempt_delay: Option<u32>,
}

#[derive(Debug, Parser)]
struct DaemonArgs {
    #[arg(short, long)]
    pub config_path: Option<PathBuf>,
}

#[derive(Debug, Parser)]
struct UsageArgs {
    /// Show detailed information
    #[arg(short, long, default_value = "false")]
    pub verbose: bool,
}

#[derive(Debug, Parser)]
#[clap(name = "nihilphase", about = "", version = env!("CARGO_PKG_VERSION"))]
enum Args {
    Daemon(DaemonArgs),
    Prefetch(PrefetchArgs),
    List(ListArgs),
    Usage(UsageArgs),
    #[clap(subcommand)]
    Config(ConfigArgs),
}

#[derive(Debug, Parser, Clone, Copy)]
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
        let runtime = runtime::Daemon::new();
        runtime.run(args.config_path);
        std::process::exit(0);
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        match args {
            Args::Prefetch(args) => {
                let client =
                    check_error!(ControlClient::new(control::CONTROL_PATH, args.proc).await);
                client
                    .prefetch(matches!(args.dest, DeviceArgs::Gpu))
                    .await
                    .unwrap();
            }
            Args::List(args) => {
                let client = check_error!(
                    ControlClient::new(control::CONTROL_PATH, ProcArgs::empty()).await
                );
                client.list_processes(args.verbose).await.unwrap();
            }
            Args::Config(args) => {
                let client = check_error!(
                    ControlClient::new(control::CONTROL_PATH, ProcArgs::empty()).await
                );
                match args {
                    ConfigArgs::Show => {
                        client.show_config().await.unwrap();
                    }
                    ConfigArgs::Update(args) => {
                        client.update_config(args).await.unwrap();
                    }
                }
            }
            Args::Usage(args) => {
                let client = check_error!(
                    ControlClient::new(control::CONTROL_PATH, ProcArgs::empty()).await
                );
                client.data_details(args.verbose).await.unwrap();
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
