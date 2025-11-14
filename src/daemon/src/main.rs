#![allow(dead_code)]

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use colored::Colorize;
use control::{client::ControlClient, parse::parse_move_ops};

use crate::control::parse::MoveOperation;

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
                let client = check_error!(ControlClient::new(control::CONTROL_PATH).await);
                client.prefetch(args).await.unwrap();
            }
            Args::List(args) => {
                let client = check_error!(ControlClient::new(control::CONTROL_PATH).await);
                client.list_processes(args.verbose).await.unwrap();
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
