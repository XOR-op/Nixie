#![allow(dead_code)]

use clap::{Parser, Subcommand};
use colored::Colorize;
use control::client::ControlClient;

mod config;
mod control;
mod error;
mod general;
#[deprecated]
mod inject;
mod logging;
mod runtime;
mod staticly;
mod uvm;

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
    /// only prefetch memory regions with size larger than filter
    #[arg(short, long, default_value = "0")]
    pub low_filter: Option<u64>,
    #[arg(short, long)]
    pub dest: DeviceArgs,
    #[command(flatten)]
    pub proc: ProcArgs,
}

#[derive(Debug, Parser)]
struct ReadDupArgs {
    /// set read duplicatoin attribute
    #[arg(short, long, conflicts_with = "unset")]
    pub set: bool,
    /// unset read duplicatoin attribute
    #[arg(short, long, conflicts_with = "set")]
    pub unset: bool,
    /// only show memory regions with size larger than filter
    #[arg(short, long)]
    pub low_filter: Option<u64>,
    #[command(flatten)]
    pub proc: ProcArgs,
}

#[derive(Debug, Parser)]
struct ReduceMoveArgs {
    /// set accessed by attribute
    #[arg(short, long, conflicts_with = "unset")]
    pub set: bool,
    /// unset accessed by attribute
    #[arg(short, long, conflicts_with = "set")]
    pub unset: bool,
    /// only show memory regions with size larger than filter
    #[arg(short, long)]
    pub low_filter: Option<u64>,
    #[arg(short, long)]
    pub high_filter: Option<u64>,
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
}

#[derive(Debug, Parser)]
#[clap(name = "nihilphase", about = "", version = env!("CARGO_PKG_VERSION"))]
enum Args {
    Daemon,
    Prefetch(PrefetchArgs),
    ReadDup(ReadDupArgs),
    ReduceMove(ReduceMoveArgs),
    List(ListArgs),
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
    if matches!(args, Args::Daemon) {
        if !is_root::is_root() {
            eprintln!("Error: nihilphase daemon must be run as root");
            std::process::exit(1);
        }
        let runtime = runtime::Daemon::new();
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
                let client =
                    check_error!(ControlClient::new(control::CONTROL_PATH, args.proc).await);
                client
                    .prefetch(matches!(args.dest, DeviceArgs::Gpu), args.low_filter)
                    .await
                    .unwrap();
            }
            Args::ReadDup(args) => {
                let is_set = is_set(args.set, args.unset);
                let client =
                    check_error!(ControlClient::new(control::CONTROL_PATH, args.proc).await);
                client.read_dup(args.low_filter, is_set).await.unwrap();
            }
            Args::ReduceMove(args) => {
                let is_set = is_set(args.set, args.unset);
                let client =
                    check_error!(ControlClient::new(control::CONTROL_PATH, args.proc).await);
                if args.high_filter.is_some() {
                    eprintln!("{}[] high filter is not supported yet", "[Warn]".yellow());
                }
                client
                    .reduce_move(args.low_filter, args.high_filter, is_set)
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
            Args::Daemon => unreachable!(),
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
