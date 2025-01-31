#![allow(dead_code)]

use clap::Parser;
use control::client::ControlClient;

mod control;
mod error;
mod general;
#[deprecated]
mod inject;
mod logging;
mod runtime;
mod uvm;

#[derive(clap::ValueEnum, Debug, Parser, Clone, Copy, Default)]
enum DeviceArgs {
    CPU,
    #[default]
    GPU,
}

#[derive(Debug, Parser)]
struct PrefetchArgs {
    /// only prefetch memory regions with size larger than filter
    #[arg(short, long, default_value = "0")]
    pub filter: u64,
    #[arg(short, long)]
    pub dest: DeviceArgs,
    #[command(flatten)]
    pub cli: CliArgs,
}

#[derive(Debug, Parser)]
struct ReadDupArgs {
    /// set read duplicatoin attribute
    #[arg(short, long)]
    pub set: bool,
    /// only show memory regions with size larger than filter
    #[arg(short, long, default_value = "0")]
    pub filter: u64,
    #[command(flatten)]
    pub cli: CliArgs,
}

#[derive(Debug, Parser)]
struct ReduceMoveArgs {
    /// set read duplicatoin attribute
    #[arg(short, long)]
    pub set: bool,
    /// only show memory regions with size larger than filter
    #[arg(short, long)]
    pub low_filter: Option<u64>,
    #[arg(short, long)]
    pub high_filter: Option<u64>,
    #[command(flatten)]
    pub cli: CliArgs,
}

#[derive(Debug, Parser)]
struct ListArgs {
    /// Show detailed information
    #[arg(short, long, default_value = "false")]
    pub verbose: bool,
}

#[derive(Debug, Parser)]
#[clap(name = "nihilphase", about = "", version = env!("CARGO_PKG_VERSION"))]
enum Args {
    Daemon,
    Prefetch(PrefetchArgs),
    ReadDup(ReadDupArgs),
    ReduceMove(ReduceMoveArgs),
    List(ListArgs),
}

#[derive(Debug, Parser)]
struct CliArgs {
    #[arg(short, long)]
    pub pid: i32,
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
                let client = ControlClient::new(control::CONTROL_PATH, args.cli.pid)
                    .await
                    .unwrap();
                client
                    .prefetch(matches!(args.dest, DeviceArgs::GPU), Some(args.filter))
                    .await
                    .unwrap();
            }
            Args::ReadDup(args) => {
                let client = ControlClient::new(control::CONTROL_PATH, args.cli.pid)
                    .await
                    .unwrap();
                client.read_dup(Some(args.filter), args.set).await.unwrap();
            }
            Args::ReduceMove(args) => {
                let client = ControlClient::new(control::CONTROL_PATH, args.cli.pid)
                    .await
                    .unwrap();
                client
                    .reduce_move(args.low_filter, args.high_filter, args.set)
                    .await
                    .unwrap();
            }
            Args::List(args) => {
                let client = ControlClient::new(control::CONTROL_PATH, 0).await.unwrap();
                client.list_processes(args.verbose).await.unwrap();
            }
            Args::Daemon => unreachable!(),
        };
    });
}
