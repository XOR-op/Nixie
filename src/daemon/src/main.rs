#![allow(dead_code)]

use clap::Parser;
use control::client::ControlClient;

mod control;
mod error;
#[deprecated]
mod inject;
mod logging;
mod runtime;
mod uvm;

#[derive(Debug, Parser)]
struct PrefetchArgs {
    #[arg(short, long, default_value = "0")]
    pub filter: u64,
    #[command(flatten)]
    pub cli: CliArgs,
}

#[derive(Debug, Parser)]
struct ReadDupArgs {
    // set read duplicatoin attribute
    #[arg(short, long)]
    pub set: bool,
    #[arg(short, long, default_value = "0")]
    pub filter: u64,
    #[command(flatten)]
    pub cli: CliArgs,
}

#[derive(Debug, Parser)]
#[clap(name = "nihilphase", about = "", version = env!("CARGO_PKG_VERSION"))]
enum Args {
    Daemon,
    Prefetch(PrefetchArgs),
    ReadDup(ReadDupArgs),
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
                client.prefetch(Some(args.filter)).await.unwrap();
            }
            Args::ReadDup(args) => {
                let client = ControlClient::new(control::CONTROL_PATH, args.cli.pid)
                    .await
                    .unwrap();
                client.read_dup(Some(args.filter), args.set).await.unwrap();
            }
            Args::Daemon => unreachable!(),
        };
    });
}
