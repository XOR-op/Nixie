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
    pub limit: u64,
    #[command(flatten)]
    pub cli: CliArgs,
}

#[derive(Debug, Parser)]
struct ReadDupArgs {
    // set read duplicatoin attribute
    #[arg(short, long)]
    pub set: bool,
    #[arg(short, long, default_value = "0")]
    pub limit: u64,
    #[command(flatten)]
    pub cli: CliArgs,
}

#[derive(Debug, Parser)]
struct StartArgs {
    #[arg(long, alias = "dylib-path")]
    pub dylib_path: Option<String>,
}

#[derive(Debug, Parser)]
#[clap(name = "nihilphase", about = "", version = env!("CARGO_PKG_VERSION"))]
enum Args {
    Start,
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
    if matches!(args, Args::Start) {
        let runtime = runtime::Daemon::new();
        runtime.start();
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
                client.prefetch(Some(args.limit)).await.unwrap();
            }
            Args::ReadDup(args) => {
                let client = ControlClient::new(control::CONTROL_PATH, args.cli.pid)
                    .await
                    .unwrap();
                client
                    .set_read_dup(Some(args.limit), args.set)
                    .await
                    .unwrap();
            }
            Args::Start => unreachable!(),
        };
    });
}
