#![allow(dead_code)]

use crate::inject::*;
use clap::Parser;

mod error;
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
struct AttributeArgs {
    // set read duplicatoin attribute
    #[arg(short, long, alias = "read-dup")]
    pub read_dup: Option<bool>,
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
#[clap(name = "nihilphased", about = "", version = env!("CARGO_PKG_VERSION"))]
enum Args {
    Start(StartArgs),
    Prefetch(PrefetchArgs),
    Attribute(AttributeArgs),
}

#[derive(Debug, Parser)]
struct CliArgs {
    #[arg(short, long)]
    pub pid: u64,
    #[arg(long, alias = "dylib-path")]
    pub dylib_path: Option<String>,
}

fn resolve_dylib_path(path: Option<String>) -> String {
    path.unwrap_or("./target/release/libcuda_hook.so".to_string())
}

fn inject(cli: CliArgs, func_sym: &str, arg1: u64, arg2: u64, arg3: u64) {
    dbg!(inject_wrapper(
        cli.pid as i32,
        resolve_dylib_path(cli.dylib_path),
        func_sym,
        arg1,
        arg2,
        arg3,
    )
    .ok());
}

fn main() {
    let args: Args = Args::parse();
    match args {
        Args::Start(args) => {
            let runtime = runtime::Daemon::new(resolve_dylib_path(args.dylib_path));
            runtime.start();
        }
        Args::Prefetch(args) => {
            inject(args.cli, "_nihilphase_prefetch", args.limit, 0, 0);
        }
        Args::Attribute(args) => {
            if let Some(read_dup) = args.read_dup {
                inject(
                    args.cli,
                    "_nihilphase_advise_read_mostly",
                    read_dup as u64,
                    args.limit,
                    0,
                );
            }
        }
    };
}
