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
#[clap(name = "AutoGMem", about = "", version = env!("CARGO_PKG_VERSION"))]
enum Args {
    Start,
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

fn inject(args: CliArgs, func_sym: &str, arg1: u64, arg2: u64, arg3: u64) {
    let dylib_base = locate_dylib_base(args.pid as i32, "libcuda_hook.so").unwrap();
    let dylib_path = args
        .dylib_path
        .unwrap_or("./target/release/libcuda_hook.so".to_string());
    let func_offset = resolve_func_offset(func_sym, &dylib_path).unwrap();
    dbg!(inject_process(
        args.pid as i32,
        dylib_base + func_offset,
        arg1,
        arg2,
        arg3
    ))
    .ok();
}

fn main() {
    let args: Args = Args::parse();
    match args {
        Args::Start => {
            let runtime = runtime::Daemon::new();
            runtime.start();
        }
        Args::Prefetch(args) => {
            inject(args.cli, "_auto_gmem_prefetch", args.limit, 0, 0);
        }
        Args::Attribute(args) => {
            if let Some(read_dup) = args.read_dup {
                inject(
                    args.cli,
                    "_auto_gmem_advise_read_mostly",
                    read_dup as u64,
                    args.limit,
                    0,
                );
            }
        }
    };
}
