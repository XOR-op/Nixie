use crate::inject::*;
use clap::Parser;

mod inject;
mod runtime;
mod uvm;

#[derive(Debug, Parser)]
struct PrefetchArgs {
    #[arg(short, long, default_value = "1")]
    pub limit: u64,
    #[command(flatten)]
    pub cli: CliArgs,
}

#[derive(Debug, Parser)]
struct AttributeArgs {
    // set read duplicatoin attribute
    #[arg(short, long, alias = "read-dup")]
    pub read_dup: Option<bool>,
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

fn inject(args: CliArgs, func_sym: &str, eax: u64) {
    let dylib_base = locate_dylib_base(args.pid as i32, "libcuda_hook.so").unwrap();
    let dylib_path = args
        .dylib_path
        .unwrap_or("./target/release/libcuda_hook.so".to_string());
    let func_offset = resolve_func_offset(func_sym, &dylib_path).unwrap();
    dbg!(inject_process(
        args.pid as i32,
        dylib_base + func_offset,
        eax
    ))
    .ok();
}

fn main() {
    let args: Args = Args::parse();
    match args {
        Args::Start => {
            let runtime = runtime::Runtime::new();
            runtime.start();
        }
        Args::Prefetch(args) => {
            inject(args.cli, "_auto_gmem_prefetch", args.limit);
        }
        Args::Attribute(args) => {
            if let Some(read_dup) = args.read_dup {
                inject(args.cli, "_auto_gmem_advise_read_mostly", read_dup as u64);
            }
        }
    };
}
