use crate::inject::*;
use clap::Parser;

mod inject;

#[derive(Debug, Parser)]
struct PrefetchArgs {
    #[arg(short, long, default_value = "1")]
    pub limit: u64,
}

#[derive(Debug, Parser)]
struct AttributeArgs {
    // set read-mostly attribute
    #[arg(short, long)]
    pub read_mostly: bool,
}

#[derive(Debug, Parser)]
enum ProgramArgs {
    Prefetch(PrefetchArgs),
    Attribute(AttributeArgs),
}

#[derive(Debug, Parser)]
#[clap(name = "AutoGMem", about = "", version = env!("CARGO_PKG_VERSION"))]
struct Args {
    #[arg(short, long)]
    pub pid: u64,
    #[command(subcommand)]
    pub subcmd: ProgramArgs,
}

fn main() {
    let args: Args = Args::parse();
    let dylib_base = locate_dylib_base(args.pid as i32, "libcuda_hook.so").unwrap();
    match args.subcmd {
        ProgramArgs::Prefetch(PrefetchArgs { limit }) => {
            let func_offset =
                resolve_func_offset("_auto_gmem_prefetch", "./target/release/libcuda_hook.so")
                    .unwrap();
            dbg!(inject_process(
                args.pid as i32,
                dylib_base + func_offset,
                limit
            ))
            .ok();
        }
        ProgramArgs::Attribute(AttributeArgs { read_mostly }) => {
            let func_offset = resolve_func_offset(
                "_auto_gmem_advise_read_mostly",
                "./target/release/libcuda_hook.so",
            )
            .unwrap();
            dbg!(inject_process(
                args.pid as i32,
                dylib_base + func_offset,
                read_mostly as u64
            ))
            .ok();
        }
    }
}
