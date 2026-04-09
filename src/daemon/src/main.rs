#![allow(dead_code)]

use std::{io, path::PathBuf};

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{
    generate,
    shells::{Bash, Fish, Zsh},
};
use colored::Colorize;
use control::{client::ControlClient, parse::parse_move_ops};

use crate::{
    config::{CliConfig, init_config},
    control::parse::{MoveOperation, parse_pid, parse_size},
    runtime::{Priority, PriorityLevel},
};
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

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
    /// Move operations in the form `<pid>:<src>-><dest>=<size>` where `<pid>` is a PID like `1234` or an index like `idx3` / `i3`
    #[arg(value_parser = parse_move_ops)]
    pub move_ops: std::vec::Vec<MoveOperation>, // qualify as std::vec::Vec to make clap happy; see https://github.com/clap-rs/clap/issues/4808
}

#[derive(Debug, Parser)]
struct ProcessArgs {
    /// Show detailed information
    #[arg(short, long, default_value = "false")]
    pub verbose: bool,
    /// Show in JSON format
    #[arg(long, default_value = "false")]
    pub json: bool,
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
    /// Set device memory limit spec (e.g. "g:0.95", "g:31g", "g:31g/3:24g")
    #[arg(short = 'l', long)]
    pub device_limit: Option<String>,
}

#[derive(Debug, Parser)]
struct DaemonArgs {
    /// Path to a Nixie config file
    #[arg(short, long)]
    pub config_path: Option<PathBuf>,
    /// Set shared memory size (e.g., "32g", "1024m")
    #[arg(long, value_parser = parse_size, visible_aliases = ["shm"])]
    pub shmem: Option<u64>,
    /// Set host memory size (e.g., "32g", "1024m")
    #[arg(long, value_parser = parse_size, visible_aliases = ["host", "ram","paged"])]
    pub hostmem: Option<u64>,
    /// Set device memory limit spec (e.g. "g:0.95", "g:31g", "g:31g/3:24g")
    #[arg(long, visible_aliases = ["dlimit"], short = 'l')]
    pub device_limit: Option<String>,
    /// Enable auto prefetching
    #[arg(long, default_value = "true")]
    pub auto_prefetch: Option<bool>,
}

#[derive(Debug, Parser)]
#[command(args_conflicts_with_subcommands = true)]
struct StatusArgs {
    /// Show detailed information
    #[arg(short, long, default_value = "false")]
    pub verbose: bool,
    #[clap(subcommand)]
    pub command: Option<StatusCommand>,
}

#[derive(Debug, Subcommand)]
enum StatusCommand {
    /// Show process information
    Process(ProcessArgs),
}

#[derive(Clone, Copy, Debug, Subcommand)]
enum SetPriorityLevel {
    /// Set priority to interactive
    Interactive,
    /// Set priority to low-interactive
    LowInteractive,
    /// Set priority to batch
    Batch,
    /// Set priority to background
    Background,
}

impl SetPriorityLevel {
    fn to_fixed(self) -> Priority {
        match self {
            SetPriorityLevel::Interactive => Priority::Fixed(PriorityLevel::Interactive),
            SetPriorityLevel::LowInteractive => Priority::Fixed(PriorityLevel::LowInteractive),
            SetPriorityLevel::Batch => Priority::Fixed(PriorityLevel::Batch),
            SetPriorityLevel::Background => Priority::Fixed(PriorityLevel::Background),
        }
    }
}

#[derive(Debug, Parser)]
struct PriorityTargetArgs {
    /// Process selector: PID like `1234` or tracked index like `idx3` / `i3`
    #[arg(short, long)]
    pid: String,
}

#[derive(Debug, Parser)]
struct PrioritySetArgs {
    /// Process selector: PID like `1234` or tracked index like `idx3` / `i3`
    #[arg(short, long)]
    pid: String,
    #[clap(subcommand)]
    level: SetPriorityLevel,
}

#[derive(Debug, Subcommand)]
enum PriorityArgs {
    /// Unset priority to dynamic
    Unset(PriorityTargetArgs),
    /// Set priority to fixed level
    Set(PrioritySetArgs),
}

#[derive(Debug, Parser)]
struct HistoryArgs {
    /// Process selector: PID like `1234` or tracked index like `idx3` / `i3`
    #[arg(short, long)]
    pid: String,
}

#[derive(Debug, Parser)]
struct RunArgs {
    /// Command to run
    #[arg(required = true)]
    command: String,
    /// Arguments for the command
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
    /// Set CUDA_VISIBLE_DEVICES (e.g., "0", "0,1", uuids)
    #[arg(short = 'd', long)]
    device: Option<String>,
}

#[derive(Clone, Copy, Debug, Subcommand)]
enum CompletionShell {
    /// Generate a Bash completion script
    Bash,
    /// Generate a Zsh completion script
    Zsh,
    /// Generate a Fish completion script
    Fish,
}

#[derive(Debug, Parser)]
/// Control the Nixie daemon and managed processes
#[clap(name = "nixie", version = env!("CARGO_PKG_VERSION"))]
enum Args {
    /// Start the Nixie daemon
    Daemon(DaemonArgs),
    /// Move process buffers between memory locations
    Prefetch(PrefetchArgs),
    /// Show runtime status and process information
    Status(StatusArgs),
    /// Manage process priority
    #[clap(subcommand)]
    Priority(PriorityArgs),
    /// Generate shell completion scripts
    #[clap(subcommand)]
    Completion(CompletionShell),
    /// Show process history
    History(HistoryArgs),
    /// Run a command under Nixie sidecar injection
    Run(RunArgs),
    /// Show and update configuration
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

fn parse_pid_or_exit(pid: &str) -> ProcArgs {
    parse_pid(pid)
        .map_err(|e| {
            eprintln!("{}: {}", "Error".red(), e);
            std::process::exit(1);
        })
        .unwrap()
}

fn generate_completion(shell: CompletionShell) {
    let mut cmd = Args::command();
    let mut stdout = io::stdout();
    match shell {
        CompletionShell::Bash => generate(Bash, &mut cmd, "nixie", &mut stdout),
        CompletionShell::Zsh => generate(Zsh, &mut cmd, "nixie", &mut stdout),
        CompletionShell::Fish => generate(Fish, &mut cmd, "nixie", &mut stdout),
    }
}

fn main() {
    let args: Args = Args::parse();
    let args = match args {
        Args::Completion(shell) => {
            generate_completion(shell);
            return;
        }
        Args::Daemon(args) => {
            crate::logging::init_tracing();
            tracing::info!("Starting daemon...");
            if unsafe { cudarc::driver::sys::cuInit(0) }
                != cudarc::driver::sys::cudaError_enum::CUDA_SUCCESS
            {
                tracing::error!("Failed to initialize CUDA");
                return;
            }
            let cli_config = CliConfig {
                shmem_size: args.shmem,
                hostmem_size: args.hostmem,
                device_limit: args.device_limit,
                automatic_prefetch: args.auto_prefetch,
            };
            if let Err(e) = init_config(args.config_path, cli_config) {
                tracing::error!("Failed to init config: {}", e);
                return;
            }
            let config = crate::config::load_config();
            let runtime = runtime::Daemon::new(
                config.shmem_size_mb * 1024 * 1024,
                config.hostmem_size_mb * 1024 * 1024,
            );
            runtime.run();
            std::process::exit(0);
        }
        args => args,
    };
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
            Args::Priority(args) => {
                let client = check_error!(ControlClient::new(control::CONTROL_PATH).await);
                match args {
                    PriorityArgs::Unset(args) => {
                        let pid = parse_pid_or_exit(&args.pid);
                        client
                            .set_priority(pid, control::SetPriorityLevel::FixToDynamic)
                            .await
                            .unwrap();
                    }
                    PriorityArgs::Set(args) => {
                        let pid = parse_pid_or_exit(&args.pid);
                        client
                            .set_priority(
                                pid,
                                control::SetPriorityLevel::Set(args.level.to_fixed()),
                            )
                            .await
                            .unwrap();
                    }
                }
            }
            Args::History(args) => {
                let client = check_error!(ControlClient::new(control::CONTROL_PATH).await);
                let pid = parse_pid_or_exit(&args.pid);
                client.show_history(pid).await.unwrap();
            }
            Args::Status(args) => {
                let client = check_error!(ControlClient::new(control::CONTROL_PATH).await);
                match args.command {
                    Some(StatusCommand::Process(args)) => {
                        if args.json {
                            if args.verbose {
                                eprintln!(
                                    "{}",
                                    "Error: JSON output does not support verbose mode".red()
                                );
                                std::process::exit(1);
                            }
                            client.list_processes_json().await.unwrap();
                        } else {
                            client.list_processes(args.verbose).await.unwrap();
                        }
                    }
                    None => {
                        client.data_details(true, args.verbose).await.unwrap();
                    }
                }
            }
            Args::Run(args) => {
                run_command(args);
            }
            Args::Completion(_) | Args::Daemon(_) => unreachable!(),
        };
    });
}

fn find_sidecar_path() -> Option<PathBuf> {
    use std::env;

    let sidecar_name = "libnixiesidecar.so";

    // Check relative to executable
    let exe_path = env::current_exe().ok()?;
    let exe_dir = exe_path.parent()?;
    let sidecar_path = exe_dir.join(sidecar_name);
    if sidecar_path.exists() {
        return Some(sidecar_path);
    }

    // Check ../lib relative to executable
    let alt_path = exe_dir.join("../lib").join(sidecar_name);
    if alt_path.exists() {
        return Some(alt_path);
    }

    // Check LD_LIBRARY_PATH
    if let Ok(ld_library_path) = env::var("LD_LIBRARY_PATH") {
        for lib_path in ld_library_path.split(':') {
            let full_path = PathBuf::from(lib_path).join(sidecar_name);
            if full_path.exists() {
                return Some(full_path);
            }
        }
    }

    // System default library paths
    let system_lib_paths = vec!["/usr/local/lib", "/usr/lib", "/usr/lib64", "/lib", "/lib64"];

    for lib_path in system_lib_paths {
        let full_path = PathBuf::from(lib_path).join(sidecar_name);
        if full_path.exists() {
            return Some(full_path);
        }
    }

    None
}

fn run_command(args: RunArgs) {
    use std::process::Command;
    let sidecar_path = match find_sidecar_path() {
        Some(path) => path,
        None => {
            eprintln!(
                "{}: Could not find sidecar library 'libnixiesidecar.so'",
                "Error".red()
            );
            std::process::exit(1);
        }
    };

    let mut env_vars: Vec<(&str, String)> =
        vec![("LD_PRELOAD", sidecar_path.to_string_lossy().into_owned())];

    // Set CUDA_VISIBLE_DEVICES if provided
    if let Some(device) = args.device {
        env_vars.push(("CUDA_VISIBLE_DEVICES", device));
    }

    let mut cmd = Command::new(&args.command);
    cmd.args(&args.args);

    for (key, value) in env_vars {
        cmd.env(key, value);
    }

    match cmd.spawn() {
        Ok(mut child) => match child.wait() {
            Ok(status) => {
                std::process::exit(status.code().unwrap_or(1));
            }
            Err(e) => {
                eprintln!("{}: Failed to wait for process: {}", "Error".red(), e);
                std::process::exit(1);
            }
        },
        Err(e) => {
            eprintln!("{}: Failed to spawn process: {}", "Error".red(), e);
            std::process::exit(1);
        }
    }
}

fn is_set(set: bool, unset: bool) -> bool {
    if set ^ unset {
        set
    } else {
        eprintln!("{}: set or unset must be specified", "Error".red());
        std::process::exit(1);
    }
}
