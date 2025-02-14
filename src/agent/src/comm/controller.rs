use std::collections::{hash_map::Entry, HashMap};

use colored::Colorize;
use cudarc::driver::sys::{cudaError_enum, lib as cuda_lib, CUcontext, CUdevice};
use nihilipc::{rpc::DaemonClient, S2AMessage};

use super::msg::A2SMessage;
use crate::{info_eprintln, memory::prefetch, schedule::Scheduler, warn_eprintln, GENERIC_DATA};

/// handler for agent<->daemon communication
pub(crate) struct Controller {
    process_recv: flume::Receiver<A2SMessage>,
    daemon_recv: flume::Receiver<S2AMessage>,
    daemon_client: DaemonClient,
    sched_ctrl: &'static Scheduler,
}

impl Controller {
    pub fn new(
        process_recv: flume::Receiver<A2SMessage>,
        daemon_recv: flume::Receiver<S2AMessage>,
        daemon_client: DaemonClient,
        sched_ctrl: &'static Scheduler,
    ) -> Self {
        Self {
            process_recv,
            daemon_recv,
            daemon_client,
            sched_ctrl,
        }
    }

    pub async fn run(self) {
        let mut ctxs = CudaContextGuard {
            cuda_ctxs: HashMap::new(),
        };
        while let Some(msg) = self.select_on_recv().await {
            match msg {
                SidecarSelect::Process(msg) => {
                    if let Err(e) = match msg {
                        A2SMessage::Handshake(msg) => {
                            self.daemon_client
                                .handshake(tarpc::context::current(), msg)
                                .await
                        }
                        A2SMessage::InitInfo(msg) => {
                            self.daemon_client
                                .initialize(tarpc::context::current(), msg)
                                .await
                        }
                        A2SMessage::NofityActivity(msg) => {
                            self.daemon_client
                                .notify_activity(tarpc::context::current(), msg)
                                .await
                        }
                    } {
                        warn_eprintln!(
                            "{} {}: {}",
                            "[libcuda_hook]".bold(),
                            "Failed to send message to daemon".red(),
                            e
                        );
                    }
                }
                SidecarSelect::Daemon(msg) => match msg {
                    S2AMessage::SetAttr(args) => {
                        ctxs.set_current_ctx(args.device);
                        info_eprintln!(
                            "{} {}: {:?}=>{:?} address={}, len={}, device={}",
                            "[libcuda_hook]".bold(),
                            "rpc_set_attribute".blue(),
                            args.value,
                            args.will_set,
                            args.addr
                                .map_or_else(|| "None".to_string(), |x| format!("{:#x}", x)),
                            args.len,
                            args.device,
                        );
                        let mut ptr_mapping = GENERIC_DATA.get().unwrap().lock_ptr_mapping();
                        if let Some(addr) = args.addr {
                            crate::memory::set_attribute_single(
                                &mut ptr_mapping,
                                args.value,
                                args.will_set,
                                addr,
                                args.len,
                                args.device,
                            );
                        } else {
                            crate::memory::set_attribute(
                                &mut ptr_mapping,
                                args.value,
                                args.will_set,
                                args.len,
                            )
                        }
                    }
                    S2AMessage::Prefetch(args) => {
                        ctxs.set_current_ctx(args.device);
                        info_eprintln!(
                            "{} {}: address={}, len={:#x}, device={}",
                            "[libcuda_hook]".bold(),
                            "rpc_prefetch".blue(),
                            "#TODO".yellow(),
                            args.len,
                            "#TODO".yellow(),
                        );
                        prefetch::filtered_prefetch(args.len);
                    }
                    S2AMessage::Scheduling(args) => {
                        self.sched_ctrl.set_allow_running(args.enable);
                    }
                },
            }
        }
        info_eprintln!("Sidecar controller exited")
    }

    async fn select_on_recv(&self) -> Option<SidecarSelect> {
        futures::select! {
            msg = self.process_recv.recv_async() => msg.ok().map(SidecarSelect::Process),
            msg = self.daemon_recv.recv_async() => msg.ok().map(SidecarSelect::Daemon),
        }
    }
}

enum SidecarSelect {
    Process(A2SMessage),
    Daemon(S2AMessage),
}

struct CudaContextGuard {
    cuda_ctxs: HashMap<i32, CUcontext>,
}

impl CudaContextGuard {
    fn get_dev_ctx(&mut self, device_idx: CUdevice) -> CUcontext {
        match self.cuda_ctxs.entry(device_idx) {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                let mut ctx = std::ptr::null_mut();
                let res =
                    unsafe { cuda_lib().cuDevicePrimaryCtxRetain(&mut ctx as *mut _, device_idx) };
                if res != cudaError_enum::CUDA_SUCCESS {
                    warn_eprintln!(
                        "Failed to retain context for device {}: {:?}",
                        device_idx,
                        res
                    );
                }
                e.insert(ctx);
                info_eprintln!(
                    "{} {}: device={}",
                    "[libcuda_hook]".bold(),
                    "init_dev_ctx".blue(),
                    device_idx
                );
                ctx
            }
        }
    }

    fn set_current_ctx(&mut self, device_idx: CUdevice) {
        let ctx = self.get_dev_ctx(device_idx);
        let res = unsafe { cuda_lib().cuCtxSetCurrent(ctx) };
        if res != cudaError_enum::CUDA_SUCCESS {
            warn_eprintln!("Failed to set current context: {:?}", res);
        }
    }
}

impl Drop for CudaContextGuard {
    fn drop(&mut self) {
        self.cuda_ctxs.keys().for_each(|dev| {
            let res = unsafe { cuda_lib().cuDevicePrimaryCtxRelease_v2(*dev) };
            if res != cudaError_enum::CUDA_SUCCESS {
                warn_eprintln!("Failed to release context: {:?}", res);
            }
        });
        self.cuda_ctxs.clear();
    }
}
