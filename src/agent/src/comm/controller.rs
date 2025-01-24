use std::collections::{hash_map::Entry, HashMap};

use colored::Colorize;
use cudarc::driver::sys::{cudaError_enum, CUcontext, CUdevice};
use nihilipc::{rpc::DaemonClient, S2CMessage};

use super::msg::C2SMessage;
use crate::{info_eprintln, schedule::SchedControl, snippet, utils::should_log, warn_eprintln};

/// handler for agent<->daemon communication
pub(crate) struct Controller {
    process_recv: flume::Receiver<C2SMessage>,
    daemon_recv: flume::Receiver<S2CMessage>,
    daemon_client: DaemonClient,
    sched_ctrl: &'static SchedControl,
}

impl Controller {
    pub fn new(
        process_recv: flume::Receiver<C2SMessage>,
        daemon_recv: flume::Receiver<S2CMessage>,
        daemon_client: DaemonClient,
        sched_ctrl: &'static SchedControl,
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
                        C2SMessage::InitClient(msg) => {
                            self.daemon_client
                                .init_client(tarpc::context::current(), msg)
                                .await
                        }
                        C2SMessage::UvmFd(msg) => {
                            self.daemon_client
                                .set_uvm_fd(tarpc::context::current(), msg)
                                .await
                        }
                        C2SMessage::ShmPath(msg) => {
                            self.daemon_client
                                .set_shm_path(tarpc::context::current(), msg)
                                .await
                        }
                        C2SMessage::MemoryUsage(_msg) => unimplemented!("MemoryUsage"),
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
                    S2CMessage::ReadDup(args) => {
                        ctxs.set_current_ctx(args.device);
                        info_eprintln!(
                            "{} {}: =>{} address={:#x}, len={:#x}, device={}",
                            "[libcuda_hook]".bold(),
                            "rpc_read_duplication".blue(),
                            args.value,
                            args.addr,
                            args.len,
                            args.device
                        );
                        advise_read_mostly_for(args.value, args.addr, args.len, args.device);
                    }
                    S2CMessage::Prefetch(args) => {
                        ctxs.set_current_ctx(args.device);
                        info_eprintln!(
                            "{} {}: address={}, len={:#x}, device={}",
                            "[libcuda_hook]".bold(),
                            "rpc_prefetch".blue(),
                            "#TODO".yellow(),
                            args.len,
                            "#TODO".yellow(),
                        );
                        snippet::_nihilphase_prefetch(args.len);
                    }
                    S2CMessage::GrantRunningToken(args) => {
                        info_eprintln!(
                            "{} {}: time={:?}",
                            "[libcuda_hook]".bold(),
                            "UNIMPLEMENTED rpc_grant_running_token".red(),
                            args.time,
                        );
                        self.sched_ctrl.update_time(args.time);
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
    Process(C2SMessage),
    Daemon(S2CMessage),
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
                let res = unsafe {
                    cudarc::driver::sys::cuDevicePrimaryCtxRetain(&mut ctx as *mut _, device_idx)
                };
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
        let res = unsafe { cudarc::driver::sys::cuCtxSetCurrent(ctx) };
        if res != cudaError_enum::CUDA_SUCCESS {
            warn_eprintln!("Failed to set current context: {:?}", res);
        }
    }
}

impl Drop for CudaContextGuard {
    fn drop(&mut self) {
        self.cuda_ctxs.keys().for_each(|dev| {
            let res = unsafe { cudarc::driver::sys::cuDevicePrimaryCtxRelease_v2(*dev) };
            if res != cudaError_enum::CUDA_SUCCESS {
                warn_eprintln!("Failed to release context: {:?}", res);
            }
        });
        self.cuda_ctxs.clear();
    }
}

// TODO: check address in client side; should read allocation record before calling
fn advise_read_mostly_for(read_mostly: bool, address: u64, length: u64, device: i32) {
    let res = unsafe {
        cudarc::driver::sys::cuMemAdvise(
            address,
            length as usize,
            if read_mostly {
                cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_SET_READ_MOSTLY
            } else {
                cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_UNSET_READ_MOSTLY
            },
            device,
        )
    };
    if res != cudaError_enum::CUDA_SUCCESS {
        warn_eprintln!("Failed to set read mostly: {:?}", res);
    }
}
