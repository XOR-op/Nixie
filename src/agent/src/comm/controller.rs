use colored::Colorize;
use nihilipc::{rpc::DaemonClient, S2AMessage};

use super::msg::A2SMessage;
use crate::{
    info_eprintln, memory::prefetch, schedule::Scheduler, utils::set_device, warn_eprintln,
    GENERIC_DATA,
};

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
                        set_device(args.device);
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
                        info_eprintln!(
                            "{} {}: address={}, len={:#x}, to_gpu={}",
                            "[libcuda_hook]".bold(),
                            "rpc_prefetch".blue(),
                            "#TODO".yellow(),
                            args.len,
                            args.to_gpu
                        );
                        prefetch::filtered_prefetch_non_blocking(args.len, args.to_gpu);
                    }
                    S2AMessage::Scheduling(args) => {
                        self.sched_ctrl.set_allow_running(args);
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
