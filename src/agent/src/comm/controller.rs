use colored::Colorize;
use nihil_common::rpc::DaemonClient;

use super::msg::{A2SMessage, S2AMessage};
use crate::{info_eprintln, schedule::Scheduler, warn_eprintln};

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
                            if let Ok(Some(resp)) = self
                                .daemon_client
                                .handshake(tarpc::context::current(), msg)
                                .await
                            {
                                super::init::init_buffer_by_handshake_resp(resp);
                            } else {
                                panic!("Handshake failed, daemon did not respond with handshake response");
                            }
                            Ok(())
                        }
                        A2SMessage::NofityActivity(msg) => {
                            self.daemon_client
                                .notify_activity(tarpc::context::current(), msg)
                                .await
                        }
                        A2SMessage::MemoryRequest(msg) => {
                            self.daemon_client
                                .request_memory(tarpc::context::current(), msg)
                                .await
                        }
                        A2SMessage::MigrationResponse(msg) => {
                            self.daemon_client
                                .migrate_response_async(tarpc::context::current(), msg)
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
                    S2AMessage::MigrationRequest(args) => {
                        todo!()
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
