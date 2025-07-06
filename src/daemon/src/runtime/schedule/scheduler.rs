use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

use hashlink::LinkedHashMap;
use nihil_common::{
    general::CallParameter, ActivityUpdate, GlobalDeviceId, SchedulingArgs, MAX_GPUS,
};
use tokio::sync::mpsc;

use crate::{
    config::load_config,
    control::{ProcessResidualData, ProcessResidualRequest},
    error::ScheduleError,
    runtime::schedule::{statistics::PreemptionReason, PriorityLevel},
};

use crate::runtime::{daemon_server::DaemonServerHandle, ProcCtlReq};

use super::{
    migration::DataMigrationTask, policy::ScheduleQueue, statistics::StopReason, ShmBufferManager,
};

#[derive(Debug, Clone, Copy)]
pub(super) enum ActiveClientState {
    None,
    Active { pid: i32, since: Instant },
    LastActive { pid: i32, last_active: Instant },
}

pub struct Scheduler {
    list: Arc<RwLock<LinkedHashMap<i32, DaemonServerHandle>>>,
    rpc_data_rx: mpsc::UnboundedReceiver<(i32, ActivityUpdate)>,
    active_client: ActiveClientState,
    sched_queue: ScheduleQueue,
    shmem_buffer: ShmBufferManager,
}

impl Scheduler {
    pub fn new(
        list: Arc<RwLock<LinkedHashMap<i32, DaemonServerHandle>>>,
        rpc_data_rx: mpsc::UnboundedReceiver<(i32, ActivityUpdate)>,
        shmem_buffer: ShmBufferManager,
    ) -> Self {
        Self {
            list,
            rpc_data_rx,
            active_client: ActiveClientState::None,
            sched_queue: ScheduleQueue::new(),
            shmem_buffer,
        }
    }

    pub async fn run(mut self) {
        tracing::info!("Starting scheduler...");
        let mut last_polled = Instant::now();
        loop {
            let sleep_duration = Duration::from_millis(100).saturating_sub(last_polled.elapsed());
            tokio::select! {
                Some((pid, data)) = self.rpc_data_rx.recv() => {
                    self.received_data(pid, data, &mut last_polled).await;
                }
                _ = tokio::time::sleep(sleep_duration) => {
                    self.poll_queue(&mut last_polled).await;
                }
            }
        }
    }

    async fn received_data(&mut self, pid: i32, data: ActivityUpdate, last_polled: &mut Instant) {
        self.sched_queue.push(pid, data);
        self.poll_queue(last_polled).await;
    }

    async fn poll_queue(&mut self, last_polled: &mut Instant) {
        if let ActiveClientState::Active { pid, .. } = self.active_client {
            let control = self.list.write().await;
            if control.get(&pid).is_none() {
                // the active process has exited
                self.active_client = ActiveClientState::None;
                self.sched_queue.remove_client(pid);
                tracing::debug!("Process {} exited", pid);
            }
        }
        self.sched_queue.update_active(self.active_client);
        if let Some(req) = self.sched_queue.pop(self.active_client) {
            if let Err(e) = self.handle_activity_update(req.pid, req.args).await {
                tracing::error!(
                    "Scheduler handling activity update from {}: {:?}",
                    req.pid,
                    e
                );
            }
        }
        *last_polled = Instant::now();
    }
    async fn handle_activity_update(
        &mut self,
        pid: i32,
        data: ActivityUpdate,
    ) -> Result<(), ScheduleError> {
        match data {
            ActivityUpdate::RequestScheduling { memory_request } => {
                // TODO: handle memory request
                let _ = memory_request;
                self.handle_sched_request(pid).await
            }
            ActivityUpdate::Idle => {
                self.handle_activity_idle(pid).await;
                Ok(())
            }
        }
    }

    async fn handle_sched_request(&mut self, incoming_pid: i32) -> Result<(), ScheduleError> {
        let control = self.list.write().await;
        let config = load_config();
        let mut swap_out_mb = None;

        if let Some((active_pid, _previous_running)) = match self.active_client {
            ActiveClientState::None => None,
            ActiveClientState::Active { pid, .. } => {
                let incoming_level = self
                    .sched_queue
                    .get_client(incoming_pid)
                    // incoming client not exists, by default with the highest
                    .map_or_else(|| PriorityLevel::max(), |c| c.priority.level());

                if let Some(client) = self.sched_queue.get_client_mut(pid) {
                    let preemption_reason = {
                        if incoming_level > client.priority.level() {
                            PreemptionReason::HigherPriority
                        } else {
                            PreemptionReason::RoundRobin
                        }
                    };
                    client.make_idle(StopReason::PreemptedBy(incoming_pid, preemption_reason));
                }
                Some((pid, true))
            }
            ActiveClientState::LastActive { pid, .. } => {
                if let Some(client) = self.sched_queue.get_client_mut(pid) {
                    client.make_idle(StopReason::LazyIdle);
                }
                Some((pid, false))
            }
        } {
            if incoming_pid != active_pid {
                // active pid can exit before scheduler knows in `else` branch
                if let Some(cur_handle) = control.get(&active_pid) {
                    tracing::trace!("Scheduling out process {}", active_pid);
                    let cur_client = cur_handle.client();
                    let disable_current_fut = tokio::spawn(async move {
                        cur_client
                            .schedule(tarpc::context::current(), SchedulingArgs::Disable)
                            .await
                    });

                    // get info about incoming process
                    let incoming_request = if let Some(new_handle) = control.get(&incoming_pid) {
                        let (para, fut) = CallParameter::new(ProcessResidualRequest {
                            pid: incoming_pid,
                            on_gpu: false,
                            gpu_list: (0..MAX_GPUS)
                                .into_iter()
                                .map(|i| GlobalDeviceId(i as i32))
                                .collect(),
                        });
                        let _ = new_handle
                            .inst_tx()
                            .send(ProcCtlReq::ListProcessResidual(para));
                        fut.await
                    } else {
                        None
                    }
                    .unwrap_or_else(|| ProcessResidualData {
                        pid: incoming_pid,
                        allocations: HashMap::new(),
                    });

                    // make sure the current process is device-synchronized
                    disable_current_fut
                        .await
                        .unwrap()
                        .map_err(|e| ScheduleError::RpcError("schedule out", active_pid, e))?;

                    let cur_residual = {
                        let (para, fut) = CallParameter::new(ProcessResidualRequest {
                            pid: active_pid,
                            on_gpu: true,
                            gpu_list: incoming_request.allocations.keys().cloned().collect(),
                        });
                        let _ = cur_handle
                            .inst_tx()
                            .send(ProcCtlReq::ListProcessResidual(para));
                        fut.await
                    }
                    .expect("Failed to get current process residual data");

                    let task = two_processes_plan(&incoming_request, &cur_residual);
                    swap_out_mb = task.get_src().get(0).map(|(_, spec, _, _)| {
                        spec.device_map
                            .values()
                            .map(|entries| entries.iter().map(|entry| entry.size).sum::<u64>())
                            .sum::<u64>()
                            / (1024 * 1024) // convert to MB
                    });
                    task.run().await
                } else {
                    // the previous active process has exited
                    self.active_client = ActiveClientState::None;
                    self.sched_queue.remove_client(active_pid);
                }
            }
        }

        // use the larger one between preempt_delay and schedule_delay
        if let Some(delay) = config
            .schedule_delay
            .map(|d| config.preempt_delay.unwrap_or_default().max(d))
        {
            tokio::time::sleep(delay).await;
        }

        let client = self.sched_queue.get_client_mut_or_insert(incoming_pid);

        self.active_client = ActiveClientState::Active {
            pid: incoming_pid,
            since: Instant::now(),
        };
        tracing::trace!("Scheduling in process {}", incoming_pid);
        control
            .get(&incoming_pid)
            .unwrap()
            .client()
            .schedule(
                tarpc::context::current(),
                // Only prefetch when the active process is different from the new one
                SchedulingArgs::Enable,
            )
            .await
            .map_err(|e| ScheduleError::RpcError("schedule in", incoming_pid, e))?;
        client.make_active();
        let cooldown = ScheduleQueue::compute_cooldown(
            swap_out_mb.unwrap_or_default(),
            config.schedule_cooldown,
            client.priority,
        );
        tracing::debug!(
            "Process {}: {:?}, cooldown={:.2}s",
            incoming_pid,
            client,
            cooldown.as_secs_f64()
        );

        // prevent thrashing
        self.sched_queue.cooldown(Some(cooldown));
        Ok(())
    }

    async fn handle_activity_idle(&mut self, pid: i32) {
        // TODO: LastActive
        if let ActiveClientState::Active {
            pid: active_pid, ..
        } = self.active_client
        {
            if let Some(client) = self.sched_queue.get_client_mut(pid) {
                if active_pid == pid {
                    client.make_resident_idle(StopReason::Idle);
                    self.active_client = ActiveClientState::LastActive {
                        pid,
                        last_active: Instant::now(),
                    };
                    tracing::debug!("Process {} becomes idle", pid);
                    self.sched_queue.cooldown(None);
                    return;
                }
            }
        }
        tracing::error!("Process {} becomes idle but is not active client", pid);
    }
}

fn two_processes_plan(
    dst_data: &ProcessResidualData,
    src_data: &ProcessResidualData,
) -> DataMigrationTask {
}
