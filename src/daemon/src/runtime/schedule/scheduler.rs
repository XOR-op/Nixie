use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

use hashlink::LinkedHashMap;
use nihil_common::{
    general::CallParameter, rpc::SidecarClient, ActivityUpdate, GlobalDeviceId, MemoryRequest,
    SchedulingArgs, MAX_GPUS,
};
use tokio::sync::mpsc;

use crate::{
    config::load_config,
    control::{ProcessResidualData, ProcessResidualRequest},
    error::ScheduleError,
    runtime::{
        daemon_server::DeviceOrdinalMapping,
        schedule::{
            control::{GetStateResponse, ScheduleControlReq},
            policy::IdleRequestType,
            statistics::PreemptionReason,
            PriorityLevel,
        },
        swap::{
            migration_plan::{two_processes_task, DstRequestArgs},
            HybridBufferManager, ShmBufferManager,
        },
    },
};

use crate::runtime::{daemon_server::DaemonServerHandle, ProcCtlReq};

use super::{
    policy::{GenericRequest, ScheduleQueue},
    statistics::StopReason,
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
    prefetch_rx: mpsc::UnboundedReceiver<(i32, ActivityUpdate)>,
    control_msg_rx: mpsc::UnboundedReceiver<ScheduleControlReq>,
    active_client: ActiveClientState,
    sched_queue: ScheduleQueue,
    shmem_buffer: Arc<ShmBufferManager>,
    hybrid_buffer: Arc<HybridBufferManager>,
}

impl Scheduler {
    pub fn new(
        list: Arc<RwLock<LinkedHashMap<i32, DaemonServerHandle>>>,
        rpc_data_rx: mpsc::UnboundedReceiver<(i32, ActivityUpdate)>,
        prefetch_rx: mpsc::UnboundedReceiver<(i32, ActivityUpdate)>,
        control_msg_rx: mpsc::UnboundedReceiver<ScheduleControlReq>,
        shmem_buffer: Arc<ShmBufferManager>,
        hybrid_buffer: Arc<HybridBufferManager>,
    ) -> Self {
        Self {
            list,
            rpc_data_rx,
            prefetch_rx,
            control_msg_rx,
            active_client: ActiveClientState::None,
            sched_queue: ScheduleQueue::new(),
            shmem_buffer,
            hybrid_buffer,
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
                Some((pid, data)) = self.prefetch_rx.recv() => {
                    self.received_prioritized_data(pid, data, &mut last_polled).await;
                }
                Some(req) = self.control_msg_rx.recv() =>{
                    self.handle_ctrl(req).await;
                }
                _ = tokio::time::sleep(sleep_duration) => {
                    self.poll_queue(&mut last_polled).await;
                }
            }
        }
    }

    async fn handle_ctrl(&mut self, req: ScheduleControlReq) {
        match req {
            ScheduleControlReq::GetState(param) => {
                let (pid, ret_tx) = param.into_parts();
                let res = self
                    .sched_queue
                    .get_client(pid)
                    .map(|stat| (stat.priority, stat.state.as_client_state()));
                let (state, priority) = match res {
                    Some((priority, state)) => (Some(state), Some(priority)),
                    None => (None, None),
                };
                ret_tx.ret(GetStateResponse { state, priority });
            }
        }
    }

    async fn received_prioritized_data(
        &mut self,
        pid: i32,
        data: ActivityUpdate,
        last_polled: &mut Instant,
    ) {
        self.sched_queue.prioritized_push(pid, data);
        self.poll_queue(last_polled).await;
    }

    async fn received_data(&mut self, pid: i32, data: ActivityUpdate, last_polled: &mut Instant) {
        tracing::trace!("Received data from process {}: {:?}", pid, data);
        self.sched_queue.schedule_push(pid, data);
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
        if let Some(req) = self.sched_queue.schedule_pop(self.active_client) {
            tracing::trace!("Handling request from process {}: {:?}", req.pid(), req);
            match req {
                GenericRequest::Idle(req) => match req.request_type {
                    IdleRequestType::Idle => self.handle_activity_idle(req.pid),
                    IdleRequestType::Yield => self.handle_activity_yield(req.pid),
                },
                GenericRequest::Schedule(req) => {
                    let res = match req.args {
                        ActivityUpdate::RequestScheduling => {
                            self.handle_sched_request(req.pid, None).await
                        }
                        ActivityUpdate::YieldThenRequestSchedulingAndMem { memory_request } => {
                            self.handle_sched_request(req.pid, Some(memory_request))
                                .await
                        }
                        ActivityUpdate::Idle => unreachable!(),
                    };
                    if let Err(e) = res {
                        tracing::error!(
                            "Scheduler handling activity update from {}: {:?}",
                            req.pid,
                            e
                        );
                    }
                }
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
            ActivityUpdate::RequestScheduling => self.handle_sched_request(pid, None).await,
            ActivityUpdate::YieldThenRequestSchedulingAndMem { memory_request } => {
                self.handle_activity_yield(pid);
                self.handle_sched_request(pid, Some(memory_request)).await
            }
            ActivityUpdate::Idle => {
                self.handle_activity_idle(pid);
                Ok(())
            }
        }
    }

    async fn handle_sched_request(
        &mut self,
        incoming_pid: i32,
        mem_req: Option<MemoryRequest>,
    ) -> Result<(), ScheduleError> {
        let control = self.list.write().await;
        let config = load_config();
        let mut swap_out = None;
        if mem_req.is_some() {
            tracing::debug!(
                "Process {} requests scheduling with memory requirement: {:?}",
                incoming_pid,
                mem_req
            );
        }

        if let Some((active_pid, previous_proc_is_running)) = match self.active_client {
            ActiveClientState::None => None,
            ActiveClientState::Active { pid, .. } => {
                if pid != incoming_pid {
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
                }
                Some((pid, true))
            }
            ActiveClientState::LastActive { pid, .. } => {
                if pid != incoming_pid {
                    if let Some(client) = self.sched_queue.get_client_mut(pid) {
                        client.make_idle(StopReason::LazyIdle);
                    }
                }
                Some((pid, false))
            }
        } {
            if control.get(&active_pid).is_none() {
                // the previous active process has exited
                self.active_client = ActiveClientState::None;
                self.sched_queue.remove_client(active_pid);
            }
            let disable_current_fut = if incoming_pid != active_pid && previous_proc_is_running {
                // active pid can exit before scheduler knows in `else` branch
                if let Some(cur_handle) = control.get(&active_pid) {
                    tracing::trace!("Scheduling out process {}", active_pid);
                    Some({
                        let cur_client = cur_handle.client();
                        tokio::spawn(async move {
                            cur_client
                                .schedule(tarpc::context::current(), SchedulingArgs::Disable)
                                .await
                        })
                    })
                } else {
                    None
                }
            } else {
                None
            };
            swap_out = Self::perform_migration(
                incoming_pid,
                active_pid,
                disable_current_fut,
                mem_req,
                &control,
                &self.shmem_buffer,
                &self.hybrid_buffer,
            )
            .await?;
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
            swap_out.map(|x| x / (1024 * 1024)).unwrap_or_default(),
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

    async fn perform_migration(
        incoming_pid: i32,
        active_pid: i32,
        disable_fut: Option<tokio::task::JoinHandle<Result<(), tarpc::client::RpcError>>>,
        mem_req: Option<MemoryRequest>,
        control: &LinkedHashMap<i32, DaemonServerHandle>,
        shmem_buffer: &Arc<ShmBufferManager>,
        hybrid_buffer: &Arc<HybridBufferManager>,
    ) -> Result<Option<u64>, ScheduleError> {
        let mut swap_out = None;
        // disable current process if needed; and migrate VRAM
        if incoming_pid != active_pid || mem_req.is_some() {
            // get info about incoming process
            let Some(new_handle) = control.get(&incoming_pid) else {
                return Err(ScheduleError::InvalidClient(incoming_pid));
            };
            let (incoming_request, devs) = if let Some(mem_req) = mem_req {
                let requirement = mem_req
                    .mem_req
                    .into_iter()
                    .enumerate()
                    .filter_map(|(global_id, entries)| {
                        let size = entries.iter().sum::<u64>();
                        if size > 0 {
                            Some((GlobalDeviceId(global_id as i32), size))
                        } else {
                            None
                        }
                    })
                    .collect::<HashMap<_, _>>();
                let devs = requirement.keys().cloned().collect();
                (DstRequestArgs::Allocation(requirement), devs)
            } else {
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
                let res = fut
                    .await
                    .expect("Failed to get incoming process residual data");
                let devs = res.allocations.keys().cloned().collect();
                (DstRequestArgs::ResidualData(res), devs)
            };

            // make sure the current process is device-synchronized
            if let Some(fut) = disable_fut {
                fut.await
                    .unwrap()
                    .map_err(|e| ScheduleError::RpcError("schedule out", active_pid, e))?;
            }

            let others = collect_all_residuals(
                control
                    .iter()
                    .filter_map(|(pid, handle)| {
                        if *pid != incoming_pid {
                            Some((*pid, handle))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
                    .as_slice(),
                true,
                devs,
            )
            .await;

            let task = two_processes_task(
                (
                    incoming_pid,
                    incoming_request,
                    new_handle.client(),
                    new_handle.dev_mapping(),
                ),
                &others,
                shmem_buffer.clone(),
                hybrid_buffer.clone(),
            );
            // for statistics
            swap_out = task.get_src().get(0).map(|(_, spec, _, _)| {
                spec.device_map
                    .values()
                    .map(|entries| entries.iter().map(|entry| entry.size).sum::<u64>())
                    .sum::<u64>()
            });
            task.run().await
        }
        Ok(swap_out)
    }

    fn handle_activity_idle(&mut self, pid: i32) {
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

    fn handle_activity_yield(&mut self, pid: i32) {
        if let ActiveClientState::Active {
            pid: active_pid, ..
        } = self.active_client
        {
            if let Some(client) = self.sched_queue.get_client_mut(pid) {
                if active_pid == pid {
                    client.make_resident_idle(StopReason::YieldAndPending);
                    self.active_client = ActiveClientState::LastActive {
                        pid,
                        last_active: Instant::now(),
                    };
                    self.sched_queue.cooldown(None);
                    return;
                }
            }
        }
        // it's ok that the incoming pid is not active, we just ignore it
    }
}

async fn collect_all_residuals(
    list: &[(i32, &DaemonServerHandle)],
    on_gpu: bool,
    gpu_list: Vec<GlobalDeviceId>,
) -> Vec<(
    i32,
    ProcessResidualData,
    SidecarClient,
    Arc<DeviceOrdinalMapping>,
)> {
    let fut_list = list
        .iter()
        .map(|(pid, handle)| {
            let (para, fut) = CallParameter::new(ProcessResidualRequest {
                pid: *pid,
                on_gpu,
                gpu_list: gpu_list.clone(),
            });
            let _ = handle.inst_tx().send(ProcCtlReq::ListProcessResidual(para));
            tokio::time::timeout(
                Duration::from_millis(1000),
                futures::FutureExt::map(fut, move |res| {
                    res.map(|data| {
                        let mapping = handle.dev_mapping().clone();
                        (*pid, data, handle.client().clone(), mapping)
                    })
                }),
            )
        })
        .collect::<Vec<_>>();
    let results = futures::future::join_all(fut_list).await;
    results
        .into_iter()
        .filter_map(|x| x.ok().flatten())
        .collect::<Vec<_>>()
}
