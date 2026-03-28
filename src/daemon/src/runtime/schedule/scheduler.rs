use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use itertools::Itertools;
use tokio::sync::RwLock;

use hashlink::LinkedHashMap;
use nixie_common::{
    ActivityUpdate, ActivityUpdateContent, GlobalDeviceId, MAX_GPUS, MemoryRequest, SchedulingArgs,
    general::CallParameter, rpc::SidecarClient,
};
use tokio::sync::mpsc;

use crate::{
    config::load_config,
    control::{PrefetchArgs, PrefetchResponse, ProcessResidualData, ProcessResidualRequest},
    error::ScheduleError,
    runtime::{
        daemon_server::DeviceOrdinalMapping,
        migration::{
            BufferId, BufferLocation, DataManagerHandle,
            migration_plan::{
                DeviceRequestArgs, MigrationRequirement, gpu_prefetch_task, local_prefetch_task,
                realtime_migrate_task,
            },
        },
        schedule::{
            PriorityLevel, ScheduleRpcMessage,
            control::{GetStateResponse, ScheduleControlReq},
            policy::{IdleRequest, IdleRequestType},
            statistics::PreemptionReason,
        },
    },
};

use crate::runtime::{ProcCtlReq, daemon_server::DaemonServerHandle};

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
    rpc_data_rx: mpsc::UnboundedReceiver<(i32, ScheduleRpcMessage)>,
    control_msg_rx: mpsc::UnboundedReceiver<ScheduleControlReq>,
    active_client: ActiveClientState,
    sched_queue: ScheduleQueue,
    data_manager: DataManagerHandle,
}

impl Scheduler {
    pub fn new(
        list: Arc<RwLock<LinkedHashMap<i32, DaemonServerHandle>>>,
        rpc_data_rx: mpsc::UnboundedReceiver<(i32, ScheduleRpcMessage)>,
        control_msg_rx: mpsc::UnboundedReceiver<ScheduleControlReq>,
        data_manager: DataManagerHandle,
    ) -> Self {
        Self {
            list,
            rpc_data_rx,
            control_msg_rx,
            active_client: ActiveClientState::None,
            sched_queue: ScheduleQueue::new(data_manager.clone()),
            data_manager,
        }
    }

    pub async fn run(mut self) {
        tracing::info!("Starting scheduler...");
        let mut last_polled = Instant::now();
        loop {
            let sleep_duration = Duration::from_millis(100).saturating_sub(last_polled.elapsed());
            tokio::select! {
                Some((pid, data)) = self.rpc_data_rx.recv() => {
                    self.received_data(pid, data).await;
                    while let Ok((pid, data)) = self.rpc_data_rx.try_recv() {
                        self.received_data(pid, data).await;
                    }
                    self.poll_queue(&mut last_polled).await;
                }
                Some(req) = self.control_msg_rx.recv() =>{
                    self.handle_ctrl(req, &mut last_polled).await;
                }
                _ = tokio::time::sleep(sleep_duration) => {
                    self.poll_queue(&mut last_polled).await;
                }
            }
        }
    }

    async fn handle_ctrl(&mut self, req: ScheduleControlReq, last_polled: &mut Instant) {
        match req {
            ScheduleControlReq::GetState(param) => {
                let (pid, ret_tx) = param.into_parts();
                let res = self
                    .sched_queue
                    .get_client(pid)
                    .map(|stat| (stat.priority(), stat.state_ref().as_client_state()));
                let (state, priority) = match res {
                    Some((priority, state)) => (Some(state), Some(priority)),
                    None => (None, None),
                };
                if ret_tx.ret(GetStateResponse { state, priority }).is_err() {
                    tracing::warn!("Failed to send GetStateResponse");
                }
            }
            ScheduleControlReq::Prefetch(param) => {
                // Only after prefetch request is processed will result be sent back
                self.sched_queue.push_prefetch(param);
                self.poll_queue(last_polled).await;
            }
            ScheduleControlReq::SetPriority(param) => {
                let (args, ret_tx) = param.into_parts();
                let res = self.sched_queue.set_priority(args.pid, args.level);
                if ret_tx.ret(res).is_err() {
                    tracing::warn!("Failed to send SetPriorityResponse");
                }
                self.poll_queue(last_polled).await;
            }
            ScheduleControlReq::GetHistory(param) => {
                let (args, ret_tx) = param.into_parts();
                let res = self.sched_queue.get_history(args.pid);
                if ret_tx.ret(res).is_err() {
                    tracing::warn!("Failed to send GetHistoryResult");
                }
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

    async fn received_data(&mut self, pid: i32, data: ScheduleRpcMessage) {
        match data {
            ScheduleRpcMessage::ActivityUpdate(data) => {
                tracing::trace!("Received data from process {}: {:?}", pid, data);
                self.sched_queue.schedule_push(pid, data);
            }
            ScheduleRpcMessage::GpuMemoryFreeUpdate(data) => {
                let dev_mapping = {
                    let control = self.list.read().await;
                    let Some(handle) = control.get(&pid).map(|h| h.dev_mapping()) else {
                        return;
                    };
                    handle
                };
                let buf_ids = data
                    .freed_memory
                    .into_iter()
                    .filter_map(|(proc_dev_id, mem_handle_id, size)| {
                        Some(BufferId {
                            pid,
                            device_id: dev_mapping.visible_to_real(proc_dev_id)?,
                            block_id: mem_handle_id,
                            size: size as u32,
                        })
                    })
                    .collect::<Vec<_>>();
                self.data_manager.shm.batch_release(&buf_ids);
                self.data_manager.hostmem.batch_release(&buf_ids);
                self.data_manager.storage.batch_release(&buf_ids);
            }
        }
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
        // if we meet pending or yield, we continue trying to poll the queue
        while let Some(req) = self.sched_queue.schedule_pop(self.active_client) {
            match req {
                GenericRequest::Idle(req) => match req.request_type {
                    IdleRequestType::Idle => self.handle_activity_idle(req),
                    IdleRequestType::Yield => self.handle_activity_yield(req.pid),
                },
                GenericRequest::Schedule(req) => {
                    let res = match req.args.content {
                        ActivityUpdateContent::RequestScheduling => {
                            self.handle_sched_request(req.pid, None).await
                        }
                        ActivityUpdateContent::YieldThenRequestSchedulingAndMem {
                            memory_request,
                        } => {
                            // convert local device ids to global device ids
                            self.handle_sched_request(req.pid, Some(memory_request))
                                .await
                        }
                        ActivityUpdateContent::Idle => unreachable!(),
                    };
                    if let Err(e) = res {
                        tracing::error!(
                            "Scheduler handling activity update from {}: {:?}",
                            req.pid,
                            e
                        );
                    }
                    break;
                }
                GenericRequest::Prefetch(req) => {
                    let should_reset_last_active = match self.active_client {
                        ActiveClientState::LastActive { pid, .. } => {
                            req.parameter.param.list.iter().any(|r| r.pid == pid)
                        }
                        _ => false,
                    };
                    if should_reset_last_active {
                        self.active_client = ActiveClientState::None;
                        tracing::trace!("Clearing active client due to prefetch request");
                    }
                    let (param, ret_tx) = req.parameter.into_parts();
                    let rx_used = param.rx_used;
                    let res = self.handle_prefetch_request(param).await;
                    // TODO: handle error of `res`
                    if rx_used && ret_tx.ret(res.unwrap_or(PrefetchResponse)).is_err() {
                        tracing::warn!("Failed to send PrefetchResponse");
                    }
                    break;
                }
            }
        }
        *last_polled = Instant::now();
    }

    async fn handle_sched_request(
        &mut self,
        incoming_pid: i32,
        mem_req: Option<Box<MemoryRequest>>,
    ) -> Result<(), ScheduleError> {
        let control = self.list.write().await;
        let config = load_config();
        let mut swap_out = None;
        if mem_req.is_some() {
            tracing::debug!(
                "Process {} requests memory: {}",
                incoming_pid,
                nixie_common::general::pretty_size(
                    mem_req
                        .as_ref()
                        .unwrap()
                        .mem_req
                        .iter()
                        .map(|v| v.1.iter().sum::<u64>())
                        .sum::<u64>()
                )
            );
        } else {
            tracing::debug!("Process {} requests scheduling", incoming_pid);
        }

        if let Some((active_pid, previous_proc_is_running)) = match self.active_client {
            ActiveClientState::None => None,
            ActiveClientState::Active { pid, .. } => {
                if pid != incoming_pid {
                    let incoming_level = self
                        .sched_queue
                        .get_client(incoming_pid)
                        // incoming client not exists, by default with the highest
                        .map_or_else(PriorityLevel::max, |c| c.priority().level());

                    if let Some(client) = self.sched_queue.get_client_mut(pid) {
                        let preemption_reason = {
                            if incoming_level > client.priority().level() {
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
                if pid != incoming_pid
                    && let Some(client) = self.sched_queue.get_client_mut(pid)
                {
                    client.make_idle(StopReason::LazyIdle);
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
                self.data_manager.clone(),
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
            client.priority(),
        );
        // prevent thrashing
        self.sched_queue.cooldown(Some(cooldown));
        Ok(())
    }

    async fn handle_prefetch_request(
        &self,
        prefetch_args: PrefetchArgs,
    ) -> Result<PrefetchResponse, ScheduleError> {
        tracing::debug!("Received prefetch request: {:?}", prefetch_args);
        let (active_pid, active_idle_pid) = match self.active_client {
            ActiveClientState::Active { pid, .. } => (Some(pid), None),
            ActiveClientState::LastActive { pid, .. } => (None, Some(pid)),
            ActiveClientState::None => (None, None),
        };

        if let Some(pid) = active_pid
            && prefetch_args.list.iter().any(|req| req.pid == pid)
        {
            return Err(ScheduleError::InvalidPrefetchRequest(format!(
                "Cannot prefetch for the active process {}",
                pid
            )));
        }
        if prefetch_args
            .list
            .iter()
            .any(|r| matches!(r.from, BufferLocation::Gpu(_)))
        {
            return Err(ScheduleError::InvalidPrefetchRequest(
                "Cannot prefetch from GPU buffers".to_string(),
            ));
        }

        let (gpu_req, other_req) = prefetch_args
            .list
            .iter()
            .partition::<Vec<_>, _>(|r| matches!(r.to, BufferLocation::Gpu(_)));

        // handle GPU prefetch requests first if any
        if !gpu_req.is_empty() {
            let control = self.list.write().await;
            let in_pid = gpu_req[0].pid;
            if !gpu_req.iter().all(|r| r.pid == in_pid) {
                let error_msg = "All GPU prefetch requests must belong to the same process";
                tracing::warn!("{}", error_msg);
                return Err(ScheduleError::InvalidPrefetchRequest(error_msg.to_string()));
            }
            // first collect residual
            let (para, fut) = CallParameter::new(ProcessResidualRequest {
                pid: in_pid,
                on_gpu: false,
                gpu_list: (0..MAX_GPUS).map(|i| GlobalDeviceId(i as i32)).collect(),
            });
            let Some(new_handle) = control.get(&in_pid) else {
                return Err(ScheduleError::InvalidClient(in_pid));
            };
            let _ = new_handle
                .inst_tx()
                .send(ProcCtlReq::ListProcessResidual(para));
            let res_cadidate = fut
                .await
                .expect("Failed to get incoming process residual data");

            // match residuals and requests
            let res = {
                let mut res = HashMap::new();
                for req in gpu_req {
                    let BufferLocation::Gpu(dev_id) = req.to else {
                        unreachable!();
                    };
                    if let Some(data) = res_cadidate.allocations.get(&dev_id) {
                        let mut accu_size = 0;
                        let mut entries = Vec::new();
                        for entry in data.iter() {
                            if accu_size >= req.size {
                                break;
                            }
                            accu_size += entry.size as u64;
                            entries.push(entry.clone());
                        }
                        res.insert(dev_id, entries);
                    }
                }
                ProcessResidualData {
                    pid: in_pid,
                    allocations: res,
                }
            };

            let devs = res.allocations.keys().cloned().collect();
            let mut others = collect_all_residuals(
                control
                    .iter()
                    .filter_map(|(pid, handle)| {
                        if !(*pid == in_pid || active_pid.is_some_and(|active| active == *pid)) {
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

            // put active idle process to the end of others
            if let Some(idle_pid) = active_idle_pid
                && let Some(idle_idx) = others.iter().position(|(pid, _, _, _)| *pid == idle_pid)
            {
                let idle_entry = others.remove(idle_idx);
                others.push(idle_entry);
            }

            // then create GPU prefetch task
            let Some(gpu_task) = gpu_prefetch_task(
                active_pid,
                (in_pid, res, new_handle.client(), new_handle.dev_mapping()),
                others,
                self.data_manager.clone(),
            ) else {
                let error_msg = "Cannot create GPU prefetch task";
                tracing::warn!("{}", error_msg);
                return Err(ScheduleError::InvalidPrefetchRequest(error_msg.to_string()));
            };
            gpu_task.run().await;
        }

        if !other_req.is_empty() {
            let Some(task) = local_prefetch_task::<SidecarClient, DataManagerHandle>(
                other_req
                    .into_iter()
                    .map(|e| {
                        (
                            e.pid,
                            MigrationRequirement {
                                from: e.from,
                                to: e.to,
                                size: e.size,
                                allow_incomplete: true,
                            },
                        )
                    })
                    .sorted_by(|a, b| a.0.cmp(&b.0))
                    .fold(Vec::new(), |mut acc, item| {
                        if let Some(last) = acc.last_mut()
                            && last.0 == item.0
                        {
                            last.1.push(item.1);
                            return acc;
                        }
                        acc.push((item.0, vec![item.1]));
                        acc
                    }),
                self.data_manager.clone(),
            ) else {
                let error_msg = "Cannot create prefetch task";
                tracing::warn!("{}", error_msg);
                return Err(ScheduleError::InvalidPrefetchRequest(error_msg.to_string()));
            };
            task.run().await;
        }
        Ok(PrefetchResponse)
    }

    async fn perform_migration(
        incoming_pid: i32,
        active_pid: i32,
        disable_fut: Option<tokio::task::JoinHandle<Result<(), tarpc::client::RpcError>>>, // future of dev sync completion
        mem_req: Option<Box<MemoryRequest>>,
        control: &LinkedHashMap<i32, DaemonServerHandle>,
        data_manager: DataManagerHandle,
    ) -> Result<Option<u64>, ScheduleError> {
        let mut swap_out = None;
        // disable current process if needed; and migrate VRAM
        if incoming_pid != active_pid || mem_req.is_some() {
            // get info about incoming process
            let Some(new_handle) = control.get(&incoming_pid) else {
                return Err(ScheduleError::InvalidClient(incoming_pid));
            };
            let (incoming_request, devs) = if let Some(mem_req) = mem_req {
                let requirement = {
                    let mut req_map = HashMap::new();
                    let dev_mapping = new_handle.dev_mapping();
                    for (local_dev, entries) in mem_req.mem_req.into_iter() {
                        let size = entries.iter().sum::<u64>();
                        if size > 0 {
                            let real_dev = dev_mapping
                                .visible_to_real(local_dev)
                                .ok_or_else(|| ScheduleError::Unavailable(format!(
                                    "Device mapping not found for local device id {:?} of process {}",
                                    local_dev, incoming_pid
                                )))?;
                            req_map.insert(real_dev, size);
                        }
                    }
                    req_map
                };
                let devs = requirement.keys().cloned().collect();
                (DeviceRequestArgs::Allocation(requirement), devs)
            } else {
                let (para, fut) = CallParameter::new(ProcessResidualRequest {
                    pid: incoming_pid,
                    on_gpu: false,
                    gpu_list: (0..MAX_GPUS).map(|i| GlobalDeviceId(i as i32)).collect(),
                });
                let _ = new_handle
                    .inst_tx()
                    .send(ProcCtlReq::ListProcessResidual(para));
                let res = fut
                    .await
                    .expect("Failed to get incoming process residual data");
                let devs = res.allocations.keys().cloned().collect();
                (DeviceRequestArgs::ResidualData(res), devs)
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

            let Some(task) = realtime_migrate_task(
                (
                    incoming_pid,
                    incoming_request,
                    new_handle.client(),
                    new_handle.dev_mapping(),
                ),
                others,
                data_manager,
                true,
            ) else {
                return Err(ScheduleError::Unavailable(
                    "Cannot create migration task".to_string(),
                ));
            };
            // for statistics
            swap_out = task.get_out_from_gpu().first().map(|(_, spec, _, _)| {
                spec.device_map
                    .values()
                    .map(|entries| entries.iter().map(|entry| entry.size as u64).sum::<u64>())
                    .sum::<u64>()
            });
            task.run().await
        }
        Ok(swap_out)
    }

    fn handle_activity_idle(&mut self, req: IdleRequest) {
        // TODO: LastActive
        let elapsed = req.time.elapsed();
        if elapsed > Duration::from_millis(1) {
            tracing::warn!(
                "Process {} sent idle request after {}ms, which is too long ago",
                req.pid,
                elapsed.as_millis()
            );
        }
        if let ActiveClientState::Active {
            pid: active_pid, ..
        } = self.active_client
            && let Some(client) = self.sched_queue.get_client_mut(req.pid)
            && active_pid == req.pid
        {
            client.make_resident_idle(StopReason::Idle);
            self.active_client = ActiveClientState::LastActive {
                pid: req.pid,
                last_active: Instant::now(),
            };
            tracing::debug!("Process {} becomes idle", req.pid);
            self.sched_queue.cooldown(None);
            return;
        }
        tracing::error!("Process {} becomes idle but is not active client", req.pid);
    }

    fn handle_activity_yield(&mut self, pid: i32) {
        if let ActiveClientState::Active {
            pid: active_pid, ..
        } = self.active_client
            && let Some(client) = self.sched_queue.get_client_mut(pid)
            && active_pid == pid
        {
            client.make_resident_idle(StopReason::YieldAndPending);
            self.active_client = ActiveClientState::LastActive {
                pid,
                last_active: Instant::now(),
            };
            self.sched_queue.cooldown(None);
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
                Duration::from_millis(1200),
                futures::FutureExt::map(fut, move |res| {
                    res.map(|data| {
                        let mapping = handle.dev_mapping().clone();
                        (*pid, data, handle.client().clone(), mapping)
                    })
                }),
            )
        })
        .collect::<Vec<_>>();
    let fut_pids = list.iter().map(|(pid, _)| *pid).collect::<Vec<_>>();
    let results = futures::future::join_all(fut_list).await;
    let res = results
        .into_iter()
        .filter_map(|x| x.ok().flatten())
        .collect::<Vec<_>>();
    if res.len() < fut_pids.len() {
        tracing::warn!(
            "Failed to collect all residuals: expected {:?}, got {:?}",
            fut_pids,
            res.iter().map(|(pid, _, _, _)| *pid).collect::<Vec<_>>()
        );
    }
    res
}
