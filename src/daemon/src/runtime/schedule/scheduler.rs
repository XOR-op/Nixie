use std::{
    collections::HashMap,
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

use hashlink::LinkedHashMap;
use nihil_common::{general::CallParameter, ActivityUpdate, MemoryUsage, SchedulingArgs};
use tokio::sync::mpsc;

use crate::{
    config::load_config,
    error::ScheduleError,
    runtime::schedule::{statistics::PreemptionReason, PriorityLevel},
};

use crate::runtime::{
    daemon_server::{DaemonServerHandle, DeviceOrdinalMapping},
    ProcCtlReq, ProcessMetadata,
};

use super::{policy::ScheduleQueue, statistics::StopReason};

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
}

impl Scheduler {
    pub fn new(
        list: Arc<RwLock<LinkedHashMap<i32, DaemonServerHandle>>>,
        rpc_data_rx: mpsc::UnboundedReceiver<(i32, ActivityUpdate)>,
    ) -> Self {
        Self {
            list,
            rpc_data_rx,
            active_client: ActiveClientState::None,
            sched_queue: ScheduleQueue::new(),
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
            ActivityUpdate::RequestScheduling {
                mem_usage_per_device,
            } => self.handle_sched_request(pid, mem_usage_per_device).await,
            ActivityUpdate::Idle => {
                self.handle_activity_idle(pid).await;
                Ok(())
            }
        }
    }

    async fn handle_sched_request(
        &mut self,
        incoming_pid: i32,
        mem_usage_per_device: Vec<MemoryUsage>,
    ) -> Result<(), ScheduleError> {
        let control = self.list.write().await;
        let config = load_config();
        let mut should_prefetch = false;
        let mut swap_out_mb = None;

        if let Some((active_pid, previous_running)) = match self.active_client {
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
                should_prefetch = true;
                // active pid can exit before scheduler knows
                if let Some(handle) = control.get(&active_pid) {
                    tracing::trace!("Scheduling out process {}", active_pid);

                    let (swap_out, old_remaining_est) = {
                        let cur_proc_dev_mapping = handle.dev_mapping();
                        if let Some(new_handle) = control.get(&incoming_pid) {
                            let (para, fut) = CallParameter::new(());
                            let _ = handle.inst_tx().send(ProcCtlReq::List(para));
                            let cur_list = fut.await;
                            Self::compute_eviction(
                                &mem_usage_per_device,
                                cur_list,
                                cur_proc_dev_mapping,
                                new_handle.dev_mapping(),
                            )
                        } else {
                            (Vec::new(), 0)
                        }
                    };
                    {
                        let swap_out_in_gb = swap_out
                            .iter()
                            .map(|x| x.map_or(0.0, |x| x.get() as f64))
                            .sum::<f64>()
                            / 1024.0;
                        tracing::trace!(
                            "Swap out {:2} GB from process {}, remaining estimate: {} MB",
                            swap_out_in_gb,
                            active_pid,
                            old_remaining_est / (1024 * 1024)
                        );
                        tracing::trace!("Swap out {:?} from {}", swap_out, active_pid);
                    }

                    swap_out_mb = Some(swap_out.iter().map(|x| x.map_or(0, |x| x.get())).sum());
                    handle
                        .client()
                        .schedule(tarpc::context::current(), SchedulingArgs::Disable)
                        .await
                        .map_err(|e| ScheduleError::RpcError("schedule out", active_pid, e))?;
                    // update statistics for old process
                    if let Some(client) = self.sched_queue.get_client_mut(active_pid) {
                        client.update_on_gpu_mem_est(old_remaining_est);
                    }
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
        client.make_active(mem_usage_per_device.iter().map(|x| x.mem_usage_bytes).sum());
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

    // Devices in and out are not global indexed but per process
    // We should perform corresponding mapping accordingly
    fn compute_eviction(
        mem_usage_per_device: &[MemoryUsage],
        cur_proc_list: Option<ProcessMetadata>,
        old_proc_mapping: &DeviceOrdinalMapping,
        new_proc_mapping: &DeviceOrdinalMapping,
    ) -> (Vec<Option<NonZeroU64>>, u64) {
        let global_config = load_config();
        // For new process
        let mem_demanding = mem_usage_per_device
            .iter()
            .enumerate()
            .filter_map(|(i, dev)| {
                new_proc_mapping
                    .visible_to_real(i as i32)
                    .map(|real_dev| (real_dev, dev))
            })
            .collect::<HashMap<_, _>>();
        // For old process
        let mem_occupied = cur_proc_list
            .map(|list| {
                let mut map = HashMap::new();
                for alloc in list.allocations {
                    if let Some(real_dev) = old_proc_mapping.visible_to_real(alloc.device) {
                        use std::collections::hash_map::Entry;
                        match map.entry(real_dev) {
                            Entry::Occupied(mut e) => {
                                let val: &mut MemoryUsage = e.get_mut();
                                val.mem_usage_bytes += alloc.size;
                                val.alloc_count += 1;
                            }
                            Entry::Vacant(e) => {
                                e.insert(MemoryUsage {
                                    mem_usage_bytes: alloc.size,
                                    alloc_count: 1,
                                });
                            }
                        }
                    }
                }
                map
            })
            .unwrap_or_default();
        // For old process
        let dev_threshold = global_config.device_threshold;
        let mem_evicted_mb = mem_occupied
            .iter()
            .filter_map(|(dev, occupied)| {
                mem_demanding.get(dev).and_then(|demanding| {
                    // now we assume only these two processes are using GPU memory
                    let mem_evicted_mb = ((demanding.mem_usage_bytes + occupied.mem_usage_bytes)
                        / (1024 * 1024))
                        // estimated 5% of the total memory is reserved for drivers and other usages
                        .saturating_sub(
                            ((global_config.device_memory_mb[*dev as usize] as f64) * dev_threshold)
                                as u64,
                        );
                    if mem_evicted_mb > 0 {
                        Some((
                            old_proc_mapping.real_to_visible(*dev).unwrap(),
                            mem_evicted_mb,
                        ))
                    } else {
                        None
                    }
                })
            })
            .collect::<HashMap<_, _>>();
        // Construct the eviction list
        let mut swap_out = Vec::new();
        for (dev, mb) in mem_evicted_mb {
            let dev = dev as usize;
            if dev >= swap_out.len() {
                swap_out.resize(dev + 1, None);
            }
            swap_out[dev] = NonZeroU64::new(mb);
        }
        let old_remaining_total = mem_occupied
            .values()
            .map(|mem| mem.mem_usage_bytes)
            .sum::<u64>()
            .saturating_sub(
                swap_out
                    .iter()
                    .flatten()
                    .map(|mb| mb.get() * 1024 * 1024)
                    .sum::<u64>(),
            );
        (swap_out, old_remaining_total)
    }
}
