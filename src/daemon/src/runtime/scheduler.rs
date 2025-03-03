use std::{
    collections::HashMap,
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

use hashlink::LinkedHashMap;
use nihilipc::{ActivityUpdate, MemoryUsage, SchedulingArgs};
use tokio::sync::mpsc;

use crate::{
    config::load_config, error::ScheduleError, general::CallParameter, runtime::ProcCtlReq,
};

use super::{
    daemon_server::{DaemonServerHandle, DeviceOrdinalMapping},
    sched_policy::ScheduleQueue,
    ProcessMetadata,
};

pub struct Scheduler {
    list: Arc<RwLock<LinkedHashMap<i32, DaemonServerHandle>>>,
    rpc_data_rx: mpsc::UnboundedReceiver<(i32, ActivityUpdate)>,
    active_client: Option<i32>,
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
            active_client: None,
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
        if let Some(req) = self.sched_queue.pop() {
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
        let control = self.list.write().await;
        if let Some(active_pid) = self.active_client {
            if pid != active_pid {
                // active pid can exit before scheduler knows
                if let Some(handle) = control.get(&active_pid) {
                    tracing::trace!("Scheduling out process {}", active_pid);

                    let (swap_out, old_remaining_est) = {
                        let cur_proc_dev_mapping = handle.dev_mapping();
                        if let Some(new_handle) = control.get(&pid) {
                            let (para, fut) = CallParameter::new(());
                            let _ = handle.inst_tx().send(ProcCtlReq::List(para));
                            let cur_list = fut.await;
                            Self::compute_eviction(
                                &data,
                                cur_list,
                                &cur_proc_dev_mapping,
                                &new_handle.dev_mapping(),
                            )
                        } else {
                            (Vec::new(), 0)
                        }
                    };
                    tracing::trace!("Swap out {:?} from {}", swap_out, active_pid);

                    handle
                        .client()
                        .schedule(
                            tarpc::context::current(),
                            SchedulingArgs::Disable {
                                swap_out_mb: swap_out,
                            },
                        )
                        .await
                        .map_err(|e| ScheduleError::RpcError("schedule out", active_pid, e))?;

                    // update statistics for old process
                    if let Some(client) = self.sched_queue.get_client_mut(active_pid) {
                        client.schedule_out(old_remaining_est);
                    }
                } else {
                    self.active_client = None;
                    self.sched_queue.remove_client(active_pid);
                }
            }
        }

        let config = load_config();
        if let Some(delay) = config.schedule_delay {
            tokio::time::sleep(delay).await;
        }

        let client = self.sched_queue.get_client_mut_or_insert(pid);

        self.active_client = Some(pid);
        tracing::trace!("Scheduling in process {}", pid);
        control
            .get(&pid)
            .unwrap()
            .client()
            .schedule(
                tarpc::context::current(),
                SchedulingArgs::Enable { prefetch: true },
            )
            .await
            .map_err(|e| ScheduleError::RpcError("schedule in", pid, e))?;
        client.schedule_in(
            data.mem_usage_per_device
                .iter()
                .map(|x| x.mem_usage_bytes)
                .sum(),
        );

        // prevent thrashing
        self.sched_queue.cooldown(config.schedule_cooldown);
        Ok(())
    }

    // Devices in and out are not global indexed but per process
    // We should perform corresponding mapping accordingly
    fn compute_eviction(
        data: &ActivityUpdate,
        cur_proc_list: Option<ProcessMetadata>,
        old_proc_mapping: &DeviceOrdinalMapping,
        new_proc_mapping: &DeviceOrdinalMapping,
    ) -> (Vec<Option<NonZeroU64>>, u64) {
        let global_config = load_config();
        // For new process
        let mem_demanding = data
            .mem_usage_per_device
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
                mem_demanding
                    .get(dev)
                    .map(|demanding| {
                        // now we assume only these two processes are using GPU memory
                        let mem_evicted_mb = ((demanding.mem_usage_bytes
                            + occupied.mem_usage_bytes)
                            / (1024 * 1024))
                            // estimated 5% of the total memory is reserved for drivers and other usages
                            .saturating_sub(
                                ((global_config.device_memory_mb[*dev as usize] as f64)
                                    * dev_threshold) as u64,
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
                    .flatten()
            })
            .collect::<HashMap<_, _>>();
        // Construct the eviction list
        let mut swap_out = Vec::new();
        for (dev, mb) in mem_evicted_mb {
            let dev = dev as usize;
            if dev >= swap_out.len() {
                swap_out.resize(dev + 1, None);
            }
            swap_out[dev] = NonZeroU64::new(mb as u64);
        }
        let old_remaining_total = mem_occupied
            .iter()
            .map(|(_, mem)| mem.mem_usage_bytes)
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
