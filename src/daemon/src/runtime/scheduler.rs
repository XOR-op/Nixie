use std::{
    collections::HashMap,
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

use hashlink::LinkedHashMap;
use nihilipc::{ActivityUpdate, SchedulingArgs};
use ringbuf::{traits::RingBuffer, HeapRb};
use tokio::sync::mpsc;

use crate::{config::load_config, error::ScheduleError};

use super::daemon_server::{DaemonServerHandle, DeviceOrdinalMapping};

pub struct Scheduler {
    list: Arc<RwLock<LinkedHashMap<i32, DaemonServerHandle>>>,
    rpc_data_rx: mpsc::UnboundedReceiver<(i32, ActivityUpdate)>,
    active_client: Option<i32>,
    clients: HashMap<i32, ClientStatistics>,
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
            clients: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        tracing::info!("Starting scheduler...");
        loop {
            tokio::select! {
                Some((pid, data)) = self.rpc_data_rx.recv() => {
                    if let Err(e) = self.handle_activity_update(pid, data).await{
                        tracing::error!("Scheduler handling activity update from {}: {:?}", pid, e);
                    }
                }
            }
        }
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

                    let swap_out = {
                        let cur_proc_dev_mapping = handle.dev_mapping();
                        if let Some(new_handle) = control.get(&pid) {
                            Self::compute_eviction(
                                &data,
                                &cur_proc_dev_mapping,
                                &new_handle.dev_mapping(),
                            )
                        } else {
                            Vec::new()
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
                    if let Some(client) = self.clients.get_mut(&active_pid) {
                        client.schedule_out();
                    }
                } else {
                    self.active_client = None;
                    self.clients.remove(&active_pid);
                }
            }
        }
        let client = self
            .clients
            .entry(pid)
            .or_insert_with(|| ClientStatistics::new(pid));

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
        client.schedule_in();
        Ok(())
    }

    // Devices in and out are not global indexed but per process
    // We should perform corresponding mapping accordingly
    fn compute_eviction(
        data: &ActivityUpdate,
        old_proc_mapping: &DeviceOrdinalMapping,
        new_proc_mapping: &DeviceOrdinalMapping,
    ) -> Vec<Option<NonZeroU64>> {
        let global_config = load_config();
        let mem_demanding = data
            .mem_usage_per_device
            .iter()
            .enumerate()
            .filter_map(|(i, dev)| {
                old_proc_mapping
                    .visible_to_real(i as i32)
                    .map(|real_dev| (real_dev, dev))
            })
            .collect::<HashMap<_, _>>();
        let mem_evicted_mb = mem_demanding
            .iter()
            .filter_map(|(dev, demanding)| {
                new_proc_mapping.real_to_visible(*dev).map(|new_dev| {
                    (
                        new_dev,
                        ((global_config.device_memory_mb[new_dev as usize] as f64 * 0.95) as u64)
                            .saturating_sub(demanding.mem_usage_bytes / 1024 / 1024),
                    )
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
            swap_out[dev] = NonZeroU64::new(mb as u64);
        }
        swap_out
    }
}

struct ClientStatistics {
    pid: i32,
    mem_usage: usize,
    is_active: bool,
    schedule_start: Instant,
    last_update: Instant,
    active_time_history: HeapRb<Duration>,
}

impl ClientStatistics {
    pub fn new(pid: i32) -> Self {
        Self {
            pid,
            mem_usage: 0,
            is_active: false,
            schedule_start: Instant::now(),
            last_update: Instant::now(),
            active_time_history: HeapRb::new(32),
        }
    }

    pub fn schedule_in(&mut self) {
        self.schedule_start = Instant::now();
        self.last_update = Instant::now();
        self.is_active = true;
    }

    pub fn schedule_out(&mut self) {
        self.active_time_history
            .push_overwrite(Instant::now() - self.schedule_start);
        self.is_active = false;
    }
}
