use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

use hashlink::LinkedHashMap;
use nihilipc::{ActivityUpdate, SchedulingArgs};
use ringbuf::{traits::RingBuffer, HeapRb};
use tokio::sync::mpsc;

use crate::error::ScheduleError;

use super::daemon_server::DaemonServerHandle;

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
        _data: ActivityUpdate,
    ) -> Result<(), ScheduleError> {
        let control = self.list.write().await;
        if let Some(active_pid) = self.active_client {
            if pid != active_pid {
                // active pid can exit before scheduler knows
                if let Some(handle) = control.get(&active_pid) {
                    tracing::trace!("Scheduling out process {}", active_pid);
                    handle
                        .client()
                        .schedule(tarpc::context::current(), SchedulingArgs { enable: false })
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
            .schedule(tarpc::context::current(), SchedulingArgs { enable: true })
            .await
            .map_err(|e| ScheduleError::RpcError("schedule in", pid, e))?;
        client.schedule_in();
        Ok(())
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
