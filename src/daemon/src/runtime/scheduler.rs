use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

use hashlink::LinkedHashMap;
use nihilipc::ActivityUpdate;
use ringbuf::{traits::RingBuffer, HeapRb};
use tokio::sync::mpsc;

use super::daemon_server::DaemonServerHandle;

pub enum ScheduleError {
    InvalidClient,
    InternalError,
}

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
                    self.handle_activity_update(pid, data);
                }
            }
        }
    }

    fn handle_activity_update(&mut self, pid: i32, data: ActivityUpdate) {
        if let Some(client) = self.clients.get_mut(&pid) {
            client.keep_alive();
        }
        todo!()
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
            active_time_history: HeapRb::new(100),
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

    pub fn keep_alive(&mut self) {
        self.last_update = Instant::now();
    }
}
