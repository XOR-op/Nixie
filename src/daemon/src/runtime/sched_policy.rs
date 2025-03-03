use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use nihilipc::ActivityUpdate;
use ringbuffer::{AllocRingBuffer, RingBuffer};

pub(super) struct ClientStatistics {
    pid: i32,
    // across all devices
    allocated_mem_est: u64,
    // across all devices
    on_gpu_mem_est: u64,
    is_active: bool,
    schedule_start: Instant,
    last_update: Instant,
    active_time_history: AllocRingBuffer<Duration>,
}

impl std::fmt::Debug for ClientStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let history = self
            .active_time_history
            .iter()
            .map(|x| x.as_secs())
            .collect::<Vec<_>>();
        write!(f, "ClientStatistics {{ alloc_est: {} MB, on_gpu_est: {} MB, active: {}, schedule_start: {:?}s ago, last_update: {:?}s ago, active_time_history: {:?} }}", 
            self.allocated_mem_est/1024/1024, self.on_gpu_mem_est/1024/1024, self.is_active,
            self.schedule_start.elapsed().as_secs(), self.last_update.elapsed().as_secs(), history)
    }
}

impl ClientStatistics {
    pub fn new(pid: i32) -> Self {
        Self {
            pid,
            allocated_mem_est: 0,
            on_gpu_mem_est: 0,
            is_active: false,
            schedule_start: Instant::now(),
            last_update: Instant::now(),
            active_time_history: AllocRingBuffer::new(32),
        }
    }

    pub fn schedule_in(&mut self, mem_est: u64) {
        self.schedule_start = Instant::now();
        self.last_update = Instant::now();
        self.allocated_mem_est = mem_est;
        self.is_active = true;
    }

    pub fn schedule_out(&mut self, on_gpu_est: u64) {
        self.active_time_history
            .push(Instant::now() - self.schedule_start);
        self.is_active = false;
        self.on_gpu_mem_est = on_gpu_est;
    }
}

#[derive(Debug, Clone)]
pub struct SchedRequest {
    pub pid: i32,
    pub args: ActivityUpdate,
    pub time: Instant,
}

pub struct ScheduleQueue {
    sched_req: VecDeque<SchedRequest>,
    clients: HashMap<i32, ClientStatistics>,
    cooldown_until: Instant,
}

impl ScheduleQueue {
    pub fn new() -> Self {
        Self {
            sched_req: VecDeque::new(),
            clients: HashMap::new(),
            cooldown_until: Instant::now(),
        }
    }

    pub fn push(&mut self, pid: i32, args: ActivityUpdate) {
        self.sched_req.push_back(SchedRequest {
            pid,
            args,
            time: Instant::now(),
        });
    }

    pub fn pop(&mut self) -> Option<SchedRequest> {
        if Instant::now() < self.cooldown_until {
            return None;
        }
        self.sched_req.pop_front()
    }

    pub fn get_client(&self, pid: i32) -> Option<&ClientStatistics> {
        self.clients.get(&pid)
    }

    pub fn get_client_mut(&mut self, pid: i32) -> Option<&mut ClientStatistics> {
        self.clients.get_mut(&pid)
    }

    pub fn get_client_mut_or_insert(&mut self, pid: i32) -> &mut ClientStatistics {
        self.clients
            .entry(pid)
            .or_insert_with(|| ClientStatistics::new(pid))
    }

    pub fn remove_client(&mut self, pid: i32) {
        self.clients.remove(&pid);
    }

    pub fn cooldown(&mut self, duration: Option<Duration>) {
        self.cooldown_until = Instant::now() + duration.unwrap_or_default();
    }
}
