use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use nihilipc::ActivityUpdate;
use ringbuffer::{AllocRingBuffer, RingBuffer};

#[derive(Clone)]
struct RunningChunk {
    start: Instant,
    end: Instant,
}

impl std::fmt::Debug for RunningChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}ms", (self.end - self.start).as_millis())
    }
}

#[derive(Clone)]
enum ClientState {
    Active { since: Instant },
    Idle,
    ResidentIdle,
    ScheduleWaiting { since: Instant },
}

impl std::fmt::Debug for ClientState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientState::Active { since } => {
                write!(f, "Active since {:.2}s ago", since.elapsed().as_secs_f64())
            }
            ClientState::Idle => {
                write!(f, "Idle")
            }
            ClientState::ResidentIdle => {
                write!(f, "ResidentIdle")
            }
            ClientState::ScheduleWaiting { since } => {
                write!(
                    f,
                    "ScheduleWaiting since {:.2}s ago",
                    since.elapsed().as_secs_f64()
                )
            }
        }
    }
}

impl ClientState {
    pub fn is_active(&self) -> bool {
        matches!(self, ClientState::Active { .. })
    }
}

pub(crate) struct ClientStatistics {
    pid: i32,
    // across all devices
    allocated_mem_est: u64,
    // across all devices
    on_gpu_mem_est: u64,

    state: ClientState,
    active_time_history: AllocRingBuffer<RunningChunk>,
}

impl std::fmt::Debug for ClientStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let history = self.active_time_history.iter().collect::<Vec<_>>();
        write!(f, "ClientStatistics {{ alloc_est: {} MB, on_gpu_est: {} MB, state: [{:?}], active_time_history: {:?} }}", 
            self.allocated_mem_est/1024/1024, self.on_gpu_mem_est/1024/1024, self.state, history)
    }
}

impl ClientStatistics {
    pub fn new(pid: i32) -> Self {
        Self {
            pid,
            allocated_mem_est: 0,
            on_gpu_mem_est: 0,
            state: ClientState::Idle,
            active_time_history: AllocRingBuffer::new(32),
        }
    }

    pub fn make_active(&mut self, mem_est: u64) {
        if self.state.is_active() {
            tracing::error!("make_active: Client {} is already active", self.pid);
        }
        self.state = ClientState::Active {
            since: Instant::now(),
        };
        self.allocated_mem_est = mem_est;
    }

    pub fn make_resident_idle(&mut self) {
        match &self.state {
            ClientState::Active { since } => {
                self.active_time_history.push(RunningChunk {
                    start: *since,
                    end: Instant::now(),
                });
            }
            state => {
                tracing::error!(
                    "make_resident_idle: Client {} is not active, but in state {:?}",
                    self.pid,
                    state
                )
            }
        }
        self.state = ClientState::ResidentIdle;
    }

    pub fn make_idle(&mut self) {
        match &self.state {
            ClientState::Active { since } => {
                self.active_time_history.push(RunningChunk {
                    start: *since,
                    end: Instant::now(),
                });
            }
            ClientState::ResidentIdle => {}
            state => {
                tracing::error!(
                    "make_idle: Client {} is not active, but in state {:?}",
                    self.pid,
                    state
                )
            }
        }
        self.state = ClientState::Idle;
    }

    pub fn update_on_gpu_mem_est(&mut self, on_gpu_est: u64) {
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

// interface to the scheduler
impl ScheduleQueue {
    pub fn new() -> Self {
        Self {
            sched_req: VecDeque::new(),
            clients: HashMap::new(),
            cooldown_until: Instant::now(),
        }
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

// scheduling logic
impl ScheduleQueue {
    pub fn push(&mut self, pid: i32, args: ActivityUpdate) {
        match &args {
            ActivityUpdate::Idle => {
                // higher priority for idle
                self.sched_req.push_front(SchedRequest {
                    pid,
                    args,
                    time: Instant::now(),
                });
            }
            _ => {
                self.sched_req.push_back(SchedRequest {
                    pid,
                    args,
                    time: Instant::now(),
                });
            }
        }
    }

    pub fn pop(&mut self) -> Option<SchedRequest> {
        if Instant::now() < self.cooldown_until {
            return None;
        }
        self.compute_preemption();
        self.sched_req.pop_front()
    }

    fn compute_prioritization(&mut self) {
        let current = Instant::now();
        // sort by priority
        self.sched_req.make_contiguous().sort_by(|a, b| {
            let Some(stat_a) = self.clients.get(&a.pid) else {
                return std::cmp::Ordering::Greater;
            };
            let Some(stat_b) = self.clients.get(&b.pid) else {
                return std::cmp::Ordering::Less;
            };
            let rank_a = ranking_running_history(stat_a.active_time_history.iter(), &current);
            let rank_b = ranking_running_history(stat_b.active_time_history.iter(), &current);
            rank_a.total_cmp(&rank_b)
        });
    }

    // determine if preemption event needs to be generated
    fn compute_preemption(&mut self) {}
}

fn ranking_running_history<'a, I>(history: I, current: &Instant) -> f64
where
    I: Iterator<Item = &'a RunningChunk>,
{
    // sum of weighted running time
    let mut total = 0.0;
    for chunk in history {
        let duration = chunk.end - chunk.start;
        let weight = (current.saturating_duration_since(chunk.start) + duration / 2).as_secs_f64();
        total += duration.as_secs_f64() * weight;
    }
    total
}
