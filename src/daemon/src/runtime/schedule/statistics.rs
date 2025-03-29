use std::time::{Duration, Instant};

use ringbuffer::{AllocRingBuffer, RingBuffer};

use super::{Priority, PriorityLevel};

#[derive(Clone)]
pub(super) struct RunningChunk {
    pub start: Instant,
    pub end: Instant,
}

impl std::fmt::Debug for RunningChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}ms", (self.end - self.start).as_millis())
    }
}

#[derive(Clone)]
pub(super) enum ClientState {
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
    pub(super) pid: i32,

    // across all devices
    pub(super) allocated_mem_est: u64,
    // across all devices
    pub(super) on_gpu_mem_est: u64,
    pub(super) active_time_history: AllocRingBuffer<RunningChunk>,

    pub(super) state: ClientState,
    pub(super) priority: Priority,
    pub(super) last_priority_update: Instant,
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
            priority: Priority::default_dynamic(),
            last_priority_update: Instant::now(),
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
        self.last_priority_update = Instant::now();
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
        self.last_priority_update = Instant::now();
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
        self.last_priority_update = Instant::now();
    }

    pub fn increase_priority(&mut self, until: Option<PriorityLevel>) -> bool {
        self.last_priority_update = Instant::now();
        self.priority.increase(until)
    }

    pub fn decrease_priority(&mut self, until: Option<PriorityLevel>) -> bool {
        self.last_priority_update = Instant::now();
        self.priority.decrease(until)
    }

    pub fn priority_since(&self) -> Duration {
        self.last_priority_update.elapsed()
    }

    pub fn update_on_gpu_mem_est(&mut self, on_gpu_est: u64) {
        self.on_gpu_mem_est = on_gpu_est;
    }
}
