use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use super::{Priority, PriorityLevel};

#[derive(Clone, Copy, Debug)]
pub enum PreemptionReason {
    RoundRobin,
    HigherPriority,
}

#[derive(Clone, Copy, Debug)]
pub enum StopReason {
    Idle,
    /// The process is in fact idle, but the states have not been evicted yet.
    LazyIdle,
    PreemptedBy(i32, PreemptionReason),
}

#[derive(Clone)]
pub struct RunningChunk {
    pub start: Instant,
    pub end: Instant,
    pub reason: StopReason,
}

impl RunningChunk {
    pub fn duration(&self) -> Duration {
        self.end - self.start
    }
}

impl std::fmt::Debug for RunningChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}ms({:?})",
            (self.end - self.start).as_millis(),
            self.reason
        )
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

pub struct History {
    inner: VecDeque<RunningChunk>,
    max_size: usize,
}

impl History {
    pub fn new(max_size: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(max_size),
            max_size,
        }
    }

    pub fn push(&mut self, chunk: RunningChunk) {
        if self.inner.len() == self.max_size {
            self.inner.pop_front();
        }
        self.inner.push_back(chunk);
    }

    pub fn iter(&self) -> impl Iterator<Item = &RunningChunk> {
        self.inner.iter()
    }
}

pub struct ClientStatistics {
    pub pid: i32,
    pub active_time_history: History,
    pub state: ClientState,
    pub priority: Priority,
    pub last_priority_update: Instant,
}

impl std::fmt::Debug for ClientStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let history = self.active_time_history.iter().collect::<Vec<_>>();
        write!(
            f,
            "ClientStatistics {{state: [{:?}], active_time_history: {:?} }}",
            self.state, history
        )
    }
}

impl ClientStatistics {
    pub fn new(pid: i32) -> Self {
        Self {
            pid,
            state: ClientState::Idle,
            active_time_history: History::new(32),
            priority: Priority::default_dynamic(),
            last_priority_update: Instant::now(),
        }
    }

    pub fn make_active(&mut self) {
        if self.state.is_active() {
            tracing::error!("make_active: Client {} is already active", self.pid);
        }
        self.state = ClientState::Active {
            since: Instant::now(),
        };
        self.last_priority_update = Instant::now();
    }

    pub fn make_resident_idle(&mut self, reason: StopReason) {
        match &self.state {
            ClientState::Active { since } => {
                self.active_time_history.push(RunningChunk {
                    start: *since,
                    end: Instant::now(),
                    reason,
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

    pub fn make_idle(&mut self, reason: StopReason) {
        match &self.state {
            ClientState::Active { since } => {
                self.active_time_history.push(RunningChunk {
                    start: *since,
                    end: Instant::now(),
                    reason,
                });
            }
            ClientState::ResidentIdle => {
                assert!(matches!(reason, StopReason::LazyIdle));
            }
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
}

// Statistics helpers
impl ClientStatistics {
    pub fn priority_upd_since(&self) -> Duration {
        self.last_priority_update.elapsed()
    }

    pub fn last_unfinished_active_time(&self, since: Option<Instant>) -> Option<Duration> {
        let mut last_unfinished = None;
        for entry in self.active_time_history.inner.iter().rev() {
            if matches!(entry.reason, StopReason::Idle) {
                break;
            }
            if since.is_some_and(|s| entry.start < s) {
                last_unfinished =
                    Some(last_unfinished.unwrap_or_default() + (entry.end - since.unwrap()));
                break;
            } else {
                last_unfinished = Some(last_unfinished.unwrap_or_default() + entry.duration());
            }
        }
        last_unfinished
    }

    pub fn last_unfinished_schedule(&self, since: Option<Instant>) -> u32 {
        let mut last_unfinished = 0;
        for entry in self.active_time_history.inner.iter().rev() {
            if matches!(entry.reason, StopReason::Idle) {
                break;
            }
            last_unfinished += 1;
            if since.is_some_and(|s| entry.start < s) {
                break;
            }
        }
        last_unfinished
    }
}
