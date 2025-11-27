use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

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
    YieldAndPending,
    PreemptedBy(i32, PreemptionReason),
}

#[derive(Clone)]
pub struct RunningChunk {
    pub start: Instant,
    pub end: Instant,
    pub start_priority: PriorityLevel,
    pub end_priority: PriorityLevel,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ClientState {
    Active,
    Idle,
    ScheduleWaiting,
}

#[derive(Clone)]
pub(super) enum InternalClientState {
    Active {
        since: Instant,
        start_priority: PriorityLevel,
    },
    Idle,
    ResidentIdle,
    ScheduleWaiting {
        since: Instant,
    },
}

impl std::fmt::Debug for InternalClientState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InternalClientState::Active { since, .. } => {
                write!(f, "Active since {:.2}s ago", since.elapsed().as_secs_f64())
            }
            InternalClientState::Idle => {
                write!(f, "Idle")
            }
            InternalClientState::ResidentIdle => {
                write!(f, "ResidentIdle")
            }
            InternalClientState::ScheduleWaiting { since } => {
                write!(
                    f,
                    "ScheduleWaiting since {:.2}s ago",
                    since.elapsed().as_secs_f64()
                )
            }
        }
    }
}

impl InternalClientState {
    pub fn is_active(&self) -> bool {
        matches!(self, InternalClientState::Active { .. })
    }

    pub fn as_client_state(&self) -> ClientState {
        match self {
            InternalClientState::Active { .. } => ClientState::Active,
            InternalClientState::Idle | InternalClientState::ResidentIdle => ClientState::Idle,
            InternalClientState::ScheduleWaiting { .. } => ClientState::ScheduleWaiting,
        }
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

enum StateSinceLastActive {
    Active(Instant),
    Idle(Instant),
}

pub struct ClientStatistics {
    pid: i32,
    active_time_history: History,
    state: InternalClientState,
    priority: Priority,
    time_used_in_current_priority: Duration,
    last_in_current_priority: Instant,
    last_priority_update: Instant,
    last_state: StateSinceLastActive,
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
            state: InternalClientState::Idle,
            active_time_history: History::new(64),
            priority: Priority::default_dynamic(),
            time_used_in_current_priority: Duration::ZERO,
            last_in_current_priority: Instant::now(),
            last_priority_update: Instant::now(),
            last_state: StateSinceLastActive::Idle(Instant::now()),
        }
    }

    pub fn make_active(&mut self) {
        if self.state.is_active() {
            tracing::warn!("make_active: Client {} is already active", self.pid);
        }
        self.state = InternalClientState::Active {
            since: Instant::now(),
            start_priority: self.priority.level(),
        };
        self.last_state = StateSinceLastActive::Active(Instant::now());
        self.last_in_current_priority = Instant::now();
    }

    pub fn make_resident_idle(&mut self, reason: StopReason) {
        match &self.state {
            InternalClientState::Active {
                since,
                start_priority,
            } => {
                self.active_time_history.push(RunningChunk {
                    start: *since,
                    end: Instant::now(),
                    start_priority: *start_priority,
                    end_priority: self.priority.level(),
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
        self.state = InternalClientState::ResidentIdle;
        self.last_state = StateSinceLastActive::Idle(Instant::now());
    }

    pub fn make_idle(&mut self, reason: StopReason) {
        match &self.state {
            InternalClientState::Active {
                since,
                start_priority,
            } => {
                self.active_time_history.push(RunningChunk {
                    start: *since,
                    end: Instant::now(),
                    start_priority: *start_priority,
                    end_priority: self.priority.level(),
                    reason,
                });
            }
            InternalClientState::ResidentIdle => {
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
        self.state = InternalClientState::Idle;
        self.last_state = StateSinceLastActive::Idle(Instant::now());
    }

    pub fn increase_priority(&mut self, until: Option<PriorityLevel>) -> bool {
        self.last_priority_update = Instant::now();
        let changed = self.priority.increase(until);
        self.time_used_in_current_priority = Duration::ZERO;
        changed
    }

    pub fn decrease_priority(&mut self, until: Option<PriorityLevel>) -> bool {
        self.last_priority_update = Instant::now();
        let changed = self.priority.decrease(until);
        self.time_used_in_current_priority = Duration::ZERO;
        changed
    }

    pub fn set_priority(&mut self, priority: Priority) {
        self.last_priority_update = Instant::now();
        self.priority = priority;
        self.time_used_in_current_priority = Duration::ZERO;
    }

    pub fn update_if_active(&mut self) {
        if let InternalClientState::Active {
            since: _,
            start_priority: _,
        } = &self.state
        {
            self.time_used_in_current_priority += self.last_in_current_priority.elapsed();
            self.last_in_current_priority = Instant::now();
        }
    }
}

// Statistics helpers
impl ClientStatistics {
    pub fn priority_upd_since(&self) -> Duration {
        self.last_priority_update.elapsed()
    }

    pub fn idle_since(&self) -> Option<Duration> {
        match &self.last_state {
            StateSinceLastActive::Idle(since) => Some(since.elapsed()),
            StateSinceLastActive::Active(_) => None,
        }
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

    #[inline(always)]
    pub fn priority(&self) -> Priority {
        self.priority
    }

    #[inline(always)]
    pub fn pid(&self) -> i32 {
        self.pid
    }

    #[inline(always)]
    pub fn state_ref(&self) -> &InternalClientState {
        &self.state
    }

    pub fn accumulated_time_in_current_priority(&self) -> Duration {
        self.time_used_in_current_priority
    }
}
