use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant, SystemTime},
};

use nihil_common::{ActivityUpdate, ActivityUpdateContent, general::CallParameter};

use crate::control::{
    GetHistoryResponse, GetHistoryResult, HistoryEntry, PrefetchArgs, PrefetchResponse,
    SetPriorityLevel, SetPriorityResponse,
};

use super::{Priority, scheduler::ActiveClientState};

use super::{PriorityLevel, statistics::ClientStatistics};

#[derive(Debug, Clone)]
pub struct SchedRequest {
    pub pid: i32,
    pub args: ActivityUpdate,
    pub time: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleRequestType {
    Idle,
    Yield,
}

#[derive(Debug, Clone)]
pub struct IdleRequest {
    pub pid: i32,
    pub time: Instant,
    pub request_type: IdleRequestType,
}

#[derive(Debug)]
pub struct PrefetchRequest {
    pub time: Instant,
    pub parameter: CallParameter<PrefetchArgs, PrefetchResponse>,
}

#[derive(Debug)]
pub enum GenericRequest {
    Idle(IdleRequest),
    Schedule(SchedRequest),
    Prefetch(PrefetchRequest),
}

impl GenericRequest {
    pub fn pid(&self) -> Option<i32> {
        match self {
            GenericRequest::Idle(req) => Some(req.pid),
            GenericRequest::Schedule(req) => Some(req.pid),
            GenericRequest::Prefetch(_) => None,
        }
    }

    pub fn req_type(&self) -> &'static str {
        match self {
            GenericRequest::Idle(_) => "Idle",
            GenericRequest::Schedule(_) => "Schedule",
            GenericRequest::Prefetch(_) => "Prefetch",
        }
    }
}

fn priority_level_to_time_quantum(level: PriorityLevel) -> Duration {
    match level {
        PriorityLevel::Interactive => Duration::from_secs(8),
        PriorityLevel::LowInteractive => Duration::from_secs(16),
        PriorityLevel::HighBatch => Duration::from_secs(32),
        PriorityLevel::Batch => Duration::from_secs(64),
        PriorityLevel::Background => Duration::from_secs(128),
    }
}

fn priority_level_to_cooldown(level: PriorityLevel) -> Duration {
    match level {
        PriorityLevel::Interactive => Duration::from_secs(4),
        PriorityLevel::LowInteractive => Duration::from_secs(8),
        PriorityLevel::HighBatch => Duration::from_secs(16),
        PriorityLevel::Batch => Duration::from_secs(32),
        PriorityLevel::Background => Duration::from_secs(64),
    }
}

pub struct ScheduleQueue {
    sched_req: VecDeque<SchedRequest>,
    idle_req_queue: VecDeque<IdleRequest>,
    prefetch_queue: VecDeque<PrefetchRequest>,
    clients: HashMap<i32, ClientStatistics>,
    cooldown_until: Instant,
    active_client: ActiveClientState,
    last_mlfq_reset_timer: Instant,
}

// interface to the scheduler
impl ScheduleQueue {
    pub fn new() -> Self {
        Self {
            sched_req: VecDeque::new(),
            idle_req_queue: VecDeque::new(),
            prefetch_queue: VecDeque::new(),
            clients: HashMap::new(),
            cooldown_until: Instant::now(),
            active_client: ActiveClientState::None,
            last_mlfq_reset_timer: Instant::now(),
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
        self.sched_req.retain(|req| req.pid != pid);
        self.idle_req_queue.retain(|req| req.pid != pid);
    }

    pub fn cooldown(&mut self, duration: Option<Duration>) {
        self.cooldown_until = Instant::now() + duration.unwrap_or_default();
    }

    pub fn compute_cooldown(
        migration_mb: u64,
        config_cooldown: Option<Duration>,
        current_priority: Priority,
    ) -> Duration {
        // max of both
        let pcie_speed = 16.0; // GB/s
        let migration_s = Duration::from_secs_f64(migration_mb as f64 / 1024.0 / pcie_speed * 1.5);
        let cooldown = migration_s * 2;
        let cooldown = config_cooldown.unwrap_or_default().max(cooldown);
        cooldown.max(priority_level_to_time_quantum(current_priority.level()))
    }
}

// scheduling logic
impl ScheduleQueue {
    pub fn schedule_push(&mut self, pid: i32, args: ActivityUpdate) {
        let client = self.get_client_mut_or_insert(pid);
        client.record_message_id(args.message_id);
        match &args.content {
            ActivityUpdateContent::Idle => {
                // higher priority for idle
                self.idle_req_queue.push_back(IdleRequest {
                    pid,
                    time: Instant::now(),
                    request_type: IdleRequestType::Idle,
                });
            }
            ActivityUpdateContent::YieldThenRequestSchedulingAndMem { .. } => {
                self.idle_req_queue.push_back(IdleRequest {
                    pid,
                    time: Instant::now(),
                    request_type: IdleRequestType::Yield,
                });
                self.sched_req.push_back(SchedRequest {
                    pid,
                    args,
                    time: Instant::now(),
                });
            }
            ActivityUpdateContent::RequestScheduling => {
                self.sched_req.push_back(SchedRequest {
                    pid,
                    args,
                    time: Instant::now(),
                });
            }
        }
    }

    pub fn prioritized_push(&mut self, pid: i32, args: ActivityUpdate) {
        let client = self.get_client_mut_or_insert(pid);
        client.record_message_id(args.message_id);
        match &args.content {
            ActivityUpdateContent::Idle => {
                // higher priority for idle
                self.idle_req_queue.push_front(IdleRequest {
                    pid,
                    time: Instant::now(),
                    request_type: IdleRequestType::Idle,
                });
            }
            ActivityUpdateContent::RequestScheduling => {
                self.sched_req.push_front(SchedRequest {
                    pid,
                    args,
                    time: Instant::now(),
                });
            }
            ActivityUpdateContent::YieldThenRequestSchedulingAndMem { .. } => {
                tracing::error!(
                    "Prioritized push with YieldThenRequestSchedulingAndMem is not supported"
                );
            }
        }
    }

    pub fn push_prefetch(&mut self, parameter: CallParameter<PrefetchArgs, PrefetchResponse>) {
        self.prefetch_queue.push_back(PrefetchRequest {
            time: Instant::now(),
            parameter,
        });
    }

    pub fn schedule_pop(&mut self, active_client: ActiveClientState) -> Option<GenericRequest> {
        self.update_priority();
        self.compute_prioritization();
        if self.idle_req_queue.len() > 1 {
            tracing::warn!(
                "There are {} idle requests pending: {:?}",
                self.idle_req_queue.len(),
                self.idle_req_queue
            );
        }
        if let Some(front) = self.idle_req_queue.pop_front() {
            return Some(GenericRequest::Idle(front));
        } else if let Some(prefetch_req) = self.prefetch_queue.pop_front() {
            return Some(GenericRequest::Prefetch(prefetch_req));
        }
        let will_preempt = self.compute_can_preempt(active_client);
        match will_preempt {
            PreemptionDecision::DenyPreempt => None,
            PreemptionDecision::AllowPreempt {
                follow_same_priority_cooldown,
            } => {
                if follow_same_priority_cooldown && Instant::now() < self.cooldown_until {
                    None
                } else {
                    self.sched_req.pop_front().map(GenericRequest::Schedule)
                }
            }
        }
    }

    pub fn update_active(&mut self, active_client: ActiveClientState) {
        self.active_client = active_client;
    }

    pub fn set_priority(&mut self, pid: i32, new_level: SetPriorityLevel) -> SetPriorityResponse {
        let Some(client) = self.get_client_mut(pid) else {
            return SetPriorityResponse::FailureProcessNotExist;
        };
        match new_level {
            SetPriorityLevel::FixToDynamic => match client.priority() {
                Priority::Fixed(level) => {
                    client.set_priority(Priority::Dynamic { level, weight: 0 });
                    SetPriorityResponse::Success
                }
                Priority::Dynamic { .. } => SetPriorityResponse::FailurePriorityNotFixed,
            },
            SetPriorityLevel::UnsetToDefault => match client.priority() {
                Priority::Dynamic { .. } => SetPriorityResponse::FailurePriorityNotFixed,
                Priority::Fixed(_) => {
                    client.set_priority(Priority::default_dynamic());
                    SetPriorityResponse::Success
                }
            },
            SetPriorityLevel::Set(level) => {
                client.set_priority(level);
                SetPriorityResponse::Success
            }
        }
    }

    fn update_priority(&mut self) {
        if self.last_mlfq_reset_timer.elapsed() > Duration::from_secs(300) {
            self.reset_all_priorities();
            self.last_mlfq_reset_timer = Instant::now();
            tracing::trace!("All process priorities have been reset due to inactivity");
        }

        let active = match self.active_client {
            ActiveClientState::Active { pid, since } => Some((pid, since)),
            _ => None,
        };
        for (_, client) in self.clients.iter_mut() {
            if active.is_some_and(|(pid, _)| pid == client.pid()) {
                client.update_if_active();
                // is the active process
                if client.accumulated_time_in_current_priority()
                    > priority_level_to_time_quantum(client.priority().level())
                    && client.priority_upd_since() > Duration::from_secs(10)
                {
                    #[allow(clippy::collapsible_if)]
                    if client.decrease_priority(Some(PriorityLevel::Batch)) {
                        tracing::debug!(
                            "Process {}: priority decreased to {:?}",
                            client.pid(),
                            client.priority().level()
                        );
                    }
                }
            } else {
                // is an idle process
                if client.accumulated_time_in_current_priority()
                    > priority_level_to_time_quantum(client.priority().level())
                {
                    #[allow(clippy::collapsible_if)]
                    if client.decrease_priority(Some(PriorityLevel::Batch)) {
                        tracing::debug!(
                            "Idle Process {}: priority decreased to {:?}",
                            client.pid(),
                            client.priority().level()
                        );
                    }
                } else if client.priority_upd_since()
                    > priority_level_to_time_quantum(client.priority().level()) * 2
                    && client.idle_since().is_some_and(|d| {
                        // TQ of last level + accumulated time in current level
                        d > priority_level_to_time_quantum(client.priority().level()) / 2
                            + client.accumulated_time_in_current_priority()
                    })
                {
                    #[allow(clippy::collapsible_if)]
                    if client.increase_priority(None) {
                        tracing::debug!(
                            "Idle Process {}: priority increased to {:?}",
                            client.pid(),
                            client.priority().level()
                        );
                    }
                }
            }
        }
    }

    fn reset_all_priorities(&mut self) -> bool {
        let mut changed = false;
        for (_, client) in self.clients.iter_mut() {
            if matches!(client.priority(), Priority::Dynamic { .. }) {
                if client.priority().level() != PriorityLevel::Interactive {
                    changed = true;
                }
                client.set_priority(Priority::default_dynamic());
            }
        }
        changed
    }

    fn compute_prioritization(&mut self) {
        // sort by priority
        self.sched_req.make_contiguous().sort_by(|a, b| {
            let Some(stat_a) = self.clients.get(&a.pid) else {
                return std::cmp::Ordering::Greater;
            };
            let Some(stat_b) = self.clients.get(&b.pid) else {
                return std::cmp::Ordering::Less;
            };
            match stat_a.priority().level().cmp(&stat_b.priority().level()) {
                std::cmp::Ordering::Equal => {
                    // if same priority, sort by time
                    a.time.cmp(&b.time)
                }
                std::cmp::Ordering::Less => {
                    // a has lower priority
                    std::cmp::Ordering::Greater
                }
                std::cmp::Ordering::Greater => {
                    // a has higher priority
                    std::cmp::Ordering::Less
                }
            }
        });
    }

    // determine if preemption event needs to be generated
    fn compute_can_preempt(&mut self, active_client: ActiveClientState) -> PreemptionDecision {
        if let ActiveClientState::Active { pid, .. } = active_client
            && let Some(queue_front) = self.sched_req.front()
        {
            if queue_front.pid == pid {
                return PreemptionDecision::AllowPreempt {
                    follow_same_priority_cooldown: false,
                };
            }
            if let Some(active_stat) = self.clients.get(&pid)
                && let Some(queue_front_stats) = self.clients.get(&queue_front.pid)
            {
                // only preempt if the most front process has higher or equal priority
                return if queue_front_stats.priority().level() >= active_stat.priority().level() {
                    PreemptionDecision::AllowPreempt {
                        follow_same_priority_cooldown: queue_front_stats.priority().level()
                            == active_stat.priority().level(),
                    }
                } else {
                    PreemptionDecision::DenyPreempt
                };
            }
            tracing::warn!(
                "Active client {} or queue front client {} stats not found",
                pid,
                queue_front.pid
            );
            return PreemptionDecision::DenyPreempt;
        }
        PreemptionDecision::AllowPreempt {
            follow_same_priority_cooldown: false,
        }
    }

    pub fn get_history(&self, pid: i32) -> GetHistoryResult {
        match self.clients.get(&pid) {
            Some(client) => {
                let history = client.get_history();
                let now = Instant::now();
                let entries = history
                    .into_iter()
                    .map(|chunk| {
                        // Calculate when the chunk started relative to now
                        let time_since_start = now.duration_since(chunk.start);
                        HistoryEntry {
                            start: SystemTime::now() - time_since_start,
                            duration_ms: chunk.duration().as_millis(),
                            start_priority: chunk.start_priority,
                            end_priority: chunk.end_priority,
                            stop_reason: format!("{:?}", chunk.reason),
                        }
                    })
                    .collect();
                GetHistoryResult::Success(GetHistoryResponse { entries })
            }
            None => GetHistoryResult::FailureProcessNotExist,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum PreemptionDecision {
    AllowPreempt { follow_same_priority_cooldown: bool },
    DenyPreempt,
}
