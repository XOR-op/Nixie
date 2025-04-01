use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use nihilipc::ActivityUpdate;

use super::scheduler::ActiveClientState;

use super::{
    statistics::{ClientStatistics, RunningChunk},
    PriorityLevel,
};

#[derive(Debug, Clone)]
pub struct SchedRequest {
    pub pid: i32,
    pub args: ActivityUpdate,
    pub time: Instant,
}

pub struct ScheduleQueue {
    sched_req: VecDeque<SchedRequest>,
    notify_req: VecDeque<SchedRequest>,
    clients: HashMap<i32, ClientStatistics>,
    cooldown_until: Instant,
    active_client: ActiveClientState,
}

// interface to the scheduler
impl ScheduleQueue {
    pub fn new() -> Self {
        Self {
            sched_req: VecDeque::new(),
            notify_req: VecDeque::new(),
            clients: HashMap::new(),
            cooldown_until: Instant::now(),
            active_client: ActiveClientState::None,
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
                self.notify_req.push_front(SchedRequest {
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

    pub fn pop(&mut self, active_client: ActiveClientState) -> Option<SchedRequest> {
        self.update_priority();
        self.compute_prioritization();
        if let Some(front) = self.notify_req.pop_front() {
            return Some(front);
        }
        let will_preempt = self.compute_preemption(active_client);
        if Instant::now() < self.cooldown_until || !will_preempt {
            None
        } else {
            self.sched_req.pop_front()
        }
    }

    pub fn update_active(&mut self, active_client: ActiveClientState) {
        self.active_client = active_client;
    }

    fn update_priority(&mut self) {
        let active = match self.active_client {
            ActiveClientState::Active { pid, since } => Some((pid, since)),
            _ => None,
        };
        for (_, client) in self.clients.iter_mut() {
            if let Some(active_since) = active
                .filter(|(pid, _)| *pid == client.pid)
                .map(|(_, since)| since)
            {
                // active client
                #[allow(clippy::collapsible_if)]
                if active_since.elapsed() > Duration::from_secs(10)
                    && client.priority_since() > Duration::from_secs(10)
                {
                    if client.decrease_priority(Some(PriorityLevel::Batch)) {
                        tracing::trace!(
                            "Process {}: priority decreased to {:?}",
                            client.pid,
                            client.priority.level()
                        );
                    }
                }
            } else if client.priority_since() > Duration::from_secs(30) {
                #[allow(clippy::collapsible_if)]
                if client.increase_priority(None) {
                    tracing::trace!(
                        "Process {}: priority increased to {:?}",
                        client.pid,
                        client.priority.level()
                    );
                }
            }
        }
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
            match stat_a.priority.level().cmp(&stat_b.priority.level()) {
                std::cmp::Ordering::Equal => {
                    // if same priority, sort by time
                    a.time.cmp(&b.time)
                }
                other => other,
            }
        });
    }

    // determine if preemption event needs to be generated
    fn compute_preemption(&mut self, active_client: ActiveClientState) -> bool {
        if let ActiveClientState::Active { pid, .. } = active_client {
            if let Some(active_stat) = self.clients.get(&pid) {
                if let Some(front) = self.sched_req.front() {
                    if let Some(front_stats) = self.clients.get(&front.pid) {
                        // only preempt if the most front process has higher or equal priority
                        return front_stats.priority.level() >= active_stat.priority.level();
                    }
                }
            }
            return false;
        }
        true
    }
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
