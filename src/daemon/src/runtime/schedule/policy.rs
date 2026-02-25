use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant, SystemTime},
};

use nihil_common::{ActivityUpdate, ActivityUpdateContent, general::CallParameter};

use crate::{
    config::load_config,
    control::{
        GetHistoryResponse, GetHistoryResult, HistoryEntry, PrefetchArgs, PrefetchMsg,
        PrefetchResponse, SetPriorityLevel, SetPriorityResponse,
    },
    runtime::migration::{BufferLocation, DataManagerHandle},
};

use super::{Priority, scheduler::ActiveClientState};

use super::{PriorityLevel, statistics::ClientStatistics};

#[derive(Debug, Clone)]
pub struct SchedRequest {
    pub pid: i32,
    pub args: ActivityUpdate,
    pub time: Instant,
    pub is_yield: bool,
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

// This TQ is used to determine when to decrease priority, not for preemption
fn priority_level_to_time_quantum(level: PriorityLevel) -> Duration {
    match level {
        PriorityLevel::Interactive => Duration::from_secs(8),
        PriorityLevel::LowInteractive => Duration::from_secs(16),
        PriorityLevel::HighBatch => Duration::from_secs(32),
        PriorityLevel::Batch => Duration::from_secs(64),
        PriorityLevel::Background => Duration::from_secs(128),
    }
}

// This decides when the process will be preempted, sharing some functionality with classic TQ
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
    data_manager_handle: DataManagerHandle,
    sched_req: VecDeque<SchedRequest>,
    idle_req_queue: VecDeque<IdleRequest>,
    prefetch_queue: VecDeque<PrefetchRequest>,
    clients: HashMap<i32, ClientStatistics>,
    cooldown_until: Instant,
    active_client: ActiveClientState,
    last_mlfq_reset_timer: Instant,
    last_auto_prefetch_pop: Instant,
    last_schedule_pop: Instant,
}

// interface to the scheduler
impl ScheduleQueue {
    pub fn new(data_manager_handle: DataManagerHandle) -> Self {
        Self {
            data_manager_handle,
            sched_req: VecDeque::new(),
            idle_req_queue: VecDeque::new(),
            prefetch_queue: VecDeque::new(),
            clients: HashMap::new(),
            cooldown_until: Instant::now(),
            active_client: ActiveClientState::None,
            last_mlfq_reset_timer: Instant::now(),
            last_auto_prefetch_pop: Instant::now(),
            last_schedule_pop: Instant::now(),
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
                client.set_is_in_schedule_queue(true);
                self.idle_req_queue.push_back(IdleRequest {
                    pid,
                    time: Instant::now(),
                    request_type: IdleRequestType::Yield,
                });
                self.sched_req.push_back(SchedRequest {
                    pid,
                    args,
                    time: Instant::now(),
                    is_yield: true,
                });
            }
            ActivityUpdateContent::RequestScheduling => {
                client.set_is_in_schedule_queue(true);
                self.sched_req.push_back(SchedRequest {
                    pid,
                    args,
                    time: Instant::now(),
                    is_yield: false,
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
                client.set_is_in_schedule_queue(true);
                self.sched_req.push_front(SchedRequest {
                    pid,
                    args,
                    time: Instant::now(),
                    is_yield: false,
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
        if let PreemptionDecision::AllowPreempt {
            follow_same_priority_cooldown,
        } = will_preempt
            // (higher priority) or (same priority but cooldown passed)
            && !(follow_same_priority_cooldown && Instant::now() < self.cooldown_until)
        {
            return self.sched_req.pop_front().map(|front| {
                self.last_schedule_pop = Instant::now();
                // sanity check
                if self.sched_req.iter().filter(|r| r.pid == front.pid).count() > 1 {
                    tracing::warn!(
                        "There are multiple scheduling requests for {}: {:?}",
                        front.pid,
                        self.sched_req
                            .iter()
                            .filter(|r| r.pid == front.pid)
                            .collect::<Vec<_>>()
                    );
                }
                let client_stat = self.get_client_mut_or_insert(front.pid);
                client_stat.set_is_in_schedule_queue(false);
                GenericRequest::Schedule(front)
            });
        }

        // auto prefetch if allowed
        let config_allow_auto_prefetch = load_config().automatic_prefetch;
        let already_prefetched = self.last_auto_prefetch_pop > self.last_schedule_pop;
        if config_allow_auto_prefetch && !self.sched_req.is_empty() && !already_prefetched {
            let pid = self.sched_req.front().unwrap().pid;
            let new_plan = construct_prefetch_plan(pid, &self.data_manager_handle);
            if !new_plan.is_empty() {
                tracing::debug!("Auto prefetch plan for pid {}: {:?}", pid, new_plan);
                self.last_auto_prefetch_pop = Instant::now();
                let (req, _unused_rx) = CallParameter::new(PrefetchArgs {
                    list: new_plan,
                    rx_used: false,
                });
                Some(GenericRequest::Prefetch(PrefetchRequest {
                    time: Instant::now(),
                    parameter: req,
                }))
            } else {
                None
            }
        } else {
            None
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
}

fn construct_prefetch_plan(pid: i32, data_manager: &DataManagerHandle) -> Vec<PrefetchMsg> {
    let mut shm_usage = data_manager
        .shm
        .dump_buffers()
        .into_keys()
        .map(|buf| if buf.pid == pid { buf.size as usize } else { 0 })
        .sum::<usize>();
    let mut hostmem_usage = data_manager
        .hostmem
        .dump_buffers()
        .into_keys()
        .map(|buf| if buf.pid == pid { buf.size as usize } else { 0 })
        .sum::<usize>();
    let mut disk_usage = data_manager
        .storage
        .dump_buffers()
        .into_keys()
        .map(|buf| if buf.pid == pid { buf.size as usize } else { 0 })
        .sum::<usize>();
    let shm_capacity = data_manager.shm.capacity();
    let hostmem_capacity = data_manager.hostmem.capacity();
    let mut prefetch_list = Vec::new();
    if disk_usage > 0 && shm_usage < shm_capacity {
        let can_moved = (shm_capacity - shm_usage).min(disk_usage);
        prefetch_list.push(PrefetchMsg {
            pid,
            from: BufferLocation::Storage,
            to: BufferLocation::Shm,
            size: can_moved as u64,
        });
        shm_usage += can_moved;
        disk_usage -= can_moved;
    }
    if hostmem_usage > 0 && shm_usage < shm_capacity {
        let can_moved = (shm_capacity - shm_usage).min(hostmem_usage);
        prefetch_list.push(PrefetchMsg {
            pid,
            from: BufferLocation::HostMem,
            to: BufferLocation::Shm,
            size: can_moved as u64,
        });
        hostmem_usage -= can_moved;
    }
    if disk_usage > 0 && hostmem_usage < hostmem_capacity {
        let can_moved = (hostmem_capacity - hostmem_usage).min(disk_usage);
        prefetch_list.push(PrefetchMsg {
            pid,
            from: BufferLocation::Storage,
            to: BufferLocation::HostMem,
            size: can_moved as u64,
        });
    }
    prefetch_list
}

impl ScheduleQueue {
    fn update_priority(&mut self) {
        // if self.last_mlfq_reset_timer.elapsed() > Duration::from_secs(600) {
        //     self.reset_all_priorities();
        //     self.last_mlfq_reset_timer = Instant::now();
        //     tracing::trace!("All process priorities have been reset due to inactivity");
        // }

        let active = match self.active_client {
            ActiveClientState::Active { pid, since } => Some((pid, since)),
            _ => None,
        };
        let priority_level_count = self
            .clients
            .values()
            .fold(HashMap::new(), |mut acc, client| {
                let level = client.priority().level();
                *acc.entry(level).or_insert(0u32) += 1;
                acc
            });
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
                        // scheduling pending is not considered as full idle time
                        let multiplier_base = priority_level_count
                            .get(&client.priority().level())
                            .cloned()
                            .unwrap_or_default()
                            .max(2)
                            + 1;
                        let calc_d = d.saturating_sub(
                            client
                                .get_time_in_schedule_queue()
                                .map(|t| t * (multiplier_base - 1) / multiplier_base)
                                .unwrap_or_default(),
                        );
                        // TQ of last level + accumulated time in current level
                        calc_d
                            > priority_level_to_time_quantum(client.priority().level()) / 2
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
            if a.is_yield != b.is_yield {
                // yield has higher priority because sidecars will hold the metadata lock
                return if a.is_yield {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                };
            }
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

#[cfg(test)]
mod tests {
    use std::{
        io::IoSlice,
        num::NonZeroU32,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, Instant},
    };

    use nihil_common::{
        GlobalDeviceId, MAX_ALLOCATION_SIZE, MIN_ALLOCATION_SIZE, MemoryRequest,
        ProcessLocalDeviceId, shm::PhysicalMemoryHandleId,
    };

    use crate::runtime::{
        migration::{
            BufferId, DataManagerHandle, HostMemBufferManager, ShmBufferManager,
            StorageBufferManager,
        },
        schedule::{Priority, PriorityLevel},
    };

    use super::*;

    static TEST_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

    struct TestQueueEnv {
        queue: ScheduleQueue,
        data_manager: DataManagerHandle,
        _tmp_dir: tempfile::TempDir,
    }

    fn next_test_id() -> u64 {
        TEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
    }

    fn unique_shm_path() -> String {
        format!(
            "/nihilphase-policy-test-{}-{}",
            std::process::id(),
            next_test_id()
        )
    }

    fn new_test_data_manager() -> (DataManagerHandle, tempfile::TempDir) {
        let tmp_dir = tempfile::tempdir().expect("failed to create tempdir for policy tests");
        let storage_path = PathBuf::from(tmp_dir.path()).join("policy_test_storage.bin");
        let shm = Arc::new(
            ShmBufferManager::new(&unique_shm_path(), MAX_ALLOCATION_SIZE)
                .expect("failed to create shm buffer manager"),
        );
        let hostmem = Arc::new(HostMemBufferManager::new(
            8 * MIN_ALLOCATION_SIZE,
            MIN_ALLOCATION_SIZE,
            false,
        ));
        let storage = Arc::new(
            StorageBufferManager::new(&storage_path)
                .expect("failed to create storage buffer manager"),
        );
        (
            DataManagerHandle {
                shm,
                hostmem,
                storage,
            },
            tmp_dir,
        )
    }

    fn new_test_queue() -> TestQueueEnv {
        let (data_manager, tmp_dir) = new_test_data_manager();
        let queue = ScheduleQueue::new(data_manager.clone());
        TestQueueEnv {
            queue,
            data_manager,
            _tmp_dir: tmp_dir,
        }
    }

    fn req_scheduling(message_id: u64) -> ActivityUpdate {
        ActivityUpdate {
            message_id,
            content: ActivityUpdateContent::RequestScheduling,
        }
    }

    fn req_idle(message_id: u64) -> ActivityUpdate {
        ActivityUpdate {
            message_id,
            content: ActivityUpdateContent::Idle,
        }
    }

    fn req_yield_then_schedule(message_id: u64) -> ActivityUpdate {
        let mut mem_req = std::array::from_fn(|i| (ProcessLocalDeviceId(i as i32), Vec::new()));
        mem_req[0].1.push(1);
        ActivityUpdate {
            message_id,
            content: ActivityUpdateContent::YieldThenRequestSchedulingAndMem {
                memory_request: Box::new(MemoryRequest { mem_req }),
            },
        }
    }

    fn test_buf_id(pid: i32, idx: u32, size: usize) -> BufferId {
        BufferId {
            pid,
            device_id: GlobalDeviceId(0),
            block_id: PhysicalMemoryHandleId::new(1, NonZeroU32::new(idx).unwrap()),
            size: size as u32,
        }
    }

    fn set_client_priority(queue: &mut ScheduleQueue, pid: i32, priority: Priority) {
        let client = queue.get_client_mut_or_insert(pid);
        client.set_priority(priority);
    }

    #[test]
    fn compute_cooldown_uses_max_of_inputs() {
        let from_migration = ScheduleQueue::compute_cooldown(
            65536,
            None,
            Priority::Fixed(PriorityLevel::Interactive),
        );
        assert_eq!(from_migration, Duration::from_secs(12));

        let from_config = ScheduleQueue::compute_cooldown(
            1,
            Some(Duration::from_secs(200)),
            Priority::Fixed(PriorityLevel::Background),
        );
        assert_eq!(from_config, Duration::from_secs(200));

        let from_priority_floor = ScheduleQueue::compute_cooldown(
            1,
            Some(Duration::from_secs(1)),
            Priority::Fixed(PriorityLevel::Interactive),
        );
        assert_eq!(from_priority_floor, Duration::from_secs(8));
    }

    #[test]
    fn schedule_push_enqueues_idle_yield_and_schedule_correctly() {
        let mut env = new_test_queue();

        env.queue.schedule_push(1, req_idle(1));
        env.queue.schedule_push(2, req_yield_then_schedule(1));
        env.queue.schedule_push(3, req_scheduling(1));

        assert_eq!(env.queue.idle_req_queue.len(), 2);
        assert_eq!(env.queue.idle_req_queue[0].pid, 1);
        assert_eq!(
            env.queue.idle_req_queue[0].request_type,
            IdleRequestType::Idle
        );
        assert_eq!(env.queue.idle_req_queue[1].pid, 2);
        assert_eq!(
            env.queue.idle_req_queue[1].request_type,
            IdleRequestType::Yield
        );

        assert_eq!(env.queue.sched_req.len(), 2);
        assert_eq!(env.queue.sched_req[0].pid, 2);
        assert!(env.queue.sched_req[0].is_yield);
        assert_eq!(env.queue.sched_req[1].pid, 3);
        assert!(!env.queue.sched_req[1].is_yield);

        assert!(
            env.queue
                .get_client(1)
                .expect("client 1 should exist")
                .get_time_in_schedule_queue()
                .is_none()
        );
        assert!(
            env.queue
                .get_client(2)
                .expect("client 2 should exist")
                .get_time_in_schedule_queue()
                .is_some()
        );
        assert!(
            env.queue
                .get_client(3)
                .expect("client 3 should exist")
                .get_time_in_schedule_queue()
                .is_some()
        );
    }

    #[test]
    fn prioritized_push_puts_requests_at_front() {
        let mut env = new_test_queue();

        env.queue.schedule_push(1, req_idle(1));
        env.queue.schedule_push(2, req_scheduling(1));

        let idle_before = env.queue.idle_req_queue.len();
        let sched_before = env.queue.sched_req.len();

        env.queue.prioritized_push(3, req_idle(1));
        env.queue.prioritized_push(4, req_scheduling(1));
        env.queue.prioritized_push(5, req_yield_then_schedule(1));

        assert_eq!(env.queue.idle_req_queue.len(), idle_before + 1);
        assert_eq!(env.queue.sched_req.len(), sched_before + 1);

        assert_eq!(env.queue.idle_req_queue[0].pid, 3);
        assert_eq!(env.queue.idle_req_queue[1].pid, 1);

        assert_eq!(env.queue.sched_req[0].pid, 4);
        assert!(!env.queue.sched_req[0].is_yield);
        assert_eq!(env.queue.sched_req[1].pid, 2);
    }

    #[test]
    fn compute_prioritization_orders_by_yield_then_priority_then_time() {
        let mut env = new_test_queue();

        env.queue.schedule_push(10, req_scheduling(1));
        env.queue.schedule_push(20, req_scheduling(1));
        env.queue.schedule_push(30, req_yield_then_schedule(1));

        set_client_priority(
            &mut env.queue,
            10,
            Priority::Fixed(PriorityLevel::Interactive),
        );
        set_client_priority(
            &mut env.queue,
            20,
            Priority::Fixed(PriorityLevel::Interactive),
        );
        set_client_priority(
            &mut env.queue,
            30,
            Priority::Fixed(PriorityLevel::Background),
        );

        for req in env.queue.sched_req.iter_mut() {
            req.time = match req.pid {
                10 => Instant::now() + Duration::from_millis(5),
                20 => Instant::now() + Duration::from_millis(1),
                30 => Instant::now() + Duration::from_millis(3),
                _ => req.time,
            };
        }

        env.queue.compute_prioritization();
        let ordered = env
            .queue
            .sched_req
            .iter()
            .map(|req| req.pid)
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec![30, 20, 10]);
    }

    #[test]
    fn compute_can_preempt_decision_matrix() {
        let active_since = Instant::now();

        let mut env = new_test_queue();
        env.queue.schedule_push(1, req_scheduling(1));
        let decision = env.queue.compute_can_preempt(ActiveClientState::Active {
            pid: 1,
            since: active_since,
        });
        match decision {
            PreemptionDecision::AllowPreempt {
                follow_same_priority_cooldown,
            } => assert!(!follow_same_priority_cooldown),
            _ => panic!("expected allow preempt when queue front equals active pid"),
        }

        let mut env = new_test_queue();
        env.queue.schedule_push(2, req_scheduling(1));
        env.queue.get_client_mut_or_insert(1);
        set_client_priority(&mut env.queue, 1, Priority::Fixed(PriorityLevel::Batch));
        set_client_priority(
            &mut env.queue,
            2,
            Priority::Fixed(PriorityLevel::Interactive),
        );
        let decision = env.queue.compute_can_preempt(ActiveClientState::Active {
            pid: 1,
            since: active_since,
        });
        match decision {
            PreemptionDecision::AllowPreempt {
                follow_same_priority_cooldown,
            } => assert!(!follow_same_priority_cooldown),
            _ => panic!("expected allow preempt for higher-priority queue front"),
        }

        let mut env = new_test_queue();
        env.queue.schedule_push(2, req_scheduling(1));
        env.queue.get_client_mut_or_insert(1);
        set_client_priority(&mut env.queue, 1, Priority::Fixed(PriorityLevel::Batch));
        set_client_priority(&mut env.queue, 2, Priority::Fixed(PriorityLevel::Batch));
        let decision = env.queue.compute_can_preempt(ActiveClientState::Active {
            pid: 1,
            since: active_since,
        });
        match decision {
            PreemptionDecision::AllowPreempt {
                follow_same_priority_cooldown,
            } => assert!(follow_same_priority_cooldown),
            _ => panic!("expected allow preempt for equal-priority queue front"),
        }

        let mut env = new_test_queue();
        env.queue.schedule_push(2, req_scheduling(1));
        env.queue.get_client_mut_or_insert(1);
        set_client_priority(
            &mut env.queue,
            1,
            Priority::Fixed(PriorityLevel::Interactive),
        );
        set_client_priority(&mut env.queue, 2, Priority::Fixed(PriorityLevel::Batch));
        let decision = env.queue.compute_can_preempt(ActiveClientState::Active {
            pid: 1,
            since: active_since,
        });
        assert!(matches!(decision, PreemptionDecision::DenyPreempt));

        let mut env = new_test_queue();
        env.queue.schedule_push(2, req_scheduling(1));
        let decision = env.queue.compute_can_preempt(ActiveClientState::Active {
            pid: 99,
            since: active_since,
        });
        assert!(matches!(decision, PreemptionDecision::DenyPreempt));
    }

    #[test]
    fn schedule_pop_prefers_idle_then_prefetch_then_schedule() {
        let mut env = new_test_queue();

        env.queue.schedule_push(1, req_idle(1));
        let (prefetch_param, _unused_rx) = CallParameter::new(PrefetchArgs {
            list: Vec::new(),
            rx_used: false,
        });
        env.queue.push_prefetch(prefetch_param);
        env.queue.schedule_push(2, req_scheduling(1));

        let first = env
            .queue
            .schedule_pop(ActiveClientState::None)
            .expect("first pop should exist");
        assert!(matches!(first, GenericRequest::Idle(_)));
        assert_eq!(first.pid(), Some(1));
        assert_eq!(first.req_type(), "Idle");

        let second = env
            .queue
            .schedule_pop(ActiveClientState::None)
            .expect("second pop should exist");
        assert!(matches!(second, GenericRequest::Prefetch(_)));
        assert_eq!(second.pid(), None);
        assert_eq!(second.req_type(), "Prefetch");

        assert!(
            env.queue
                .get_client(2)
                .expect("client 2 should exist")
                .get_time_in_schedule_queue()
                .is_some()
        );
        let third = env
            .queue
            .schedule_pop(ActiveClientState::None)
            .expect("third pop should exist");
        assert!(matches!(third, GenericRequest::Schedule(_)));
        assert_eq!(third.pid(), Some(2));
        assert_eq!(third.req_type(), "Schedule");
        assert!(
            env.queue
                .get_client(2)
                .expect("client 2 should exist")
                .get_time_in_schedule_queue()
                .is_none()
        );
    }

    #[test]
    fn set_priority_transitions_are_consistent() {
        let mut env = new_test_queue();

        assert!(matches!(
            env.queue.set_priority(42, SetPriorityLevel::FixToDynamic),
            SetPriorityResponse::FailureProcessNotExist
        ));

        env.queue.schedule_push(42, req_scheduling(1));

        assert!(matches!(
            env.queue.set_priority(
                42,
                SetPriorityLevel::Set(Priority::Fixed(PriorityLevel::Background)),
            ),
            SetPriorityResponse::Success
        ));
        assert_eq!(
            env.queue
                .get_client(42)
                .expect("client should exist")
                .priority(),
            Priority::Fixed(PriorityLevel::Background)
        );

        assert!(matches!(
            env.queue.set_priority(42, SetPriorityLevel::FixToDynamic),
            SetPriorityResponse::Success
        ));
        assert_eq!(
            env.queue
                .get_client(42)
                .expect("client should exist")
                .priority(),
            Priority::Dynamic {
                level: PriorityLevel::Background,
                weight: 0
            }
        );

        assert!(matches!(
            env.queue.set_priority(42, SetPriorityLevel::FixToDynamic),
            SetPriorityResponse::FailurePriorityNotFixed
        ));

        assert!(matches!(
            env.queue.set_priority(
                42,
                SetPriorityLevel::Set(Priority::Fixed(PriorityLevel::LowInteractive)),
            ),
            SetPriorityResponse::Success
        ));
        assert!(matches!(
            env.queue.set_priority(42, SetPriorityLevel::UnsetToDefault),
            SetPriorityResponse::Success
        ));
        assert_eq!(
            env.queue
                .get_client(42)
                .expect("client should exist")
                .priority(),
            Priority::default_dynamic()
        );

        assert!(matches!(
            env.queue.set_priority(42, SetPriorityLevel::UnsetToDefault),
            SetPriorityResponse::FailurePriorityNotFixed
        ));
    }

    #[test]
    fn remove_client_clears_pending_entries() {
        let mut env = new_test_queue();

        env.queue.schedule_push(1, req_idle(1));
        env.queue.schedule_push(1, req_scheduling(2));
        env.queue.schedule_push(2, req_idle(1));
        env.queue.schedule_push(3, req_scheduling(1));

        env.queue.remove_client(1);

        assert!(env.queue.get_client(1).is_none());
        assert!(env.queue.idle_req_queue.iter().all(|req| req.pid != 1));
        assert!(env.queue.sched_req.iter().all(|req| req.pid != 1));
        assert!(env.queue.idle_req_queue.iter().any(|req| req.pid == 2));
        assert!(env.queue.sched_req.iter().any(|req| req.pid == 3));
    }

    #[test]
    fn construct_prefetch_plan_builds_expected_moves() {
        let env = new_test_queue();
        let pid = 7;

        let shm_buf = test_buf_id(pid, 1, 63 * MIN_ALLOCATION_SIZE);
        let _guard = env
            .data_manager
            .shm
            .try_reserve(&shm_buf)
            .expect("failed to reserve shm buffer");

        let hostmem_buf = test_buf_id(pid, 2, MIN_ALLOCATION_SIZE);
        env.data_manager
            .hostmem
            .store(&hostmem_buf, &vec![1u8; MIN_ALLOCATION_SIZE])
            .expect("failed to store hostmem buffer");

        let storage_buf = test_buf_id(pid, 3, 2 * MIN_ALLOCATION_SIZE);
        let storage_data = vec![2u8; 2 * MIN_ALLOCATION_SIZE];
        let storage_slices = [IoSlice::new(storage_data.as_slice())];
        env.data_manager
            .storage
            .store_vectored(&storage_buf, &storage_slices)
            .expect("failed to store storage buffer");

        let plan = construct_prefetch_plan(pid, &env.data_manager);
        assert_eq!(plan.len(), 2);
        assert_eq!(plan[0].pid, pid);
        assert_eq!(plan[0].from, BufferLocation::Storage);
        assert_eq!(plan[0].to, BufferLocation::Shm);
        assert_eq!(plan[0].size, MIN_ALLOCATION_SIZE as u64);
        assert_eq!(plan[1].pid, pid);
        assert_eq!(plan[1].from, BufferLocation::Storage);
        assert_eq!(plan[1].to, BufferLocation::HostMem);
        assert_eq!(plan[1].size, MIN_ALLOCATION_SIZE as u64);

        env.data_manager
            .hostmem
            .batch_release(std::slice::from_ref(&hostmem_buf));
        env.data_manager
            .storage
            .batch_release(std::slice::from_ref(&storage_buf));
        env.data_manager
            .shm
            .release(&shm_buf)
            .expect("failed to release shm buffer");

        let env2 = new_test_queue();
        let pid2 = 8;

        let shm_buf2 = test_buf_id(pid2, 4, 62 * MIN_ALLOCATION_SIZE);
        let _guard2 = env2
            .data_manager
            .shm
            .try_reserve(&shm_buf2)
            .expect("failed to reserve shm buffer for second case");

        let hostmem_buf2 = test_buf_id(pid2, 5, MIN_ALLOCATION_SIZE);
        env2.data_manager
            .hostmem
            .store(&hostmem_buf2, &vec![3u8; MIN_ALLOCATION_SIZE])
            .expect("failed to store hostmem buffer for second case");

        let plan2 = construct_prefetch_plan(pid2, &env2.data_manager);
        assert_eq!(plan2.len(), 1);
        assert_eq!(plan2[0].pid, pid2);
        assert_eq!(plan2[0].from, BufferLocation::HostMem);
        assert_eq!(plan2[0].to, BufferLocation::Shm);
        assert_eq!(plan2[0].size, MIN_ALLOCATION_SIZE as u64);

        env2.data_manager
            .hostmem
            .batch_release(std::slice::from_ref(&hostmem_buf2));
        env2.data_manager
            .shm
            .release(&shm_buf2)
            .expect("failed to release shm buffer for second case");
    }
}
