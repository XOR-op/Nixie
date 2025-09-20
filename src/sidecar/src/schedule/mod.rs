use std::{
    sync::{Condvar, Mutex, MutexGuard, atomic::AtomicBool},
    time::Duration,
};

use nihil_common::{ActivityUpdate, MemoryRequest, SchedulingArgs, general::CallParameter};
use stats::LaunchStats;

use crate::{check_cu_err, env_config::sidecar_config, set_device};

mod stats;
pub(crate) use stats::LaunchType;

pub(crate) static SCHED_CTL: Scheduler = Scheduler::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgramState {
    Running,
    Paused,
}

struct Context {
    program_state: ProgramState,
    stats: LaunchStats,
}

impl Context {
    pub const fn new() -> Self {
        Self {
            program_state: ProgramState::Paused,
            stats: LaunchStats::new(),
        }
    }
}

pub(crate) struct Scheduler {
    allow_running: Mutex<Context>,
    cond_var: Condvar,
}

impl Scheduler {
    pub const fn new() -> Self {
        Self {
            allow_running: Mutex::new(Context::new()),
            cond_var: Condvar::new(),
        }
    }

    pub fn set_allow_running(&'static self, params: CallParameter<SchedulingArgs, ()>) {
        self.spawn_idle_monitor_once();
        let mut allow_running = self.allow_running.lock().unwrap();
        match params.param {
            SchedulingArgs::Enable => {
                allow_running.program_state = ProgramState::Running;
            }
            SchedulingArgs::Disable => {
                allow_running.program_state = ProgramState::Paused;
                let mut dev_count = 0;
                unsafe {
                    check_cu_err!(
                        cudarc::driver::sys::cuDeviceGetCount(&mut dev_count),
                        "get device count"
                    )
                };
                for i in 0..dev_count {
                    set_device(i);
                    unsafe {
                        check_cu_err!(
                            cudarc::driver::sys::cuCtxSynchronize(),
                            "synchronize all contexts"
                        )
                    };
                }
            }
        }
        params.ret(());
        self.cond_var.notify_all();
    }

    pub fn pause_then_require_memory(
        &'static self,
        launch_type: LaunchType,
        mem_req: MemoryRequest,
    ) {
        let mut sched_ctx = self.allow_running.lock().unwrap();
        sched_ctx.program_state = ProgramState::Paused;
        Self::launch_allowed_with(sched_ctx, &self.cond_var, launch_type, Some(mem_req));
    }

    fn launch_allowed_with(
        mut sched_ctx: MutexGuard<Context>,
        cond_var: &Condvar,
        launch_type: LaunchType,
        mem_req: Option<MemoryRequest>,
    ) {
        if sched_ctx.program_state == ProgramState::Paused {
            crate::comm::update_activity(match mem_req {
                Some(req) => ActivityUpdate::YieldThenRequestSchedulingAndMem {
                    memory_request: req,
                },
                None => ActivityUpdate::RequestScheduling,
            });
        }
        while sched_ctx.program_state == ProgramState::Paused {
            sched_ctx = cond_var.wait(sched_ctx).unwrap();
        }
        match launch_type {
            LaunchType::Kernel => sched_ctx.stats.record_launch_kernel(),
            LaunchType::Graph => sched_ctx.stats.record_launch_graph(),
            LaunchType::Malloc => sched_ctx.stats.record_launch_malloc(),
            LaunchType::Transfer => sched_ctx.stats.record_launch_transfer(),
        }
    }

    pub fn launch_allowed(&'static self, launch_type: LaunchType) {
        Self::launch_allowed_with(
            self.allow_running.lock().unwrap(),
            &self.cond_var,
            launch_type,
            None,
        );
    }

    fn spawn_idle_monitor_once(&'static self) {
        static SPAWNED: AtomicBool = AtomicBool::new(false);
        if SPAWNED
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
        {
            const MALLOC_INTERVAL: Duration = Duration::from_millis(200);
            const KERNEL_INTERVAL: Duration = Duration::from_millis(500);
            const GRAPH_INTERVAL: Duration = Duration::from_millis(500);
            const TRANSFER_INTERVAL: Duration = Duration::from_millis(500);
            assert!(KERNEL_INTERVAL <= GRAPH_INTERVAL);
            if sidecar_config().auto_idle {
                tokio::spawn(async {
                    tokio::time::sleep(GRAPH_INTERVAL).await;
                    loop {
                        {
                            let mut context = self.allow_running.lock().unwrap();
                            if context.program_state == ProgramState::Running {
                                // check idle
                                if context.stats.graph_elapsed() > GRAPH_INTERVAL
                                    && context.stats.kernel_elapsed() > KERNEL_INTERVAL
                                    && context.stats.malloc_elapsed() > MALLOC_INTERVAL
                                    && context.stats.kernel_elapsed() > TRANSFER_INTERVAL
                                {
                                    // should not use disable() here since we don't need prefetch
                                    context.program_state = ProgramState::Paused;
                                    crate::comm::update_activity(ActivityUpdate::Idle);
                                }
                            }
                        }
                        tokio::time::sleep(KERNEL_INTERVAL).await;
                    }
                });
            }
        }
    }
}
