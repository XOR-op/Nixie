use std::{
    sync::{atomic::AtomicBool, Condvar, Mutex, MutexGuard},
    time::Duration,
};

use nihil_common::{general::CallParameter, ActivityUpdate, MemoryRequest, SchedulingArgs};
use stats::LaunchStats;

use cudarc::driver::sys::lib as cuda_lib;

use crate::{check_cu_err, env_config::agent_config, set_device};

mod stats;
pub(crate) use stats::LaunchType;

pub(crate) static SCHED_CTL: Scheduler = Scheduler::new();

struct Context {
    allow_running: bool,
    stats: LaunchStats,
}

impl Context {
    pub const fn new() -> Self {
        Self {
            allow_running: false,
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
                allow_running.allow_running = true;
            }
            SchedulingArgs::Disable => {
                allow_running.allow_running = false;
                let mut dev_count = 0;
                unsafe {
                    check_cu_err!(
                        cuda_lib().cuDeviceGetCount(&mut dev_count),
                        "get device count"
                    )
                };
                for i in 0..dev_count {
                    set_device(i);
                    unsafe {
                        check_cu_err!(cuda_lib().cuCtxSynchronize(), "synchronize all contexts")
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
        sched_ctx.allow_running = false;
        crate::comm::update_activity(ActivityUpdate::Idle);
        Self::launch_allowed_with(sched_ctx, &self.cond_var, launch_type, Some(mem_req));
    }

    fn launch_allowed_with(
        mut sched_ctx: MutexGuard<Context>,
        cond_var: &Condvar,
        launch_type: LaunchType,
        mem_req: Option<MemoryRequest>,
    ) {
        if !sched_ctx.allow_running {
            crate::comm::update_activity(match mem_req {
                Some(req) => ActivityUpdate::RequestSchedulingAndMem {
                    memory_request: req,
                },
                None => ActivityUpdate::RequestScheduling,
            });
        }
        while !sched_ctx.allow_running {
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
            if agent_config().auto_idle {
                tokio::spawn(async {
                    tokio::time::sleep(GRAPH_INTERVAL).await;
                    loop {
                        {
                            let mut context = self.allow_running.lock().unwrap();
                            if context.allow_running {
                                // check idle
                                if context.stats.graph_elapsed() > GRAPH_INTERVAL
                                    && context.stats.kernel_elapsed() > KERNEL_INTERVAL
                                    && context.stats.malloc_elapsed() > MALLOC_INTERVAL
                                    && context.stats.kernel_elapsed() > TRANSFER_INTERVAL
                                {
                                    // should not use disable() here since we don't need prefetch
                                    context.allow_running = false;
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
