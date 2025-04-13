use std::{
    sync::{atomic::AtomicBool, Condvar, Mutex},
    time::Duration,
};

use nihilipc::{ActivityUpdate, MemoryUsage, SchedulingArgs};
use stats::LaunchStats;

use crate::{
    env_config::agent_config, init::init_generic_data, utils::CudaContextGuard, GENERIC_DATA,
};

mod mem_ctl;
mod stats;
mod uvm_api;
pub(crate) use stats::LaunchType;

pub(crate) static SCHED_CTL: Scheduler = Scheduler::new();

struct Context {
    allow_running: bool,
    // whether we should notify the daemon we want to run
    need_prefetch: bool,
    stats: LaunchStats,
}

impl Context {
    pub const fn new() -> Self {
        Self {
            allow_running: false,
            need_prefetch: true,
            stats: LaunchStats::new(),
        }
    }

    pub fn disable(&mut self) {
        self.allow_running = false;
        self.need_prefetch = true;
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

    pub fn set_allow_running(&'static self, params: SchedulingArgs) {
        self.spawn_idle_monitor_once();
        let mut allow_running = self.allow_running.lock().unwrap();
        match params {
            SchedulingArgs::Enable { prefetch } => {
                allow_running.allow_running = true;
                allow_running.need_prefetch = prefetch;
            }
            SchedulingArgs::Disable { swap_out_mb, delay } => {
                allow_running.disable();
                if let Some(delay) = delay {
                    std::thread::sleep(delay);
                }
                mem_ctl::release_gpu_mem(swap_out_mb, false);
            }
        }
        self.cond_var.notify_all();
    }

    pub fn launch_allowed(&'static self, launch_type: LaunchType) {
        let mut sched_ctx = self.allow_running.lock().unwrap();
        if !sched_ctx.allow_running {
            // request to run
            let ptr_mapping = GENERIC_DATA
                .get_or_init(init_generic_data)
                .lock_ptr_mapping();
            let mut allocs = Vec::new();
            for entry in ptr_mapping.iter() {
                if allocs.len() <= entry.device as usize {
                    allocs.resize(
                        entry.device as usize + 1,
                        MemoryUsage {
                            mem_usage_bytes: 0,
                            alloc_count: 0,
                        },
                    );
                }
                allocs[entry.device as usize].mem_usage_bytes += entry.len as u64;
                allocs[entry.device as usize].alloc_count += 1;
            }
            drop(ptr_mapping);
            crate::comm::update_activity(ActivityUpdate::RequestScheduling {
                mem_usage_per_device: allocs,
            });
        }
        while !sched_ctx.allow_running {
            sched_ctx = self.cond_var.wait(sched_ctx).unwrap();
        }
        if sched_ctx.need_prefetch {
            sched_ctx.need_prefetch = false;
            // prefetch and notify daemon
            let _guard = CudaContextGuard::new();
            crate::memory::prefetch::filtered_prefetch_impl(20, true, true);
        }
        match launch_type {
            LaunchType::Kernel => sched_ctx.stats.record_launch_kernel(),
            LaunchType::Graph => sched_ctx.stats.record_launch_graph(),
        }
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
            const KERNEL_INTERVAL: Duration = Duration::from_millis(800);
            const GRAPH_INTERVAL: Duration = Duration::from_millis(1000);
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
