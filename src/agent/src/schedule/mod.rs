use std::{
    sync::{atomic::AtomicBool, Condvar, Mutex},
    time::Duration,
};

use nihil_common::{ActivityUpdate, MemoryUsage, SchedulingArgs};
use stats::LaunchStats;

use cudarc::driver::sys::lib as cuda_lib;

use crate::{
    check_cu_err, env_config::agent_config, init::init_generic_data, set_device, GENERIC_DATA,
};

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

    pub fn set_allow_running(&'static self, params: SchedulingArgs) {
        self.spawn_idle_monitor_once();
        let mut allow_running = self.allow_running.lock().unwrap();
        match params {
            SchedulingArgs::Enable {} => {
                allow_running.allow_running = true;
            }
            SchedulingArgs::Disable {} => {
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
        self.cond_var.notify_all();
    }

    pub fn launch_allowed(&'static self, launch_type: LaunchType) {
        let mut sched_ctx = self.allow_running.lock().unwrap();
        if !sched_ctx.allow_running {
            // request to run
            let table = GENERIC_DATA.get_or_init(init_generic_data).lock();
            let mut allocs = Vec::new();
            for entry in table.entry.iter() {
                if allocs.len() <= entry.device as usize {
                    allocs.resize(
                        entry.device as usize + 1,
                        MemoryUsage {
                            on_gpu_bytes: 0,
                            off_gpu_bytes: 0,
                            alloc_count: 0,
                        },
                    );
                }
                let (on_gpu, off_gpu) = table.handle_list.memory_usage(entry.handle_idx);
                allocs[entry.device as usize].on_gpu_bytes += on_gpu as u64;
                allocs[entry.device as usize].off_gpu_bytes += off_gpu as u64;
                allocs[entry.device as usize].alloc_count += 1;
            }
            drop(table);
            crate::comm::update_activity(ActivityUpdate::RequestScheduling {
                mem_usage_per_device: allocs,
            });
        }
        while !sched_ctx.allow_running {
            sched_ctx = self.cond_var.wait(sched_ctx).unwrap();
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
            const KERNEL_INTERVAL: Duration = Duration::from_millis(500);
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
