use std::sync::{Condvar, Mutex};

use nihilipc::SchedulingArgs;

use crate::memory::prefetch::release_gpu_mem;

pub(crate) static SCHED_CTL: Scheduler = Scheduler::new();

struct Context {
    allow_running: bool,
    // whether we should notify the daemon we want to run
    need_prefetch: bool,
}

impl Context {
    pub const fn new() -> Self {
        Self {
            allow_running: false,
            need_prefetch: true,
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

    pub fn set_allow_running(&self, params: SchedulingArgs) {
        let mut allow_running = self.allow_running.lock().unwrap();
        match params {
            SchedulingArgs::Enable { prefetch } => {
                allow_running.allow_running = true;
                allow_running.need_prefetch = prefetch;
            }
            SchedulingArgs::Disable { swap_out_mb } => {
                allow_running.disable();
                if let Some(mb) = swap_out_mb {
                    // swap out to cpu synchronously
                    release_gpu_mem(mb.get(), true);
                }
            }
        }
        self.cond_var.notify_all();
    }

    pub fn launch_allowed(&self) {
        let mut sched_ctx = self.allow_running.lock().unwrap();
        if !sched_ctx.allow_running {
            // request to run
            crate::comm::notify_activity();
        }
        while !sched_ctx.allow_running {
            sched_ctx = self.cond_var.wait(sched_ctx).unwrap();
        }
        if sched_ctx.need_prefetch {
            sched_ctx.need_prefetch = false;
            // prefetch and notify daemon
            crate::memory::filtered_prefetch_non_blocking(20, true);
        }
    }
}
