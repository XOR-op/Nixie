use std::sync::{Condvar, Mutex};

pub(crate) static SCHED_CTL: Scheduler = Scheduler::new();

struct Context {
    allow_running: bool,
    // whether we should notify the daemon we want to run
    need_notify: bool,
}

impl Context {
    pub const fn new() -> Self {
        Self {
            allow_running: true,
            need_notify: false,
        }
    }

    pub fn disable(&mut self) {
        self.allow_running = false;
        self.need_notify = true;
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

    pub fn set_allow_running(&self, allow: bool) {
        let mut allow_running = self.allow_running.lock().unwrap();
        if allow {
            allow_running.allow_running = true;
            self.cond_var.notify_all();
        } else {
            allow_running.disable();
        }
    }

    pub fn launch_allowed(&self) {
        let mut sched_ctx = self.allow_running.lock().unwrap();
        while !sched_ctx.allow_running {
            sched_ctx = self.cond_var.wait(sched_ctx).unwrap();
        }
        if sched_ctx.need_notify {
            sched_ctx.need_notify = false;
            // notify daemon
            todo!("notify daemon")
        }
    }
}
