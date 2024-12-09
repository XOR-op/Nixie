use std::{
    sync::{Condvar, Mutex},
    time::SystemTime,
};

pub(crate) static SCHED_CTRL: SchedControl = SchedControl::new();

pub(crate) struct SchedControl {
    allow_running_until: Mutex<SystemTime>,
    cond_var: Condvar,
}

impl SchedControl {
    pub const fn new() -> Self {
        Self {
            allow_running_until: Mutex::new(SystemTime::UNIX_EPOCH),
            cond_var: Condvar::new(),
        }
    }

    pub fn update_time(&self, new_time: SystemTime) {
        let mut allowed_running_until = self.allow_running_until.lock().unwrap();
        if new_time > *allowed_running_until {
            *allowed_running_until = new_time;
            self.cond_var.notify_all();
        }
    }

    pub fn wait_until_schedulable(&self) {
        let mut allowed_running_until = self.allow_running_until.lock().unwrap();
        while SystemTime::now() > *allowed_running_until {
            allowed_running_until = self.cond_var.wait(allowed_running_until).unwrap();
        }
    }
}
