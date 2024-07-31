use std::{
    sync::{Mutex, OnceLock},
    time::{Duration, SystemTime},
};

pub(crate) static SCHED_SIG_RECV: OnceLock<crossbeam::channel::Receiver<()>> = OnceLock::new();
pub(crate) static ALLOWED_RUNNING_UNTIL: Mutex<SystemTime> = Mutex::new(SystemTime::UNIX_EPOCH);

pub fn wait_until_schedulable() {
    let allowed_running_until = ALLOWED_RUNNING_UNTIL.lock().unwrap();
    if SystemTime::now() < *allowed_running_until {
        return;
    }
    drop(allowed_running_until);

    loop {
        crossbeam::select! {
            recv(SCHED_SIG_RECV.get().unwrap()) -> r => {
                if r.is_err() {
                    // Disconnected, allow anyway
                    return;
                }
            }
            default(Duration::from_millis(30)) => {}
        }
        let allowed_running_until = ALLOWED_RUNNING_UNTIL.lock().unwrap();
        if SystemTime::now() < *allowed_running_until {
            return;
        }
        drop(allowed_running_until);
    }
}
