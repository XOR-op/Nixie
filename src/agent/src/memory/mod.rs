mod attribute;
mod autodup;
pub(crate) mod prefetch;

use std::sync::{Mutex, OnceLock};

pub(crate) use attribute::{set_attribute, set_attribute_single};
pub(crate) use prefetch::filtered_prefetch;

pub(crate) fn get_dup_daemon() -> &'static Mutex<autodup::DupDaemon> {
    static DUP_DAEMON: OnceLock<Mutex<autodup::DupDaemon>> = OnceLock::new();
    match DUP_DAEMON.get() {
        Some(daemon) => daemon,
        None => {
            let r = DUP_DAEMON.get_or_init(|| Mutex::new(autodup::DupDaemon::new()));
            autodup::DupDaemon::spawn_daemon(r);
            r
        }
    }
}
