pub mod daemon;
mod daemon_server;
pub mod proc_ctl;
mod schedule;
pub mod scheduler;
pub mod shm;

use std::path::Path;

pub use daemon::Daemon;

use crate::{
    control::{AttrMsg, ProcessMetadata},
    error::DaemonError,
    general::CallParameter,
};

fn get_user() -> Option<nix::unistd::User> {
    if let Ok(n) = std::env::var("SUDO_USER") {
        nix::unistd::User::from_name(&n).ok()?
    } else {
        let user_name = unsafe { nix::libc::getlogin() };
        if user_name.is_null() {
            return None;
        }
        let name = unsafe { std::ffi::CStr::from_ptr(user_name) }
            .to_string_lossy()
            .into_owned();
        nix::unistd::User::from_name(&name).ok()?
    }
}

fn socket_chown<P: AsRef<Path>>(path: P) -> Result<(), DaemonError> {
    let path = path.as_ref();
    if let Some(user) = get_user() {
        nix::unistd::chown(path, Some(user.uid), Some(user.gid))
            .map_err(|e| DaemonError::Errno("chown", e))?;
    }
    Ok(())
}

pub(crate) enum ProcCtlReq {
    SetAttr(AttrMsg),
    List(CallParameter<(), ProcessMetadata>),
}
