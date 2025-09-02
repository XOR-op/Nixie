pub mod daemon;
mod daemon_server;
mod migration;
pub mod proc_ctl;
mod schedule;
pub mod shm;

use std::{collections::HashMap, path::Path};

use crate::{
    control::{ProcessMetadata, ProcessResidualData, ProcessResidualRequest},
    error::DaemonError,
};
use cudarc::driver::result::device;
pub use daemon::Daemon;
use nihil_common::{general::CallParameter, GlobalDeviceId};
pub(crate) use schedule::{ClientState, Priority};

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

pub(super) fn get_allowed_devices_mem() -> Result<HashMap<GlobalDeviceId, u64>, DaemonError> {
    let dev_count = device::get_count().map_err(|e| DaemonError::Cuda("get dev count", e.0))?;
    let mut mem_info = HashMap::with_capacity(dev_count as usize);
    for dev_id in 0..dev_count {
        let device_handle =
            device::get(dev_id as i32).map_err(|e| DaemonError::Cuda("get device", e.0))?;
        let mem = unsafe { device::total_mem(device_handle) }
            .map_err(|e| DaemonError::Cuda("get total memory", e.0))?;
        let mem = mem * 95 / 100; // reserve 5% for system use
        mem_info.insert(GlobalDeviceId(dev_id), mem as u64);
    }
    Ok(mem_info)
}

pub(crate) enum ProcCtlReq {
    List(CallParameter<(), ProcessMetadata>),
    ListProcessResidual(CallParameter<ProcessResidualRequest, ProcessResidualData>),
}
