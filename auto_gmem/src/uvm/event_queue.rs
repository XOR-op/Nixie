use std::{
    mem::ManuallyDrop,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
};

use crate::error::AutoGMemError;

use super::{
    uvm_api::{uvm_tools_init_event_tracker, UvmToolsInitEventTrackerParams},
    uvm_binding::{UvmEventEntry_V2, UvmToolsEventControlData_V2},
    PageBackedArray,
};

pub(crate) struct EventQueue {
    uvm_tools_handle: ManuallyDrop<OwnedFd>,
    uvm_fd: ManuallyDrop<OwnedFd>,
    event_buffer: PageBackedArray<UvmEventEntry_V2>,
    control_buffer: PageBackedArray<UvmToolsEventControlData_V2>,
}

impl EventQueue {
    pub fn new(uvm_fd: OwnedFd, len: usize) -> Result<Self, AutoGMemError> {
        if !is_pow2(len) {
            return Err(AutoGMemError::Invalid("EventQueue::len must be power of 2"));
        }
        let uvm_tools_handle = unsafe {
            let uvm_tools_handle =
                nix::libc::open(cr"/dev/nvidia-uvm-tools".as_ptr(), nix::libc::O_RDWR);
            if uvm_tools_handle < 0 {
                return Err(AutoGMemError::Errno(
                    nix::errno::Errno::from_raw(uvm_tools_handle),
                    "open /dev/nvidia-uvm-tools",
                ));
            }
            ManuallyDrop::new(OwnedFd::from_raw_fd(uvm_tools_handle))
        };
        let event_buffer = PageBackedArray::<UvmEventEntry_V2>::new(len);
        let control_buffer = PageBackedArray::<UvmToolsEventControlData_V2>::new(1);

        // ioctl to init event tracker
        let mut args = UvmToolsInitEventTrackerParams::create_event_queue(
            event_buffer.as_ptr() as u64,
            event_buffer.len() as u64,
            control_buffer.as_ptr() as u64,
            uvm_fd.as_raw_fd() as u32,
        );
        unsafe {
            uvm_tools_init_event_tracker(uvm_tools_handle.as_raw_fd(), &mut args as *mut _)
                .map_err(|e| AutoGMemError::Errno(e, "uvm_tools_init_event_tracker"))?
        };
        match args.result() {
            (0, 2) => {
                tracing::info!("Opened UVM event queue successfully");
                Ok(Self {
                    uvm_tools_handle,
                    uvm_fd: ManuallyDrop::new(uvm_fd),
                    event_buffer,
                    control_buffer,
                })
            }
            (e, ver) => Err(AutoGMemError::Invalid2(format!(
                "uvm_tools_init_event_tracker failed with error: {}, version: {}",
                e, ver
            ))),
        }
    }
}

impl Drop for EventQueue {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.uvm_tools_handle);
            ManuallyDrop::drop(&mut self.uvm_fd);
        }
    }
}

fn is_pow2(n: usize) -> bool {
    n & (n - 1) == 0
}
