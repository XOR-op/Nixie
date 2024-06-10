use std::{
    mem::ManuallyDrop,
    ops::Range,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
};

use crate::error::AutoGMemError;

use super::{
    uvm_api::{
        uvm_tools_event_queue_enable_events, uvm_tools_init_event_tracker,
        UvmToolsEventQueueEnableEventsParams, UvmToolsInitEventTrackerParams,
    },
    uvm_binding::{UvmEventEntry_V1, UvmEventType, UvmToolsEventControlData_V1},
    PageBackedArray,
};

pub(crate) struct EventQueue {
    uvm_tools_handle: ManuallyDrop<OwnedFd>,
    uvm_fd: ManuallyDrop<OwnedFd>,
    event_buffer: PageBackedArray<UvmEventEntry_V1>,
    control_buffer: PageBackedArray<UvmToolsEventControlData_V1>,
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
        let event_buffer = PageBackedArray::<UvmEventEntry_V1>::new(len);
        let control_buffer = PageBackedArray::<UvmToolsEventControlData_V1>::new(1);

        // ioctl to init event tracker
        let mut args = UvmToolsInitEventTrackerParams::create_event_queue(
            event_buffer.as_ptr() as u64,
            event_buffer.len() as u64,
            control_buffer.as_ptr() as u64,
            uvm_fd.as_raw_fd() as u32,
        );
        let res = unsafe {
            uvm_tools_init_event_tracker(uvm_tools_handle.as_raw_fd(), &mut args as *mut _)
                .map_err(|e| AutoGMemError::Errno(e, "uvm_tools_init_event_tracker"))?
        };
        tracing::debug!("uvm_tools_init_event_tracker -> {:?}", res);
        match args.result() {
            (0, 0) | (0, 1) => {
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

    pub fn enable_event(&self, event_type: UvmEventType) -> Result<(), AutoGMemError> {
        let mut args = UvmToolsEventQueueEnableEventsParams {
            event_type_flags: 1 << event_type as u64,
            rm_status: 0,
        };
        unsafe {
            uvm_tools_event_queue_enable_events(
                self.uvm_tools_handle.as_raw_fd(),
                &mut args as *mut _,
            )
        }
        .map_err(|e| AutoGMemError::Errno(e, "uvm_tools_event_queue_enable_events"))?;
        if args.rm_status != 0 {
            Err(AutoGMemError::Invalid2(format!(
                "uvm_tools_event_queue_enable_events failed with error: {}",
                args.rm_status
            )))
        } else {
            Ok(())
        }
    }

    pub fn read_events<F>(&mut self, mut callback: F) -> u32
    where
        F: FnMut(&UvmEventEntry_V1) -> bool,
    {
        let mut completed = 0;
        // [behind, ahead)
        let put_behind = unsafe { std::ptr::read_volatile(self.put_behind_ptr()) };
        let get_behind = unsafe { std::ptr::read_volatile(self.get_behind_ptr()) };
        if put_behind == get_behind {
            return 0;
        }
        // We make sure we have some events to read
        unsafe { std::ptr::write_volatile(self.get_ahead_ptr_mut(), put_behind) };

        // Read events
        for i in circular_buffer_index_range(get_behind, put_behind, self.event_buffer.len() as u32)
        {
            let idx = self.wrap_around(i);
            let event = &self.event_buffer.as_slice()[idx];
            if callback(event) {
                completed += 1;
            }
        }
        // update get_behind
        unsafe { std::ptr::write_volatile(self.get_behind_ptr_mut(), put_behind) };

        completed
    }
}

// Helper functions
impl EventQueue {
    /// Get the number of events in the buffer
    fn buffer_usage(&self) -> u32 {
        let put_ahead = unsafe { std::ptr::read_volatile(self.put_ahead_ptr()) };
        let get_behind = unsafe { std::ptr::read_volatile(self.get_behind_ptr()) };
        (self.event_buffer.len() as u32 + put_ahead - get_behind)
            & (self.event_buffer.len() as u32 - 1)
    }

    #[inline(always)]
    fn wrap_around(&self, idx: usize) -> usize {
        debug_assert!(is_pow2(self.event_buffer.len()));
        idx & (self.event_buffer.len() - 1)
    }

    #[inline(always)]
    fn put_behind_ptr(&self) -> *const u32 {
        &self.control_buffer.as_slice()[0].put_behind as *const u32
    }

    #[inline(always)]
    fn put_behind_ptr_mut(&mut self) -> *mut u32 {
        &mut self.control_buffer.as_slice_mut()[0].put_behind as *mut u32
    }

    #[inline(always)]
    fn put_ahead_ptr(&self) -> *const u32 {
        &self.control_buffer.as_slice()[0].put_ahead as *const u32
    }

    #[inline(always)]
    fn put_ahead_ptr_mut(&mut self) -> *mut u32 {
        &mut self.control_buffer.as_slice_mut()[0].put_ahead as *mut u32
    }

    #[inline(always)]
    fn get_behind_ptr(&self) -> *const u32 {
        &self.control_buffer.as_slice()[0].get_behind as *const u32
    }

    #[inline(always)]
    fn get_behind_ptr_mut(&mut self) -> *mut u32 {
        &mut self.control_buffer.as_slice_mut()[0].get_behind as *mut u32
    }

    #[inline(always)]
    fn get_ahead_ptr(&self) -> *const u32 {
        &self.control_buffer.as_slice()[0].get_ahead as *const u32
    }

    #[inline(always)]
    fn get_ahead_ptr_mut(&mut self) -> *mut u32 {
        &mut self.control_buffer.as_slice_mut()[0].get_ahead as *mut u32
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

fn circular_buffer_index_range(get_idx: u32, put_idx: u32, len: u32) -> Range<usize> {
    if get_idx <= put_idx {
        (get_idx as usize)..(put_idx as usize)
    } else {
        (get_idx as usize)..((len + put_idx) as usize)
    }
}
