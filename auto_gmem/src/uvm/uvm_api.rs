#[repr(C)]
#[derive(Debug, Clone, Default)]
pub(crate) struct NVUuid {
    pub bytes: [u8; 16],
}

pub(crate) type NVStatus = u32;

#[repr(C)]
pub(crate) struct UvmToolsInitEventTrackerParams {
    pub queue_buffer: u64,      // must be page aligned
    pub queue_buffer_size: u64, // must be power of 2
    pub control_buffer: u64,    // must be page aligned
    pub processor: NVUuid,
    pub all_processors: u32,
    pub uvm_fd: u32,
    pub rm_status: NVStatus, // out
    pub requested_version: u32,
    pub granted_version: u32, // out
}

impl UvmToolsInitEventTrackerParams {
    pub fn create_event_queue(
        queue_buffer: u64,
        queue_buffer_size: u64,
        control_buffer: u64,
        uvm_fd: u32,
    ) -> Self {
        Self {
            queue_buffer,
            queue_buffer_size,
            control_buffer,
            processor: Default::default(),
            all_processors: Default::default(),
            uvm_fd,
            rm_status: Default::default(),
            requested_version: 2, // UvmToolsEventQueueVersion_V2
            granted_version: Default::default(),
        }
    }

    pub fn result(&self) -> (NVStatus, u32) {
        (self.rm_status, self.granted_version)
    }
}

const UVM_TOOLS_INIT_EVENT_TRACKER_IOCTL: u32 = 56;

nix::ioctl_readwrite_bad!(uvm_tools_init_event_tracker, UVM_TOOLS_INIT_EVENT_TRACKER_IOCTL, UvmToolsInitEventTrackerParams);