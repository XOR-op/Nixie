use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(super) struct LaunchStats {
    last_kernel: SystemTime,
    last_graph: SystemTime,
    last_malloc: SystemTime,
    last_transfer: SystemTime,
    last_transfer_size: usize,
    last_sync_start: SystemTime,
    last_sync_end: SystemTime,
}

impl LaunchStats {
    pub const fn new() -> Self {
        LaunchStats {
            last_kernel: UNIX_EPOCH,
            last_graph: UNIX_EPOCH,
            last_malloc: UNIX_EPOCH,
            last_transfer: UNIX_EPOCH,
            last_transfer_size: 0,
            last_sync_start: UNIX_EPOCH,
            last_sync_end: UNIX_EPOCH,
        }
    }

    pub fn record_launch_kernel(&mut self) {
        self.last_kernel = SystemTime::now();
    }

    pub fn record_launch_graph(&mut self) {
        self.last_graph = SystemTime::now();
    }

    pub fn record_launch_malloc(&mut self) {
        self.last_malloc = SystemTime::now();
    }

    pub fn record_launch_transfer(&mut self, size: usize) {
        self.last_transfer = SystemTime::now();
        self.last_transfer_size = size;
    }

    pub fn record_sync_start(&mut self) {
        self.last_sync_start = SystemTime::now();
    }

    pub fn record_sync_end(&mut self) {
        self.last_sync_end = SystemTime::now();
    }

    pub fn kernel_elapsed(&self) -> Duration {
        SystemTime::now()
            .duration_since(self.last_kernel)
            .unwrap_or_default()
    }

    pub fn graph_elapsed(&self) -> Duration {
        SystemTime::now()
            .duration_since(self.last_graph)
            .unwrap_or_default()
    }

    pub fn malloc_elapsed(&self) -> Duration {
        SystemTime::now()
            .duration_since(self.last_malloc)
            .unwrap_or_default()
    }

    pub fn transfer_elapsed(&self) -> (Duration, usize) {
        (
            SystemTime::now()
                .duration_since(self.last_transfer)
                .unwrap_or_default(),
            self.last_transfer_size,
        )
    }

    pub fn pending_sync_elapsed(&self) -> Option<Duration> {
        if self.last_sync_end < self.last_sync_start {
            Some(
                SystemTime::now()
                    .duration_since(self.last_sync_start)
                    .unwrap_or_default(),
            )
        } else {
            None
        }
    }

    pub fn sync_elapsed(&self) -> Duration {
        SystemTime::now()
            .duration_since(self.last_sync_end)
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum LaunchType {
    Kernel,
    Graph,
    Malloc,
    Transfer(usize),
}
