use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(super) struct LaunchStats {
    last_kernel: SystemTime,
    last_graph: SystemTime,
}

impl LaunchStats {
    pub const fn new() -> Self {
        LaunchStats {
            last_kernel: UNIX_EPOCH,
            last_graph: UNIX_EPOCH,
        }
    }

    pub fn record_launch_kernel(&mut self) {
        self.last_kernel = SystemTime::now();
    }

    pub fn record_launch_graph(&mut self) {
        self.last_graph = SystemTime::now();
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
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum LaunchType {
    Kernel,
    Graph,
}
