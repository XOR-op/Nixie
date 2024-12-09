use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use ringbuf::{traits::RingBuffer, HeapRb};

pub enum ScheduleError {
    InvalidClient,
    InternalError,
}

pub struct Scheduler {
    active_client: Option<i32>,
    clients: HashMap<i32, ClientStatistics>,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            active_client: None,
            clients: HashMap::new(),
        }
    }

    pub fn add_client(&mut self, pid: i32) {
        self.clients.insert(pid, ClientStatistics::new(pid));
    }

    pub fn remove_client(&mut self, pid: i32) {
        self.clients.remove(&pid);
    }

    pub fn try_schedule(&mut self, new_pid: i32) -> Result<Option<i32>, ScheduleError> {
        if self.clients.get(&new_pid).is_none() {
            return Err(ScheduleError::InvalidClient);
        }
        if self.active_client.is_none() {
            self.clients
                .get_mut(&new_pid)
                .expect("Infallible")
                .schedule_in();
            self.active_client = Some(new_pid);
            return Ok(None);
        }

        // Some client has been scheduled
        let client = self
            .clients
            .get_mut(self.active_client.as_ref().unwrap())
            .ok_or(ScheduleError::InternalError)?;
        debug_assert!(client.is_active);
        if client.pid == new_pid {
            client.keep_alive();
            Ok(None)
        } else {
            // If current client is inactive, schedule out
            if client.last_update.elapsed() > Duration::from_millis(10) {
                client.schedule_out();
                let old_pid = client.pid;
                self.active_client = Some(new_pid);
                self.clients
                    .get_mut(&new_pid)
                    .expect("Infallible")
                    .schedule_in();
                Ok(Some(old_pid))
            } else {
                Ok(None)
            }
        }
    }

    pub fn update_mem_usage(&mut self, pid: i32, mem_usage: usize) -> Result<(), ScheduleError> {
        if let Some(client) = self.clients.get_mut(&pid) {
            client.mem_usage = mem_usage;
            Ok(())
        } else {
            Err(ScheduleError::InvalidClient)
        }
    }

    pub fn get_active_client(&self) -> Option<i32> {
        self.active_client
    }
}

struct ClientStatistics {
    pid: i32,
    mem_usage: usize,
    is_active: bool,
    schedule_start: Instant,
    last_update: Instant,
    active_time_history: HeapRb<Duration>,
}

impl ClientStatistics {
    pub fn new(pid: i32) -> Self {
        Self {
            pid,
            mem_usage: 0,
            is_active: false,
            schedule_start: Instant::now(),
            last_update: Instant::now(),
            active_time_history: HeapRb::new(100),
        }
    }

    pub fn schedule_in(&mut self) {
        self.schedule_start = Instant::now();
        self.last_update = Instant::now();
        self.is_active = true;
    }

    pub fn schedule_out(&mut self) {
        self.active_time_history
            .push_overwrite(Instant::now() - self.schedule_start);
        self.is_active = false;
    }

    pub fn keep_alive(&mut self) {
        self.last_update = Instant::now();
    }
}
