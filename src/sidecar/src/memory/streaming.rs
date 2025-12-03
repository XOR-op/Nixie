use std::sync::{Mutex, OnceLock};
use std::thread;

use cudarc::driver::sys::CUevent;
use cudarc::driver::sys::cudaError_enum;
use nihil_common::general::{CallParameter, CallReturnChannel};
use nihil_common::{MAX_GPUS, MigrationArgs, MigrationResponse};

use crate::init::should_have_initialized;
use crate::memory::{default_alloc_prop, map_mem_handle, unmap_and_release_mem_handle};
use crate::{CuStreamWrapper, GENERIC_DATA, check_cu_err, set_device, warn_eprintln};
use crate::{debug_eprintln, global_shm_buffer};

use super::default_access_desc;

pub static MEMORY_MIGRATION_CTL: OnceLock<MemoryMigrationControl> = OnceLock::new();

pub fn init_memory_migration_ctl() -> MemoryMigrationControl {
    MemoryMigrationControl::new()
}

pub struct MemoryMigrationControl {
    migrators: Mutex<Vec<StreamingMemoryMigrator>>,
}

impl MemoryMigrationControl {
    pub fn new() -> Self {
        let device_cnt = unsafe {
            let mut count = 0;
            check_cu_err!(
                cudarc::driver::sys::cuDeviceGetCount(&mut count),
                "Failed to get device count"
            );
            count
        };
        let migrators = (0..device_cnt).map(StreamingMemoryMigrator::new).collect();
        Self {
            migrators: Mutex::new(migrators),
        }
    }

    pub fn migrate(&self, task: CallParameter<MigrationArgs, MigrationResponse>) {
        let mut migrators = self.migrators.lock().unwrap();
        let device_id = task.param.device.0;
        if device_id < 0 || device_id >= MAX_GPUS as i32 {
            warn_eprintln!("Invalid device ID: {}", device_id);
        }
        migrators[device_id as usize].migrate(task);
    }
}

struct CUeventWrapper(CUevent);
unsafe impl Send for CUeventWrapper {}

struct TaskItem {
    event: CUeventWrapper,
    args: MigrationArgs,
    is_valid: bool,
    ret_chan: CallReturnChannel<MigrationResponse>,
}

struct StreamingMemoryMigrator {
    device_id: i32,
    d2h_stream: CuStreamWrapper,
    h2d_stream: CuStreamWrapper,
    d2h_sender: flume::Sender<TaskItem>,
    h2d_sender: flume::Sender<TaskItem>,
}

impl StreamingMemoryMigrator {
    pub fn new(device_id: i32) -> Self {
        let (d2h_tx, d2h_rx) = flume::unbounded();
        let (h2d_tx, h2d_rx) = flume::unbounded();
        thread::spawn(move || {
            Self::worker_thread(device_id, d2h_rx);
        });
        thread::spawn(move || {
            Self::worker_thread(device_id, h2d_rx);
        });
        Self {
            device_id,
            d2h_stream: CuStreamWrapper::new(device_id),
            h2d_stream: CuStreamWrapper::new(device_id),
            d2h_sender: d2h_tx,
            h2d_sender: h2d_tx,
        }
    }

    pub fn migrate(&mut self, args: CallParameter<MigrationArgs, MigrationResponse>) {
        let (args, ret_chan) = args.into_parts();
        set_device(self.device_id);
        let total_size = args.size.iter().map(|&s| s as usize).sum::<usize>();
        let mut event = std::ptr::null_mut();
        check_cu_err!(
            unsafe {
                cudarc::driver::sys::cuEventCreate(
                    &mut event,
                    cudarc::driver::sys::CUevent_flags_enum::CU_EVENT_DISABLE_TIMING as u32,
                )
            },
            "Failed to create CUDA event"
        );
        assert_eq!(args.host_buffer_offset.len(), args.size.len());
        assert!(!args.size.is_empty());
        let mut table = GENERIC_DATA
            .get_or_init(should_have_initialized)
            .lock(self.device_id as usize);
        if args.host_to_device {
            // allocate physical memory
            let mut cu_handle = 0u64;
            let res = unsafe {
                cudarc::driver::sys::cuMemCreate(
                    &mut cu_handle,
                    total_size,
                    &default_alloc_prop(self.device_id),
                    0,
                )
            };
            if res == cudaError_enum::CUDA_ERROR_OUT_OF_MEMORY {
                todo!("Fallback")
            }
            check_cu_err!(res, "Failed to allocate memory on device");
            let virtual_addr = {
                let phy_handle = table
                    .handle_list
                    .get_handle_mut(args.handle_idx)
                    .expect("PhyHandle should remain valid in migration");
                assert_eq!(phy_handle.size, total_size);
                assert!(!phy_handle.on_gpu);
                phy_handle.cu_handle = Some(cu_handle);
                map_mem_handle(phy_handle, &default_access_desc(args.device.0));
                phy_handle.on_gpu = true;
                phy_handle.addr
            };

            let mut accu_offset = 0u64;
            // h2d copy
            for (base, size) in args.host_buffer_offset.iter().zip(args.size.iter()) {
                check_cu_err!(
                    unsafe {
                        cudarc::driver::sys::cuMemcpyHtoDAsync_v2(
                            virtual_addr + accu_offset,
                            match get_host_buffer_ptr(*base, *size) {
                                Some(x) => x as *const _,
                                None => return,
                            },
                            *size as usize,
                            self.h2d_stream.0,
                        )
                    },
                    "Failed to copy memory from host to device"
                );
                accu_offset += *size as u64;
            }
            check_cu_err!(
                unsafe { cudarc::driver::sys::cuEventRecord(event, self.h2d_stream.0) },
                "Failed to record CUDA event for h2d copy"
            );
            // send request to worker thread
            if let Err(e) = self.h2d_sender.send(TaskItem {
                event: CUeventWrapper(event),
                args,
                is_valid: true,
                ret_chan,
            }) {
                warn_eprintln!("Failed to send h2d migration request: {}", e);
            }
        } else {
            // d2h
            let virtual_addr = {
                let phy_handle = table
                    .handle_list
                    .get_handle(args.handle_idx)
                    .expect("PhyHandle should remain valid in migration");
                if !phy_handle.valid {
                    // already freed
                    if let Err(e) = self.d2h_sender.send(TaskItem {
                        event: CUeventWrapper(event),
                        args,
                        is_valid: false,
                        ret_chan,
                    }) {
                        warn_eprintln!("Failed to send d2h migration request: {}", e);
                    }
                    return;
                }
                assert!(phy_handle.on_gpu);
                assert_eq!(phy_handle.size, total_size);
                phy_handle.addr
            };
            let mut accu_offset = 0u64;
            for (base, size) in args.host_buffer_offset.iter().zip(args.size.iter()) {
                let host_buffer_ptr = match get_host_buffer_ptr(*base, *size) {
                    Some(x) => x,
                    None => return,
                };
                check_cu_err!(
                    unsafe {
                        cudarc::driver::sys::cuMemcpyDtoHAsync_v2(
                            host_buffer_ptr as *mut _,
                            virtual_addr + accu_offset,
                            *size as usize,
                            self.d2h_stream.0,
                        )
                    },
                    "Failed to copy memory from device to host"
                );
                accu_offset += *size as u64;
            }
            check_cu_err!(
                unsafe { cudarc::driver::sys::cuEventRecord(event, self.d2h_stream.0) },
                "Failed to record CUDA event for d2h copy"
            );
            // send request to worker thread
            if let Err(e) = self.d2h_sender.send(TaskItem {
                event: CUeventWrapper(event),
                args,
                is_valid: true,
                ret_chan,
            }) {
                warn_eprintln!("Failed to send d2h migration request: {}", e);
            }
        }
    }

    fn worker_thread(device_id: i32, req_queue: flume::Receiver<TaskItem>) {
        set_device(device_id);
        while let Ok(TaskItem {
            event,
            args,
            is_valid,
            ret_chan,
        }) = req_queue.recv()
        {
            // wait for the event to complete
            check_cu_err!(
                unsafe { cudarc::driver::sys::cuEventSynchronize(event.0) },
                "Failed to synchronize CUDA event"
            );
            check_cu_err!(
                unsafe { cudarc::driver::sys::cuEventDestroy_v2(event.0) },
                "Failed to destroy CUDA event"
            );
            let response = if is_valid {
                // send back response
                let response = MigrationResponse::Success {
                    handle_idx: args.handle_idx,
                    device: args.device,
                    size: args.size.iter().map(|&s| s as u64).sum(),
                };
                // copy finished, do the post-processing
                if !args.host_to_device {
                    let mut table = GENERIC_DATA
                        .get_or_init(should_have_initialized)
                        .lock(device_id as usize);
                    let handle = table
                        .handle_list
                        .get_handle_mut(args.handle_idx)
                        .expect("PhyHandle shoule remain valid in migration");
                    // d2h
                    unmap_and_release_mem_handle(handle);
                    handle.on_gpu = false;
                }
                response
            } else {
                debug_eprintln!("Already freed memory handle during migration: {:?}", args);
                MigrationResponse::AlreadyFreed {
                    handle_idx: args.handle_idx,
                    device: args.device,
                    size: args.size.iter().map(|&s| s as u64).sum(),
                }
            };
            if ret_chan.ret(response).is_err() {
                debug_eprintln!("Failed to send migration response");
            }
        }
    }
}

fn get_host_buffer_ptr(host_buffer_offset: u64, size: u32) -> Option<*mut std::ffi::c_void> {
    let ptr = unsafe { global_shm_buffer().at_offset(host_buffer_offset, size as usize) };
    if ptr.is_none() {
        warn_eprintln!(
            "Invalid host buffer: offset={} size={}",
            host_buffer_offset,
            size
        );
        return None;
    };
    Some(ptr.unwrap() as *mut std::ffi::c_void)
}
