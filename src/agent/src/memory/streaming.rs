use std::thread;

use cudarc::driver::sys::CUevent;
use cudarc::driver::sys::{cudaError_enum, lib as cuda_lib};
use nihil_common::{MigrationArgs, MigrationResponse};

use crate::comm::migration_response_async;
use crate::init_generic_data;
use crate::memory::{default_alloc_prop, map_mem_handle, unmap_and_release_mem_handle};
use crate::{check_cu_err, set_device, warn_eprintln, CuStreamWrapper, GENERIC_DATA};

use super::default_access_desc;

struct CUeventWrapper(CUevent);
unsafe impl Send for CUeventWrapper {}

pub struct StreamingMemoryMigrator {
    device_id: i32,
    d2h_stream: CuStreamWrapper,
    h2d_stream: CuStreamWrapper,
    d2h_sender: flume::Sender<(CUeventWrapper, MigrationArgs)>,
    h2d_sender: flume::Sender<(CUeventWrapper, MigrationArgs)>,
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

    pub fn migrate(&mut self, args: MigrationArgs) {
        set_device(self.device_id);
        let mut event = std::ptr::null_mut();
        check_cu_err!(
            unsafe {
                cuda_lib().cuEventCreate(
                    &mut event,
                    cudarc::driver::sys::CUevent_flags_enum::CU_EVENT_DISABLE_TIMING as u32,
                )
            },
            "Failed to create CUDA event"
        );
        if args.host_to_device {
            // allocate physical memory
            let mut cu_handle = 0u64;
            let res = unsafe {
                cuda_lib().cuMemCreate(
                    &mut cu_handle,
                    args.size as usize,
                    &default_alloc_prop(self.device_id),
                    0,
                )
            };
            if res == cudaError_enum::CUDA_ERROR_OUT_OF_MEMORY {
                todo!("Fallback")
            }
            check_cu_err!(res, "Failed to allocate memory on device");
            let virtual_addr = {
                let mut table = GENERIC_DATA
                    .get_or_init(init_generic_data)
                    .lock(self.device_id as usize);
                let phy_handle = table
                    .handle_list
                    .get_handle_mut(args.handle_idx)
                    .expect("PhyHandle should remain valid in migration");
                assert_eq!(phy_handle.size, args.size as usize);
                assert!(!phy_handle.on_gpu);
                phy_handle.cu_handle = Some(cu_handle);
                map_mem_handle(&phy_handle, &default_access_desc(args.device));
                phy_handle.on_gpu = true;
                phy_handle.addr
            };
            // h2d copy
            check_cu_err!(
                unsafe {
                    cuda_lib().cuMemcpyHtoDAsync_v2(
                        virtual_addr,
                        todo!("Get host buffer pointer"),
                        args.size as usize,
                        self.h2d_stream.0,
                    )
                },
                "Failed to copy memory from host to device"
            );
            check_cu_err!(
                unsafe { cuda_lib().cuEventRecord(event, self.h2d_stream.0) },
                "Failed to record CUDA event for h2d copy"
            );
            // send request to worker thread
            if let Err(e) = self.h2d_sender.send((CUeventWrapper(event), args)) {
                warn_eprintln!("Failed to send h2d migration request: {}", e);
            }
        } else {
            // h2d
            let virtual_addr = {
                let table = GENERIC_DATA
                    .get_or_init(init_generic_data)
                    .lock(self.device_id as usize);
                let phy_handle = table
                    .handle_list
                    .get_handle(args.handle_idx)
                    .expect("PhyHandle should remain valid in migration");
                assert!(phy_handle.on_gpu);
                assert_eq!(phy_handle.size, args.size as usize);
                phy_handle.addr
            };
            check_cu_err!(
                unsafe {
                    cuda_lib().cuMemcpyDtoHAsync_v2(
                        todo!("Get host buffer pointer"),
                        virtual_addr,
                        args.size as usize,
                        self.d2h_stream.0,
                    )
                },
                "Failed to copy memory from device to host"
            );
            check_cu_err!(
                unsafe { cuda_lib().cuEventRecord(event, self.d2h_stream.0) },
                "Failed to record CUDA event for d2h copy"
            );
            // send request to worker thread
            if let Err(e) = self.d2h_sender.send((CUeventWrapper(event), args)) {
                warn_eprintln!("Failed to send d2h migration request: {}", e);
            }
        }
    }

    fn worker_thread(device_id: i32, req_queue: flume::Receiver<(CUeventWrapper, MigrationArgs)>) {
        set_device(device_id);
        while let Some((event, args)) = req_queue.recv().ok() {
            // wait for the event to complete
            check_cu_err!(
                unsafe { cuda_lib().cuEventSynchronize(event.0) },
                "Failed to synchronize CUDA event"
            );
            // send back response
            let response = MigrationResponse {
                device: args.device,
                size: args.size,
                host_buffer_idx: args.host_buffer_idx,
            };
            // copy finished, do the post-processing
            if !args.host_to_device {
                let mut table = GENERIC_DATA
                    .get_or_init(init_generic_data)
                    .lock(device_id as usize);
                let handle = table
                    .handle_list
                    .get_handle_mut(args.handle_idx)
                    .expect("PhyHandle shoule remain valid in migration");
                // d2h
                unmap_and_release_mem_handle(handle);
                handle.on_gpu = false;
            }
            migration_response_async(vec![response]);
        }
    }
}
