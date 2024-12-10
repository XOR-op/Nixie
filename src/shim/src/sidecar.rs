use std::{
    collections::{hash_map::Entry, HashMap},
    io::Read,
    os::unix::net::UnixStream,
};

use colored::Colorize;
use cudarc::driver::sys::{cudaError_enum, CUcontext, CUdevice};
use nihilipc::S2CMessage;

use crate::schedule::SchedControl;

pub(crate) struct Sidecar {
    recv: UnixStream,
    sched_ctrl: &'static SchedControl,
}

impl Sidecar {
    pub fn new(stream: UnixStream, sched_ctrl: &'static SchedControl) -> Self {
        Self {
            recv: stream,
            sched_ctrl,
        }
    }

    pub fn run(mut self) -> std::io::Result<()> {
        let mut ctxs = CudaContextGuard {
            cuda_ctxs: HashMap::new(),
        };
        let mut len_buf = [0u8; 4];
        let mut buf = [0u8; 4096];
        loop {
            match self.recv.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        return Ok(());
                    } else {
                        return Err(e);
                    }
                }
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            self.recv.read_exact(&mut buf[..len as usize])?;
            let args = bincode::deserialize::<S2CMessage>(&buf[..len])
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            match args {
                S2CMessage::SetReadDup(args) => {
                    ctxs.set_current_ctx(args.device);
                    eprintln!(
                        "{} {}: =>{} address={:#x}, len={:#x}, device={}",
                        "[libcuda_hook]".bold(),
                        "rpc_read_duplication".blue(),
                        args.value,
                        args.addr,
                        args.len,
                        args.device
                    );
                    advise_read_mostly_for(args.value, args.addr, args.len, args.device);
                }
                S2CMessage::GrantRunningToken(args) => {
                    eprintln!(
                        "{} {}: time={:?}",
                        "[libcuda_hook]".bold(),
                        "UNIMPLEMENTED rpc_grant_running_token".red(),
                        args.time,
                    );
                    self.sched_ctrl.update_time(args.time);
                }
            }
        }
    }
}

struct CudaContextGuard {
    cuda_ctxs: HashMap<i32, CUcontext>,
}

impl CudaContextGuard {
    fn get_dev_ctx(&mut self, device_idx: CUdevice) -> CUcontext {
        match self.cuda_ctxs.entry(device_idx) {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                let mut ctx = std::ptr::null_mut();
                let res = unsafe {
                    cudarc::driver::sys::cuDevicePrimaryCtxRetain(&mut ctx as *mut _, device_idx)
                };
                if res != cudaError_enum::CUDA_SUCCESS {
                    eprintln!(
                        "Failed to retain context for device {}: {:?}",
                        device_idx, res
                    );
                }
                e.insert(ctx);
                eprintln!(
                    "{} {}: device={}",
                    "[libcuda_hook]".bold(),
                    "init_dev_ctx".blue(),
                    device_idx
                );
                ctx
            }
        }
    }

    fn set_current_ctx(&mut self, device_idx: CUdevice) {
        let ctx = self.get_dev_ctx(device_idx);
        let res = unsafe { cudarc::driver::sys::cuCtxSetCurrent(ctx) };
        if res != cudaError_enum::CUDA_SUCCESS {
            eprintln!("Failed to set current context: {:?}", res);
        }
    }
}

impl Drop for CudaContextGuard {
    fn drop(&mut self) {
        self.cuda_ctxs.keys().for_each(|dev| {
            let res = unsafe { cudarc::driver::sys::cuDevicePrimaryCtxRelease_v2(*dev) };
            if res != cudaError_enum::CUDA_SUCCESS {
                eprintln!("Failed to release context: {:?}", res);
            }
        });
        self.cuda_ctxs.clear();
    }
}

fn advise_read_mostly_for(read_mostly: bool, address: u64, length: u64, device: i32) {
    let res = unsafe {
        cudarc::driver::sys::cuMemAdvise(
            address,
            length as usize,
            if read_mostly {
                cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_SET_READ_MOSTLY
            } else {
                cudarc::driver::sys::CUmem_advise_enum::CU_MEM_ADVISE_UNSET_READ_MOSTLY
            },
            device,
        )
    };
    if res != cudaError_enum::CUDA_SUCCESS {
        eprintln!("Failed to set read mostly: {:?}", res);
    }
}
