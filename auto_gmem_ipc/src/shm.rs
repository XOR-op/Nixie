use core::{ffi::c_int, pin::Pin};

use nix::libc;

use crate::sync::IpcMutex;

pub struct Shm {
    all_len: u32,
    pub ptr_mapping: IpcMutex<ShmVec<(u64, usize), 4096>>,
}

impl Shm {
    pub const SHM_STRUCT_SIZE: u32 = core::mem::size_of::<Self>() as u32;

    pub fn init_at(shm_fd: i32, len: u32) -> Result<Pin<&'static mut Self>, c_int> {
        if len < core::mem::size_of::<Self>() as u32 {
            return Err(libc::EINVAL);
        }
        unsafe {
            // create mmap
            let ptr = libc::mmap(
                core::ptr::null_mut(),
                len as usize,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                shm_fd,
                0,
            );
            if ptr == libc::MAP_FAILED {
                return Err(nix::errno::Errno::last_raw());
            }
            let val = Self {
                all_len: len,
                ptr_mapping: IpcMutex::new(ShmVec::new()),
            };
            core::ptr::write(ptr as *mut Self, val);
            Ok(Pin::new(&mut *(ptr as *mut Self)))
        }
    }

    pub unsafe fn open_readonly_at(shm_fd: i32, len: u32) -> Result<Pin<&'static mut Self>, c_int> {
        if len < core::mem::size_of::<Self>() as u32 {
            return Err(libc::EINVAL);
        }
        // create mmap
        let ptr = libc::mmap(
            core::ptr::null_mut(),
            len as usize,
            libc::PROT_READ,
            libc::MAP_SHARED,
            shm_fd,
            0,
        );
        if ptr == libc::MAP_FAILED {
            Err(nix::errno::Errno::last_raw())
        } else {
            let r = Pin::new(&mut *(ptr as *mut Self));
            r.ptr_mapping.increase_ref_count();
            Ok(r)
        }
    }

    unsafe fn close(&mut self) {
        self.ptr_mapping.close();
        libc::munmap(
            self as *const Self as *mut libc::c_void,
            self.all_len as usize,
        );
    }
}

pub struct ShmGuard {
    pub inner: Pin<&'static mut Shm>,
}

impl ShmGuard {
    pub fn new(shm: Pin<&'static mut Shm>) -> Self {
        Self { inner: shm }
    }
}

impl Drop for ShmGuard {
    fn drop(&mut self) {
        unsafe {
            self.inner.close();
        }
    }
}

pub struct ShmVec<T, const N: usize> {
    len: u32,
    data: [T; N],
}

impl<T, const N: usize> ShmVec<T, N> {
    pub fn new() -> Self {
        Self {
            len: 0,
            data: unsafe { core::mem::zeroed() },
        }
    }

    pub fn push(&mut self, val: T) -> Result<(), ()> {
        if self.len as usize >= N {
            return Err(());
        }
        self.data[self.len as usize] = val;
        self.len += 1;
        Ok(())
    }
}
