use std::pin::Pin;

use nix::libc;

pub struct ShmHeader {
    all_len: u32,
    pub ptr_mapping: ShmVec<(u64, usize), 4096>,
}

impl ShmHeader {
    pub fn init_at(shm_fd: i32, len: u32) -> std::io::Result<Pin<&'static mut Self>> {
        if len < std::mem::size_of::<Self>() as u32 {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        unsafe {
            // create mmap
            let ptr = libc::mmap(
                std::ptr::null_mut(),
                len as usize,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                shm_fd,
                0,
            );
            if ptr == libc::MAP_FAILED {
                return Err(std::io::Error::last_os_error());
            }
            let val = Self {
                all_len: len,
                ptr_mapping: ShmVec::new(),
            };
            std::ptr::write(ptr as *mut Self, val);
            Ok(Pin::new(&mut *(ptr as *mut Self)))
        }
    }

    pub unsafe fn open_readonly_at(
        shm_fd: i32,
        len: u32,
    ) -> std::io::Result<Pin<&'static mut Self>> {
        if len < std::mem::size_of::<Self>() as u32 {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        // create mmap
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            len as usize,
            libc::PROT_READ,
            libc::MAP_SHARED,
            shm_fd,
            0,
        );
        if ptr == libc::MAP_FAILED {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(Pin::new(&mut *(ptr as *mut Self)))
        }
    }
}

impl Drop for ShmHeader {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(
                self as *mut Self as *mut libc::c_void,
                self.all_len as usize,
            );
        }
    }
}

pub struct ShmVec<T, const N: usize> {
    data: [T; N],
    len: usize,
}

impl<T, const N: usize> ShmVec<T, N> {
    pub fn new() -> Self {
        Self {
            data: unsafe { std::mem::zeroed() },
            len: 0,
        }
    }

    pub fn as_slice(&self) -> &[T] {
        &self.data[..self.len]
    }

    pub fn push(&mut self, val: T) -> Result<(), ()> {
        if self.len >= N {
            return Err(());
        }
        self.data[self.len] = val;
        self.len += 1;
        Ok(())
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        Some(std::mem::replace(&mut self.data[self.len], unsafe {
            std::mem::zeroed()
        }))
    }

    pub fn remove(&mut self, idx: usize) -> Option<T> {
        if idx >= self.len {
            return None;
        }
        self.len -= 1;
        let val = std::mem::replace(&mut self.data[idx], unsafe { std::mem::zeroed() });
        for i in idx..self.len {
            self.data[i] = std::mem::replace(&mut self.data[i + 1], unsafe { std::mem::zeroed() });
        }
        Some(val)
    }
}
