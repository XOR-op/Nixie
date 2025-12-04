use nix::libc;

pub struct ShmBuffer {
    // control data
    shm_path: String,
    shm_fd: i32,
    is_creator: bool,
    // buffer
    shm_addr: u64,
    shm_size: usize,
}

impl ShmBuffer {
    pub fn new(shm_path: &str, shm_size: usize, is_creator: bool) -> Result<Self, std::io::Error> {
        let oflag = if is_creator {
            libc::O_RDWR | libc::O_CREAT
        } else {
            libc::O_RDWR
        };
        let cstr_shm_path = std::ffi::CString::new(shm_path).unwrap();
        let shm_fd = unsafe {
            libc::shm_open(
                cstr_shm_path.as_ptr() as *const i8,
                oflag,
                libc::S_IRUSR | libc::S_IWUSR,
            )
        };
        if shm_fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        if unsafe { libc::ftruncate(shm_fd, shm_size as libc::off_t) } < 0 {
            unsafe { libc::close(shm_fd) };
            if is_creator {
                unsafe { libc::shm_unlink(cstr_shm_path.as_ptr() as *const i8) };
            }
            return Err(std::io::Error::last_os_error());
        }

        let shm_addr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                shm_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                shm_fd,
                0,
            )
        };
        if shm_addr == libc::MAP_FAILED {
            unsafe { libc::close(shm_fd) };
            if is_creator {
                unsafe { libc::shm_unlink(cstr_shm_path.as_ptr() as *const i8) };
            }
            return Err(std::io::Error::last_os_error());
        }

        Ok(Self {
            shm_path: shm_path.to_string(),
            shm_fd,
            shm_size,
            shm_addr: shm_addr as u64,
            is_creator,
        })
    }

    /// # Safety
    /// The caller must ensure the shm buffer is valid
    pub unsafe fn at_offset(&self, offset: u64, size: usize) -> Option<*mut u8> {
        if offset + size as u64 > self.shm_size as u64 {
            return None;
        }
        Some((self.shm_addr + offset) as *mut u8)
    }

    pub fn size(&self) -> usize {
        self.shm_size
    }
}

impl Drop for ShmBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.shm_addr as *mut libc::c_void, self.shm_size);
            libc::close(self.shm_fd);
            if self.is_creator {
                libc::shm_unlink(self.shm_path.as_ptr() as *const i8);
            }
        }
    }
}
