use core::{cell::UnsafeCell, sync::atomic::AtomicU8};
use nix::libc;

use crate::shm::ReInitializable;

pub struct IpcMutex<T> {
    lock: libc::sem_t,
    ref_count: AtomicU8,
    inner: UnsafeCell<T>,
}

impl<T> IpcMutex<T> {
    pub fn new(val: T) -> Self {
        let mut v = Self {
            inner: UnsafeCell::new(val),
            ref_count: AtomicU8::new(1),
            lock: unsafe { core::mem::zeroed() },
        };
        unsafe {
            libc::sem_init(&mut v.lock, 1, 1);
        }
        v
    }

    pub fn increase_ref_count(&self) {
        self.ref_count
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    }

    fn lock_inner(&self) {
        loop {
            let ret = unsafe { libc::sem_wait(&self.lock as *const _ as *mut _) };
            match ret {
                0 => break,
                libc::EINTR => continue,
                _ => panic!("sem_wait failed: {}", nix::errno::Errno::last()),
            }
        }
    }

    fn unlock_inner(&self) {
        let ret = unsafe { libc::sem_post(&self.lock as *const _ as *mut _) };
        if ret != 0 {
            panic!("sem_post failed: {}", nix::errno::Errno::last());
        }
    }

    pub fn lock(&'_ self) -> IpcMutexGuard<'_, T> {
        self.lock_inner();
        IpcMutexGuard { lock: self }
    }

    /// # Safety
    ///
    /// This involves libc::semaphore
    pub unsafe fn close(&mut self) {
        let old_ref_count = self
            .ref_count
            .fetch_sub(1, core::sync::atomic::Ordering::Relaxed);
        if old_ref_count == 1 {
            unsafe { libc::sem_destroy(&mut self.lock) };
        }
    }
}

impl<T: ReInitializable> ReInitializable for IpcMutex<T> {
    unsafe fn reinit_from_uninited(&mut self) {
        self.ref_count = AtomicU8::new(1);
        self.lock = core::mem::zeroed();
        libc::sem_init(&mut self.lock, 1, 1);
        // Safety: T and UnsafeCell<T> share the same memory layout.
        self.inner.get_mut().reinit_from_uninited();
    }
}

unsafe impl<T: Sync> Sync for IpcMutex<T> {}

pub struct IpcMutexGuard<'a, T> {
    lock: &'a IpcMutex<T>,
}

impl<T> Drop for IpcMutexGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.unlock_inner();
    }
}

impl<T> core::ops::Deref for IpcMutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.inner.get() }
    }
}

impl<T> core::ops::DerefMut for IpcMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.inner.get() }
    }
}
