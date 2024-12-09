use std::alloc;
use std::ptr::NonNull;

pub(crate) mod event_queue;
pub(crate) mod uvm_api;
pub(crate) mod uvm_binding;

const PAGE_SIZE: usize = 4096;

pub(crate) struct PageBackedArray<T> {
    ptr: NonNull<T>,
    /// Requested length in elements
    ele_len: usize,
    /// Real allocated size in bytes
    all_len: usize,
}

impl<T> PageBackedArray<T> {
    pub fn new(size: usize) -> Self {
        let array_size = std::mem::size_of::<T>() * size; // in bytes
        let page_num = array_size.div_ceil(PAGE_SIZE);
        let rounded_size = page_num * PAGE_SIZE; // in bytes
        let layout = alloc::Layout::from_size_align(rounded_size, PAGE_SIZE).unwrap();
        let ptr = unsafe { alloc::alloc(layout) };
        Self {
            ptr: match NonNull::new(ptr as *mut T) {
                Some(ptr) => ptr,
                None => alloc::handle_alloc_error(layout),
            },
            ele_len: size,
            all_len: rounded_size,
        }
    }

    /// Get the length of the elements
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.ele_len
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.ele_len) }
    }

    #[inline(always)]
    pub fn as_slice_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.ele_len) }
    }

    #[inline(always)]
    pub fn as_ptr(&self) -> *const T {
        self.ptr.as_ptr()
    }

    #[inline(always)]
    pub unsafe fn as_mut(&mut self) -> *mut T {
        self.ptr.as_mut()
    }
}

impl<T> Drop for PageBackedArray<T> {
    fn drop(&mut self) {
        let layout = alloc::Layout::from_size_align(self.all_len, PAGE_SIZE).unwrap();
        unsafe { alloc::dealloc(self.ptr.as_ptr() as *mut u8, layout) }
    }
}

unsafe impl<T> Send for PageBackedArray<T> {}
unsafe impl<T> Sync for PageBackedArray<T> {}

mod tests {
    #[test]
    fn test_array() {
        use super::{PageBackedArray, PAGE_SIZE};
        for _ in 0..10 {
            let aligned_array = PageBackedArray::<u64>::new(1137);
            assert_eq!(aligned_array.ele_len, 1137);
            assert_eq!(aligned_array.all_len, PAGE_SIZE * 3);
            assert_eq!(
                aligned_array.ptr.as_ptr() as *const _ as usize % PAGE_SIZE,
                0
            );
        }
    }
}
