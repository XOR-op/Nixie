use std::alloc;
use std::ptr::NonNull;

mod event_queue;
mod uvm_api;

pub struct PageBackedArray<T> {
    ptr: NonNull<T>,
    /// Requested length in elements
    ele_len: usize,
    /// Real allocated size in bytes
    all_len: usize,
}

impl<T> PageBackedArray<T> {
    pub fn new(size: usize) -> Self {
        let array_size = std::mem::size_of::<T>() * size;
        let page_num = array_size.div_ceil(4096);
        let rounded_size = page_num * 4096;
        let layout = alloc::Layout::from_size_align(rounded_size, 4096).unwrap();
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
    pub fn len(&self) -> usize {
        self.ele_len
    }

    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.ele_len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.ele_len) }
    }
}

impl<T> Drop for PageBackedArray<T> {
    fn drop(&mut self) {
        let layout = alloc::Layout::from_size_align(self.all_len, 4096).unwrap();
        unsafe { alloc::dealloc(self.ptr.as_ptr() as *mut u8, layout) }
    }
}

mod tests {
    #[test]
    fn test_array() {
        use super::PageBackedArray;
        for _ in 0..10 {
            let aligned_array = PageBackedArray::<u64>::new(1137);
            assert_eq!(aligned_array.ele_len, 1137);
            assert_eq!(aligned_array.all_len, 4096 * 3);
            assert_eq!(aligned_array.ptr.as_ptr() as *const _ as usize % 4096, 0);
        }
    }
}
