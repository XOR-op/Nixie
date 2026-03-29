use std::ffi::c_void;

use cudarc::driver::sys::{
    CUcontext, CUdevice, CUdeviceptr, CUevent, CUmemAccessDesc, CUmemAllocationProp,
    CUmemGenericAllocationHandle, CUresult, CUstream,
};

macro_rules! wrap_cu_fn {
    ($(fn $name:ident($($arg:ident: $ty:ty),* $(,)?) -> $ret:ty;)+) => {
        $(
            #[allow(non_snake_case)]
            #[inline(always)]
            pub(crate) unsafe fn $name($($arg: $ty),*) -> $ret {
                unsafe { cudarc::driver::sys::$name($($arg),*) }
            }
        )+
    };
}

wrap_cu_fn! {
    fn cuCtxGetCurrent(pctx: *mut CUcontext) -> CUresult;
    fn cuCtxGetDevice(device: *mut CUdevice) -> CUresult;
    fn cuCtxSetCurrent(ctx: CUcontext) -> CUresult;
    fn cuCtxSynchronize() -> CUresult;
    fn cuDeviceGetCount(count: *mut ::core::ffi::c_int) -> CUresult;
    fn cuDevicePrimaryCtxRetain(pctx: *mut CUcontext, dev: CUdevice) -> CUresult;
    fn cuEventCreate(ph_event: *mut CUevent, flags: ::core::ffi::c_uint) -> CUresult;
    fn cuEventDestroy_v2(h_event: CUevent) -> CUresult;
    fn cuEventQuery(h_event: CUevent) -> CUresult;
    fn cuEventRecord(h_event: CUevent, h_stream: CUstream) -> CUresult;
    fn cuEventSynchronize(h_event: CUevent) -> CUresult;
    fn cuInit(flags: ::core::ffi::c_uint) -> CUresult;
    fn cuMemAddressReserve(
        ptr: *mut CUdeviceptr,
        size: usize,
        alignment: usize,
        addr: CUdeviceptr,
        flags: ::core::ffi::c_ulonglong,
    ) -> CUresult;
    fn cuMemCreate(
        handle: *mut CUmemGenericAllocationHandle,
        size: usize,
        prop: *const CUmemAllocationProp,
        flags: ::core::ffi::c_ulonglong,
    ) -> CUresult;
    fn cuMemHostRegister_v2(
        ptr: *mut c_void,
        bytesize: usize,
        flags: ::core::ffi::c_uint,
    ) -> CUresult;
    fn cuMemMap(
        ptr: CUdeviceptr,
        size: usize,
        offset: usize,
        handle: CUmemGenericAllocationHandle,
        flags: ::core::ffi::c_ulonglong,
    ) -> CUresult;
    fn cuMemRelease(handle: CUmemGenericAllocationHandle) -> CUresult;
    fn cuMemSetAccess(
        ptr: CUdeviceptr,
        size: usize,
        desc: *const CUmemAccessDesc,
        count: usize,
    ) -> CUresult;
    fn cuMemUnmap(ptr: CUdeviceptr, size: usize) -> CUresult;
    fn cuMemcpyDtoHAsync_v2(
        dst_host: *mut c_void,
        src_device: CUdeviceptr,
        byte_count: usize,
        h_stream: CUstream,
    ) -> CUresult;
    fn cuMemcpyHtoDAsync_v2(
        dst_device: CUdeviceptr,
        src_host: *const c_void,
        byte_count: usize,
        h_stream: CUstream,
    ) -> CUresult;
    fn cuStreamCreate(ph_stream: *mut CUstream, flags: ::core::ffi::c_uint) -> CUresult;
    fn cuStreamSynchronize(h_stream: CUstream) -> CUresult;
}
