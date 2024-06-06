pub(crate) type NvStatus = u32;

#[repr(C)]
pub(crate) struct UvmCreateEventQueueParams {
    session_index: i32,
    event_queue_index: u32,
    queue_size: u64,
    timestamp_type: u32,
    rm_status: NvStatus,
}

#[repr(C)]
pub(crate) struct UvmRemoveEventQueueParams {
    session_index: i32,
    event_queue_index: u32,
    rm_status: NvStatus,
}
