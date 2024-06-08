use std::os::fd::OwnedFd;

pub(crate) struct EventQueue {
    uvm_tools_handle: OwnedFd,
    uvm_fd: OwnedFd,
    
}