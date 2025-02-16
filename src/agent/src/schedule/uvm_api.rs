#[repr(C)]
#[derive(Debug, Clone, Default)]
pub(crate) struct NVUuid {
    pub bytes: [u8; 16],
}

// from kernel-open/nvidia-uvm/nvCpuUuid.c
pub(super) const NV_PROCESSOR_UUID_CPU_DEFAULT: NVUuid = NVUuid {
    bytes: [
        // Produced via uuidgen(1): 73772a14-2c41-4750-a27b-d4d74e0f5ea6:
        0xa6, 0x5e, 0x0f, 0x4e, 0xd7, 0xd4, 0x7b, 0xa2, 0x50, 0x47, 0x41, 0x2c, 0x14, 0x2a, 0x77,
        0x73,
    ],
};

pub(crate) type NVStatus = u32;

// UVM_SET_PREFERRED_LOCATION_PARAMS
#[repr(C)]
#[derive(Debug, Clone)]
pub struct UvmSetPreferredLocationParams {
    pub requested_base: u64,
    pub length: u64,
    pub processor: NVUuid,
    pub preferred_cpu_numa_node: i32,
    pub rm_status: NVStatus, // out
}

#[repr(C)]
#[derive(Debug, Clone)]
pub struct UvmSetReadDuplicationParams {
    pub requested_base: u64,
    pub length: u64,
    pub rm_status: NVStatus, // out
}

pub type UvmUnsetPreferredLocationParams = UvmSetReadDuplicationParams;

const UVM_SET_PREFERRED_LOCATION_IOCTL: u32 = 42;
const UVM_UNSET_PREFERRED_LOCATION_IOCTL: u32 = 43;
const UVM_ENABLE_READ_DUPLICATION_IOCTL: u32 = 44;
const UVM_DISABLE_READ_DUPLICATION_IOCTL: u32 = 45;

nix::ioctl_readwrite_bad!(
    uvm_set_preferred_location,
    UVM_SET_PREFERRED_LOCATION_IOCTL,
    UvmSetPreferredLocationParams
);

nix::ioctl_readwrite_bad!(
    uvm_unset_preferred_location,
    UVM_UNSET_PREFERRED_LOCATION_IOCTL,
    UvmUnsetPreferredLocationParams
);

nix::ioctl_readwrite_bad!(
    uvm_enable_read_duplication,
    UVM_ENABLE_READ_DUPLICATION_IOCTL,
    UvmSetReadDuplicationParams
);

nix::ioctl_readwrite_bad!(
    uvm_disable_read_duplication,
    UVM_DISABLE_READ_DUPLICATION_IOCTL,
    UvmSetReadDuplicationParams
);
