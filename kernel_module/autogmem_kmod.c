#include <linux/kprobes.h>
#include <linux/module.h>
#include <linux/printk.h>
#include <nvidia-uvm/uvm_gpu.h>

static int autogmem_kprobe_pre_handler(struct kprobe *p, struct pt_regs *regs) {
    pr_info("AutoGMem: pre_handler\n");
    return 0;
}

static struct kprobe kp = {
    .symbol_name = "preprocess_fault_batch",
    .pre_handler = autogmem_kprobe_pre_handler,
};

static int autogmem_register_probe(void) {
    if (register_kprobe(&kp) < 0) {
        pr_err("AutoGMem: Failed to register probe\n");
        return -1;
    }
    pr_info("AutoGMem: Registered probe\n");
    return 0;
}

int init_module(void) {
    pr_info("AutoGMem: Module loading\n");
    if (autogmem_register_probe() < 0) {
        return -1;
    }
    pr_info("AutoGMem: Module loaded\n");
    return 0;
}

void cleanup_module(void) {
    unregister_kprobe(&kp);
    pr_info("AutoGMem: Module unloaded\n");
}

MODULE_SOFTDEP("pre: nvidia_uvm");
MODULE_LICENSE("GPL");