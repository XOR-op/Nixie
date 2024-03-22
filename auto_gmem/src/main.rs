use crate::inject::*;

mod inject;
fn main() {
    println!("Hello, world!");
    let pid = 694868;
    let func_offset =
        resolve_func_offset("_auto_gmem_prefetch", "./target/release/libcuda_hook.so").unwrap();
    let dylib_base = locate_dylib_base(pid, "libcuda_hook.so").unwrap();
    let offset = func_offset + dylib_base;
    dbg!(inject_process(pid, offset));
}
