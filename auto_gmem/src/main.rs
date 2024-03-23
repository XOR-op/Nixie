use crate::inject::*;

mod inject;
fn main() {
    println!("Hello, world!");
    let pid = std::env::args().nth(1).unwrap().parse::<i32>().unwrap();
    let length = std::env::args()
        .nth(2)
        .map(|s| s.parse::<u64>().unwrap())
        .unwrap_or(1);
    let func_offset =
        resolve_func_offset("_auto_gmem_prefetch", "./target/release/libcuda_hook.so").unwrap();
    let dylib_base = locate_dylib_base(pid, "libcuda_hook.so").unwrap();
    let offset = func_offset + dylib_base;
    dbg!(inject_process(pid, offset, length)).ok();
}
