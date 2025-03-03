use std::{fmt::Display, path::Path};

use nix::libc::{self, waitpid, PTRACE_ATTACH, SIGSTOP, SIGTRAP};

#[derive(Debug, Clone, Copy)]
pub enum InjectErrorStage {
    Preparing,
    Attach,
    WaitAttach(i32),
    BackupCtx(&'static str),
    InjectCode,
    WaitExec(i32),
    RecoverCtx(&'static str),
}

#[derive(Debug, Clone, Copy)]
pub struct InjectError {
    pub stage: InjectErrorStage,
    pub errno: nix::errno::Errno,
}

impl InjectError {
    pub fn new(stage: InjectErrorStage) -> Self {
        InjectError {
            stage,
            errno: nix::errno::Errno::last(),
        }
    }
}

impl Display for InjectError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "InjectError: {:?} failed with errno {}",
            self.stage, self.errno
        )
    }
}

macro_rules! check_err {
    ($e:expr,$stage:expr) => {
        if $e == -1 {
            return Err(InjectError {
                stage: $stage,
                errno: nix::errno::Errno::last(),
            });
        }
    };
}

pub fn inject_wrapper(
    pid: i32,
    dylib_path: String,
    func_sym: &str,
    arg1: u64,
    arg2: u64,
    arg3: u64,
) -> Result<u64, InjectError> {
    let dylib_base = locate_dylib_base(pid, "libcuda_hook.so")
        .ok_or(InjectError::new(InjectErrorStage::Preparing))?;
    let func_offset = resolve_func_offset(func_sym, &dylib_path)
        .ok_or(InjectError::new(InjectErrorStage::Preparing))?;
    inject_process(pid, dylib_base + func_offset, arg1, arg2, arg3)
}

pub fn inject_process(
    pid: i32,
    func_offset: u64,
    arg1: u64,
    arg2: u64,
    arg3: u64,
) -> Result<u64, InjectError> {
    unsafe {
        // Attach to the process
        check_err!(
            libc::ptrace(PTRACE_ATTACH, pid, 0, 0),
            InjectErrorStage::Attach
        );
        let mut status = 0;
        waitpid(pid, &mut status as *mut _, 0);
        if !(libc::WIFSTOPPED(status) && libc::WSTOPSIG(status) == SIGSTOP) {
            return Err(InjectError::new(InjectErrorStage::WaitAttach(status)));
        }
        // Backup the context
        let mut user_regs: libc::user_regs_struct = std::mem::zeroed();
        check_err!(
            libc::ptrace(libc::PTRACE_GETREGS, pid, 0, &mut user_regs as *mut _),
            InjectErrorStage::BackupCtx("GETREGS")
        );
        let regs_bak = user_regs;
        let text_bak: i64 = libc::ptrace(libc::PTRACE_PEEKTEXT, pid, regs_bak.rip, 0);
        check_err!(text_bak, InjectErrorStage::BackupCtx("PEEKTEXT"));
        // Inject the code
        /*
         *  41 ff d1 => call r9
         *  cc => int3
         */
        let base_code: u64 = 0xccd1ff41;
        let code: u64 = base_code | (text_bak as u64 & 0xffffffff00000000);
        user_regs.r9 = func_offset;
        user_regs.rdi = arg1;
        user_regs.rsi = arg2;
        user_regs.rdx = arg3;
        user_regs.rax = 0;
        // Align rsp to 16 bytes boundary according to x86-64 ABI
        // 128 bytes for red zone
        user_regs.rsp = (user_regs.rsp - 128) & !0xf;
        check_err!(
            libc::ptrace(libc::PTRACE_SETREGS, pid, 0, &user_regs as *const _),
            InjectErrorStage::InjectCode
        );
        check_err!(
            libc::ptrace(libc::PTRACE_POKETEXT, pid, regs_bak.rip, code),
            InjectErrorStage::InjectCode
        );
        check_err!(
            libc::ptrace(libc::PTRACE_CONT, pid, 0, 0),
            InjectErrorStage::InjectCode
        );
        // Wait injected code to finish
        let mut status = 0;
        waitpid(pid, &mut status as *mut _, 0);
        let mut should_debug = false;
        if libc::WSTOPSIG(status) != SIGTRAP {
            should_debug = true;
            if libc::WIFSTOPPED(status) {
                tracing::warn!(
                    "Injected code failed with status: {}: {}",
                    status,
                    libc::WSTOPSIG(status)
                );
            }
            // return Err(InjectError::new(InjectErrorStage::WaitExec(status)));
        }
        // Retrive return value
        check_err!(
            libc::ptrace(libc::PTRACE_GETREGS, pid, 0, &mut user_regs as *mut _),
            InjectErrorStage::RecoverCtx("GETREGS")
        );
        let ret_val = user_regs.rax;

        if should_debug {
            tracing::trace!(
                "new PC=: {:#x}/{}, old PC={:#x}, new regs={:?}",
                user_regs.rip,
                user_regs.rip,
                regs_bak.rip,
                user_regs
            );
        }
        // Recover the context
        check_err!(
            libc::ptrace(libc::PTRACE_POKETEXT, pid, regs_bak.rip, text_bak),
            InjectErrorStage::RecoverCtx("POKETEXT")
        );
        check_err!(
            libc::ptrace(libc::PTRACE_SETREGS, pid, 0, &regs_bak as *const _),
            InjectErrorStage::RecoverCtx("SETREGS")
        );
        check_err!(
            libc::ptrace(libc::PTRACE_DETACH, pid, 0, 0),
            InjectErrorStage::RecoverCtx("DETACH")
        );
        Ok(ret_val)
    }
}

pub fn locate_dylib_base(pid: i32, so_name: &str) -> Option<u64> {
    let maps_path = format!("/proc/{}/maps", pid);
    let maps = std::fs::read_to_string(maps_path).ok()?;
    for line in maps.lines() {
        if line.contains(so_name) && line.contains("r-xp") {
            let addr = line.split("-").next()?;
            let in_lib_offset =
                u64::from_str_radix(line.split_ascii_whitespace().nth(2)?, 16).ok()?;
            return Some(u64::from_str_radix(addr, 16).ok()? - in_lib_offset);
        }
    }
    None
}

pub fn resolve_func_offset<P: AsRef<Path>>(func_sym: &str, dylib_path: P) -> Option<u64> {
    let dylib = std::fs::read(dylib_path).ok()?;
    let elf = goblin::elf::Elf::parse(&dylib).ok()?;
    for sym in elf.dynsyms.iter() {
        if let Some(name) = elf.dynstrtab.get_at(sym.st_name) {
            if name == func_sym {
                return Some(sym.st_value);
            }
        }
    }
    None
}
