use std::{fmt::Display, io, path::Path};

use nix::libc::{self, waitpid, PTRACE_ATTACH, SIGSTOP};

#[derive(Debug, Clone, Copy)]
pub enum InjectErrorStage {
    Attach,
    WaitAttach(i32),
    BackupCtx,
    InjectCode,
    RecoverCtx,
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

macro_rules! check_zero {
    ($e:expr,$stage:expr) => {
        if $e != 0 {
            return Err(InjectError {
                stage: $stage,
                errno: nix::errno::Errno::last(),
            });
        }
    };
}

pub fn inject_process(pid: i32, func_offset: u64) -> Result<(), InjectError> {
    // Attach to the process
    check_zero!(
        unsafe { libc::ptrace(PTRACE_ATTACH, pid, 0, 0) },
        InjectErrorStage::Attach
    );
    unsafe {
        let mut status = 0;
        waitpid(pid, &mut status as *mut _, 0);
        if status != SIGSTOP {
            return Err(InjectError::new(InjectErrorStage::WaitAttach(status)));
        }
    };
    // Backup the context
    let mut user_regs: libc::user_regs_struct = unsafe { std::mem::zeroed() };
    check_zero!(
        unsafe { libc::ptrace(libc::PTRACE_GETREGS, pid, 0, &mut user_regs as *mut _) },
        InjectErrorStage::BackupCtx
    );
    let regs_bak = user_regs.clone();
    let text_bak = unsafe { libc::ptrace(libc::PTRACE_PEEKTEXT, pid, user_regs.rip, 0) };
    check_zero!(text_bak, InjectErrorStage::BackupCtx);
    // Inject the code
    /*
     *  ff d0 => call rax
     *  cc => int3
     */
    let code: usize = 0xccd0ff;
    user_regs.rax = func_offset as u64;
    check_zero!(
        unsafe { libc::ptrace(libc::PTRACE_POKETEXT, pid, user_regs.rip, code) },
        InjectErrorStage::InjectCode
    );
    check_zero!(
        unsafe { libc::ptrace(libc::PTRACE_CONT, pid, 0, 0) },
        InjectErrorStage::InjectCode
    );
    // Recover the context
    todo!("Recover the context");
    Ok(())
}

pub fn locate_dylib_base(pid: i32, so_name: &str) -> Option<u64> {
    let maps_path = format!("/proc/{}/maps", pid);
    let maps = std::fs::read_to_string(maps_path).ok()?;
    for line in maps.lines() {
        if line.contains(so_name) && line.contains("r-xp") {
            let addr = line.split("-").next()?;
            dbg!(addr);
            return u64::from_str_radix(addr, 16).ok();
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
