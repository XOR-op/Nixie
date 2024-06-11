use std::{
    io::Write,
    os::unix::net::UnixStream,
    sync::{Mutex, OnceLock},
};

use auto_gmem_ipc::{Message, UvmFileDescriptor};
use colored::Colorize;

static COMM: OnceLock<Option<Mutex<UnixStream>>> = OnceLock::new();

fn init_comm_inner() -> std::io::Result<UnixStream> {
    let mut comm = UnixStream::connect("/tmp/auto_gmem.sock")?;
    let pid = std::process::id();
    let message = Message::ClientHello(auto_gmem_ipc::ClientHello { pid: pid as i32 });
    let buf = bincode::serialize(&message).unwrap();
    let length = buf.len() as u32;
    let length_buf = length.to_le_bytes();
    comm.write_all(&length_buf)?;
    comm.write_all(&buf)?;
    Ok(comm)
}

fn init_comm() -> Option<Mutex<UnixStream>> {
    match init_comm_inner() {
        Ok(comm) => Some(Mutex::new(comm)),
        Err(e) => {
            eprintln!(
                "{} {}: {}",
                "[libcuda_hook]".bold(),
                "Failed to connect to AutoGMem daemon".red(),
                e
            );
            None
        }
    }
}

pub(crate) fn notify_fd(fd: i32) {
    let Some(lock) = COMM.get_or_init(|| init_comm()) else {
        return;
    };
    let mut comm = lock.lock().unwrap();
    let message = Message::UvmFd(UvmFileDescriptor { fd });
    let buf = bincode::serialize(&message).unwrap();
    let length = buf.len() as u32;
    let length_buf = length.to_le_bytes();
    let mut coalesced_buf = Vec::with_capacity(4 + buf.len());
    coalesced_buf.extend_from_slice(&length_buf);
    coalesced_buf.extend_from_slice(&buf);
    if comm.write_all(&coalesced_buf).is_err() {
        eprintln!("Failed to send UvmFd message to AutoGMem Daemon")
    }
}
