use std::{
    io::Write,
    os::unix::net::UnixStream,
    sync::{Mutex, OnceLock},
};

use colored::Colorize;
use nihilipc::{C2SMessage, ShmPath, UvmFileDescriptor};

static COMM: OnceLock<Option<Mutex<UnixStream>>> = OnceLock::new();

fn init_comm_inner() -> std::io::Result<UnixStream> {
    let mut comm = UnixStream::connect("/tmp/nihilphase.sock")?;
    let pid = std::process::id();
    let message = C2SMessage::ClientHello(nihilipc::ClientHello { pid: pid as i32 });
    comm.write_all(&construct_message(message))?;
    Ok(comm)
}

fn init_comm() -> Option<Mutex<UnixStream>> {
    match init_comm_inner() {
        Ok(comm) => Some(Mutex::new(comm)),
        Err(e) => {
            eprintln!(
                "{} {}: {}",
                "[libcuda_hook]".bold(),
                "Failed to connect to Nihilphase daemon".red(),
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
    let message = C2SMessage::UvmFd(UvmFileDescriptor { fd });
    if comm.write_all(&construct_message(message)).is_err() {
        eprintln!("Failed to send UvmFd message to Nihilphase Daemon")
    }
}

pub(crate) fn nofity_shm(path: String) {
    let Some(lock) = COMM.get_or_init(|| init_comm()) else {
        return;
    };
    let mut comm = lock.lock().unwrap();
    let message = C2SMessage::ShmPath(ShmPath { path });
    if comm.write_all(&construct_message(message)).is_err() {
        eprintln!("Failed to send ShmPath message to Nihilphase Daemon")
    }
}

fn construct_message(message: C2SMessage) -> Vec<u8> {
    let buf = bincode::serialize(&message).unwrap();
    let length = buf.len() as u32;
    let length_buf = length.to_le_bytes();
    let mut coalesced_buf = Vec::with_capacity(4 + buf.len());
    coalesced_buf.extend_from_slice(&length_buf);
    coalesced_buf.extend_from_slice(&buf);
    coalesced_buf
}

pub(crate) fn try_duplicate_comm() -> Option<UnixStream> {
    let Some(lock) = COMM.get_or_init(|| init_comm()) else {
        return None;
    };
    lock.lock().unwrap().try_clone().ok()
}
