use std::{
    io::Write,
    os::unix::net::UnixStream,
    sync::{Mutex, OnceLock},
};

use auto_gmem_ipc::{Message, UvmFileDescriptor};

static COMM: OnceLock<Mutex<UnixStream>> = OnceLock::new();

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

fn init_comm() -> Mutex<UnixStream> {
    Mutex::new(init_comm_inner().expect("Failed to connect to AutoGMem Daemon"))
}

pub(crate) fn notify_fd(fd: i32) {
    let mut comm = COMM.get_or_init(|| init_comm()).lock().unwrap();
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
