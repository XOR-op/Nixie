use std::{io::Read, os::unix::net::UnixStream};

use auto_gmem_ipc::S2CMessage;

use crate::snippet::advise_read_mostly_for;

pub(crate) struct Sidecar {
    recv: UnixStream,
}

impl Sidecar {
    pub fn new(stream: UnixStream) -> Self {
        Self { recv: stream }
    }

    pub fn run(mut self) -> std::io::Result<()> {
        let mut len_buf = [0u8; 4];
        let mut buf = [0u8; 4096];
        loop {
            match self.recv.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        return Ok(());
                    } else {
                        return Err(e);
                    }
                }
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            self.recv.read_exact(&mut buf[..len as usize])?;
            let args = bincode::deserialize::<S2CMessage>(&buf[..len])
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            match args {
                S2CMessage::SetReadDup(args) => {
                    advise_read_mostly_for(args.value, args.addr, args.len, args.device);
                }
            }
        }
    }
}
