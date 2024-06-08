use std::path::PathBuf;

use tokio::{io::AsyncReadExt, net::UnixListener};

use crate::error::AutoGMemError;
use auto_gmem_ipc::Message;

pub struct Runtime {
    control_path: PathBuf,
}

impl Runtime {
    pub fn new() -> Self {
        Self {
            control_path: PathBuf::from("/tmp/auto_gmem.sock"),
        }
    }

    pub fn start(self) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let r: std::io::Result<()> = rt.block_on(async move {
            let controller = UnixListener::bind(self.control_path)?;
            loop {
                let (stream, _) = controller.accept().await?;
                tokio::spawn(async move {
                    let _ = Self::serve_conn(stream).await;
                });
            }
        });

        if let Err(e) = r {
            eprintln!("Error: {}", e);
        }
    }

    async fn serve_conn(mut stream: tokio::net::UnixStream) -> Result<(), AutoGMemError> {
        let mut length_buf = [0u8; 4];
        let mut peer_pid = None;

        while stream.read_exact(&mut length_buf).await.is_ok() {
            // read entire message
            let length = u32::from_le_bytes(length_buf);
            let mut buf = vec![0u8; length as usize];
            stream.read_exact(&mut buf).await?;
            let message = bincode::deserialize(&buf)?;

            // make sure the peer process has registered itself
            if peer_pid.is_none() && !matches!(message, Message::ClientHello(_)) {
                return Err(AutoGMemError::InvalidMessage);
            }
            match message {
                Message::ClientHello(hello) => {
                    peer_pid = Some(hello.pid);
                    println!("ClientHello: {:?}", hello);
                }
                Message::UvmFd(fd) => {
                    println!("UvmFd: {:?}", fd);
                }
            }
        }
        Ok(())
    }
}
