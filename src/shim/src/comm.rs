use std::sync::{Mutex, OnceLock};

use colored::Colorize;
use futures::StreamExt;
use nihilipc::{
    rpc::{rpc_multiplex_twoway, DaemonClient, Sidecar},
    InitClient, SetReadDupArgs, ShmPath, UvmFd,
};
use tarpc::{
    context::Context,
    server::{BaseChannel, Channel},
    tokio_util::codec::LengthDelimitedCodec,
};
use tokio::net::UnixStream;

use crate::msg::C2SMessage;

static COMM: OnceLock<Option<flume::Sender<C2SMessage>>> = OnceLock::new();

fn init_comm_inner() -> std::io::Result<flume::Sender<C2SMessage>> {
    let (tx, rx) = flume::unbounded();
    let conn = std::os::unix::net::UnixStream::connect("/tmp/nihilphase.sock")?;
    conn.set_nonblocking(true)?;
    let conn = tokio::net::UnixStream::from_std(conn)?;
    std::thread::spawn(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(create_comm(conn, rx))
    });
    Ok(tx)
}

async fn create_comm(
    conn: UnixStream,
    rx: flume::Receiver<C2SMessage>,
) -> std::io::Result<DaemonClient> {
    let pid = std::process::id() as i32;
    let mut codec_builder = LengthDelimitedCodec::builder();
    codec_builder.max_frame_length(64 * 1024 * 1024);
    let framed = codec_builder.new_framed(conn);
    let transport = tarpc::serde_transport::new(framed, tokio_serde::formats::Cbor::default());
    let (server_ret, client_ret, inbound_fut, outbound_fut) = rpc_multiplex_twoway(transport);
    tokio::spawn(inbound_fut);
    tokio::spawn(outbound_fut);
    let client = DaemonClient::new(Default::default(), client_ret).spawn();
    let server = SidecarServer {};
    tokio::spawn(
        BaseChannel::with_defaults(server_ret)
            .execute(server.serve())
            .for_each(|response| async move {
                tokio::spawn(response);
            }),
    );
    todo!("serve sidecar");
    Ok(client)
}

fn init_comm() -> Option<flume::Sender<C2SMessage>> {
    match init_comm_inner() {
        Ok(chan) => {
            chan.send(C2SMessage::InitClient(InitClient {
                pid: std::process::id() as i32,
            }));
            Some(chan)
        }
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
    let Some(chan) = COMM.get_or_init(|| init_comm()) else {
        return;
    };
    chan.send(C2SMessage::UvmFd(UvmFd { fd }));
}

pub(crate) fn nofity_shm(path: String) {
    let Some(chan) = COMM.get_or_init(|| init_comm()) else {
        return;
    };
    chan.send(C2SMessage::ShmPath(ShmPath { path }));
}

#[derive(Clone)]
pub(crate) struct SidecarServer {}

impl nihilipc::rpc::Sidecar for SidecarServer {
    async fn set_read_dup(self, context: Context, params: SetReadDupArgs) -> () {
        todo!()
    }
}
