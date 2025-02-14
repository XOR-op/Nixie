use std::sync::OnceLock;

use colored::Colorize;
use futures::StreamExt;
use nihilipc::{
    rpc::{rpc_multiplex_twoway, DaemonClient, Sidecar},
    ActivityUpdate, AttrArgs, Handshake, InitInfo, PrefetchArgs, S2AMessage,
};
use tarpc::{
    context::Context,
    server::{BaseChannel, Channel},
    tokio_util::codec::LengthDelimitedCodec,
};
use tokio::net::UnixStream;

use super::{controller::Controller, msg::A2SMessage};
use crate::schedule;

macro_rules! chan_send {
    ($result:expr) => {
        if let Err(e) = $result {
            eprintln!("Error at {}:{}: {:?}", file!(), line!(), e);
        }
    };
}

static COMM: OnceLock<Option<flume::Sender<A2SMessage>>> = OnceLock::new();

fn init_comm_inner() -> std::io::Result<flume::Sender<A2SMessage>> {
    let (tx, rx) = flume::unbounded();
    let conn = std::os::unix::net::UnixStream::connect("/tmp/nihilphase.sock")?;
    conn.set_nonblocking(true)?;
    std::thread::spawn(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                match tokio::net::UnixStream::from_std(conn) {
                    Ok(conn) => create_comm(conn, rx).await,
                    Err(e) => eprintln!(
                        "Tokio UnixStream failed to initialize at {}:{}: {:?}",
                        file!(),
                        line!(),
                        e
                    ),
                }
            })
    });
    Ok(tx)
}

async fn create_comm(conn: UnixStream, p2s_rx: flume::Receiver<A2SMessage>) {
    let mut codec_builder = LengthDelimitedCodec::builder();
    codec_builder.max_frame_length(64 * 1024 * 1024);
    let framed = codec_builder.new_framed(conn);
    let transport = tarpc::serde_transport::new(framed, tokio_serde::formats::Cbor::default());
    let (server_ret, client_ret, inbound_fut, outbound_fut) = rpc_multiplex_twoway(transport);
    tokio::spawn(inbound_fut);
    tokio::spawn(outbound_fut);
    let client = DaemonClient::new(Default::default(), client_ret).spawn();
    let (d2s_tx, d2s_rx) = flume::unbounded();
    let server = SidecarServer { sender: d2s_tx };
    tokio::spawn(
        BaseChannel::with_defaults(server_ret)
            .execute(server.serve())
            .for_each(|response| async move {
                tokio::spawn(response);
            }),
    );
    let sidecar = Controller::new(p2s_rx, d2s_rx, client, &schedule::SCHED_CTL);
    sidecar.run().await
}

fn init_comm() -> Option<flume::Sender<A2SMessage>> {
    match init_comm_inner() {
        Ok(chan) => {
            chan_send!(chan.send(A2SMessage::Handshake(Handshake {
                pid: std::process::id() as i32,
            })));
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

pub(crate) fn notify_init_info(fd: i32, shm_path: String, visible_devices: String) {
    let Some(chan) = COMM.get_or_init(|| init_comm()) else {
        return;
    };
    chan_send!(chan.send(A2SMessage::InitInfo(InitInfo {
        fd,
        shm_path,
        visible_devices
    })));
}

pub(crate) fn notify_activity() {
    let Some(chan) = COMM.get_or_init(|| init_comm()) else {
        return;
    };
    chan_send!(chan.send(A2SMessage::NofityActivity(ActivityUpdate {})));
}

#[derive(Clone)]
pub(crate) struct SidecarServer {
    sender: flume::Sender<S2AMessage>,
}

impl nihilipc::rpc::Sidecar for SidecarServer {
    async fn set_attr(self, _context: Context, params: AttrArgs) -> () {
        chan_send!(self.sender.send(S2AMessage::SetAttr(params)));
    }

    async fn prefetch(self, _context: Context, params: PrefetchArgs) -> () {
        chan_send!(self.sender.send(S2AMessage::Prefetch(params)));
    }
}
