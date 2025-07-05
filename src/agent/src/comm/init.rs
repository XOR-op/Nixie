use std::sync::OnceLock;
use tokio::net::UnixStream;

use crate::comm::controller::Controller;

use colored::Colorize;
use futures::StreamExt;

use nihil_common::rpc::{rpc_multiplex_twoway, DaemonClient, Sidecar};
use nihil_common::{Handshake, HandshakeResponse};
use tarpc::{
    server::{BaseChannel, Channel},
    tokio_util::codec::LengthDelimitedCodec,
};

use crate::comm::msg::A2SMessage;
use crate::{schedule, GENERIC_DATA};

use crate::GenericData;

use super::communication::SidecarServer;

pub(crate) static COMM: OnceLock<Option<flume::Sender<A2SMessage>>> = OnceLock::new();

fn init_generic_data_inner() -> (GenericData, String) {
    // create ptr mapping
    let uuid = uuid::Uuid::new_v4();
    let shm_path = format!(
        "/nihilphase_ipc-{}-{}.shm",
        std::process::id(),
        uuid.to_string().split_at(8).0
    );
    let result = GenericData::new(&shm_path);
    (result, shm_path)
}

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

// this function should be called in a separate thread
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

pub(super) fn init_comm() -> Option<flume::Sender<A2SMessage>> {
    match init_comm_inner() {
        Ok(chan) => {
            let (data, shm_path) = init_generic_data_inner();
            if GENERIC_DATA.set(data).is_err() {
                panic!("Failed to set GENERIC_DATA, it should only be set once");
            }
            let cuda_visible_devices = std::env::var("CUDA_VISIBLE_DEVICES").unwrap_or_default();
            if chan
                .send(A2SMessage::Handshake(Handshake {
                    pid: std::process::id() as i32,
                    shm_path,
                    visible_devices: cuda_visible_devices,
                }))
                .is_err()
            {
                eprintln!("Error at {}:{}: failed to send", file!(), line!());
            }
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

pub(crate) fn init_comm_entrypoint() {
    COMM.get_or_init(init_comm);
}

pub(super) fn init_buffer_by_handshake_resp(resp: HandshakeResponse) {}
