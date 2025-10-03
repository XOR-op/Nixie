use std::collections::HashMap;

use nihil_common::GlobalDeviceId;
use tokio::sync::mpsc;

use super::BufferLocation;

use super::BufferId;

pub(super) fn create_data_ready_channel<T>(
    device_ids: T,
    shm_to_hybrid: HashMap<BufferId, BufferLocation>,
) -> (
    InDataReadyTx,
    HashMap<GlobalDeviceId, (InDataReadyRx, OutDataReadyTx)>,
    OutDataReadyRx,
)
where
    T: Iterator<Item = GlobalDeviceId>,
{
    let device_ids = device_ids.collect::<Vec<_>>();
    let (in_tx, mut in_rx_map) = create_in_data_ready_channel(&device_ids);
    let (mut out_tx_map, out_rx) = create_out_data_ready_channel(&device_ids, shm_to_hybrid);
    let mut rx_map = HashMap::new();
    for device_id in device_ids {
        let in_rx = in_rx_map.remove(&device_id).unwrap();
        let out_tx = out_tx_map.remove(&device_id).unwrap().clone();
        rx_map.insert(device_id, (in_rx, out_tx));
    }
    (in_tx, rx_map, out_rx)
}

pub(super) fn create_in_data_ready_channel(
    device_ids: &[GlobalDeviceId],
) -> (InDataReadyTx, HashMap<GlobalDeviceId, InDataReadyRx>) {
    let mut inners = HashMap::new();
    let mut rx_map = HashMap::new();
    for device_id in device_ids {
        let (tx, rx) = mpsc::unbounded_channel();
        inners.insert(*device_id, tx);
        rx_map.insert(*device_id, InDataReadyRx { inner: rx });
    }
    (InDataReadyTx { inners }, rx_map)
}

#[derive(Clone)]
pub(super) struct InDataReadyTx {
    inners: HashMap<GlobalDeviceId, mpsc::UnboundedSender<(BufferId, BufferLocation)>>,
}

impl InDataReadyTx {
    pub fn send(
        &self,
        buffer_id: BufferId,
        location: BufferLocation,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<(BufferId, BufferLocation)>> {
        if let Some(chan) = self.inners.get(&buffer_id.device_id) {
            chan.send((buffer_id, location))
        } else {
            panic!(
                "Invalid device id {} for device size = {}",
                buffer_id.device_id.0,
                self.inners.len()
            );
        }
    }
}

pub(super) struct InDataReadyRx {
    inner: mpsc::UnboundedReceiver<(BufferId, BufferLocation)>,
}

impl InDataReadyRx {
    pub async fn recv(&mut self) -> Option<(BufferId, BufferLocation)> {
        self.inner.recv().await
    }

    pub async fn try_recv(&mut self) -> Option<(BufferId, BufferLocation)> {
        self.inner.try_recv().ok()
    }
}

pub(super) fn create_out_data_ready_channel(
    device_ids: &[GlobalDeviceId],
    shm_to_hybrid: HashMap<BufferId, BufferLocation>,
) -> (HashMap<GlobalDeviceId, OutDataReadyTx>, OutDataReadyRx) {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut tx_map = HashMap::new();
    for device_id in device_ids {
        tx_map.insert(
            *device_id,
            OutDataReadyTx {
                inner: tx.clone(),
                device_id: *device_id,
            },
        );
    }
    (
        tx_map,
        OutDataReadyRx {
            inner: rx,
            move_to: shm_to_hybrid,
        },
    )
}

#[derive(Clone)]
pub(super) struct OutDataReadyTx {
    device_id: GlobalDeviceId,
    inner: mpsc::UnboundedSender<BufferId>,
}

impl OutDataReadyTx {
    pub fn send(
        &self,
        buffer_id: BufferId,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<BufferId>> {
        assert_eq!(self.device_id, buffer_id.device_id);
        self.inner.send(buffer_id)
    }
}

pub(super) struct OutDataReadyRx {
    inner: mpsc::UnboundedReceiver<BufferId>,
    move_to: HashMap<BufferId, BufferLocation>,
}

impl OutDataReadyRx {
    pub async fn recv(&mut self) -> Option<(BufferId, BufferLocation)> {
        while let Some(buf_id) = self.inner.recv().await {
            if let Some(location) = self.move_to.remove(&buf_id) {
                return Some((buf_id, location));
            }
        }
        None
    }
}

#[derive(Clone)]
pub(super) struct RequestForSpaceTx {
    inner: mpsc::UnboundedSender<()>,
}

impl RequestForSpaceTx {
    pub fn request(&self, n: usize) {
        for _ in 0..n {
            if let Err(_) = self.inner.send(()) {
                tracing::warn!("Failed to request shm space");
            }
        }
    }
}
pub(super) struct RequestForSpaceRx {
    inner: mpsc::UnboundedReceiver<()>,
}

impl RequestForSpaceRx {
    pub async fn listen(&mut self) -> Option<()> {
        self.inner.recv().await
    }
}

pub(super) fn create_request_for_space_channel() -> (RequestForSpaceTx, RequestForSpaceRx) {
    let (tx, rx) = mpsc::unbounded_channel();
    (
        RequestForSpaceTx { inner: tx },
        RequestForSpaceRx { inner: rx },
    )
}
