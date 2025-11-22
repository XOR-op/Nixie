use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nihil_common::GlobalDeviceId;
use tokio::sync::mpsc;

use crate::runtime::migration::AllocationCount;
use crate::runtime::migration::ShmBufferManager;
use crate::runtime::migration::ShmBufferRequest;
use crate::runtime::migration::shm_buffer::ShmBlock;

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
            Ok(())
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

pub(super) struct RequestForSpaceTx {
    inner: mpsc::UnboundedSender<AllocationCount>,
    prio_channel: mpsc::UnboundedSender<AllocationCount>,
    notify_gpu: tokio::sync::Mutex<mpsc::UnboundedReceiver<ShmRequestRxResp>>,
    notify_backend: tokio::sync::Mutex<mpsc::UnboundedReceiver<ShmRequestRxResp>>,
}

impl RequestForSpaceTx {
    pub fn backend_request(&self, n_count: AllocationCount) {
        if self.inner.send(n_count).is_err() {
            tracing::warn!("Failed to request shm space");
        }
    }

    pub fn gpu_request(&self, n_count: AllocationCount) {
        if self.prio_channel.send(n_count).is_err() {
            tracing::warn!("Failed to request shm space with priority");
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ShmRequestRxResp {
    Ready,
    BusyWait,
}

pub(super) struct RequestForSpaceRx {
    inner: mpsc::UnboundedReceiver<AllocationCount>,
    prio_channel: mpsc::UnboundedReceiver<AllocationCount>,
    notify_gpu: mpsc::UnboundedSender<ShmRequestRxResp>,
    notify_backend: mpsc::UnboundedSender<ShmRequestRxResp>,
}

impl RequestForSpaceRx {
    pub async fn listen(&mut self) -> Option<ShmBufferRequest> {
        tokio::select! {
            biased;
            res = self.prio_channel.recv() => res.map(ShmBufferRequest::FromGPU),
            res = self.inner.recv() => res.map(ShmBufferRequest::FromBackend),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed() && self.prio_channel.is_closed()
    }

    pub fn notify_gpu(&self, resp: ShmRequestRxResp) {
        if self.notify_gpu.send(resp).is_err() {
            tracing::warn!("Failed to notify GPU shm request response");
        }
    }

    pub fn notify_backend(&self, resp: ShmRequestRxResp) {
        if self.notify_backend.send(resp).is_err() {
            tracing::warn!("Failed to notify Backend shm request response");
        }
    }
}

pub(super) fn create_request_for_space_channel() -> (RequestForSpaceTx, RequestForSpaceRx) {
    let (tx, rx) = mpsc::unbounded_channel();
    let (prio_tx, prio_rx) = mpsc::unbounded_channel();
    let (notify_gpu_tx, notify_gpu_rx) = mpsc::unbounded_channel();
    let (notify_backend_tx, notify_backend_rx) = mpsc::unbounded_channel();
    (
        RequestForSpaceTx {
            inner: tx,
            prio_channel: prio_tx,
            notify_gpu: tokio::sync::Mutex::new(notify_gpu_rx),
            notify_backend: tokio::sync::Mutex::new(notify_backend_rx),
        },
        RequestForSpaceRx {
            inner: rx,
            prio_channel: prio_rx,
            notify_gpu: notify_gpu_tx,
            notify_backend: notify_backend_tx,
        },
    )
}

pub(super) struct ShmCoordinator {
    inner: std::sync::Mutex<ShmCoordinatorInner>,
    shm_mgr: Arc<ShmBufferManager>,
    req_for_shm: RequestForSpaceTx,
}

struct ShmCoordinatorInner {
    gpu_to_shm_pending: u32,
    backend_to_shm_pending: u32,
}

impl ShmCoordinator {
    pub fn new(shm_mgr: Arc<ShmBufferManager>, req_for_shm: RequestForSpaceTx) -> Self {
        Self {
            inner: std::sync::Mutex::new(ShmCoordinatorInner {
                gpu_to_shm_pending: 0,
                backend_to_shm_pending: 0,
            }),
            shm_mgr,
            req_for_shm,
        }
    }

    pub async fn reserve_from_backend(&self, buf_id: &BufferId) -> Arc<[ShmBlock]> {
        let mut im_pending = false;
        #[allow(unused_assignments)]
        let mut should_wait = false;
        let mut resp_busy_wait = false;
        loop {
            {
                let mut inner = self.inner.lock().unwrap();
                if inner.gpu_to_shm_pending == 0 {
                    if let Some(blks) = self.shm_mgr.try_reserve(buf_id) {
                        if im_pending {
                            assert!(inner.backend_to_shm_pending > 0);
                            inner.backend_to_shm_pending -= 1;
                        }
                        return blks;
                    }
                    if !resp_busy_wait {
                        if im_pending {
                            tracing::warn!(
                                "Backend pending reservation but still no space {:?}",
                                buf_id
                            );
                        } else {
                            im_pending = true;
                            inner.backend_to_shm_pending += 1;
                        }
                        self.req_for_shm
                            .backend_request(buf_id.get_allocation_count());
                        should_wait = true;
                    }
                } else {
                    should_wait = false
                }
            }
            // wait for notification
            if should_wait && !resp_busy_wait {
                match self.req_for_shm.notify_backend.lock().await.recv().await {
                    Some(ShmRequestRxResp::Ready) => {}
                    Some(ShmRequestRxResp::BusyWait) => {
                        resp_busy_wait = true;
                    }
                    None => {
                        tracing::warn!("Backend shm reservation notified but unexpected response");
                        tokio::task::yield_now().await;
                        tokio::time::sleep(Duration::from_micros(500)).await;
                    }
                }
            } else {
                tokio::time::sleep(Duration::from_micros(500)).await;
            }
        }
    }

    pub async fn reserve_from_gpu(&self, buf_id: &BufferId) -> Arc<[ShmBlock]> {
        let mut im_pending = false;
        #[allow(unused_assignments)]
        let mut should_wait = false;
        loop {
            {
                let mut inner = self.inner.lock().unwrap();
                // pending GPU has higher priority
                if inner.backend_to_shm_pending == 0 || im_pending {
                    if let Some(blks) = self.shm_mgr.try_reserve(buf_id) {
                        if im_pending {
                            assert!(inner.gpu_to_shm_pending > 0);
                            inner.gpu_to_shm_pending -= 1;
                        }
                        return blks;
                    }
                    if im_pending {
                        tracing::warn!("GPU pending reservation but still no space, {:?}", buf_id);
                    } else {
                        im_pending = true;
                        inner.gpu_to_shm_pending += 1;
                    }
                    self.req_for_shm.gpu_request(buf_id.get_allocation_count());
                    should_wait = true;
                } else {
                    should_wait = false
                }
            }
            // wait for notification
            if should_wait {
                match self.req_for_shm.notify_gpu.lock().await.recv().await {
                    Some(ShmRequestRxResp::Ready) => {}
                    _ => {
                        tracing::warn!("GPU shm reservation notified but unexpected response");
                        tokio::task::yield_now().await;
                    }
                }
            } else {
                tokio::task::yield_now().await;
            }
        }
    }

    pub fn shm_buffer_manager(&self) -> Arc<ShmBufferManager> {
        self.shm_mgr.clone()
    }
}
