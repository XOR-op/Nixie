use std::collections::HashMap;
use std::sync::Arc;

use nihil_common::GlobalDeviceId;
use tokio::sync::Notify;
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

#[derive(Clone)]
pub(super) struct RequestForSpaceTx {
    inner: mpsc::UnboundedSender<AllocationCount>,
    prio_channel: mpsc::UnboundedSender<AllocationCount>,
}

impl RequestForSpaceTx {
    pub fn request(&self, n_count: AllocationCount) {
        if self.inner.send(n_count).is_err() {
            tracing::warn!("Failed to request shm space");
        }
    }

    pub fn prio_request(&self, n_count: AllocationCount) {
        if self.prio_channel.send(n_count).is_err() {
            tracing::warn!("Failed to request shm space with priority");
        }
    }
}

pub(super) struct RequestForSpaceRx {
    inner: mpsc::UnboundedReceiver<AllocationCount>,
    prio_channel: mpsc::UnboundedReceiver<AllocationCount>,
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
}

pub(super) fn create_request_for_space_channel() -> (RequestForSpaceTx, RequestForSpaceRx) {
    let (tx, rx) = mpsc::unbounded_channel();
    let (prio_tx, prio_rx) = mpsc::unbounded_channel();
    (
        RequestForSpaceTx {
            inner: tx,
            prio_channel: prio_tx,
        },
        RequestForSpaceRx {
            inner: rx,
            prio_channel: prio_rx,
        },
    )
}

pub(super) struct ShmCoordinator {
    inner: std::sync::Mutex<ShmCoordinatorInner>,
    shm_mgr: Arc<ShmBufferManager>,
    req_for_shm: RequestForSpaceTx,
    resp_for_gpu: Arc<Notify>,
    resp_for_backend: Arc<Notify>,
}

struct ShmCoordinatorInner {
    gpu_to_shm_pending: u32,
    backend_to_shm_pending: u32,
}

impl ShmCoordinator {
    pub fn new(
        shm_mgr: Arc<ShmBufferManager>,
        req_for_shm: RequestForSpaceTx,
        resp_for_gpu: Arc<Notify>,
        resp_for_backend: Arc<Notify>,
    ) -> Self {
        Self {
            inner: std::sync::Mutex::new(ShmCoordinatorInner {
                gpu_to_shm_pending: 0,
                backend_to_shm_pending: 0,
            }),
            shm_mgr,
            req_for_shm,
            resp_for_gpu,
            resp_for_backend,
        }
    }

    pub async fn reserve_from_backend(&self, buf_id: &BufferId) -> Arc<[ShmBlock]> {
        let mut im_pending = false;
        #[allow(unused_assignments)]
        let mut should_wait = false;
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
                    if im_pending {
                        tracing::warn!(
                            "Backend pending reservation but still no space {:?}",
                            buf_id
                        );
                    } else {
                        im_pending = true;
                        inner.backend_to_shm_pending += 1;
                    }
                    self.req_for_shm.request(buf_id.get_allocation_count());
                    should_wait = true;
                } else {
                    should_wait = false
                }
            }
            // wait for notification
            if should_wait {
                self.resp_for_backend.notified().await;
            } else {
                tokio::task::yield_now().await;
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
                    self.req_for_shm.prio_request(buf_id.get_allocation_count());
                    should_wait = true;
                } else {
                    should_wait = false
                }
            }
            // wait for notification
            if should_wait {
                self.resp_for_gpu.notified().await;
            } else {
                tokio::task::yield_now().await;
            }
        }
    }

    pub fn shm_buffer_manager(&self) -> Arc<ShmBufferManager> {
        self.shm_mgr.clone()
    }
}
