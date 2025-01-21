use std::future::Future;

use tokio::sync::mpsc;

pub struct CallFuture<R> {
    rx: mpsc::Receiver<R>,
}

impl<R> Future for CallFuture<R> {
    type Output = Option<R>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<R>> {
        self.rx.poll_recv(cx)
    }
}

pub struct CallParameter<P, R> {
    pub param: P,
    pub ret_tx: mpsc::Sender<R>,
}

impl<P, R> CallParameter<P, R> {
    pub fn new(param: P) -> (Self, CallFuture<R>) {
        let (tx, rx) = mpsc::channel(1);
        let param = Self { param, ret_tx: tx };
        let future = CallFuture { rx };
        (param, future)
    }
}

pub(crate) fn pretty_size(size_in_bytes: u64) -> String {
    let units = ["B", "KB", "MB", "GB"];
    let mut size = size_in_bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < units.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    format!("{:.2} {}", size, units[unit])
}
