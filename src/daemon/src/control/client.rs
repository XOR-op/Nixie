use tarpc::tokio_util::codec::LengthDelimitedCodec;
use tokio_serde::formats::Cbor;

use crate::error::NihilphaseError;

use super::{ControllableClient, PrefetchMsg, ReadDupMsg};

pub(crate) struct ControlClient {
    client: ControllableClient,
    pid: i32,
}

impl ControlClient {
    pub async fn new(path: &str, pid: i32) -> Result<Self, NihilphaseError> {
        let conn = tokio::net::UnixStream::connect(path).await?;
        let conn = tarpc::serde_transport::new(
            LengthDelimitedCodec::builder().new_framed(conn),
            Cbor::default(),
        );
        let client = ControllableClient::new(Default::default(), conn).spawn();
        Ok(Self { client, pid })
    }

    pub async fn read_dup(&self, size_low: Option<u64>, set: bool) -> Result<(), NihilphaseError> {
        self.client
            .read_dup(
                tarpc::context::current(),
                ReadDupMsg {
                    pid: self.pid,
                    size_low,
                    size_high: None,
                    set,
                },
            )
            .await?;
        Ok(())
    }

    pub async fn prefetch(&self, size_low: Option<u64>) -> Result<(), NihilphaseError> {
        self.client
            .prefetch(
                tarpc::context::current(),
                PrefetchMsg {
                    pid: self.pid,
                    size_low,
                    size_high: None,
                    to_gpu: true,
                },
            )
            .await?;
        Ok(())
    }
}
