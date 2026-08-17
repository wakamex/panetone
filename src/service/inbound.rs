use thiserror::Error;

use crate::channels::{
    ChannelDeliveryError, InboundBatch, InboundMessage, SignalSubscriber, SlackSocket,
};
use crate::store::{InboxItem, StoreError, StoreHandle};

#[derive(Debug, Error)]
pub enum InboundIngestError {
    #[error(transparent)]
    Channel(#[from] ChannelDeliveryError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

#[derive(Clone)]
pub struct InboundIngestor {
    store: StoreHandle,
}

impl InboundIngestor {
    pub fn new(store: StoreHandle) -> Self {
        Self { store }
    }

    pub async fn persist(
        &self,
        message: InboundMessage,
        now_ms: i64,
    ) -> Result<bool, InboundIngestError> {
        let item = InboxItem {
            id: message.effect_id(),
            channel: message.channel,
            external_id: message.external_id,
            destination: message.destination,
            sender: message.sender,
            body: message.body,
            state: "pending".into(),
            created_at_ms: now_ms,
        };
        Ok(self.store.accept_inbox(item).await?)
    }

    pub async fn persist_telegram_batch(
        &self,
        cursor_key: &str,
        batch: InboundBatch,
        now_ms: i64,
    ) -> Result<usize, InboundIngestError> {
        let mut inserted = 0;
        for message in batch.messages {
            inserted += usize::from(self.persist(message, now_ms).await?);
        }
        self.store
            .set_metadata(cursor_key.into(), batch.next_offset.to_string())
            .await?;
        Ok(inserted)
    }

    pub async fn ingest_slack_once(
        &self,
        socket: &mut SlackSocket,
        now_ms: i64,
    ) -> Result<bool, InboundIngestError> {
        let envelope = socket.next().await?;
        let inserted = match envelope.message {
            Some(message) => self.persist(message, now_ms).await?,
            None => false,
        };
        socket.acknowledge(&envelope.envelope_id).await?;
        Ok(inserted)
    }

    pub async fn ingest_signal_once(
        &self,
        subscriber: &mut SignalSubscriber,
        now_ms: i64,
    ) -> Result<bool, InboundIngestError> {
        let message = subscriber.next().await?;
        self.persist(message, now_ms).await
    }
}
