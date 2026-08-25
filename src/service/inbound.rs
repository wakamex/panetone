use thiserror::Error;

use crate::channels::{ChannelDeliveryError, InboundBatch, InboundMessage, SignalSubscriber};
use crate::domain::ChannelBinding;
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
            sender_id: message.sender_id,
            sender: message.sender,
            reply_to_external_id: message.reply_to_external_id,
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

    pub async fn persist_signal(
        &self,
        mut message: InboundMessage,
        owner: &str,
        now_ms: i64,
    ) -> Result<bool, InboundIngestError> {
        let routes = self.store.list_routes().await?;
        let matching = routes
            .iter()
            .filter(|route| {
                route.channels.iter().any(|binding| match binding {
                    ChannelBinding::Signal { group_id } => {
                        group_id.trim_end_matches('=') == message.destination.trim_end_matches('=')
                    }
                    ChannelBinding::Telegram { .. } => false,
                })
            })
            .collect::<Vec<_>>();
        let [route] = matching.as_slice() else {
            return Ok(false);
        };
        let is_debate = route.title.eq_ignore_ascii_case("debate");
        if message.sender_id.as_deref() != Some(owner) && !is_debate {
            return Ok(false);
        }
        if is_debate {
            let sender = message
                .sender
                .as_deref()
                .and_then(|name| name.split_whitespace().next())
                .filter(|name| !name.is_empty())
                .unwrap_or("Unknown");
            message.body = format!("{sender} says: {}", message.body);
        }
        self.persist(message, now_ms).await
    }

    pub async fn ingest_signal_once(
        &self,
        subscriber: &mut SignalSubscriber,
        owner: &str,
        now_ms: i64,
    ) -> Result<bool, InboundIngestError> {
        let message = subscriber.next().await?;
        self.persist_signal(message, owner, now_ms).await
    }
}
