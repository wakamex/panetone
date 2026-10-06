use thiserror::Error;

use crate::archive::{ArchiveEntry, ChatArchives, split_signal_attachments};
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
    archives: ChatArchives,
}

impl InboundIngestor {
    pub fn new(store: StoreHandle) -> Self {
        Self {
            store,
            archives: ChatArchives::default(),
        }
    }

    pub fn with_chat_archives(mut self, archives: ChatArchives) -> Self {
        self.archives = archives;
        self
    }

    pub async fn persist(
        &self,
        mut message: InboundMessage,
        now_ms: i64,
    ) -> Result<bool, InboundIngestError> {
        if let Some(quoted) = message.quoted.take() {
            let author = if quoted.own {
                Some("you".to_owned())
            } else {
                match quoted.author_name {
                    Some(name) => Some(name),
                    None => {
                        self.store
                            .latest_sender_name(message.channel, quoted.author_ids)
                            .await?
                    }
                }
            };
            message.body = format!(
                "{}\n{}",
                reply_context(author.as_deref().unwrap_or("someone"), &quoted.text),
                message.body
            );
        }
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
            receipt: None,
            steering_acknowledged: None,
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
            .filter_map(|route| {
                route.channels.iter().find_map(|binding| match binding {
                    ChannelBinding::Signal {
                        group_id,
                        allow_members,
                    } if group_id.trim_end_matches('=')
                        == message.destination.trim_end_matches('=') =>
                    {
                        Some((route, *allow_members))
                    }
                    _ => None,
                })
            })
            .collect::<Vec<_>>();
        let [(route, allow_members)] = matching.as_slice() else {
            return Ok(false);
        };
        if message.sender_id.as_deref() != Some(owner) && !allow_members {
            return Ok(false);
        }
        let route_title = route.title.clone();
        let (text, attachments) = split_signal_attachments(&message.body);
        let entry = ArchiveEntry {
            // Signal external IDs are `SENDER:TIMESTAMP_MS`.
            sent_ms: message
                .external_id
                .rsplit(':')
                .next()
                .and_then(|timestamp| timestamp.parse().ok())
                .unwrap_or(now_ms),
            sender: message.sender.clone().unwrap_or_else(|| "Unknown".into()),
            text,
            attachments,
            group: message.destination.clone(),
        };
        if *allow_members {
            let sender = message
                .sender
                .as_deref()
                .and_then(|name| name.split_whitespace().next())
                .filter(|name| !name.is_empty())
                .unwrap_or("Unknown");
            message.body = format!("{sender} says: {}", message.body);
        }
        let inserted = self.persist(message, now_ms).await?;
        // A redelivered message is already archived.
        if inserted {
            self.archives.append(&route_title, &entry);
        }
        Ok(inserted)
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

/// The line that tells the agent which message an inbound message replies to,
/// quoting the first line of the replied-to text.
fn reply_context(author: &str, text: &str) -> String {
    const MAX_CHARS: usize = 100;
    let first_line = text
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("");
    let quoted = match first_line.char_indices().nth(MAX_CHARS) {
        Some((end, _)) => format!("{}…", &first_line[..end]),
        None => first_line.to_owned(),
    };
    format!("[replying to {author}: \"{quoted}\"]")
}
