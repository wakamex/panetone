use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};

use super::{EffectId, RouteId};

const TELEGRAM_TEXT_UNITS: usize = 3900;
const SIGNAL_TEXT_CHARS: usize = 4000;
// Telegram allows 1,024 caption characters; room is left for RESEND_PREFIX.
const ATTACHMENT_CAPTION_CHARS: usize = 1024 - RESEND_PREFIX.len();
pub const MAX_CHANNEL_ATTACHMENT_BYTES: u64 = 10 * 1024 * 1024;
pub const MAX_CHANNEL_ATTACHMENTS: usize = 10;
pub const MAX_CHANNEL_ATTACHMENT_TOTAL_BYTES: u64 = 50 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelKind {
    Telegram,
    Signal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChannelBinding {
    Telegram {
        topic_id: i64,
    },
    Signal {
        group_id: String,
        #[serde(default)]
        allow_members: bool,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxState {
    Pending,
    Delivering,
    Delivered,
    Failed,
    Indeterminate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChannelAttachment {
    pub file_name: String,
    pub media_type: String,
    pub size: u64,
    pub sha256: String,
    pub data_base64: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OutboxAction {
    pub id: String,
    pub label: String,
}

impl ChannelAttachment {
    pub fn bytes(&self) -> Result<Vec<u8>, base64::DecodeError> {
        BASE64.decode(&self.data_base64)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OutboxItem {
    pub id: EffectId,
    #[serde(default)]
    pub route_id: Option<RouteId>,
    #[serde(default)]
    pub sender_harness: Option<String>,
    #[serde(default)]
    pub source_agent: Option<super::AgentBinding>,
    pub kind: ChannelKind,
    pub destination: String,
    pub body: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<ChannelAttachment>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<OutboxAction>,
    pub state: OutboxState,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub external_receipt: Option<String>,
    /// Attempts that failed after the channel may already have posted the
    /// message. A later attempt is labeled as a resend.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub uncertain_attempts: u32,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

pub const RESEND_PREFIX: &str = "[resent] ";

impl OutboxItem {
    /// The body to post on this attempt: labeled when an earlier attempt may
    /// already have appeared, so a duplicate is recognizable.
    pub fn attempt_body(&self) -> String {
        if self.uncertain_attempts == 0 {
            self.body.clone()
        } else {
            format!("{RESEND_PREFIX}{}", self.body)
        }
    }
}

pub fn chunk_outbox(mut item: OutboxItem) -> Vec<OutboxItem> {
    if !item.attachments.is_empty() {
        let chunks = split_by_chars(&item.body, ATTACHMENT_CAPTION_CHARS);
        if chunks.len() == 1 {
            return vec![item];
        }
        let parent = item.id;
        return chunks
            .into_iter()
            .enumerate()
            .map(|(index, body)| {
                item.id = EffectId::chunk(parent, index);
                item.body = body;
                if index > 0 {
                    item.attachments.clear();
                }
                item.clone()
            })
            .collect();
    }
    if !item.actions.is_empty() {
        let chunks = split_text(item.kind, &item.body);
        if chunks.len() == 1 {
            return vec![item];
        }
        let parent = item.id;
        return chunks
            .into_iter()
            .enumerate()
            .map(|(index, body)| {
                item.id = EffectId::chunk(parent, index);
                item.body = body;
                if index > 0 {
                    item.actions.clear();
                }
                item.clone()
            })
            .collect();
    }
    let chunks = split_text(item.kind, &item.body);
    if chunks.len() == 1 {
        return vec![item];
    }
    let parent = item.id;
    chunks
        .into_iter()
        .enumerate()
        .map(|(index, body)| {
            item.id = EffectId::chunk(parent, index);
            item.body = body;
            item.clone()
        })
        .collect()
}

fn split_by_chars(text: &str, limit: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut chunk = String::new();
    let mut count = 0;
    for character in text.chars() {
        if !chunk.is_empty() && count == limit {
            chunks.push(std::mem::take(&mut chunk));
            count = 0;
        }
        chunk.push(character);
        count += 1;
    }
    if !chunk.is_empty() || chunks.is_empty() {
        chunks.push(chunk);
    }
    chunks
}

fn split_text(kind: ChannelKind, text: &str) -> Vec<String> {
    let limit = match kind {
        ChannelKind::Telegram => TELEGRAM_TEXT_UNITS,
        ChannelKind::Signal => SIGNAL_TEXT_CHARS,
    };
    let mut chunks = Vec::new();
    let mut chunk = String::new();
    let mut units = 0;
    for character in text.chars() {
        let character_units = match kind {
            ChannelKind::Telegram => character.len_utf16(),
            ChannelKind::Signal => 1,
        };
        if !chunk.is_empty() && units + character_units > limit {
            chunks.push(std::mem::take(&mut chunk));
            units = 0;
        }
        chunk.push(character);
        units += character_units;
    }
    if !chunk.is_empty() || chunks.is_empty() {
        chunks.push(chunk);
    }
    chunks
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn item(kind: ChannelKind, body: String) -> OutboxItem {
        OutboxItem {
            id: EffectId::new(Uuid::from_u128(1)),
            route_id: None,
            sender_harness: None,
            source_agent: None,
            kind,
            destination: "destination".into(),
            body,
            attachments: Vec::new(),
            actions: Vec::new(),
            state: OutboxState::Pending,
            attempts: 0,
            last_error: None,
            external_receipt: None,
            uncertain_attempts: 0,
        }
    }

    #[test]
    fn telegram_chunks_count_utf16_units_and_keep_stable_ids() {
        let body = "x".repeat(3899) + "😀y";
        let chunks = chunk_outbox(item(ChannelKind::Telegram, body.clone()));
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].body.encode_utf16().count(), 3899);
        assert_eq!(chunks[1].body, "😀y");
        assert_eq!(
            chunks.iter().map(|chunk| chunk.id).collect::<Vec<_>>(),
            chunk_outbox(item(ChannelKind::Telegram, body))
                .iter()
                .map(|chunk| chunk.id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn short_output_keeps_its_original_effect_id() {
        let original = item(ChannelKind::Signal, "hello".into());
        assert_eq!(chunk_outbox(original.clone()), vec![original]);
    }

    #[test]
    fn long_attachment_caption_sends_the_file_once() {
        let mut original = item(ChannelKind::Signal, "x".repeat(1025));
        original.attachments.push(ChannelAttachment {
            file_name: "scene.png".into(),
            media_type: "image/png".into(),
            size: 1,
            sha256: "hash".into(),
            data_base64: "eA==".into(),
        });
        let mut chunks = chunk_outbox(original);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].attachments.len(), 1);
        assert_eq!(chunks[1].body, "x".repeat(10));
        assert!(chunks[1].attachments.is_empty());
        // A resent caption still fits Telegram's 1,024-character limit.
        chunks[0].uncertain_attempts = 1;
        assert_eq!(chunks[0].attempt_body().chars().count(), 1024);
    }
}
