use serde::{Deserialize, Serialize};

use super::{EffectId, RouteId};

const TELEGRAM_TEXT_UNITS: usize = 3900;
const SIGNAL_TEXT_CHARS: usize = 4000;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelKind {
    Telegram,
    Signal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChannelBinding {
    Telegram { topic_id: i64 },
    Signal { group_id: String },
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
    pub state: OutboxState,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub external_receipt: Option<String>,
}

pub fn chunk_outbox(mut item: OutboxItem) -> Vec<OutboxItem> {
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
            state: OutboxState::Pending,
            attempts: 0,
            last_error: None,
            external_receipt: None,
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
}
