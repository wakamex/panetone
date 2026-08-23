use serde::{Deserialize, Serialize};

use super::{EffectId, RouteId};

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
