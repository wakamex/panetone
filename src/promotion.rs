use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::domain::{AgentBinding, ChannelKind, Route, RouteId};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyRecordKind {
    Control,
    Return,
    Debate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LegacyDecision {
    NoReplay,
    ExternallyVerified,
    MapDebateToSignal {
        route_id: RouteId,
        expected_legacy_destination: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OperatorAction {
    SetDeliveryHold {
        held: bool,
    },
    SetRouteEnabled {
        route_id: RouteId,
        enabled: bool,
    },
    InitializeEventCursor {
        sequence: u64,
    },
    AcknowledgeEventCursorGap {
        requested_after_sequence: u64,
        evidence: String,
    },
    InitializeInboundCursor {
        channel: ChannelKind,
        cursor: u64,
    },
    ReconcileRoute {
        route_id: RouteId,
        binding: AgentBinding,
        #[serde(default)]
        replace_identity: bool,
    },
    DisposeLegacy {
        record_kind: LegacyRecordKind,
        record_id: String,
        decision: LegacyDecision,
        evidence: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventCursorGap {
    pub requested_after_sequence: u64,
    pub oldest_available_sequence: u64,
    pub latest_sequence: u64,
    pub recovery_catalog_as_of_sequence: u64,
    pub fresh_catalog_as_of_sequence: u64,
    pub recorded_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OperatorMutation {
    pub operation_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent: Option<Value>,
    pub action: OperatorAction,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PromotionStatus {
    pub delivery_hold: bool,
    pub event_cursor: Option<u64>,
    pub event_cursor_gap: Option<EventCursorGap>,
    pub telegram_update_offset: Option<u64>,
    pub enabled_routes: BTreeSet<RouteId>,
    pub operator_actions: u64,
    pub unresolved_legacy_controls: u64,
    pub unresolved_legacy_returns: u64,
    pub held_legacy_debate: u64,
}

impl PromotionStatus {
    pub fn delivery_allowed(&self, route_id: RouteId) -> bool {
        !self.delivery_hold && self.enabled_routes.contains(&route_id)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OperatorOutcome {
    pub operation_id: Uuid,
    pub replayed: bool,
    pub detail: String,
    pub promotion: PromotionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route: Option<Route>,
}
