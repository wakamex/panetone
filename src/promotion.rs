use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{AgentBinding, Route, RouteId};

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
    ReconcileRoute {
        route_id: RouteId,
        binding: AgentBinding,
    },
    DisposeLegacy {
        record_kind: LegacyRecordKind,
        record_id: String,
        decision: LegacyDecision,
        evidence: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OperatorMutation {
    pub operation_id: Uuid,
    pub action: OperatorAction,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PromotionStatus {
    pub delivery_hold: bool,
    pub event_cursor: Option<u64>,
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
