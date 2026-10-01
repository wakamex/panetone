use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::domain::{SendCommand, WorkflowId};

pub const CONTROL_SCHEMA: &str = "panetone.control.v1";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ControlRequest {
    pub schema: String,
    pub id: Uuid,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SendParams {
    #[serde(rename = "from", skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(rename = "to")]
    pub target: String,
    pub message: String,
    #[serde(default)]
    pub source_pane_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_agent: Option<SourceAgent>,
    #[serde(default)]
    pub return_final: bool,
    #[serde(default)]
    pub steer: bool,
    #[serde(default)]
    pub timeout_ms: u64,
}

/// The exact calling agent incarnation, as `wakterm agent caller` reports it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceAgent {
    pub agent_id: String,
    pub incarnation_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RouteInspectParams {
    pub title: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RouteEnsureParams {
    pub title: String,
    #[serde(default)]
    pub telegram_topic_id: Option<i64>,
    #[serde(default)]
    pub signal_group_id: Option<String>,
    #[serde(default)]
    pub signal_allow_members: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OutputDispositionParams {
    pub route: String,
    pub agent_id: String,
    pub incarnation_id: String,
    pub after_sequence: u64,
    pub expected_text: String,
}

impl SendParams {
    pub fn into_command(self, id: Uuid, source: String) -> SendCommand {
        SendCommand {
            id: WorkflowId::new(id),
            source,
            target: self.target,
            message: self.message,
            return_final: self.return_final,
            steer: self.steer,
            timeout_ms: self.timeout_ms,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ControlResponse {
    pub schema: String,
    pub id: Uuid,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ControlError>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ControlError {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

pub fn success_response(id: Uuid, result: Value) -> ControlResponse {
    ControlResponse {
        schema: CONTROL_SCHEMA.into(),
        id,
        ok: true,
        result: Some(result),
        error: None,
    }
}

pub fn error_response(
    id: Uuid,
    code: impl Into<String>,
    message: impl Into<String>,
    details: Option<Value>,
) -> ControlResponse {
    ControlResponse {
        schema: CONTROL_SCHEMA.into(),
        id,
        ok: false,
        result: None,
        error: Some(ControlError {
            code: code.into(),
            message: message.into(),
            details,
        }),
    }
}
