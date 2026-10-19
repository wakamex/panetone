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
    /// The name of a sender outside Wakterm, such as a remote assistant with
    /// shell access. Such a send is one-way.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    #[serde(default)]
    pub return_final: bool,
    #[serde(default)]
    pub reply: bool,
    /// Wait for a busy target to become idle instead of steering its active
    /// turn.
    #[serde(default)]
    pub queue: bool,
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
    /// The Signal account that serves the group, one the daemon is
    /// configured with. Omitted, the binding keeps its account, and a new
    /// binding uses the default account.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal_account: Option<String>,
    /// Send the owner's direct messages to the serving account to this route.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub signal_direct: bool,
    /// The endchat profile directory that serves an existing route instead of
    /// a Wakterm agent, or `none` to return it to Wakterm.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endchat_profile: Option<String>,
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
            // A reply is asked for with `reply`; `return_final` is its older
            // name, and Wakterm's turn correlation is no longer requested.
            return_final: false,
            reply: self.reply || self.return_final,
            // An outside sender reads the answer from the turn its message
            // starts, so its message waits for an idle target.
            steer: !self.queue && self.sender.is_none(),
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
