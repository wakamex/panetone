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
    #[serde(rename = "from")]
    pub source: String,
    #[serde(rename = "to")]
    pub target: String,
    pub message: String,
    #[serde(default)]
    pub return_final: bool,
    #[serde(default)]
    pub timeout_ms: u64,
}

impl SendParams {
    pub fn into_command(self, id: Uuid) -> SendCommand {
        SendCommand {
            id: WorkflowId::new(id),
            source: self.source,
            target: self.target,
            message: self.message,
            return_final: self.return_final,
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
