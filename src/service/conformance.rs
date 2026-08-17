use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Instant;

use serde_json::{Value, json};

use crate::control::{
    CONTROL_SCHEMA, ControlHandler, ControlRequest, ControlResponse, SendParams, error_response,
    success_response,
};
use crate::domain::{AgentBinding, WorkflowState};
use crate::store::{ClaimResult, StoreHandle, StoredWorkflow};
use crate::supervisor::SupervisorHandle;

pub struct ConformanceService {
    store: StoreHandle,
    effect_log: PathBuf,
    effect_lock: Mutex<()>,
    health: Option<SupervisorHandle>,
    wakterm_profile: String,
    wakterm_capabilities: Vec<String>,
    control_socket: Option<PathBuf>,
    started_at: Instant,
}

impl ConformanceService {
    pub fn new(store: StoreHandle, effect_log: PathBuf) -> Self {
        Self {
            store,
            effect_log,
            effect_lock: Mutex::new(()),
            health: None,
            wakterm_profile: "current".into(),
            wakterm_capabilities: Vec::new(),
            control_socket: None,
            started_at: Instant::now(),
        }
    }

    pub fn with_runtime_status(
        mut self,
        health: SupervisorHandle,
        wakterm_profile: impl Into<String>,
        capabilities: Vec<String>,
        control_socket: PathBuf,
    ) -> Self {
        self.health = Some(health);
        self.wakterm_profile = wakterm_profile.into();
        self.wakterm_capabilities = capabilities;
        self.control_socket = Some(control_socket);
        self
    }

    async fn send(&self, request: ControlRequest, params: SendParams) -> ControlResponse {
        let id = request.id;
        if params.timeout_ms != 0 {
            return error_response(
                id,
                "invalid_params",
                "asynchronous final callbacks do not expire; params.timeout_ms must be zero",
                None,
            );
        }
        let command = params.into_command(id);
        let claim = self
            .store
            .claim(
                command,
                crate::domain::RouteId::new(uuid::Uuid::from_u128(1)),
                crate::domain::RouteId::new(uuid::Uuid::from_u128(2)),
                AgentBinding {
                    agent_id: "agent-source".into(),
                    incarnation_id: "incarnation-source-1".into(),
                    harness: "codex".into(),
                    pane_id: Some(1),
                },
                AgentBinding {
                    agent_id: "agent-target".into(),
                    incarnation_id: "incarnation-target-1".into(),
                    harness: "codex".into(),
                    pane_id: Some(2),
                },
                now_ms(),
            )
            .await;
        let claim = match claim {
            Ok(claim) => claim,
            Err(error) => {
                return error_response(
                    id,
                    "internal_error",
                    format!("durable request claim failed: {error}"),
                    None,
                );
            }
        };
        match claim {
            ClaimResult::Existing(record) => response_for_record(record),
            ClaimResult::Conflict { .. }
            | ClaimResult::Tombstone {
                same_content: false,
                ..
            } => conflict(id),
            ClaimResult::Tombstone {
                same_content: true,
                state,
            } => error_response(
                id,
                "request_expired",
                "the request payload expired but its idempotency key remains reserved",
                Some(json!({"state": state})),
            ),
            ClaimResult::New(mut record) => {
                if let Err(response) =
                    transition(&self.store, &mut record, WorkflowState::AuditPosted).await
                {
                    return response;
                }
                if let Err(response) =
                    transition(&self.store, &mut record, WorkflowState::AdmissionPrepared).await
                {
                    return response;
                }
                if record.command.message == "__target_busy__" {
                    if let Err(response) =
                        transition(&self.store, &mut record, WorkflowState::AwaitingTargetIdle)
                            .await
                    {
                        return response;
                    }
                    let result = json!({
                        "accepted": true,
                        "delivery_state": "queued",
                        "submitted": false,
                        "reply_pending": false
                    });
                    return persist_response(&self.store, record, result).await;
                }
                if let Err(error) = self.append_effect(id) {
                    return error_response(
                        id,
                        "internal_error",
                        format!("effect recording failed: {error}"),
                        None,
                    );
                }
                if record.command.message == "__hold_after_effect__" {
                    std::future::pending::<()>().await;
                }
                if let Err(response) =
                    transition(&self.store, &mut record, WorkflowState::Submitted).await
                {
                    return response;
                }
                let result = json!({
                    "accepted": true,
                    "reply_pending": record.command.return_final
                });
                record.response = Some(result.clone());
                let expected = record.workflow.state;
                record.workflow.state = WorkflowState::Completed;
                record.updated_at_ms = now_ms();
                if let Err(error) = self.store.save_workflow(record, expected).await {
                    return error_response(
                        id,
                        "request_indeterminate",
                        "prompt was accepted but its result could not be persisted",
                        Some(json!({"store_error": error.to_string()})),
                    );
                }
                success_response(id, result)
            }
        }
    }

    fn append_effect(&self, request_id: uuid::Uuid) -> std::io::Result<()> {
        let _guard = self.effect_lock.lock().expect("effect log lock is healthy");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&self.effect_log)?;
        std::fs::set_permissions(&self.effect_log, std::fs::Permissions::from_mode(0o600))?;
        writeln!(file, "{}", json!({"request_id": request_id}))?;
        file.sync_all()
    }
}

impl ControlHandler for ConformanceService {
    fn handle(
        &self,
        request: ControlRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ControlResponse> + Send + '_>> {
        Box::pin(async move {
            if request.schema != CONTROL_SCHEMA {
                return error_response(
                    request.id,
                    "unsupported_schema",
                    "only panetone.control.v1 is supported",
                    None,
                );
            }
            if request.method == "status" {
                return match self.store.status().await {
                    Ok(status) => success_response(
                        request.id,
                        json!({
                            "version": env!("CARGO_PKG_VERSION"),
                            "mode": "offline_fake",
                            "uptime_ms": self.started_at.elapsed().as_millis() as u64,
                            "store": status,
                            "wakterm": {
                                "profile": self.wakterm_profile,
                                "capabilities": self.wakterm_capabilities,
                                "general_event_consumer": self.wakterm_capabilities.iter().any(|value| value == "event_stream.v1"),
                                "connection": "fake_ready"
                            },
                            "control": {
                                "path": self.control_socket,
                                "same_uid_required": cfg!(target_os = "linux")
                            },
                            "channels": {
                                "telegram": "fake_ready",
                                "signal": "fake_ready",
                                "slack": "fake_ready"
                            },
                            "tasks": self.health.as_ref().map(SupervisorHandle::snapshot).unwrap_or_default()
                        }),
                    ),
                    Err(error) => error_response(
                        request.id,
                        "internal_error",
                        format!("status query failed: {error}"),
                        None,
                    ),
                };
            }
            if request.method != "send" {
                return error_response(
                    request.id,
                    "unknown_method",
                    "the requested control method is not supported",
                    None,
                );
            }
            match serde_json::from_value::<SendParams>(request.params.clone()) {
                Ok(params)
                    if !params.source.is_empty()
                        && !params.target.is_empty()
                        && !params.message.is_empty() =>
                {
                    self.send(request, params).await
                }
                _ => error_response(
                    request.id,
                    "invalid_request",
                    "send requires non-empty from, to, and message fields",
                    None,
                ),
            }
        })
    }
}

async fn transition(
    store: &StoreHandle,
    record: &mut StoredWorkflow,
    next: WorkflowState,
) -> Result<(), ControlResponse> {
    let expected = record.workflow.state;
    record.workflow.state = next;
    record.updated_at_ms = now_ms();
    store
        .save_workflow(record.clone(), expected)
        .await
        .map_err(|error| {
            error_response(
                record.command.id.as_uuid(),
                "internal_error",
                format!("durable transition failed: {error}"),
                None,
            )
        })
}

async fn persist_response(
    store: &StoreHandle,
    mut record: StoredWorkflow,
    result: Value,
) -> ControlResponse {
    record.response = Some(result.clone());
    let expected = record.workflow.state;
    record.updated_at_ms = now_ms();
    match store.save_workflow(record.clone(), expected).await {
        Ok(()) => success_response(record.command.id.as_uuid(), result),
        Err(error) => error_response(
            record.command.id.as_uuid(),
            "internal_error",
            format!("durable response failed: {error}"),
            None,
        ),
    }
}

fn response_for_record(record: StoredWorkflow) -> ControlResponse {
    let id = record.command.id.as_uuid();
    if let Some(result) = record.response {
        return success_response(id, result);
    }
    match record.workflow.state {
        WorkflowState::Indeterminate | WorkflowState::AdmissionPrepared => error_response(
            id,
            "request_indeterminate",
            "the service restarted or lost contact during this request; it will not be retried automatically",
            Some(json!({
                "last_state": "delivering",
                "progress": {
                    "source": record.command.source,
                    "target": record.command.target
                }
            })),
        ),
        _ => error_response(
            id,
            "request_in_progress",
            "this request is already in progress",
            Some(json!({"state": format!("{:?}", record.workflow.state).to_lowercase()})),
        ),
    }
}

fn conflict(id: uuid::Uuid) -> ControlResponse {
    error_response(
        id,
        "idempotency_conflict",
        "this request id was already used with different content",
        Some(json!({"state": "succeeded"})),
    )
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}
