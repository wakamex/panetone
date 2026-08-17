use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{AgentBinding, EffectId, RouteId, WorkflowId};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SendCommand {
    pub id: WorkflowId,
    pub source: String,
    pub target: String,
    pub message: String,
    #[serde(default)]
    pub return_final: bool,
    #[serde(default)]
    pub timeout_ms: u64,
}

pub fn semantic_request_hash(command: &SendCommand) -> String {
    let mut digest = Sha256::new();
    for value in [
        "panetone.control.v1",
        "send",
        command.source.as_str(),
        command.target.as_str(),
        command.message.as_str(),
        if command.return_final {
            "true"
        } else {
            "false"
        },
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    digest.update(command.timeout_ms.to_string().as_bytes());
    format!("{:x}", digest.finalize())
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionStatus {
    Accepted,
    Busy,
    Unsupported,
    Unavailable,
    StaleIncarnation,
    Invalid,
    ObserverFailure,
    InternalFailure,
    Indeterminate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdmissionReceipt {
    pub request_id: EffectId,
    pub status: AdmissionStatus,
    pub definitive: bool,
    pub prompt_written: Option<bool>,
    pub agent_id: Option<String>,
    pub incarnation_id: Option<String>,
    pub detail: Option<String>,
}

impl AdmissionReceipt {
    pub fn validate(
        &self,
        effect_id: EffectId,
        binding: &AgentBinding,
    ) -> Result<(), WorkflowError> {
        if self.request_id != effect_id
            || self.agent_id.as_deref() != Some(binding.agent_id.as_str())
            || self.incarnation_id.as_deref() != Some(binding.incarnation_id.as_str())
        {
            return Err(WorkflowError::ReceiptIdentity);
        }
        let valid = match self.status {
            AdmissionStatus::Accepted => self.definitive && self.prompt_written == Some(true),
            AdmissionStatus::Indeterminate => !self.definitive && self.prompt_written.is_none(),
            _ => self.definitive && self.prompt_written == Some(false),
        };
        if !valid {
            return Err(WorkflowError::ReceiptInvariant);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    Claimed,
    AuditPosted,
    AwaitingTargetIdle,
    AdmissionPrepared,
    Submitted,
    Completed,
    Failed,
    Indeterminate,
    Cancelled,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Workflow {
    pub id: WorkflowId,
    pub semantic_hash: String,
    pub source_route_id: RouteId,
    pub target_route_id: RouteId,
    pub target_effect_id: EffectId,
    pub observed_source: AgentBinding,
    pub observed_target: AgentBinding,
    pub submitted_target: Option<AgentBinding>,
    pub state: WorkflowState,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum WorkflowError {
    #[error("invalid state transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: WorkflowState,
        to: WorkflowState,
    },
    #[error("admission receipt does not match the requested effect or agent")]
    ReceiptIdentity,
    #[error("admission receipt has inconsistent definitive and prompt fields")]
    ReceiptInvariant,
}

impl Workflow {
    pub fn transition(&mut self, to: WorkflowState) -> Result<(), WorkflowError> {
        let allowed = matches!(
            (self.state, to),
            (WorkflowState::Claimed, WorkflowState::AuditPosted)
                | (WorkflowState::Claimed, WorkflowState::Failed)
                | (WorkflowState::AuditPosted, WorkflowState::AdmissionPrepared)
                | (
                    WorkflowState::AwaitingTargetIdle,
                    WorkflowState::AdmissionPrepared
                )
                | (
                    WorkflowState::AdmissionPrepared,
                    WorkflowState::AwaitingTargetIdle
                )
                | (WorkflowState::AdmissionPrepared, WorkflowState::Submitted)
                | (WorkflowState::AdmissionPrepared, WorkflowState::Failed)
                | (
                    WorkflowState::AdmissionPrepared,
                    WorkflowState::Indeterminate
                )
                | (WorkflowState::Submitted, WorkflowState::Completed)
                | (WorkflowState::Submitted, WorkflowState::Indeterminate)
                | (WorkflowState::AwaitingTargetIdle, WorkflowState::Cancelled)
        );
        if !allowed {
            return Err(WorkflowError::InvalidTransition {
                from: self.state,
                to,
            });
        }
        self.state = to;
        Ok(())
    }

    pub fn apply_target_receipt(
        &mut self,
        receipt: &AdmissionReceipt,
        binding: &AgentBinding,
    ) -> Result<WorkflowState, WorkflowError> {
        receipt.validate(self.target_effect_id, binding)?;
        let next = match receipt.status {
            AdmissionStatus::Accepted => {
                self.submitted_target = Some(binding.clone());
                WorkflowState::Submitted
            }
            AdmissionStatus::Busy => WorkflowState::AwaitingTargetIdle,
            AdmissionStatus::Indeterminate => WorkflowState::Indeterminate,
            _ => WorkflowState::Failed,
        };
        self.transition(next)?;
        Ok(next)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    Pending,
    Delivering,
    Delivered,
    Failed,
    Indeterminate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CallbackDelivery {
    pub effect_id: EffectId,
    pub source: AgentBinding,
    pub state: DeliveryState,
    pub last_error: Option<String>,
}

impl CallbackDelivery {
    pub fn prepare(&mut self) -> Result<(), WorkflowError> {
        if self.state != DeliveryState::Pending {
            return Err(WorkflowError::InvalidTransition {
                from: workflow_state_for_delivery(self.state),
                to: WorkflowState::AdmissionPrepared,
            });
        }
        self.state = DeliveryState::Delivering;
        Ok(())
    }

    pub fn apply_receipt(&mut self, receipt: &AdmissionReceipt) -> Result<(), WorkflowError> {
        receipt.validate(self.effect_id, &self.source)?;
        if self.state != DeliveryState::Delivering {
            return Err(WorkflowError::InvalidTransition {
                from: workflow_state_for_delivery(self.state),
                to: WorkflowState::Completed,
            });
        }
        self.state = match receipt.status {
            AdmissionStatus::Accepted => DeliveryState::Delivered,
            AdmissionStatus::Busy => DeliveryState::Pending,
            AdmissionStatus::Indeterminate => DeliveryState::Indeterminate,
            _ => DeliveryState::Failed,
        };
        self.last_error = receipt.detail.clone();
        Ok(())
    }

    pub fn recover_after_restart(&mut self) {
        if self.state == DeliveryState::Delivering {
            self.state = DeliveryState::Indeterminate;
            self.last_error = Some("Panetone restarted during callback admission".into());
        }
    }
}

const fn workflow_state_for_delivery(state: DeliveryState) -> WorkflowState {
    match state {
        DeliveryState::Pending => WorkflowState::AwaitingTargetIdle,
        DeliveryState::Delivering => WorkflowState::AdmissionPrepared,
        DeliveryState::Delivered => WorkflowState::Completed,
        DeliveryState::Failed => WorkflowState::Failed,
        DeliveryState::Indeterminate => WorkflowState::Indeterminate,
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn binding() -> AgentBinding {
        AgentBinding {
            agent_id: "agent-target".into(),
            incarnation_id: "incarnation-1".into(),
            harness: "codex".into(),
            pane_id: Some(9),
        }
    }

    fn workflow() -> Workflow {
        let id = WorkflowId::new(Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap());
        Workflow {
            id,
            semantic_hash: "hash".into(),
            source_route_id: RouteId::new(Uuid::nil()),
            target_route_id: RouteId::new(Uuid::max()),
            target_effect_id: EffectId::target_admission(id),
            observed_source: binding(),
            observed_target: binding(),
            submitted_target: None,
            state: WorkflowState::AdmissionPrepared,
        }
    }

    fn receipt(status: AdmissionStatus) -> AdmissionReceipt {
        let workflow = workflow();
        let (definitive, prompt_written) = match status {
            AdmissionStatus::Accepted => (true, Some(true)),
            AdmissionStatus::Indeterminate => (false, None),
            _ => (true, Some(false)),
        };
        AdmissionReceipt {
            request_id: workflow.target_effect_id,
            status,
            definitive,
            prompt_written,
            agent_id: Some("agent-target".into()),
            incarnation_id: Some("incarnation-1".into()),
            detail: None,
        }
    }

    #[test]
    fn semantic_defaults_hash_identically() {
        let id = WorkflowId::new(Uuid::nil());
        let omitted = SendCommand {
            id,
            source: "source".into(),
            target: "target".into(),
            message: "hello".into(),
            return_final: false,
            timeout_ms: 0,
        };
        let explicit = omitted.clone();
        assert_eq!(
            semantic_request_hash(&omitted),
            semantic_request_hash(&explicit)
        );
    }

    #[test]
    fn definitive_busy_queues_without_submission() {
        let mut workflow = workflow();
        let state = workflow
            .apply_target_receipt(&receipt(AdmissionStatus::Busy), &binding())
            .unwrap();
        assert_eq!(state, WorkflowState::AwaitingTargetIdle);
        assert!(workflow.submitted_target.is_none());
    }

    #[test]
    fn indeterminate_target_is_terminal() {
        let mut workflow = workflow();
        let state = workflow
            .apply_target_receipt(&receipt(AdmissionStatus::Indeterminate), &binding())
            .unwrap();
        assert_eq!(state, WorkflowState::Indeterminate);
    }

    #[test]
    fn busy_callback_returns_to_pending_with_same_effect_id() {
        let id = WorkflowId::new(Uuid::nil());
        let effect_id = EffectId::callback_admission(id);
        let source = binding();
        let mut delivery = CallbackDelivery {
            effect_id,
            source: source.clone(),
            state: DeliveryState::Pending,
            last_error: None,
        };
        delivery.prepare().unwrap();
        let mut receipt = receipt(AdmissionStatus::Busy);
        receipt.request_id = effect_id;
        delivery.apply_receipt(&receipt).unwrap();
        assert_eq!(delivery.state, DeliveryState::Pending);
        assert_eq!(delivery.effect_id, effect_id);
    }

    #[test]
    fn restart_during_callback_is_indeterminate() {
        let mut delivery = CallbackDelivery {
            effect_id: EffectId::random(),
            source: binding(),
            state: DeliveryState::Delivering,
            last_error: None,
        };
        delivery.recover_after_restart();
        assert_eq!(delivery.state, DeliveryState::Indeterminate);
    }
}
