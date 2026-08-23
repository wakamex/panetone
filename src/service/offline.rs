use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use crate::channels::{RealChannels, RecordingChannels};
use crate::domain::{
    AgentBinding, CallbackDelivery, ChannelBinding, ChannelKind, DeliveryState, EffectId,
    OutboxItem, OutboxState, Route, SendCommand, WorkflowId, WorkflowState,
};
use crate::store::{
    ClaimResult, DestinationDelivery, ReturnDelivery, StoreError, StoreHandle, StoredWorkflow,
};
use crate::wakterm::{ContractError, EventRead, FakeWakterm, TerminalResult, WaktermCli};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServiceAck {
    pub accepted: bool,
    pub delivery_state: String,
    pub submitted: bool,
    pub reply_pending: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultPoint {
    AfterClaim,
    AfterAuditEffect,
    AfterAuditCheckpoint,
    AfterAdmissionPrepared,
    AfterPromptEffect,
    AfterReceiptCheckpoint,
    AfterReturnPersist,
    AfterMirrorEffect,
    AfterCallbackPrepared,
    AfterCallbackEffect,
}

#[derive(Default)]
pub struct FaultInjector {
    armed: Mutex<Option<FaultPoint>>,
    hits: Mutex<Vec<FaultPoint>>,
}

impl FaultInjector {
    pub fn arm(&self, point: FaultPoint) {
        *self.armed.lock().expect("fault lock is healthy") = Some(point);
    }

    pub fn hits(&self) -> Vec<FaultPoint> {
        self.hits
            .lock()
            .expect("fault history lock is healthy")
            .clone()
    }

    fn hit(&self, point: FaultPoint) -> Result<(), ServiceError> {
        self.hits
            .lock()
            .expect("fault history lock is healthy")
            .push(point);
        let mut armed = self.armed.lock().expect("fault lock is healthy");
        if *armed == Some(point) {
            *armed = None;
            return Err(ServiceError::Injected(point));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("route {0} is not available")]
    RouteUnavailable(String),
    #[error("route {0} has no messaging-channel binding")]
    MissingChannel(String),
    #[error("the request id was already used with different content")]
    IdempotencyConflict,
    #[error("the request id payload has expired and remains reserved in state {0}")]
    Expired(String),
    #[error("audit delivery failed before prompt submission: {0}")]
    AuditFailed(String),
    #[error("target admission failed in state {0:?}")]
    AdmissionFailed(WorkflowState),
    #[error("adapter operation became uncertain: {0}")]
    Adapter(String),
    #[error("terminal result does not match the persisted workflow identity")]
    TerminalIdentity,
    #[error("the workflow is missing or not awaiting this operation")]
    NotPending,
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("invalid workflow transition: {0}")]
    Workflow(#[from] crate::domain::WorkflowError),
    #[error("stored service acknowledgement is invalid: {0}")]
    StoredAck(#[from] serde_json::Error),
    #[error(transparent)]
    Contract(#[from] ContractError),
    #[error("injected crash boundary at {0:?}")]
    Injected(FaultPoint),
}

pub struct OfflineService {
    store: StoreHandle,
    wakterm: WaktermBackend,
    channels: ChannelBackend,
    faults: Arc<FaultInjector>,
}

enum WaktermBackend {
    Fake(FakeWakterm),
    Real(WaktermCli),
}

enum ChannelBackend {
    Fake(RecordingChannels),
    Real(RealChannels),
}

impl WaktermBackend {
    async fn admit(
        &self,
        request_id: EffectId,
        binding: &AgentBinding,
        prompt: String,
        return_final: bool,
        timeout_ms: u64,
    ) -> Result<crate::domain::AdmissionReceipt, ServiceError> {
        match self {
            Self::Fake(wakterm) => Ok(wakterm.admit(request_id, binding, prompt, return_final)),
            Self::Real(wakterm) => wakterm
                .admit(request_id, binding, &prompt, return_final, timeout_ms)
                .await
                .map_err(|error| ServiceError::Adapter(error.to_string())),
        }
    }
}

impl ChannelBackend {
    async fn send(&self, item: &OutboxItem) -> Result<String, ServiceError> {
        match self {
            Self::Fake(channels) => channels
                .send(item)
                .map_err(|error| ServiceError::Adapter(error.to_string())),
            Self::Real(channels) => channels
                .send(item)
                .await
                .map(|receipt| receipt.external_id)
                .map_err(|error| ServiceError::Adapter(error.to_string())),
        }
    }
}

impl OfflineService {
    pub fn new(store: StoreHandle, wakterm: FakeWakterm, channels: RecordingChannels) -> Self {
        Self {
            store,
            wakterm: WaktermBackend::Fake(wakterm),
            channels: ChannelBackend::Fake(channels),
            faults: Arc::new(FaultInjector::default()),
        }
    }

    pub fn with_faults(
        store: StoreHandle,
        wakterm: FakeWakterm,
        channels: RecordingChannels,
        faults: Arc<FaultInjector>,
    ) -> Self {
        Self {
            store,
            wakterm: WaktermBackend::Fake(wakterm),
            channels: ChannelBackend::Fake(channels),
            faults,
        }
    }

    pub fn new_real(store: StoreHandle, wakterm: WaktermCli, channels: RealChannels) -> Self {
        Self {
            store,
            wakterm: WaktermBackend::Real(wakterm),
            channels: ChannelBackend::Real(channels),
            faults: Arc::new(FaultInjector::default()),
        }
    }

    pub fn wakterm(&self) -> &FakeWakterm {
        match &self.wakterm {
            WaktermBackend::Fake(wakterm) => wakterm,
            WaktermBackend::Real(_) => panic!("real service has no recording Wakterm adapter"),
        }
    }

    pub fn channels(&self) -> &RecordingChannels {
        match &self.channels {
            ChannelBackend::Fake(channels) => channels,
            ChannelBackend::Real(_) => panic!("real service has no recording channel adapter"),
        }
    }

    async fn deliver_channel_effect(
        &self,
        workflow_id: WorkflowId,
        item: OutboxItem,
        now_ms: i64,
        fault_after_send: Option<FaultPoint>,
    ) -> Result<String, ServiceError> {
        let chunks = self
            .store
            .enqueue_outbox(Some(workflow_id), item, now_ms)
            .await?;
        let mut last_receipt = String::new();
        for mut chunk in chunks {
            if chunk.state == OutboxState::Delivered {
                if let Some(receipt) = chunk.external_receipt {
                    last_receipt = receipt;
                }
                continue;
            }
            if chunk.state == OutboxState::Indeterminate {
                return Err(ServiceError::Adapter(format!(
                    "outbox chunk {} is indeterminate",
                    chunk.id
                )));
            }
            chunk.state = OutboxState::Delivering;
            chunk.attempts += 1;
            chunk.last_error = None;
            self.store.save_outbox(chunk.clone(), now_ms).await?;
            match self.channels.send(&chunk).await {
                Ok(receipt) => {
                    if let Some(point) = fault_after_send {
                        self.faults.hit(point)?;
                    }
                    chunk.state = OutboxState::Delivered;
                    chunk.external_receipt = Some(receipt.clone());
                    self.store.save_outbox(chunk, now_ms).await?;
                    last_receipt = receipt;
                }
                Err(error) => {
                    chunk.state = OutboxState::Failed;
                    chunk.last_error = Some(error.to_string());
                    self.store.save_outbox(chunk, now_ms).await?;
                    return Err(error);
                }
            }
        }
        Ok(last_receipt)
    }

    pub async fn consume_fixture_events(
        &self,
        after_sequence: u64,
    ) -> Result<EventRead, ServiceError> {
        let page = match &self.wakterm {
            WaktermBackend::Fake(wakterm) => wakterm.read_events(after_sequence)?,
            WaktermBackend::Real(_) => EventRead::Unsupported,
        };
        if let EventRead::Events {
            next_after_sequence,
            ..
        } = &page
        {
            self.store
                .set_metadata(
                    "wakterm_event_cursor".into(),
                    next_after_sequence.to_string(),
                )
                .await?;
        }
        Ok(page)
    }

    pub async fn submit(
        &self,
        command: SendCommand,
        source: &Route,
        target: &Route,
        now_ms: i64,
    ) -> Result<ServiceAck, ServiceError> {
        let source_binding = available_agent(source)?;
        let target_binding = available_agent(target)?;
        let claim = self
            .store
            .claim(
                command,
                source.id,
                target.id,
                source_binding.clone(),
                target_binding.clone(),
                now_ms,
            )
            .await?;
        let mut record = match claim {
            ClaimResult::New(record) => record,
            ClaimResult::Existing(record) => {
                if let Some(response) = record.response {
                    return Ok(serde_json::from_value(response)?);
                }
                record
            }
            ClaimResult::Conflict { .. }
            | ClaimResult::Tombstone {
                same_content: false,
                ..
            } => return Err(ServiceError::IdempotencyConflict),
            ClaimResult::Tombstone {
                same_content: true,
                state,
            } => return Err(ServiceError::Expired(state)),
        };
        match record.workflow.state {
            WorkflowState::AuditPosted => {
                return self
                    .admit_target(record, target_binding, channel_destination(target)?, now_ms)
                    .await;
            }
            WorkflowState::AwaitingTargetIdle => {
                return persist_ack(
                    &self.store,
                    record,
                    ServiceAck {
                        accepted: true,
                        delivery_state: "queued".into(),
                        submitted: false,
                        reply_pending: false,
                    },
                    now_ms,
                )
                .await;
            }
            WorkflowState::Submitted => {
                self.post_target_status(
                    &record,
                    channel_destination(target)?,
                    "submitted",
                    "prompt accepted by Wakterm",
                    now_ms,
                )
                .await?;
                let ack = ServiceAck {
                    accepted: true,
                    delivery_state: "submitted".into(),
                    submitted: true,
                    reply_pending: record.command.return_final,
                };
                record.response = Some(serde_json::to_value(&ack)?);
                transition(&self.store, &mut record, WorkflowState::Completed, now_ms).await?;
                return Ok(ack);
            }
            WorkflowState::Indeterminate | WorkflowState::Failed => {
                return Err(ServiceError::AdmissionFailed(record.workflow.state));
            }
            WorkflowState::Claimed => {}
            _ => return Err(ServiceError::NotPending),
        }
        self.faults.hit(FaultPoint::AfterClaim)?;

        let (audit_kind, audit_destination) = channel_destination(target)?;
        let audit = OutboxItem {
            id: EffectId::named(record.command.id, "target-audit"),
            route_id: Some(record.workflow.target_route_id),
            sender_harness: Some(record.workflow.observed_source.harness.clone()),
            source_agent: Some(target_binding.clone()),
            kind: audit_kind,
            destination: audit_destination,
            body: format!(
                "[pending] {} -> {}\nRequest ID: {}\n\n{}",
                record.command.source,
                record.command.target,
                record.command.id,
                record.command.message
            ),
            state: OutboxState::Pending,
            attempts: 0,
            last_error: None,
            external_receipt: None,
        };
        if let Err(error) = self
            .deliver_channel_effect(
                record.command.id,
                audit,
                now_ms,
                Some(FaultPoint::AfterAuditEffect),
            )
            .await
        {
            if matches!(error, ServiceError::Injected(_)) {
                return Err(error);
            }
            transition(&self.store, &mut record, WorkflowState::Failed, now_ms).await?;
            return Err(ServiceError::AuditFailed(error.to_string()));
        }

        transition(&self.store, &mut record, WorkflowState::AuditPosted, now_ms).await?;
        self.faults.hit(FaultPoint::AfterAuditCheckpoint)?;
        self.admit_target(
            record,
            target_binding,
            (audit_kind, channel_destination(target)?.1),
            now_ms,
        )
        .await
    }

    pub async fn retry_busy_target(
        &self,
        workflow_id: WorkflowId,
        current_target: &Route,
        now_ms: i64,
    ) -> Result<ServiceAck, ServiceError> {
        let record = self
            .store
            .get_workflow(workflow_id)
            .await?
            .ok_or(ServiceError::NotPending)?;
        if record.workflow.state != WorkflowState::AwaitingTargetIdle
            || record.workflow.target_route_id != current_target.id
        {
            return Err(ServiceError::NotPending);
        }
        let target = available_agent(current_target)?;
        self.admit_target(record, target, channel_destination(current_target)?, now_ms)
            .await
    }

    async fn admit_target(
        &self,
        mut record: StoredWorkflow,
        target: AgentBinding,
        target_channel: (ChannelKind, String),
        now_ms: i64,
    ) -> Result<ServiceAck, ServiceError> {
        let was_already_queued = record.response.is_some();
        transition(
            &self.store,
            &mut record,
            WorkflowState::AdmissionPrepared,
            now_ms,
        )
        .await?;
        self.faults.hit(FaultPoint::AfterAdmissionPrepared)?;
        let prompt = FakeWakterm::envelope(
            &record.command,
            &record.workflow.observed_source.harness,
            &target.harness,
        );
        let receipt = match self
            .wakterm
            .admit(
                record.workflow.target_effect_id,
                &target,
                prompt,
                record.command.return_final,
                record.command.timeout_ms,
            )
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                transition(
                    &self.store,
                    &mut record,
                    WorkflowState::Indeterminate,
                    now_ms,
                )
                .await?;
                self.post_target_status(
                    &record,
                    target_channel,
                    "delivery-failed",
                    "DELIVERY INDETERMINATE; see durable request state",
                    now_ms,
                )
                .await?;
                return Err(error);
            }
        };
        self.faults.hit(FaultPoint::AfterPromptEffect)?;
        let expected = record.workflow.state;
        let state = record.workflow.apply_target_receipt(&receipt, &target)?;
        record.updated_at_ms = now_ms;
        self.store.save_workflow(record.clone(), expected).await?;
        self.faults.hit(FaultPoint::AfterReceiptCheckpoint)?;
        match state {
            WorkflowState::AwaitingTargetIdle => {
                if !was_already_queued {
                    self.post_target_status(
                        &record,
                        target_channel,
                        "queued",
                        "target is busy; delivery is durably queued",
                        now_ms,
                    )
                    .await?;
                }
                persist_ack(
                    &self.store,
                    record,
                    ServiceAck {
                        accepted: true,
                        delivery_state: "queued".into(),
                        submitted: false,
                        reply_pending: false,
                    },
                    now_ms,
                )
                .await
            }
            WorkflowState::Submitted => {
                self.post_target_status(
                    &record,
                    target_channel,
                    "submitted",
                    "prompt accepted by Wakterm",
                    now_ms,
                )
                .await?;
                record.workflow.submitted_target = Some(target);
                let ack = ServiceAck {
                    accepted: true,
                    delivery_state: "submitted".into(),
                    submitted: true,
                    reply_pending: record.command.return_final,
                };
                record.response = Some(serde_json::to_value(&ack)?);
                transition(&self.store, &mut record, WorkflowState::Completed, now_ms).await?;
                Ok(ack)
            }
            WorkflowState::Indeterminate | WorkflowState::Failed => {
                self.post_target_status(
                    &record,
                    target_channel,
                    "delivery-failed",
                    "DELIVERY FAILED; see durable request state",
                    now_ms,
                )
                .await?;
                Err(ServiceError::AdmissionFailed(state))
            }
            _ => Err(ServiceError::AdmissionFailed(state)),
        }
    }

    async fn post_target_status(
        &self,
        record: &StoredWorkflow,
        (kind, destination): (ChannelKind, String),
        purpose: &str,
        detail: &str,
        now_ms: i64,
    ) -> Result<(), ServiceError> {
        let item = OutboxItem {
            id: EffectId::named(record.command.id, &format!("target-{purpose}")),
            route_id: Some(record.workflow.target_route_id),
            sender_harness: Some(record.workflow.observed_source.harness.clone()),
            source_agent: record
                .workflow
                .submitted_target
                .clone()
                .or_else(|| Some(record.workflow.observed_target.clone())),
            kind,
            destination,
            body: format!(
                "[{purpose}] {} -> {}\nRequest ID: {}\n{detail}",
                record.command.source, record.command.target, record.command.id
            ),
            state: OutboxState::Pending,
            attempts: 0,
            last_error: None,
            external_receipt: None,
        };
        match self
            .deliver_channel_effect(record.command.id, item, now_ms, None)
            .await
        {
            Ok(_) | Err(ServiceError::Adapter(_)) => {}
            Err(error) => return Err(error),
        }
        Ok(())
    }

    pub async fn accept_terminal(
        &self,
        terminal: TerminalResult,
        source_route: &Route,
        now_ms: i64,
    ) -> Result<ReturnDelivery, ServiceError> {
        let workflow_id = terminal.workflow_id;
        self.persist_terminal(terminal, source_route, now_ms)
            .await?;
        self.deliver_pending_return(workflow_id, source_route, now_ms)
            .await
    }

    pub async fn persist_terminal(
        &self,
        terminal: TerminalResult,
        source_route: &Route,
        now_ms: i64,
    ) -> Result<ReturnDelivery, ServiceError> {
        let workflow = self
            .store
            .get_workflow(terminal.workflow_id)
            .await?
            .ok_or(ServiceError::NotPending)?;
        if !workflow.command.return_final
            || workflow.workflow.observed_source.agent_id != terminal.source.agent_id
            || workflow.workflow.observed_source.incarnation_id != terminal.source.incarnation_id
            || workflow
                .workflow
                .submitted_target
                .as_ref()
                .is_none_or(|binding| {
                    binding.agent_id != terminal.target.agent_id
                        || binding.incarnation_id != terminal.target.incarnation_id
                })
            || source_route.id != workflow.workflow.source_route_id
        {
            return Err(ServiceError::TerminalIdentity);
        }
        let mirror_effect = EffectId::named(terminal.workflow_id, "return-mirror");
        let result = json!({
            "status": terminal.status,
            "message": terminal.message,
            "source": workflow.command.source,
            "target": workflow.command.target,
            "request_id": workflow.command.id
        });
        let candidate = ReturnDelivery {
            workflow_id: terminal.workflow_id,
            result,
            agent: CallbackDelivery {
                effect_id: EffectId::callback_admission(terminal.workflow_id),
                source: workflow.workflow.observed_source.clone(),
                state: DeliveryState::Pending,
                last_error: None,
            },
            mirror: DestinationDelivery {
                effect_id: mirror_effect,
                state: DeliveryState::Pending,
                attempts: 0,
                last_error: None,
                external_receipt: None,
            },
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        };
        let returned = if let Some(existing) = self.store.get_return(terminal.workflow_id).await? {
            if existing.result != candidate.result
                || existing.agent.effect_id != candidate.agent.effect_id
                || existing.agent.source != candidate.agent.source
                || existing.mirror.effect_id != candidate.mirror.effect_id
            {
                return Err(ServiceError::TerminalIdentity);
            }
            existing
        } else {
            self.store.register_return(candidate.clone()).await?;
            self.faults.hit(FaultPoint::AfterReturnPersist)?;
            candidate
        };

        Ok(returned)
    }

    pub async fn deliver_pending_return(
        &self,
        workflow_id: WorkflowId,
        source_route: &Route,
        now_ms: i64,
    ) -> Result<ReturnDelivery, ServiceError> {
        self.deliver_return(
            workflow_id,
            source_route,
            source_route.agent.clone(),
            now_ms,
        )
        .await
    }

    async fn deliver_return(
        &self,
        workflow_id: WorkflowId,
        source_route: &Route,
        current_source: Option<AgentBinding>,
        now_ms: i64,
    ) -> Result<ReturnDelivery, ServiceError> {
        let mut returned = self
            .store
            .get_return(workflow_id)
            .await?
            .ok_or(ServiceError::NotPending)?;
        let workflow = self
            .store
            .get_workflow(workflow_id)
            .await?
            .ok_or(ServiceError::NotPending)?;
        if workflow.workflow.source_route_id != source_route.id {
            return Err(ServiceError::TerminalIdentity);
        }
        let (kind, destination) = channel_destination(source_route)?;

        if matches!(
            returned.mirror.state,
            DeliveryState::Pending | DeliveryState::Failed
        ) {
            let mirror = OutboxItem {
                id: returned.mirror.effect_id,
                route_id: Some(workflow.workflow.source_route_id),
                sender_harness: workflow
                    .workflow
                    .submitted_target
                    .as_ref()
                    .map(|binding| binding.harness.clone()),
                source_agent: workflow.workflow.submitted_target.clone(),
                kind,
                destination,
                body: callback_envelope_from_value(&workflow, &returned),
                state: OutboxState::Pending,
                attempts: returned.mirror.attempts,
                last_error: returned.mirror.last_error.clone(),
                external_receipt: None,
            };
            returned.mirror.state = DeliveryState::Pending;
            returned.mirror.attempts += 1;
            match self
                .deliver_channel_effect(
                    workflow_id,
                    mirror,
                    now_ms,
                    Some(FaultPoint::AfterMirrorEffect),
                )
                .await
            {
                Ok(receipt) => {
                    returned.mirror.state = DeliveryState::Delivered;
                    returned.mirror.last_error = None;
                    returned.mirror.external_receipt = Some(receipt);
                }
                Err(error @ ServiceError::Injected(_)) => return Err(error),
                Err(error) => {
                    returned.mirror.state = DeliveryState::Failed;
                    returned.mirror.last_error = Some(error.to_string());
                }
            }
            returned.updated_at_ms = now_ms;
            self.store.save_return(returned.clone()).await?;
        }
        if returned.agent.state == DeliveryState::Pending
            && let Some(current_source) = current_source
        {
            self.deliver_return_to_agent(returned, workflow, current_source, now_ms)
                .await
        } else {
            Ok(returned)
        }
    }

    pub async fn retry_pending_return(
        &self,
        workflow_id: WorkflowId,
        now_ms: i64,
    ) -> Result<ReturnDelivery, ServiceError> {
        let returned = self
            .store
            .pending_returns()
            .await?
            .into_iter()
            .find(|record| {
                record.workflow_id == workflow_id && record.agent.state == DeliveryState::Pending
            })
            .ok_or(ServiceError::NotPending)?;
        let workflow = self
            .store
            .get_workflow(workflow_id)
            .await?
            .ok_or(ServiceError::NotPending)?;
        let current_source = returned.agent.source.clone();
        self.deliver_return_to_agent(returned, workflow, current_source, now_ms)
            .await
    }

    async fn deliver_return_to_agent(
        &self,
        mut returned: ReturnDelivery,
        workflow: StoredWorkflow,
        current_source: AgentBinding,
        now_ms: i64,
    ) -> Result<ReturnDelivery, ServiceError> {
        returned.agent.source = current_source;
        returned.agent.prepare()?;
        returned.updated_at_ms = now_ms;
        self.store.save_return(returned.clone()).await?;
        self.faults.hit(FaultPoint::AfterCallbackPrepared)?;
        let receipt = match self
            .wakterm
            .admit(
                returned.agent.effect_id,
                &returned.agent.source,
                callback_envelope_from_value(&workflow, &returned),
                false,
                0,
            )
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                returned.agent.state = DeliveryState::Indeterminate;
                returned.agent.last_error = Some(error.to_string());
                returned.updated_at_ms = now_ms;
                self.store.save_return(returned).await?;
                return Err(error);
            }
        };
        self.faults.hit(FaultPoint::AfterCallbackEffect)?;
        returned.agent.apply_receipt(&receipt)?;
        returned.updated_at_ms = now_ms;
        self.store.save_return(returned.clone()).await?;
        Ok(returned)
    }
}

async fn transition(
    store: &StoreHandle,
    record: &mut StoredWorkflow,
    next: WorkflowState,
    now_ms: i64,
) -> Result<(), ServiceError> {
    let expected = record.workflow.state;
    record.workflow.transition(next)?;
    record.updated_at_ms = now_ms;
    store.save_workflow(record.clone(), expected).await?;
    Ok(())
}

async fn persist_ack(
    store: &StoreHandle,
    mut record: StoredWorkflow,
    ack: ServiceAck,
    now_ms: i64,
) -> Result<ServiceAck, ServiceError> {
    record.response = Some(serde_json::to_value(&ack)?);
    record.updated_at_ms = now_ms;
    store
        .save_workflow(record.clone(), record.workflow.state)
        .await?;
    Ok(ack)
}

fn available_agent(route: &Route) -> Result<AgentBinding, ServiceError> {
    route
        .agent
        .clone()
        .ok_or_else(|| ServiceError::RouteUnavailable(route.title.clone()))
}

fn channel_destination(route: &Route) -> Result<(ChannelKind, String), ServiceError> {
    route
        .channels
        .first()
        .map(|binding| match binding {
            ChannelBinding::Telegram { topic_id } => (ChannelKind::Telegram, topic_id.to_string()),
            ChannelBinding::Signal { group_id } => (ChannelKind::Signal, group_id.clone()),
        })
        .ok_or_else(|| ServiceError::MissingChannel(route.title.clone()))
}

fn callback_envelope_from_value(workflow: &StoredWorkflow, returned: &ReturnDelivery) -> String {
    format!(
        "[Panetone asynchronous final return]\nFrom: {} ({})\nTo: {} ({})\nRequest ID: {}\nDelivery ID: {}\nStatus: {}\n\n{}",
        workflow.command.target,
        workflow
            .workflow
            .submitted_target
            .as_ref()
            .map_or("unknown", |binding| binding.harness.as_str()),
        workflow.command.source,
        returned.agent.source.harness,
        workflow.command.id,
        returned.mirror.effect_id,
        returned.result["status"].as_str().unwrap_or("unknown"),
        returned.result["message"].as_str().unwrap_or_default()
    )
}
