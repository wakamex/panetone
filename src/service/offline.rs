use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use crate::channels::{RealChannels, RecordingChannels};
use crate::domain::{
    AdmissionStatus, AgentBinding, CallbackDelivery, ChannelBinding, ChannelKind, DeliveryState,
    EffectId, OutboxItem, OutboxState, Route, SendCommand, WorkflowId, WorkflowState,
    signal_group_account,
};
use crate::store::{
    ClaimResult, DeliveryProof, DestinationDelivery, ReplyDebt, ReturnDelivery, StoreError,
    StoreHandle, StoredWorkflow,
};

/// How long a message steered into a busy target without acknowledgement
/// waits for the target to record taking it. Wakterm submits a steer an agent
/// left in its input box when the turn ends, following it for 10 minutes.
const SEND_PROOF_WINDOW_MS: i64 = 10 * 60 * 1000;
use crate::wakterm::{
    ContractError, EventRead, FakeWakterm, TerminalResult, WaktermCli, WaktermCliError,
};

const QUEUED_DETAIL: &str = "The target is busy, so the message is queued and Panetone delivers it when the target is idle. Do not send it again.";
const STEERED_DETAIL: &str =
    "The target was busy, so the message was delivered into its active turn.";
const TAGGED_REPLY_DETAIL: &str = "The target owes you a reply until it sends a message tagged [panetone:reply]; it arrives as a separate cross-agent message, in this turn or a later one. Panetone reminds the target after its turns until then, and panetone cancel REQUEST_ID withdraws the request.";
const ASYNC_REPLY_DETAIL: &str = "This command does not wait for completion. Panetone mirrors the final to the source route's channel when it arrives and delivers the agent callback when the source agent is idle.";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServiceAck {
    pub accepted: bool,
    pub delivery_state: String,
    pub submitted: bool,
    /// What a `queued` state means for the sender, so `submitted: false`
    /// is not read as a failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_detail: Option<String>,
    pub reply_pending: bool,
    #[serde(default)]
    pub reply_mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steering_acknowledged: Option<bool>,
}

impl ServiceAck {
    fn queued(command: &SendCommand) -> Self {
        Self::new("queued", false, command)
    }

    fn submitted(command: &SendCommand) -> Self {
        Self::new("submitted", true, command)
    }

    fn steered(acknowledged: bool, command: &SendCommand) -> Self {
        Self {
            delivery_detail: Some(STEERED_DETAIL.into()),
            steering_acknowledged: Some(acknowledged),
            ..Self::new("steered", true, command)
        }
    }

    fn new(delivery_state: &str, submitted: bool, command: &SendCommand) -> Self {
        let (reply_mode, reply_detail) = reply_mode(command);
        Self {
            accepted: true,
            delivery_state: delivery_state.into(),
            submitted,
            delivery_detail: (delivery_state == "queued").then(|| QUEUED_DETAIL.into()),
            reply_pending: reply_detail.is_some() && (submitted || command.reply),
            reply_mode: reply_mode.into(),
            reply_detail: reply_detail.map(str::to_owned),
            steering_acknowledged: None,
        }
    }

    fn restore_explanation(mut self, command: &SendCommand) -> Self {
        if self.reply_mode.is_empty() {
            let (reply_mode, reply_detail) = reply_mode(command);
            self.reply_mode = reply_mode.into();
            self.reply_detail = self
                .reply_detail
                .or_else(|| reply_detail.map(str::to_owned));
        }
        self
    }
}

/// The first line of a request, shortened, to remind both sides what a
/// reply answers.
pub fn request_summary(message: &str) -> String {
    const MAX_CHARS: usize = 120;
    let line = message
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("");
    match line.char_indices().nth(MAX_CHARS) {
        Some((end, _)) => format!("{}…", &line[..end]),
        None => line.to_owned(),
    }
}

/// How the sender of a command receives its reply, if it asked for one.
fn reply_mode(command: &SendCommand) -> (&'static str, Option<&'static str>) {
    if command.reply {
        ("tagged_reply", Some(TAGGED_REPLY_DETAIL))
    } else if command.return_final {
        ("asynchronous_final_callback", Some(ASYNC_REPLY_DETAIL))
    } else {
        ("none", None)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultPoint {
    AfterClaim,
    AfterAuditEffect,
    AfterAuditCheckpoint,
    AfterAdmissionPrepared,
    AfterPromptEffect,
    AfterSteeringEffect,
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
    #[error("target admission failed in state {state:?}: {status:?}: {detail}")]
    AdmissionRejected {
        state: WorkflowState,
        status: AdmissionStatus,
        detail: String,
    },
    #[error("adapter operation became uncertain: {0}")]
    Adapter(String),
    #[error("the target cannot take input now: {0}")]
    TargetBlocked(String),
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

    async fn steer(&self, binding: &AgentBinding, prompt: String) -> Result<bool, ServiceError> {
        match self {
            Self::Fake(wakterm) => Ok(wakterm.steer(binding, prompt)),
            Self::Real(wakterm) => wakterm
                .steer(binding, &prompt)
                .await
                .map(|receipt| receipt.acknowledged())
                .map_err(|error| match error {
                    WaktermCliError::TargetBlocked(detail) => ServiceError::TargetBlocked(detail),
                    error => ServiceError::Adapter(error.to_string()),
                }),
        }
    }
}

impl ChannelBackend {
    async fn send(
        &self,
        item: &OutboxItem,
        signal_account: Option<&str>,
    ) -> Result<String, ServiceError> {
        match self {
            Self::Fake(channels) => channels
                .send(item)
                .map_err(|error| ServiceError::Adapter(error.to_string())),
            Self::Real(channels) => {
                // A rate limit posted nothing, and the client waits it out
                // before its next send, so a short one is retried rather than
                // failing the send it is part of.
                let mut attempt = 0;
                loop {
                    attempt += 1;
                    match channels.send_as(item, signal_account).await {
                        Ok(receipt) => return Ok(receipt.external_id),
                        Err(crate::channels::ChannelDeliveryError::RateLimited {
                            retry_after_secs,
                            ..
                        }) if attempt < 4 && retry_after_secs <= 60 => continue,
                        Err(error) => return Err(ServiceError::Adapter(error.to_string())),
                    }
                }
            }
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
            let routes = match chunk.kind {
                ChannelKind::Signal => self.store.list_routes().await?,
                ChannelKind::Telegram => Vec::new(),
            };
            let signal_account = signal_group_account(&routes, chunk.route_id, &chunk.destination);
            match self.channels.send(&chunk, signal_account).await {
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
                if record.workflow.state != WorkflowState::Submitted
                    && let Some(response) = record.response.as_ref()
                {
                    return Ok(serde_json::from_value::<ServiceAck>(response.clone())?
                        .restore_explanation(&record.command));
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
                let ack = ServiceAck::queued(&record.command);
                return persist_ack(&self.store, record, ack, now_ms).await;
            }
            WorkflowState::Submitted => {
                let ack = match record.response.as_ref() {
                    Some(response) => serde_json::from_value::<ServiceAck>(response.clone())?
                        .restore_explanation(&record.command),
                    None => ServiceAck::submitted(&record.command),
                };
                return self
                    .complete_submission(record, channel_destination(target)?, ack, now_ms)
                    .await;
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
            attachments: Vec::new(),
            actions: Vec::new(),
            state: OutboxState::Pending,
            attempts: 0,
            last_error: None,
            external_receipt: None,
            uncertain_attempts: 0,
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
        // Read before anything is typed, so the target's record of taking the
        // message cannot come before it.
        let after_sequence = self
            .store
            .get_metadata("wakterm_event_cursor".into())
            .await?
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_default();
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
                prompt.clone(),
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
        // A target whose keyboard is held by a dialog cannot be steered; the
        // request then waits in the busy queue like an unsteered one.
        let steering = if receipt.status == AdmissionStatus::Busy && record.command.steer {
            match self.wakterm.steer(&target, prompt.clone()).await {
                Err(ServiceError::TargetBlocked(_)) => None,
                result => Some(result),
            }
        } else {
            None
        };
        if let Some(steering) = steering {
            record.workflow.record_target_receipt(&receipt, &target)?;
            let acknowledged = match steering {
                Ok(acknowledged) => acknowledged,
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
                        "STEERING INDETERMINATE; see durable request state",
                        now_ms,
                    )
                    .await?;
                    return Err(error);
                }
            };
            self.faults.hit(FaultPoint::AfterSteeringEffect)?;
            if !acknowledged {
                record.proof = Some(DeliveryProof {
                    agent_id: target.agent_id.clone(),
                    after_sequence,
                    input_sha256: DeliveryProof::input_sha256(&prompt),
                    tab_expanded_sha256: DeliveryProof::tab_expanded_sha256(&prompt),
                    deadline_ms: now_ms + SEND_PROOF_WINDOW_MS,
                    detail: "it was typed into the busy agent's turn, but the agent did not acknowledge it".into(),
                });
            }
            record.workflow.submitted_target = Some(target);
            let ack = ServiceAck::steered(acknowledged, &record.command);
            record.response = Some(serde_json::to_value(&ack)?);
            transition(&self.store, &mut record, WorkflowState::Submitted, now_ms).await?;
            self.faults.hit(FaultPoint::AfterReceiptCheckpoint)?;
            return self
                .complete_submission(record, target_channel, ack, now_ms)
                .await;
        }
        let expected = record.workflow.state;
        let state = record.workflow.apply_target_receipt(&receipt, &target)?;
        // A queued workflow can be retried and submitted as soon as it is saved,
        // so its acknowledgement is saved with it rather than after the status post.
        let queued = ServiceAck::queued(&record.command);
        if state == WorkflowState::AwaitingTargetIdle {
            record.response = Some(serde_json::to_value(&queued)?);
        }
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
                Ok(queued)
            }
            WorkflowState::Submitted => {
                let ack = ServiceAck::submitted(&record.command);
                self.complete_submission(record, target_channel, ack, now_ms)
                    .await
            }
            WorkflowState::Indeterminate | WorkflowState::Failed => {
                let detail = receipt
                    .detail
                    .clone()
                    .unwrap_or_else(|| "no admission detail was provided".into());
                self.post_target_status(
                    &record,
                    target_channel,
                    "delivery-failed",
                    &format!("DELIVERY FAILED: {:?}: {detail}", receipt.status),
                    now_ms,
                )
                .await?;
                Err(ServiceError::AdmissionRejected {
                    state,
                    status: receipt.status,
                    detail,
                })
            }
            _ => Err(ServiceError::AdmissionFailed(state)),
        }
    }

    async fn complete_submission(
        &self,
        mut record: StoredWorkflow,
        target_channel: (ChannelKind, String),
        ack: ServiceAck,
        now_ms: i64,
    ) -> Result<ServiceAck, ServiceError> {
        if record.command.reply
            && let Some(target) = record.workflow.submitted_target.as_ref()
        {
            self.store
                .open_reply_debt(ReplyDebt {
                    request_id: record.command.id,
                    source: record.command.source.clone(),
                    source_route_id: record.workflow.source_route_id,
                    source_agent: record.workflow.observed_source.clone(),
                    target: record.command.target.clone(),
                    target_route_id: record.workflow.target_route_id,
                    target_agent_id: target.agent_id.clone(),
                    summary: request_summary(&record.command.message),
                    after_sequence: 0,
                    created_at_ms: now_ms,
                    state: "open".into(),
                })
                .await?;
        }
        let (purpose, detail) = if ack.delivery_state == "steered" {
            (
                "steered",
                if ack.steering_acknowledged == Some(true) {
                    "active-turn steering submitted and observer acknowledged"
                } else {
                    "active-turn steering submitted without acknowledgement; waiting for the agent to record it"
                },
            )
        } else {
            ("submitted", "prompt accepted by Wakterm")
        };
        self.post_target_status(&record, target_channel, purpose, detail, now_ms)
            .await?;
        record.response = Some(serde_json::to_value(&ack)?);
        if record.proof.is_some() {
            // Settled by settle_steered_sends once the target records it.
            record.updated_at_ms = now_ms;
            self.store
                .save_workflow(record, WorkflowState::Submitted)
                .await?;
            return Ok(ack);
        }
        transition(&self.store, &mut record, WorkflowState::Completed, now_ms).await?;
        Ok(ack)
    }

    /// Settles sends steered into a busy target without acknowledgement. One
    /// completes once the target records taking the message; if the deadline
    /// passes first, it becomes indeterminate and its sender is told the
    /// message may not have arrived.
    pub async fn settle_steered_sends(&self, now_ms: i64) -> Result<usize, ServiceError> {
        let mut settled = 0;
        for mut record in self.store.awaiting_send_proof().await? {
            let Some(proof) = record.proof.clone() else {
                continue;
            };
            let next = if self.store.input_recorded(proof.clone()).await? {
                WorkflowState::Completed
            } else if now_ms >= proof.deadline_ms {
                self.notify_unconfirmed_send(&record, &proof).await;
                WorkflowState::Indeterminate
            } else {
                continue;
            };
            transition(&self.store, &mut record, next, now_ms).await?;
            settled += 1;
        }
        Ok(settled)
    }

    async fn notify_unconfirmed_send(&self, record: &StoredWorkflow, proof: &DeliveryProof) {
        let command = &record.command;
        let mut text = format!(
            "[Panetone unconfirmed send] Your message to {} (request {}, \"{}\") may not have arrived: {}, and in {} minutes Wakterm recorded no sign of it taking the message. Resend it with a new request ID if it still matters.",
            command.target,
            command.id,
            request_summary(&command.message),
            proof.detail,
            SEND_PROOF_WINDOW_MS / 60_000,
        );
        if command.reply {
            text.push_str(&format!(
                " The reply you asked for stays owed until you withdraw it with panetone cancel {}.",
                command.id
            ));
        }
        let source = &record.workflow.observed_source;
        let id = EffectId::named(command.id, "unconfirmed-send-notice");
        let notice = match self.wakterm.admit(id, source, text.clone(), false, 0).await {
            Ok(receipt) if receipt.status == AdmissionStatus::Busy => {
                self.wakterm.steer(source, text).await.map(|_| ())
            }
            Ok(receipt) if receipt.status == AdmissionStatus::Accepted => Ok(()),
            Ok(receipt) => Err(ServiceError::Adapter(format!(
                "the sender was not prompted: {:?}",
                receipt.status
            ))),
            Err(error) => Err(error),
        };
        if let Err(error) = notice {
            tracing::warn!(error = %error, request = %command.id, "unconfirmed-send notice was not delivered");
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
            attachments: Vec::new(),
            actions: Vec::new(),
            state: OutboxState::Pending,
            attempts: 0,
            last_error: None,
            external_receipt: None,
            uncertain_attempts: 0,
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
                attachments: Vec::new(),
                actions: Vec::new(),
                state: OutboxState::Pending,
                attempts: returned.mirror.attempts,
                last_error: returned.mirror.last_error.clone(),
                external_receipt: None,
                uncertain_attempts: 0,
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

/// Where Panetone's own notices about a route go: its first channel that only
/// the owner reads, or its first channel when every one is shared.
pub(crate) fn channel_destination(route: &Route) -> Result<(ChannelKind, String), ServiceError> {
    private_destination(route)
        .or_else(|| route.channels.first().map(binding_destination))
        .ok_or_else(|| ServiceError::MissingChannel(route.title.clone()))
}

/// The route's first channel that admits no one but the owner.
pub(crate) fn private_destination(route: &Route) -> Option<(ChannelKind, String)> {
    route
        .channels
        .iter()
        .find(|binding| {
            !matches!(
                binding,
                ChannelBinding::Signal {
                    allow_members: true,
                    ..
                }
            )
        })
        .map(binding_destination)
}

fn binding_destination(binding: &ChannelBinding) -> (ChannelKind, String) {
    match binding {
        ChannelBinding::Telegram { topic_id } => (ChannelKind::Telegram, topic_id.to_string()),
        ChannelBinding::Signal { group_id, .. } => (ChannelKind::Signal, group_id.clone()),
    }
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
