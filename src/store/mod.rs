use std::fs::{self, File, OpenOptions};
use std::io::Read;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::domain::{
    AdmissionReceipt, AgentBinding, CallbackDelivery, ChannelAttachment, ChannelKind,
    DeliveryState, EffectId, MAX_CHANNEL_ATTACHMENT_BYTES, MAX_CHANNEL_ATTACHMENT_TOTAL_BYTES,
    MAX_CHANNEL_ATTACHMENTS, OutboxItem, OutboxState, Route, RouteId, SEMANTIC_HASH_KIND,
    SendCommand, Workflow, WorkflowId, WorkflowState, semantic_request_hash,
};
use crate::wakterm::{AgentCatalog, ApprovalRequest, EventRecord};

pub const SCHEMA_VERSION: i64 = 7;
const COMMAND_CAPACITY: usize = 128;
/// An agent replies with only this token to stay silent in a group chat.
const NO_REPLY: &str = "<panetone:no-reply>";

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("stored JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error("database schema {found} is newer than supported schema {supported}")]
    NewerSchema { found: i64, supported: i64 },
    #[error("database schema {found} predates the supported post-cutover schema {supported}")]
    OlderSchema { found: i64, supported: i64 },
    #[error("database path must not be a symbolic link")]
    Symlink,
    #[error("the store owner task stopped")]
    Closed,
    #[error("durable state transition conflict: {0}")]
    Conflict(String),
    #[error("store owner thread failed to start")]
    Startup,
}

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct StoredWorkflow {
    pub command: SendCommand,
    pub workflow: Workflow,
    pub response: Option<Value>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ClaimResult {
    New(StoredWorkflow),
    Existing(StoredWorkflow),
    Conflict { state: String },
    Tombstone { same_content: bool, state: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DestinationDelivery {
    pub effect_id: EffectId,
    pub state: DeliveryState,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub external_receipt: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ReturnDelivery {
    pub workflow_id: WorkflowId,
    pub result: Value,
    pub agent: CallbackDelivery,
    pub mirror: DestinationDelivery,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InboxItem {
    pub id: EffectId,
    pub channel: ChannelKind,
    pub external_id: String,
    #[serde(default)]
    pub destination: String,
    #[serde(default)]
    pub sender_id: Option<String>,
    #[serde(default)]
    pub sender: Option<String>,
    #[serde(default)]
    pub reply_to_external_id: Option<String>,
    pub body: String,
    pub state: String,
    pub created_at_ms: i64,
    /// Wakterm's admission receipt, kept so a lost delivery can be traced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<AdmissionReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steering_acknowledged: Option<bool>,
    /// What proves the message reached its agent if Wakterm cannot confirm
    /// the delivery itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proof: Option<DeliveryProof>,
}

/// Wakterm recording input with the message's hash for the target agent
/// after the delivery began proves the agent received it. Any incarnation
/// counts: input typed just before a resumed process is observed is reported
/// under its new incarnation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeliveryProof {
    pub agent_id: String,
    pub after_sequence: u64,
    /// The SHA-256 of the trimmed message, as Wakterm's `input_sha256`.
    pub input_sha256: String,
    pub deadline_ms: i64,
    /// Why delivery was unconfirmed, for the sender's notice if no proof comes.
    pub detail: String,
}

impl DeliveryProof {
    pub fn input_sha256(text: &str) -> String {
        format!("{:x}", Sha256::digest(text.trim().as_bytes()))
    }
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

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct StoreStatus {
    pub schema_version: u64,
    pub workflows: u64,
    pub awaiting_target_idle: u64,
    pub failed_workflows: u64,
    pub indeterminate_workflows: u64,
    pub pending_returns: u64,
    pub unresolved_returns: u64,
    pub pending_outbox: u64,
    pub failed_outbox: u64,
    pub indeterminate_outbox: u64,
    pub pending_inbox: u64,
    pub tombstones: u64,
    pub unrouted_agent_events: u64,
    pub observer_failure_events: u64,
    pub event_cursor: Option<u64>,
    pub event_cursor_gap: Option<EventCursorGap>,
    pub telegram_update_offset: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventIngestOutcome {
    pub recorded: u64,
    pub visible_outputs: u64,
    pub unrouted: u64,
    pub next_after_sequence: u64,
    pub last_agents: Vec<RouteAgent>,
}

impl EventIngestOutcome {
    fn remember_last_agent(&mut self, candidate: &RouteAgent) {
        if let Some(existing) = self
            .last_agents
            .iter_mut()
            .find(|existing| existing.route_id == candidate.route_id)
        {
            *existing = candidate.clone();
        } else {
            self.last_agents.push(candidate.clone());
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RouteAgent {
    pub route_id: RouteId,
    pub agent: AgentBinding,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StoredAgentOutput {
    pub event: EventRecord,
    pub route_id: Option<RouteId>,
    pub disposition: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OutputDispositionSnapshot {
    pub event_cursor: Option<u64>,
    pub output: Option<StoredAgentOutput>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredApproval {
    pub request: ApprovalRequest,
    pub route_id: RouteId,
}

#[derive(Clone)]
pub struct StoreHandle {
    sender: mpsc::Sender<Command>,
    owner: Arc<Mutex<Option<JoinHandle<()>>>>,
}

enum Command {
    Claim {
        command: SendCommand,
        source_route_id: RouteId,
        target_route_id: RouteId,
        source: AgentBinding,
        target: AgentBinding,
        now_ms: i64,
        reply: oneshot::Sender<StoreResult<ClaimResult>>,
    },
    GetWorkflow {
        id: WorkflowId,
        reply: oneshot::Sender<StoreResult<Option<StoredWorkflow>>>,
    },
    SaveWorkflow {
        record: Box<StoredWorkflow>,
        expected: WorkflowState,
        reply: oneshot::Sender<StoreResult<()>>,
    },
    AwaitingTarget {
        reply: oneshot::Sender<StoreResult<Vec<StoredWorkflow>>>,
    },
    SaveRoute {
        route: Route,
        now_ms: i64,
        reply: oneshot::Sender<StoreResult<()>>,
    },
    GetRoute {
        id: RouteId,
        reply: oneshot::Sender<StoreResult<Option<Route>>>,
    },
    ListRoutes {
        reply: oneshot::Sender<StoreResult<Vec<Route>>>,
    },
    RegisterReturn {
        record: ReturnDelivery,
        reply: oneshot::Sender<StoreResult<()>>,
    },
    GetReturn {
        workflow_id: WorkflowId,
        reply: oneshot::Sender<StoreResult<Option<ReturnDelivery>>>,
    },
    SaveReturn {
        record: ReturnDelivery,
        reply: oneshot::Sender<StoreResult<()>>,
    },
    PendingReturns {
        reply: oneshot::Sender<StoreResult<Vec<ReturnDelivery>>>,
    },
    HasUnresolvedTerminal {
        reply: oneshot::Sender<StoreResult<bool>>,
    },
    EnqueueOutbox {
        workflow_id: Option<WorkflowId>,
        item: OutboxItem,
        now_ms: i64,
        reply: oneshot::Sender<StoreResult<Vec<OutboxItem>>>,
    },
    SaveOutbox {
        item: OutboxItem,
        now_ms: i64,
        reply: oneshot::Sender<StoreResult<()>>,
    },
    PendingOutbox {
        reply: oneshot::Sender<StoreResult<Vec<OutboxItem>>>,
    },
    LatestSenderName {
        channel: ChannelKind,
        sender_ids: Vec<String>,
        reply: oneshot::Sender<StoreResult<Option<String>>>,
    },
    FindDeliveredOutbox {
        channel: ChannelKind,
        destination: String,
        external_receipt: String,
        reply: oneshot::Sender<StoreResult<Option<OutboxItem>>>,
    },
    AcceptInbox {
        item: InboxItem,
        reply: oneshot::Sender<StoreResult<bool>>,
    },
    InboxInState {
        state: &'static str,
        reply: oneshot::Sender<StoreResult<Vec<InboxItem>>>,
    },
    InputRecorded {
        proof: DeliveryProof,
        reply: oneshot::Sender<StoreResult<bool>>,
    },
    SaveInbox {
        item: InboxItem,
        expected_state: String,
        route_preference: Option<(RouteId, ChannelKind)>,
        reply: oneshot::Sender<StoreResult<()>>,
    },
    SetMetadata {
        key: String,
        value: String,
        reply: oneshot::Sender<StoreResult<()>>,
    },
    GetMetadata {
        key: String,
        reply: oneshot::Sender<StoreResult<Option<String>>>,
    },
    InitializeEventCursor {
        sequence: u64,
        reply: oneshot::Sender<StoreResult<u64>>,
    },
    RebaselineEventCursor {
        sequence: u64,
        reply: oneshot::Sender<StoreResult<()>>,
    },
    IngestAgentEvents {
        expected: u64,
        next: u64,
        events: Vec<EventRecord>,
        route_agents: Vec<RouteAgent>,
        now_ms: i64,
        reply: oneshot::Sender<StoreResult<EventIngestOutcome>>,
    },
    RecoverEventCursorGap {
        gap: EventCursorGap,
        catalog: AgentCatalog,
        reply: oneshot::Sender<StoreResult<()>>,
    },
    OutputDisposition {
        agent_id: String,
        incarnation_id: String,
        after_sequence: u64,
        expected_text: String,
        reply: oneshot::Sender<StoreResult<OutputDispositionSnapshot>>,
    },
    GetApproval {
        request_id: String,
        reply: oneshot::Sender<StoreResult<Option<StoredApproval>>>,
    },
    Status {
        reply: oneshot::Sender<StoreResult<StoreStatus>>,
    },
    Shutdown {
        reply: oneshot::Sender<StoreResult<()>>,
    },
}

impl StoreHandle {
    pub fn open(path: impl Into<PathBuf>) -> StoreResult<Self> {
        let path = path.into();
        let (sender, receiver) = mpsc::channel(COMMAND_CAPACITY);
        let (ready_sender, ready_receiver) = std::sync::mpsc::sync_channel(1);
        let owner = thread::Builder::new()
            .name("panetone-store".into())
            .spawn(move || {
                let connection = open_database(&path);
                match connection {
                    Ok(mut connection) => {
                        let recovered = recover(&mut connection);
                        if let Err(error) = recovered {
                            let _ = ready_sender.send(Err(error));
                            return;
                        }
                        if ready_sender.send(Ok(())).is_err() {
                            return;
                        }
                        owner_loop(connection, receiver);
                    }
                    Err(error) => {
                        let _ = ready_sender.send(Err(error));
                    }
                }
            })?;
        ready_receiver.recv().map_err(|_| StoreError::Startup)??;
        Ok(Self {
            sender,
            owner: Arc::new(Mutex::new(Some(owner))),
        })
    }

    async fn request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<StoreResult<T>>) -> Command,
    ) -> StoreResult<T> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(build(reply))
            .await
            .map_err(|_| StoreError::Closed)?;
        response.await.map_err(|_| StoreError::Closed)?
    }

    pub async fn claim(
        &self,
        command: SendCommand,
        source_route_id: RouteId,
        target_route_id: RouteId,
        source: AgentBinding,
        target: AgentBinding,
        now_ms: i64,
    ) -> StoreResult<ClaimResult> {
        self.request(|reply| Command::Claim {
            command,
            source_route_id,
            target_route_id,
            source,
            target,
            now_ms,
            reply,
        })
        .await
    }

    pub async fn get_workflow(&self, id: WorkflowId) -> StoreResult<Option<StoredWorkflow>> {
        self.request(|reply| Command::GetWorkflow { id, reply })
            .await
    }

    pub async fn save_workflow(
        &self,
        record: StoredWorkflow,
        expected: WorkflowState,
    ) -> StoreResult<()> {
        self.request(|reply| Command::SaveWorkflow {
            record: Box::new(record),
            expected,
            reply,
        })
        .await
    }

    pub async fn awaiting_target(&self) -> StoreResult<Vec<StoredWorkflow>> {
        self.request(|reply| Command::AwaitingTarget { reply })
            .await
    }

    pub async fn save_route(&self, route: Route, now_ms: i64) -> StoreResult<()> {
        self.request(|reply| Command::SaveRoute {
            route,
            now_ms,
            reply,
        })
        .await
    }

    pub async fn get_route(&self, id: RouteId) -> StoreResult<Option<Route>> {
        self.request(|reply| Command::GetRoute { id, reply }).await
    }

    pub async fn list_routes(&self) -> StoreResult<Vec<Route>> {
        self.request(|reply| Command::ListRoutes { reply }).await
    }

    pub async fn register_return(&self, record: ReturnDelivery) -> StoreResult<()> {
        self.request(|reply| Command::RegisterReturn { record, reply })
            .await
    }

    pub async fn get_return(&self, workflow_id: WorkflowId) -> StoreResult<Option<ReturnDelivery>> {
        self.request(|reply| Command::GetReturn { workflow_id, reply })
            .await
    }

    pub async fn save_return(&self, record: ReturnDelivery) -> StoreResult<()> {
        self.request(|reply| Command::SaveReturn { record, reply })
            .await
    }

    pub async fn pending_returns(&self) -> StoreResult<Vec<ReturnDelivery>> {
        self.request(|reply| Command::PendingReturns { reply })
            .await
    }

    pub async fn has_unresolved_terminal(&self) -> StoreResult<bool> {
        self.request(|reply| Command::HasUnresolvedTerminal { reply })
            .await
    }

    pub async fn enqueue_outbox(
        &self,
        workflow_id: Option<WorkflowId>,
        item: OutboxItem,
        now_ms: i64,
    ) -> StoreResult<Vec<OutboxItem>> {
        self.request(|reply| Command::EnqueueOutbox {
            workflow_id,
            item,
            now_ms,
            reply,
        })
        .await
    }

    pub async fn save_outbox(&self, item: OutboxItem, now_ms: i64) -> StoreResult<()> {
        self.request(|reply| Command::SaveOutbox {
            item,
            now_ms,
            reply,
        })
        .await
    }

    pub async fn pending_outbox(&self) -> StoreResult<Vec<OutboxItem>> {
        self.request(|reply| Command::PendingOutbox { reply }).await
    }

    pub async fn find_outbox_agent(
        &self,
        channel: ChannelKind,
        destination: String,
        external_receipt: String,
    ) -> StoreResult<Option<AgentBinding>> {
        Ok(self
            .find_delivered_outbox(channel, destination, external_receipt)
            .await?
            .and_then(|item| item.source_agent))
    }

    /// The display name of the latest inbound message from any of these
    /// sender IDs, which a Signal quote names its author by.
    pub async fn latest_sender_name(
        &self,
        channel: ChannelKind,
        sender_ids: Vec<String>,
    ) -> StoreResult<Option<String>> {
        self.request(|reply| Command::LatestSenderName {
            channel,
            sender_ids,
            reply,
        })
        .await
    }

    /// The delivered outbox item whose channel message has this receipt.
    pub async fn find_delivered_outbox(
        &self,
        channel: ChannelKind,
        destination: String,
        external_receipt: String,
    ) -> StoreResult<Option<OutboxItem>> {
        self.request(|reply| Command::FindDeliveredOutbox {
            channel,
            destination,
            external_receipt,
            reply,
        })
        .await
    }

    pub async fn accept_inbox(&self, item: InboxItem) -> StoreResult<bool> {
        self.request(|reply| Command::AcceptInbox { item, reply })
            .await
    }

    pub async fn pending_inbox(&self) -> StoreResult<Vec<InboxItem>> {
        self.request(|reply| Command::InboxInState {
            state: "pending",
            reply,
        })
        .await
    }

    /// Inbound messages waiting for proof that their agent received them.
    pub async fn awaiting_inbox(&self) -> StoreResult<Vec<InboxItem>> {
        self.request(|reply| Command::InboxInState {
            state: "awaiting_proof",
            reply,
        })
        .await
    }

    pub async fn input_recorded(&self, proof: DeliveryProof) -> StoreResult<bool> {
        self.request(|reply| Command::InputRecorded { proof, reply })
            .await
    }

    pub async fn save_inbox(
        &self,
        item: InboxItem,
        expected_state: impl Into<String>,
        route_preference: Option<(RouteId, ChannelKind)>,
    ) -> StoreResult<()> {
        self.request(|reply| Command::SaveInbox {
            item,
            expected_state: expected_state.into(),
            route_preference,
            reply,
        })
        .await
    }

    pub async fn set_metadata(&self, key: String, value: String) -> StoreResult<()> {
        self.request(|reply| Command::SetMetadata { key, value, reply })
            .await
    }

    pub async fn get_metadata(&self, key: String) -> StoreResult<Option<String>> {
        self.request(|reply| Command::GetMetadata { key, reply })
            .await
    }

    pub async fn initialize_event_cursor(&self, sequence: u64) -> StoreResult<u64> {
        self.request(|reply| Command::InitializeEventCursor { sequence, reply })
            .await
    }

    pub async fn rebaseline_event_cursor(&self, sequence: u64) -> StoreResult<()> {
        self.request(|reply| Command::RebaselineEventCursor { sequence, reply })
            .await
    }

    pub async fn ingest_agent_events(
        &self,
        expected: u64,
        next: u64,
        events: Vec<EventRecord>,
        route_agents: Vec<RouteAgent>,
        now_ms: i64,
    ) -> StoreResult<EventIngestOutcome> {
        self.request(|reply| Command::IngestAgentEvents {
            expected,
            next,
            events,
            route_agents,
            now_ms,
            reply,
        })
        .await
    }

    pub async fn recover_event_cursor_gap(
        &self,
        gap: EventCursorGap,
        catalog: AgentCatalog,
    ) -> StoreResult<()> {
        self.request(|reply| Command::RecoverEventCursorGap {
            gap,
            catalog,
            reply,
        })
        .await
    }

    pub async fn output_disposition(
        &self,
        agent_id: String,
        incarnation_id: String,
        after_sequence: u64,
        expected_text: String,
    ) -> StoreResult<OutputDispositionSnapshot> {
        self.request(|reply| Command::OutputDisposition {
            agent_id,
            incarnation_id,
            after_sequence,
            expected_text,
            reply,
        })
        .await
    }

    pub async fn get_approval(&self, request_id: String) -> StoreResult<Option<StoredApproval>> {
        self.request(|reply| Command::GetApproval { request_id, reply })
            .await
    }

    pub async fn status(&self) -> StoreResult<StoreStatus> {
        self.request(|reply| Command::Status { reply }).await
    }

    pub async fn shutdown(&self) -> StoreResult<()> {
        self.request(|reply| Command::Shutdown { reply }).await?;
        let owner = self.owner.lock().map_err(|_| StoreError::Closed)?.take();
        if let Some(owner) = owner {
            tokio::task::spawn_blocking(move || owner.join())
                .await
                .map_err(|_| StoreError::Closed)?
                .map_err(|_| StoreError::Closed)?;
        }
        Ok(())
    }
}

fn owner_loop(mut connection: Connection, mut receiver: mpsc::Receiver<Command>) {
    while let Some(command) = receiver.blocking_recv() {
        let shutdown = matches!(command, Command::Shutdown { .. });
        handle_command(&mut connection, command);
        if shutdown {
            break;
        }
    }
}

fn handle_command(connection: &mut Connection, command: Command) {
    match command {
        Command::Claim {
            command,
            source_route_id,
            target_route_id,
            source,
            target,
            now_ms,
            reply,
        } => send_reply(
            reply,
            claim(
                connection,
                command,
                source_route_id,
                target_route_id,
                source,
                target,
                now_ms,
            ),
        ),
        Command::GetWorkflow { id, reply } => send_reply(reply, get_workflow(connection, id)),
        Command::SaveWorkflow {
            record,
            expected,
            reply,
        } => send_reply(reply, save_workflow(connection, &record, expected)),
        Command::AwaitingTarget { reply } => send_reply(
            reply,
            workflows_by_state(connection, WorkflowState::AwaitingTargetIdle),
        ),
        Command::SaveRoute {
            route,
            now_ms,
            reply,
        } => send_reply(reply, save_route(connection, &route, now_ms)),
        Command::GetRoute { id, reply } => send_reply(reply, get_route(connection, id)),
        Command::ListRoutes { reply } => send_reply(reply, list_routes(connection)),
        Command::RegisterReturn { record, reply } => {
            send_reply(reply, register_return(connection, &record))
        }
        Command::GetReturn { workflow_id, reply } => {
            send_reply(reply, get_return(connection, workflow_id))
        }
        Command::SaveReturn { record, reply } => {
            send_reply(reply, save_return(connection, &record))
        }
        Command::PendingReturns { reply } => send_reply(reply, pending_returns(connection)),
        Command::HasUnresolvedTerminal { reply } => {
            send_reply(reply, has_unresolved_terminal(connection))
        }
        Command::EnqueueOutbox {
            workflow_id,
            item,
            now_ms,
            reply,
        } => send_reply(
            reply,
            enqueue_outbox_chunks(connection, workflow_id, item, now_ms),
        ),
        Command::SaveOutbox {
            item,
            now_ms,
            reply,
        } => send_reply(reply, save_outbox(connection, &item, now_ms)),
        Command::PendingOutbox { reply } => send_reply(reply, pending_outbox(connection)),
        Command::LatestSenderName {
            channel,
            sender_ids,
            reply,
        } => send_reply(reply, latest_sender_name(connection, channel, &sender_ids)),
        Command::FindDeliveredOutbox {
            channel,
            destination,
            external_receipt,
            reply,
        } => send_reply(
            reply,
            find_delivered_outbox(connection, channel, &destination, &external_receipt),
        ),
        Command::AcceptInbox { item, reply } => send_reply(reply, accept_inbox(connection, &item)),
        Command::InboxInState { state, reply } => {
            send_reply(reply, inbox_in_state(connection, state))
        }
        Command::InputRecorded { proof, reply } => {
            send_reply(reply, input_recorded(connection, &proof))
        }
        Command::SaveInbox {
            item,
            expected_state,
            route_preference,
            reply,
        } => send_reply(
            reply,
            save_inbox(connection, &item, &expected_state, route_preference),
        ),
        Command::SetMetadata { key, value, reply } => {
            send_reply(reply, set_metadata(connection, &key, &value))
        }
        Command::GetMetadata { key, reply } => send_reply(reply, get_metadata(connection, &key)),
        Command::InitializeEventCursor { sequence, reply } => {
            send_reply(reply, initialize_event_cursor(connection, sequence))
        }
        Command::RebaselineEventCursor { sequence, reply } => {
            send_reply(reply, rebaseline_event_cursor(connection, sequence))
        }
        Command::IngestAgentEvents {
            expected,
            next,
            events,
            route_agents,
            now_ms,
            reply,
        } => send_reply(
            reply,
            ingest_agent_events(connection, expected, next, &events, &route_agents, now_ms),
        ),
        Command::RecoverEventCursorGap {
            gap,
            catalog,
            reply,
        } => send_reply(reply, recover_event_cursor_gap(connection, &gap, &catalog)),
        Command::OutputDisposition {
            agent_id,
            incarnation_id,
            after_sequence,
            expected_text,
            reply,
        } => send_reply(
            reply,
            output_disposition(
                connection,
                &agent_id,
                &incarnation_id,
                after_sequence,
                &expected_text,
            ),
        ),
        Command::GetApproval { request_id, reply } => {
            send_reply(reply, get_approval(connection, &request_id))
        }
        Command::Status { reply } => send_reply(reply, status(connection)),
        Command::Shutdown { reply } => send_reply(reply, Ok(())),
    }
}

fn send_reply<T>(reply: oneshot::Sender<StoreResult<T>>, result: StoreResult<T>) {
    let _ = reply.send(result);
}

fn open_database(path: &Path) -> StoreResult<Connection> {
    if let Ok(metadata) = fs::symlink_metadata(path)
        && metadata.file_type().is_symlink()
    {
        return Err(StoreError::Symlink);
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if !path.exists() {
        OpenOptions::new().write(true).create_new(true).open(path)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    let mut connection = Connection::open(path)?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    migrate_schema(&mut connection)?;
    Ok(connection)
}

pub(crate) fn migrate_schema(connection: &mut Connection) -> StoreResult<()> {
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > SCHEMA_VERSION {
        return Err(StoreError::NewerSchema {
            found: version,
            supported: SCHEMA_VERSION,
        });
    }
    if version == 0 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE routes (
                 route_id TEXT PRIMARY KEY,
                 route_json TEXT NOT NULL,
                 updated_at_ms INTEGER NOT NULL
             );
             CREATE TABLE idempotency_tombstones (
                 request_id TEXT PRIMARY KEY,
                 semantic_hash TEXT NOT NULL,
                 hash_kind TEXT NOT NULL DEFAULT 'semantic_v1',
                 terminal_state TEXT NOT NULL,
                 created_at_ms INTEGER NOT NULL,
                 completed_at_ms INTEGER
             );
             CREATE TABLE workflows (
                 request_id TEXT PRIMARY KEY REFERENCES idempotency_tombstones(request_id),
                 semantic_hash TEXT NOT NULL,
                 state TEXT NOT NULL,
                 record_json TEXT NOT NULL,
                 created_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL
             );
             CREATE INDEX workflows_state ON workflows(state, updated_at_ms);
             CREATE TABLE return_deliveries (
                 request_id TEXT PRIMARY KEY REFERENCES idempotency_tombstones(request_id),
                 agent_state TEXT NOT NULL,
                 mirror_state TEXT NOT NULL,
                 record_json TEXT NOT NULL,
                 created_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL
             );
             CREATE INDEX return_delivery_state
                 ON return_deliveries(agent_state, mirror_state, updated_at_ms);
             CREATE TABLE outbox (
                 effect_id TEXT PRIMARY KEY,
                 request_id TEXT,
                 channel TEXT NOT NULL,
                 destination TEXT NOT NULL,
                 state TEXT NOT NULL,
                 record_json TEXT NOT NULL,
                 created_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL
             );
             CREATE INDEX outbox_state ON outbox(state, created_at_ms);
             CREATE TABLE inbox (
                 effect_id TEXT PRIMARY KEY,
                 channel TEXT NOT NULL,
                 external_id TEXT NOT NULL,
                 state TEXT NOT NULL,
                 record_json TEXT NOT NULL,
                 created_at_ms INTEGER NOT NULL,
                 UNIQUE(channel, external_id)
             );
             CREATE TABLE metadata (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );
             CREATE TABLE agent_events (
                 sequence INTEGER PRIMARY KEY,
                 event_id TEXT NOT NULL,
                 agent_id TEXT NOT NULL,
                 incarnation_id TEXT NOT NULL,
                 kind TEXT NOT NULL,
                 route_id TEXT,
                 state TEXT NOT NULL,
                 record_json TEXT NOT NULL,
                 created_at_ms INTEGER NOT NULL,
                 UNIQUE(event_id, incarnation_id)
             );
             CREATE INDEX agent_events_state ON agent_events(state, sequence);
             CREATE INDEX agent_events_agent
                 ON agent_events(agent_id, incarnation_id, sequence);
             PRAGMA user_version = 7;",
        )?;
        transaction.commit()?;
    }
    if version == 6 {
        let transaction = connection.transaction()?;
        transaction.execute(
            "UPDATE routes
             SET route_json = json_remove(route_json, '$.agent', '$.status')",
            [],
        )?;
        transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        transaction.commit()?;
    }
    if version != 0 && version < SCHEMA_VERSION {
        if version == 6 {
            return Ok(());
        }
        return Err(StoreError::OlderSchema {
            found: version,
            supported: SCHEMA_VERSION,
        });
    }
    Ok(())
}

fn recover(connection: &mut Connection) -> StoreResult<()> {
    let prepared = workflows_by_state(connection, WorkflowState::AdmissionPrepared)?;
    for mut record in prepared {
        let expected = record.workflow.state;
        record.workflow.state = WorkflowState::Indeterminate;
        record.updated_at_ms = record.updated_at_ms.saturating_add(1);
        save_workflow(connection, &record, expected)?;
    }

    let mut statement = connection
        .prepare("SELECT record_json FROM return_deliveries WHERE agent_state = 'delivering'")?;
    let records = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    for json in records {
        let mut record: ReturnDelivery = serde_json::from_str(&json)?;
        record.agent.recover_after_restart();
        record.updated_at_ms = record.updated_at_ms.saturating_add(1);
        save_return(connection, &record)?;
    }
    connection.execute(
        // A send interrupted by a restart may have been posted.
        "UPDATE outbox SET state = 'pending',
             record_json = json_set(record_json, '$.state', 'pending',
                 '$.uncertain_attempts',
                 coalesce(json_extract(record_json, '$.uncertain_attempts'), 0) + 1)
         WHERE state = 'delivering'",
        [],
    )?;
    connection.execute(
        // An admission interrupted by a restart may have been typed, so it
        // waits for proof like any other unconfirmed delivery.
        "UPDATE inbox SET state = CASE WHEN json_extract(record_json, '$.proof') IS NULL
                 THEN 'indeterminate' ELSE 'awaiting_proof' END,
             record_json = json_set(record_json, '$.state',
                 CASE WHEN json_extract(record_json, '$.proof') IS NULL
                 THEN 'indeterminate' ELSE 'awaiting_proof' END)
         WHERE state = 'admission_prepared'",
        [],
    )?;
    Ok(())
}

fn claim(
    connection: &mut Connection,
    command: SendCommand,
    source_route_id: RouteId,
    target_route_id: RouteId,
    source: AgentBinding,
    target: AgentBinding,
    now_ms: i64,
) -> StoreResult<ClaimResult> {
    let request_id = command.id.to_string();
    let semantic_hash = semantic_request_hash(&command);
    if let Some(existing) = get_workflow(connection, command.id)? {
        return Ok(if existing.workflow.semantic_hash == semantic_hash {
            ClaimResult::Existing(existing)
        } else {
            ClaimResult::Conflict {
                state: state_name(existing.workflow.state),
            }
        });
    }
    if let Some((stored_hash, hash_kind, state)) = connection
        .query_row(
            "SELECT semantic_hash, hash_kind, terminal_state FROM idempotency_tombstones
             WHERE request_id = ?1",
            params![request_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?
    {
        return Ok(ClaimResult::Tombstone {
            same_content: hash_kind == SEMANTIC_HASH_KIND && stored_hash == semantic_hash,
            state,
        });
    }
    let workflow = Workflow {
        id: command.id,
        semantic_hash: semantic_hash.clone(),
        source_route_id,
        target_route_id,
        target_effect_id: EffectId::target_admission(command.id),
        observed_source: source,
        observed_target: target,
        submitted_target: None,
        target_admission_receipt: None,
        state: WorkflowState::Claimed,
    };
    let record = StoredWorkflow {
        command,
        workflow,
        response: None,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
    };
    let transaction = connection.transaction()?;
    transaction.execute(
        "INSERT INTO idempotency_tombstones(
             request_id, semantic_hash, hash_kind, terminal_state, created_at_ms, completed_at_ms
         ) VALUES (?1, ?2, ?3, 'claimed', ?4, NULL)",
        params![request_id, semantic_hash, SEMANTIC_HASH_KIND, now_ms],
    )?;
    transaction.execute(
        "INSERT INTO workflows(
             request_id, semantic_hash, state, record_json, created_at_ms, updated_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
        params![
            request_id,
            semantic_hash,
            state_name(record.workflow.state),
            serde_json::to_string(&record)?,
            now_ms,
        ],
    )?;
    transaction.commit()?;
    Ok(ClaimResult::New(record))
}

fn get_workflow(connection: &Connection, id: WorkflowId) -> StoreResult<Option<StoredWorkflow>> {
    let json = connection
        .query_row(
            "SELECT record_json FROM workflows WHERE request_id = ?1",
            params![id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    json.map(|value| serde_json::from_str(&value).map_err(StoreError::from))
        .transpose()
}

fn save_workflow(
    connection: &mut Connection,
    record: &StoredWorkflow,
    expected: WorkflowState,
) -> StoreResult<()> {
    let state = state_name(record.workflow.state);
    let transaction = connection.transaction()?;
    let changed = transaction.execute(
        "UPDATE workflows SET state = ?2, record_json = ?3, updated_at_ms = ?4
         WHERE request_id = ?1 AND state = ?5",
        params![
            record.command.id.to_string(),
            state,
            serde_json::to_string(record)?,
            record.updated_at_ms,
            state_name(expected),
        ],
    )?;
    if changed != 1 {
        return Err(StoreError::Conflict(format!(
            "workflow {} was not in {}",
            record.command.id,
            state_name(expected)
        )));
    }
    if matches!(
        record.workflow.state,
        WorkflowState::Completed
            | WorkflowState::Failed
            | WorkflowState::Indeterminate
            | WorkflowState::Cancelled
    ) {
        transaction.execute(
            "UPDATE idempotency_tombstones
             SET terminal_state = ?2, completed_at_ms = ?3 WHERE request_id = ?1",
            params![record.command.id.to_string(), state, record.updated_at_ms],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn workflows_by_state(
    connection: &Connection,
    state: WorkflowState,
) -> StoreResult<Vec<StoredWorkflow>> {
    let mut statement = connection
        .prepare("SELECT record_json FROM workflows WHERE state = ?1 ORDER BY created_at_ms")?;
    let json = statement
        .query_map(params![state_name(state)], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    json.into_iter()
        .map(|value| serde_json::from_str(&value).map_err(StoreError::from))
        .collect()
}

fn save_route(connection: &Connection, route: &Route, now_ms: i64) -> StoreResult<()> {
    connection.execute(
        "INSERT INTO routes(route_id, route_json, updated_at_ms) VALUES (?1, ?2, ?3)
         ON CONFLICT(route_id) DO UPDATE SET
             route_json = excluded.route_json, updated_at_ms = excluded.updated_at_ms",
        params![route.id.to_string(), serde_json::to_string(route)?, now_ms],
    )?;
    Ok(())
}

fn get_route(connection: &Connection, id: RouteId) -> StoreResult<Option<Route>> {
    let json = connection
        .query_row(
            "SELECT route_json FROM routes WHERE route_id = ?1",
            params![id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    json.map(|value| serde_json::from_str(&value).map_err(StoreError::from))
        .transpose()
}

fn list_routes(connection: &Connection) -> StoreResult<Vec<Route>> {
    let mut statement =
        connection.prepare("SELECT route_json FROM routes ORDER BY lower(json_extract(route_json, '$.title')), route_id")?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|value| serde_json::from_str(&value).map_err(StoreError::from))
        .collect()
}

fn initialize_event_cursor(connection: &Connection, sequence: u64) -> StoreResult<u64> {
    connection.execute(
        "INSERT INTO metadata(key, value) VALUES (\"wakterm_event_cursor\", ?1)
         ON CONFLICT(key) DO NOTHING",
        params![sequence.to_string()],
    )?;
    event_cursor(connection)?
        .ok_or_else(|| StoreError::Conflict("Wakterm event cursor was not initialized".into()))
}

fn event_cursor(connection: &Connection) -> StoreResult<Option<u64>> {
    get_metadata(connection, "wakterm_event_cursor")?
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| StoreError::Conflict("stored Wakterm event cursor is invalid".into()))
        })
        .transpose()
}

fn output_disposition(
    connection: &Connection,
    agent_id: &str,
    incarnation_id: &str,
    after_sequence: u64,
    expected_text: &str,
) -> StoreResult<OutputDispositionSnapshot> {
    let stored = connection
        .query_row(
            "SELECT route_id, state, record_json
             FROM agent_events
             WHERE agent_id = ?1 AND incarnation_id = ?2
               AND sequence > ?3 AND kind = 'assistant_message'
               AND json_extract(record_json, '$.text') = ?4
             ORDER BY sequence LIMIT 1",
            params![agent_id, incarnation_id, after_sequence, expected_text],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    let output = stored
        .map(
            |(route_id, disposition, record_json)| -> StoreResult<StoredAgentOutput> {
                let route_id = route_id
                    .map(|value| {
                        Uuid::parse_str(&value).map(RouteId::new).map_err(|_| {
                            StoreError::Conflict("stored event route ID is invalid".into())
                        })
                    })
                    .transpose()?;
                Ok(StoredAgentOutput {
                    event: serde_json::from_str(&record_json)?,
                    route_id,
                    disposition,
                })
            },
        )
        .transpose()?;
    Ok(OutputDispositionSnapshot {
        event_cursor: event_cursor(connection)?,
        output,
    })
}

fn get_approval(connection: &Connection, request_id: &str) -> StoreResult<Option<StoredApproval>> {
    let row = connection
        .query_row(
            "SELECT route_id, record_json
             FROM agent_events
             WHERE json_extract(record_json, '$.approval.request_id') = ?1
               AND route_id IS NOT NULL
             ORDER BY sequence DESC
             LIMIT 1",
            params![request_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    row.map(|(route_id, record_json)| {
        let event: EventRecord = serde_json::from_str(&record_json)?;
        let request = event
            .approval()
            .map_err(|detail| StoreError::Conflict(detail.into()))?
            .ok_or_else(|| StoreError::Conflict("stored event is not an approval".into()))?;
        let route_id = route_id
            .parse::<Uuid>()
            .map(RouteId::new)
            .map_err(|_| StoreError::Conflict("stored approval route id is invalid".into()))?;
        Ok(StoredApproval { request, route_id })
    })
    .transpose()
}

fn ingest_agent_events(
    connection: &mut Connection,
    expected: u64,
    next: u64,
    events: &[EventRecord],
    route_agents: &[RouteAgent],
    now_ms: i64,
) -> StoreResult<EventIngestOutcome> {
    if next < expected
        || events.last().is_some_and(|event| event.sequence != next)
        || events
            .windows(2)
            .any(|pair| pair[0].sequence >= pair[1].sequence)
        || events
            .first()
            .is_some_and(|event| event.sequence <= expected)
    {
        return Err(StoreError::Conflict(
            "Wakterm event page cursor and event order are inconsistent".into(),
        ));
    }
    let transaction = connection.transaction()?;
    let current = event_cursor(&transaction)?;
    if current != Some(expected) {
        return Err(StoreError::Conflict(format!(
            "event cursor was not at expected sequence {expected}"
        )));
    }
    let routes = list_routes(&transaction)?;
    let preferences = route_preferences(&transaction)?;
    let mut outcome = EventIngestOutcome {
        next_after_sequence: next,
        ..EventIngestOutcome::default()
    };

    for event in events {
        let record_json = serde_json::to_string(event)?;
        if let Some((stored_id, stored_json)) = transaction
            .query_row(
                "SELECT event_id, record_json FROM agent_events WHERE sequence = ?1",
                params![event.sequence],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            if stored_id != event.event_id || stored_json != record_json {
                return Err(StoreError::Conflict(format!(
                    "Wakterm event sequence {} changed content",
                    event.sequence
                )));
            }
            continue;
        }
        if transaction
            .query_row(
                "SELECT 1 FROM agent_events WHERE event_id = ?1 AND incarnation_id = ?2",
                params![event.event_id, event.incarnation_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some()
        {
            return Err(StoreError::Conflict(format!(
                "Wakterm event ID {:?} appeared at another sequence",
                event.event_id
            )));
        }

        let matching = route_agents
            .iter()
            .filter(|candidate| {
                candidate.agent.agent_id == event.agent_id
                    && candidate.agent.incarnation_id == event.incarnation_id
            })
            .collect::<Vec<_>>();
        if matching.len() > 1 {
            return Err(StoreError::Conflict(format!(
                "Wakterm event {:?} matches more than one durable route",
                event.event_id
            )));
        }
        let route_agent = matching.first().copied();
        let route = route_agent
            .and_then(|candidate| routes.iter().find(|route| route.id == candidate.route_id));
        if route_agent.is_some() && route.is_none() {
            return Err(StoreError::Conflict(
                "live agent projection references a missing route".into(),
            ));
        }

        let mut state = "recorded";
        let route_id = route.map(|route| route.id);
        let approval = event
            .approval()
            .map_err(|detail| StoreError::Conflict(detail.into()))?;
        let visible_body = event
            .visible_output_body()
            .map_err(|detail| StoreError::Conflict(detail.into()))?
            .map(|body| strip_memory_citations(&body))
            .filter(|body| !body.trim().is_empty());
        if approval.is_some() || visible_body.is_some() {
            let Some(route) = route else {
                state = "unrouted";
                outcome.unrouted += 1;
                insert_agent_event(&transaction, event, None, state, &record_json, now_ms)?;
                outcome.recorded += 1;
                continue;
            };
            if event.kind == "assistant_message"
                && visible_body
                    .as_deref()
                    .is_some_and(|body| body.trim() == NO_REPLY)
            {
                state = "suppressed";
                if let Some(candidate) = route_agent {
                    outcome.remember_last_agent(candidate);
                }
                insert_agent_event(
                    &transaction,
                    event,
                    Some(route.id),
                    state,
                    &record_json,
                    now_ms,
                )?;
                outcome.recorded += 1;
                continue;
            }
            let destination = if approval.is_some() {
                route.channels.iter().find_map(|binding| match binding {
                    crate::domain::ChannelBinding::Telegram { topic_id } => {
                        Some((ChannelKind::Telegram, topic_id.to_string()))
                    }
                    crate::domain::ChannelBinding::Signal { .. } => None,
                })
            } else {
                output_destination(
                    route,
                    preferences.get(&route.id.to_string()).map(String::as_str),
                )
            };
            let Some((kind, destination)) = destination else {
                state = "unrouted";
                outcome.unrouted += 1;
                insert_agent_event(
                    &transaction,
                    event,
                    Some(route.id),
                    state,
                    &record_json,
                    now_ms,
                )?;
                outcome.recorded += 1;
                continue;
            };
            let (body, attachments, actions) = match approval {
                Some(approval) if crate::wakterm::form::answerable(&approval) => {
                    let state = crate::wakterm::form::FormState::default();
                    (
                        crate::wakterm::form::text(&approval, &state),
                        Vec::new(),
                        crate::wakterm::form::actions(&approval, &state),
                    )
                }
                Some(approval) => {
                    let mut body = if approval.kind.starts_with("user_question") {
                        "Input needed".to_string()
                    } else {
                        "Approval needed".to_string()
                    };
                    if let Some(prompt) = approval.prompt.as_deref() {
                        body.push_str("\n\n");
                        body.push_str(prompt);
                    }
                    if let Some(command) = approval.command.as_deref() {
                        body.push_str("\n\nCommand:\n");
                        body.push_str(command);
                    }
                    if let Some(reason) = approval.reason.as_deref() {
                        body.push_str("\n\nReason: ");
                        body.push_str(reason);
                    }
                    if approval.kind == "user_question_form" {
                        body.push_str(
                            "\n\nThis form has several questions, so answer it in the agent's pane.",
                        );
                    }
                    if approval.kind == "user_question" {
                        for (index, choice) in approval.choices.iter().enumerate() {
                            if let Some(description) = choice.description.as_deref() {
                                body.push_str(&format!(
                                    "\n\n{}. {}: {}",
                                    index + 1,
                                    choice.label,
                                    description
                                ));
                            }
                        }
                    }
                    let actions = approval
                        .choices
                        .into_iter()
                        .map(|choice| crate::domain::OutboxAction {
                            id: format!("wakap:{}:{}", approval.request_id, choice.id),
                            label: choice.label,
                        })
                        .collect();
                    (body, Vec::new(), actions)
                }
                None => {
                    let (body, attachments) =
                        visible_attachments(visible_body.expect("visible output was checked"));
                    (body, attachments, Vec::new())
                }
            };
            let namespace = Uuid::new_v5(
                &Uuid::NAMESPACE_URL,
                b"https://panetone.dev/wakterm/events/v1",
            );
            let item = OutboxItem {
                id: EffectId::new(Uuid::new_v5(
                    &namespace,
                    format!("{}\0{}", event.incarnation_id, event.event_id).as_bytes(),
                )),
                route_id: Some(route.id),
                // Interactive callbacks must use the one Telegram bot that owns
                // the inbound update cursor. Harness-specific bots remain
                // appropriate for ordinary one-way output.
                sender_harness: if actions.is_empty() {
                    route_agent.map(|candidate| candidate.agent.harness.clone())
                } else {
                    None
                },
                source_agent: route_agent.map(|candidate| candidate.agent.clone()),
                kind,
                destination,
                body,
                attachments,
                actions,
                state: OutboxState::Pending,
                attempts: 0,
                last_error: None,
                external_receipt: None,
                uncertain_attempts: 0,
            };
            for chunk in crate::domain::chunk_outbox(item) {
                enqueue_outbox(&transaction, None, &chunk, now_ms)?;
            }
            state = "projected";
            outcome.visible_outputs += 1;
            if let Some(candidate) = route_agent {
                outcome.remember_last_agent(candidate);
            }
        }
        insert_agent_event(&transaction, event, route_id, state, &record_json, now_ms)?;
        outcome.recorded += 1;
    }
    let changed = transaction.execute(
        "UPDATE metadata SET value = ?2
         WHERE key = 'wakterm_event_cursor' AND value = ?1",
        params![expected.to_string(), next.to_string()],
    )?;
    if changed != 1 {
        return Err(StoreError::Conflict(format!(
            "event cursor was not at expected sequence {expected}"
        )));
    }
    transaction.commit()?;
    Ok(outcome)
}

fn recover_event_cursor_gap(
    connection: &mut Connection,
    gap: &EventCursorGap,
    catalog: &AgentCatalog,
) -> StoreResult<()> {
    if catalog.schema != "wakterm.agent-api.v1" {
        return Err(StoreError::Conflict(
            "cursor-gap recovery requires a v1 Wakterm catalog".into(),
        ));
    }
    if gap.fresh_catalog_as_of_sequence != catalog.as_of_event_sequence
        || gap.fresh_catalog_as_of_sequence.saturating_add(1) < gap.oldest_available_sequence
    {
        return Err(StoreError::Conflict(
            "fresh catalog does not establish a usable event recovery cursor".into(),
        ));
    }
    let transaction = connection.transaction()?;
    let current = event_cursor(&transaction)?;
    if current != Some(gap.requested_after_sequence) {
        return Err(StoreError::Conflict(format!(
            "event cursor was not at gap sequence {}",
            gap.requested_after_sequence
        )));
    }
    set_metadata(
        &transaction,
        "wakterm_event_cursor_gap_v1",
        &serde_json::to_string(gap)?,
    )?;
    set_metadata(
        &transaction,
        "wakterm_event_cursor",
        &gap.fresh_catalog_as_of_sequence.to_string(),
    )?;
    transaction.commit()?;
    Ok(())
}

fn insert_agent_event(
    connection: &Connection,
    event: &EventRecord,
    route_id: Option<RouteId>,
    state: &str,
    record_json: &str,
    now_ms: i64,
) -> StoreResult<()> {
    connection.execute(
        "INSERT INTO agent_events(
             sequence, event_id, agent_id, incarnation_id, kind, route_id,
             state, record_json, created_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            event.sequence,
            event.event_id,
            event.agent_id,
            event.incarnation_id,
            event.kind,
            route_id.map(|id| id.to_string()),
            state,
            record_json,
            now_ms,
        ],
    )?;
    Ok(())
}

fn route_preferences(
    connection: &Connection,
) -> StoreResult<std::collections::BTreeMap<String, String>> {
    let value = get_metadata(connection, "route_output_preferences_v1")?;
    value
        .map(|value| serde_json::from_str(&value).map_err(StoreError::from))
        .transpose()
        .map(Option::unwrap_or_default)
}

fn output_destination(route: &Route, preference: Option<&str>) -> Option<(ChannelKind, String)> {
    let selected = match preference {
        Some("tg") => Some(ChannelKind::Telegram),
        Some("sig") => Some(ChannelKind::Signal),
        _ => None,
    }
    .unwrap_or_else(|| {
        if route
            .channels
            .iter()
            .any(|binding| matches!(binding, crate::domain::ChannelBinding::Signal { .. }))
        {
            ChannelKind::Signal
        } else {
            ChannelKind::Telegram
        }
    });
    let telegram_topic = route.channels.iter().find_map(|binding| match binding {
        crate::domain::ChannelBinding::Telegram { topic_id } => Some(*topic_id),
        _ => None,
    });
    let signal_group = route.channels.iter().find_map(|binding| match binding {
        crate::domain::ChannelBinding::Signal { group_id, .. } => Some(group_id.as_str()),
        _ => None,
    });
    match selected {
        ChannelKind::Telegram => telegram_topic.map(|topic_id| (selected, topic_id.to_string())),
        ChannelKind::Signal => signal_group.map(|group_id| (selected, group_id.into())),
    }
}

/// Removes `<memory-used>...</memory-used>` blocks, which agents add to cite
/// the memories they used and which belong only in their own transcripts. A
/// block may span lines; blocks inside fenced code are kept, and an
/// unterminated block is removed to the end of the message.
fn strip_memory_citations(body: &str) -> String {
    const OPEN: &str = "<memory-used>";
    const CLOSE: &str = "</memory-used>";
    if !body.contains(OPEN) {
        return body.to_owned();
    }
    let mut retained = Vec::new();
    let mut fence = None;
    let mut in_block = false;
    for line in body.split('\n') {
        if !in_block {
            if let Some((marker, length)) = fence {
                retained.push(line.to_owned());
                if closes_markdown_fence(line, marker, length) {
                    fence = None;
                }
                continue;
            }
            if let Some(opened) = opens_markdown_fence(line) {
                fence = Some(opened);
                retained.push(line.to_owned());
                continue;
            }
        }
        let mut kept = String::new();
        let mut rest = line;
        loop {
            if in_block {
                match rest.find(CLOSE) {
                    Some(end) => {
                        rest = &rest[end + CLOSE.len()..];
                        in_block = false;
                    }
                    None => break,
                }
            } else {
                match rest.find(OPEN) {
                    Some(start) => {
                        kept.push_str(&rest[..start]);
                        rest = &rest[start + OPEN.len()..];
                        in_block = true;
                    }
                    None => {
                        kept.push_str(rest);
                        break;
                    }
                }
            }
        }
        // Drop a line that held nothing but citations.
        if !(kept.trim().is_empty() && !line.trim().is_empty()) {
            retained.push(kept.trim_end().to_owned());
        }
    }
    retained.join("\n").trim_end().to_owned()
}

fn visible_attachments(body: String) -> (String, Vec<ChannelAttachment>) {
    let mut retained = Vec::new();
    let mut requested = Vec::new();
    let mut fence = None;
    for line in body.split('\n') {
        if let Some((marker, length)) = fence {
            retained.push(line);
            if closes_markdown_fence(line, marker, length) {
                fence = None;
            }
            continue;
        }
        if let Some(opened) = opens_markdown_fence(line) {
            fence = Some(opened);
            retained.push(line);
            continue;
        }
        let trimmed = line.trim();
        if !line.starts_with("    ")
            && !line.starts_with('\t')
            && let Some(path) = trimmed
                .strip_prefix("[panetone:attach ")
                .and_then(|value| value.strip_suffix(']'))
        {
            requested.push(path.to_owned());
        } else {
            retained.push(line);
        }
    }
    if requested.is_empty() {
        return (body, Vec::new());
    }
    let clean_body = retained.join("\n").trim_end().to_owned();
    let result = if requested.len() <= MAX_CHANNEL_ATTACHMENTS {
        capture_attachments(&requested)
    } else {
        Err(format!(
            "at most {MAX_CHANNEL_ATTACHMENTS} attachment tags are supported per assistant message"
        ))
    };
    match result {
        Ok(attachments) => (clean_body, attachments),
        Err(detail) => {
            let notice = format!("Attachment unavailable: {detail}");
            if clean_body.is_empty() {
                (notice, Vec::new())
            } else {
                (format!("{clean_body}\n\n{notice}"), Vec::new())
            }
        }
    }
}

fn opens_markdown_fence(line: &str) -> Option<(u8, usize)> {
    let trimmed = line.trim_start_matches(' ');
    if line.len() - trimmed.len() > 3 {
        return None;
    }
    let line = trimmed;
    let marker = *line.as_bytes().first()?;
    if !matches!(marker, b'`' | b'~') {
        return None;
    }
    let length = line.bytes().take_while(|byte| *byte == marker).count();
    (length >= 3).then_some((marker, length))
}

fn closes_markdown_fence(line: &str, marker: u8, minimum_length: usize) -> bool {
    let trimmed = line.trim_start_matches(' ');
    if line.len() - trimmed.len() > 3 {
        return false;
    }
    let line = trimmed;
    let length = line.bytes().take_while(|byte| *byte == marker).count();
    length >= minimum_length && line[length..].trim().is_empty()
}

fn capture_attachments(requested: &[String]) -> Result<Vec<ChannelAttachment>, String> {
    let mut attachments = Vec::with_capacity(requested.len());
    let mut total = 0_u64;
    for path in requested {
        let attachment = capture_attachment(path)?;
        total = total.saturating_add(attachment.size);
        if total > MAX_CHANNEL_ATTACHMENT_TOTAL_BYTES {
            return Err(format!(
                "tagged files may total at most {MAX_CHANNEL_ATTACHMENT_TOTAL_BYTES} bytes"
            ));
        }
        attachments.push(attachment);
    }
    Ok(attachments)
}

/// Reads a file an agent tagged for posting. Any file the agent's user can
/// read is allowed, since the agent can read and send it by other means.
fn capture_attachment(requested: &str) -> Result<ChannelAttachment, String> {
    let requested = Path::new(requested);
    if !requested.is_absolute() {
        return Err("the tagged path must be absolute".into());
    }
    let mut file =
        File::open(requested).map_err(|_| "the tagged file does not exist".to_string())?;
    let path = opened_file_path(&file, requested)
        .map_err(|_| "the tagged file cannot be inspected".to_string())?;
    let metadata = file
        .metadata()
        .map_err(|_| "the tagged file cannot be inspected".to_string())?;
    if !metadata.is_file() {
        return Err("the tagged path is not a regular file".into());
    }
    if metadata.len() == 0 || metadata.len() > MAX_CHANNEL_ATTACHMENT_BYTES {
        return Err(format!(
            "the tagged file must be between 1 byte and {MAX_CHANNEL_ATTACHMENT_BYTES} bytes"
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(MAX_CHANNEL_ATTACHMENT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "the tagged file cannot be read".to_string())?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_CHANNEL_ATTACHMENT_BYTES {
        return Err("the tagged file changed size while being captured".into());
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "the tagged file name is not valid UTF-8".to_string())?;
    let file_name = name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
                character
            } else {
                '_'
            }
        })
        .take(128)
        .collect::<String>();
    if file_name.is_empty() {
        return Err("the tagged file has no usable name".into());
    }
    let media_type = mime_guess::from_path(&path)
        .first_or_octet_stream()
        .essence_str()
        .to_owned();
    Ok(ChannelAttachment {
        file_name,
        media_type,
        size: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        data_base64: BASE64.encode(bytes),
    })
}

#[cfg(target_os = "linux")]
fn opened_file_path(file: &File, _requested: &Path) -> std::io::Result<PathBuf> {
    fs::canonicalize(format!("/proc/self/fd/{}", file.as_raw_fd()))
}

#[cfg(not(target_os = "linux"))]
fn opened_file_path(_file: &File, requested: &Path) -> std::io::Result<PathBuf> {
    fs::canonicalize(requested)
}

fn register_return(connection: &Connection, record: &ReturnDelivery) -> StoreResult<()> {
    let changed = connection.execute(
        "INSERT OR IGNORE INTO return_deliveries(
             request_id, agent_state, mirror_state, record_json, created_at_ms, updated_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            record.workflow_id.to_string(),
            delivery_name(record.agent.state),
            delivery_name(record.mirror.state),
            serde_json::to_string(record)?,
            record.created_at_ms,
            record.updated_at_ms,
        ],
    )?;
    if changed == 0 {
        let existing = connection.query_row(
            "SELECT record_json FROM return_deliveries WHERE request_id = ?1",
            params![record.workflow_id.to_string()],
            |row| row.get::<_, String>(0),
        )?;
        if existing != serde_json::to_string(record)? {
            return Err(StoreError::Conflict(format!(
                "return route {} already differs",
                record.workflow_id
            )));
        }
    }
    Ok(())
}

fn get_return(
    connection: &Connection,
    workflow_id: WorkflowId,
) -> StoreResult<Option<ReturnDelivery>> {
    let json = connection
        .query_row(
            "SELECT record_json FROM return_deliveries WHERE request_id = ?1",
            params![workflow_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    json.map(|value| serde_json::from_str(&value).map_err(StoreError::from))
        .transpose()
}

fn save_return(connection: &Connection, record: &ReturnDelivery) -> StoreResult<()> {
    let changed = connection.execute(
        "UPDATE return_deliveries SET agent_state = ?2, mirror_state = ?3,
             record_json = ?4, updated_at_ms = ?5 WHERE request_id = ?1",
        params![
            record.workflow_id.to_string(),
            delivery_name(record.agent.state),
            delivery_name(record.mirror.state),
            serde_json::to_string(record)?,
            record.updated_at_ms,
        ],
    )?;
    if changed != 1 {
        return Err(StoreError::Conflict(format!(
            "return route {} is missing",
            record.workflow_id
        )));
    }
    Ok(())
}

fn pending_returns(connection: &Connection) -> StoreResult<Vec<ReturnDelivery>> {
    let mut statement = connection.prepare(
        "SELECT record_json FROM return_deliveries
         WHERE agent_state = 'pending' OR mirror_state = 'pending'
         ORDER BY created_at_ms",
    )?;
    let json = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    json.into_iter()
        .map(|value| serde_json::from_str(&value).map_err(StoreError::from))
        .collect()
}

fn has_unresolved_terminal(connection: &Connection) -> StoreResult<bool> {
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM workflows AS workflow
                 WHERE json_extract(workflow.record_json, '$.command.return_final') = 1
                   AND json_type(
                       workflow.record_json,
                       '$.workflow.submitted_target'
                   ) = 'object'
                   AND NOT EXISTS(
                       SELECT 1 FROM return_deliveries AS returned
                       WHERE returned.request_id = workflow.request_id
                   )
             )",
            [],
            |row| row.get(0),
        )
        .map_err(StoreError::from)
}

fn enqueue_outbox(
    connection: &Connection,
    workflow_id: Option<WorkflowId>,
    item: &OutboxItem,
    now_ms: i64,
) -> StoreResult<()> {
    let changed = connection.execute(
        "INSERT OR IGNORE INTO outbox(
             effect_id, request_id, channel, destination, state, record_json,
             created_at_ms, updated_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
        params![
            item.id.to_string(),
            workflow_id.map(|id| id.to_string()),
            channel_name(item.kind),
            item.destination,
            outbox_name(item.state),
            serde_json::to_string(item)?,
            now_ms,
        ],
    )?;
    if changed == 0 {
        let existing = connection.query_row(
            "SELECT record_json FROM outbox WHERE effect_id = ?1",
            params![item.id.to_string()],
            |row| row.get::<_, String>(0),
        )?;
        let existing: OutboxItem = serde_json::from_str(&existing)?;
        if existing.id != item.id
            || existing.kind != item.kind
            || existing.destination != item.destination
            || existing.body != item.body
            || existing.attachments != item.attachments
        {
            return Err(StoreError::Conflict(format!(
                "outbox effect {} already differs",
                item.id
            )));
        }
    }
    Ok(())
}

fn enqueue_outbox_chunks(
    connection: &Connection,
    workflow_id: Option<WorkflowId>,
    item: OutboxItem,
    now_ms: i64,
) -> StoreResult<Vec<OutboxItem>> {
    let chunks = crate::domain::chunk_outbox(item);
    for chunk in &chunks {
        enqueue_outbox(connection, workflow_id, chunk, now_ms)?;
    }
    chunks
        .into_iter()
        .map(|chunk| {
            connection
                .query_row(
                    "SELECT record_json FROM outbox WHERE effect_id = ?1",
                    params![chunk.id.to_string()],
                    |row| row.get::<_, String>(0),
                )
                .map_err(StoreError::from)
                .and_then(|json| serde_json::from_str(&json).map_err(StoreError::from))
        })
        .collect()
}

fn save_outbox(connection: &Connection, item: &OutboxItem, now_ms: i64) -> StoreResult<()> {
    let changed = connection.execute(
        "UPDATE outbox SET state = ?2, record_json = ?3, updated_at_ms = ?4
         WHERE effect_id = ?1",
        params![
            item.id.to_string(),
            outbox_name(item.state),
            serde_json::to_string(item)?,
            now_ms,
        ],
    )?;
    if changed != 1 {
        return Err(StoreError::Conflict(format!(
            "outbox effect {} is missing",
            item.id
        )));
    }
    Ok(())
}

fn pending_outbox(connection: &Connection) -> StoreResult<Vec<OutboxItem>> {
    let mut statement = connection.prepare(
        "SELECT record_json FROM outbox
         WHERE state = 'pending' ORDER BY created_at_ms, rowid",
    )?;
    let json = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    json.into_iter()
        .map(|value| serde_json::from_str(&value).map_err(StoreError::from))
        .collect()
}

fn rebaseline_event_cursor(connection: &Connection, sequence: u64) -> StoreResult<()> {
    set_metadata(connection, "wakterm_event_cursor", &sequence.to_string())
}

fn latest_sender_name(
    connection: &Connection,
    channel: ChannelKind,
    sender_ids: &[String],
) -> StoreResult<Option<String>> {
    let mut statement = connection.prepare(
        "SELECT json_extract(record_json, '$.sender') FROM inbox
         WHERE channel = ?1 AND json_extract(record_json, '$.sender_id') = ?2
           AND json_extract(record_json, '$.sender') IS NOT NULL
         ORDER BY created_at_ms DESC LIMIT 1",
    )?;
    for sender_id in sender_ids {
        if let Some(name) = statement
            .query_row(params![channel_name(channel), sender_id], |row| {
                row.get::<_, String>(0)
            })
            .optional()?
        {
            return Ok(Some(name));
        }
    }
    Ok(None)
}

fn find_delivered_outbox(
    connection: &Connection,
    channel: ChannelKind,
    destination: &str,
    external_receipt: &str,
) -> StoreResult<Option<OutboxItem>> {
    let json = connection
        .query_row(
            "SELECT record_json FROM outbox
             WHERE channel = ?1 AND destination = ?2 AND state = 'delivered'
               AND json_extract(record_json, '$.external_receipt') = ?3
             ORDER BY updated_at_ms DESC LIMIT 1",
            params![channel_name(channel), destination, external_receipt],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    json.map(|value| serde_json::from_str::<OutboxItem>(&value).map_err(StoreError::from))
        .transpose()
}

fn accept_inbox(connection: &Connection, item: &InboxItem) -> StoreResult<bool> {
    let changed = connection.execute(
        "INSERT OR IGNORE INTO inbox(
             effect_id, channel, external_id, state, record_json, created_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            item.id.to_string(),
            channel_name(item.channel),
            item.external_id,
            item.state,
            serde_json::to_string(item)?,
            item.created_at_ms,
        ],
    )?;
    Ok(changed == 1)
}

fn inbox_in_state(connection: &Connection, state: &str) -> StoreResult<Vec<InboxItem>> {
    let mut statement = connection
        .prepare("SELECT record_json FROM inbox WHERE state = ?1 ORDER BY created_at_ms")?;
    let records = statement
        .query_map([state], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    records
        .into_iter()
        .map(|record| serde_json::from_str(&record).map_err(StoreError::from))
        .collect()
}

fn input_recorded(connection: &Connection, proof: &DeliveryProof) -> StoreResult<bool> {
    Ok(connection
        .query_row(
            "SELECT 1 FROM agent_events
             WHERE agent_id = ?1 AND sequence > ?2
               AND kind IN ('input_accepted', 'turn_started')
               AND json_extract(record_json, '$.input_sha256') = ?3
             LIMIT 1",
            params![proof.agent_id, proof.after_sequence, proof.input_sha256],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn save_inbox(
    connection: &mut Connection,
    item: &InboxItem,
    expected_state: &str,
    route_preference: Option<(RouteId, ChannelKind)>,
) -> StoreResult<()> {
    let transaction = connection.transaction()?;
    let changed = transaction.execute(
        "UPDATE inbox SET state = ?2, record_json = ?3
         WHERE effect_id = ?1 AND state = ?4",
        params![
            item.id.to_string(),
            item.state,
            serde_json::to_string(item)?,
            expected_state,
        ],
    )?;
    if changed != 1 {
        return Err(StoreError::Conflict(format!(
            "inbox item {} was not in {expected_state}",
            item.id
        )));
    }
    if let Some((route_id, channel)) = route_preference {
        let mut preferences = route_preferences(&transaction)?;
        preferences.insert(
            route_id.to_string(),
            match channel {
                ChannelKind::Telegram => "tg",
                ChannelKind::Signal => "sig",
            }
            .into(),
        );
        set_metadata(
            &transaction,
            "route_output_preferences_v1",
            &serde_json::to_string(&preferences)?,
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn set_metadata(connection: &Connection, key: &str, value: &str) -> StoreResult<()> {
    connection.execute(
        "INSERT INTO metadata(key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
    )?;
    Ok(())
}

fn get_metadata(connection: &Connection, key: &str) -> StoreResult<Option<String>> {
    Ok(connection
        .query_row(
            "SELECT value FROM metadata WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()?)
}

fn status(connection: &Connection) -> StoreResult<StoreStatus> {
    Ok(StoreStatus {
        schema_version: SCHEMA_VERSION as u64,
        workflows: count(connection, "SELECT COUNT(*) FROM workflows")?,
        awaiting_target_idle: count(
            connection,
            "SELECT COUNT(*) FROM workflows WHERE state = 'awaiting_target_idle'",
        )?,
        failed_workflows: count(
            connection,
            "SELECT COUNT(*) FROM workflows WHERE state = 'failed'",
        )?,
        indeterminate_workflows: count(
            connection,
            "SELECT COUNT(*) FROM workflows WHERE state = 'indeterminate'",
        )?,
        pending_returns: count(
            connection,
            "SELECT COUNT(*) FROM return_deliveries
             WHERE agent_state = 'pending' OR mirror_state = 'pending'",
        )?,
        unresolved_returns: count(
            connection,
            "SELECT COUNT(*) FROM return_deliveries
             WHERE agent_state IN ('pending', 'failed', 'indeterminate')
                OR mirror_state IN ('pending', 'failed', 'indeterminate')",
        )?,
        pending_outbox: count(
            connection,
            "SELECT COUNT(*) FROM outbox WHERE state = 'pending'",
        )?,
        failed_outbox: count(
            connection,
            "SELECT COUNT(*) FROM outbox WHERE state = 'failed'",
        )?,
        indeterminate_outbox: count(
            connection,
            "SELECT COUNT(*) FROM outbox WHERE state = 'indeterminate'",
        )?,
        pending_inbox: count(
            connection,
            "SELECT COUNT(*) FROM inbox WHERE state = 'pending'",
        )?,
        tombstones: count(connection, "SELECT COUNT(*) FROM idempotency_tombstones")?,
        unrouted_agent_events: count(
            connection,
            "SELECT COUNT(*) FROM agent_events WHERE state = 'unrouted'",
        )?,
        observer_failure_events: count(
            connection,
            "SELECT COUNT(*) FROM agent_events WHERE kind = 'observer_failure'",
        )?,
        event_cursor: event_cursor(connection)?,
        event_cursor_gap: get_metadata(connection, "wakterm_event_cursor_gap_v1")?
            .map(|value| serde_json::from_str(&value).map_err(StoreError::from))
            .transpose()?,
        telegram_update_offset: get_metadata(connection, "telegram_update_offset")?
            .map(|value| {
                value.parse::<u64>().map_err(|_| {
                    StoreError::Conflict("stored Telegram update offset is invalid".into())
                })
            })
            .transpose()?,
    })
}

fn count(connection: &Connection, sql: &str) -> StoreResult<u64> {
    Ok(connection.query_row(sql, [], |row| row.get(0))?)
}

fn state_name(state: WorkflowState) -> String {
    serde_json::to_value(state)
        .expect("workflow state serializes")
        .as_str()
        .expect("workflow state is a string")
        .into()
}

fn delivery_name(state: DeliveryState) -> String {
    serde_json::to_value(state)
        .expect("delivery state serializes")
        .as_str()
        .expect("delivery state is a string")
        .into()
}

fn outbox_name(state: OutboxState) -> String {
    serde_json::to_value(state)
        .expect("outbox state serializes")
        .as_str()
        .expect("outbox state is a string")
        .into()
}

fn channel_name(channel: ChannelKind) -> String {
    serde_json::to_value(channel)
        .expect("channel kind serializes")
        .as_str()
        .expect("channel kind is a string")
        .into()
}
