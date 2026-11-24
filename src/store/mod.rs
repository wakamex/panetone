use std::fs::{self, File, OpenOptions};
use std::io::Read;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
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

mod replies;
mod rooms;

pub use replies::{ReplyDebt, ReplyMessage, TurnEnd};
pub use rooms::{
    ROOM_MARKER, ROOM_NAMES_KEY, RoomDelivery, RoomMessage, RoomSettings, room_of,
    take_marker as rooms_take_marker,
};

pub const SCHEMA_VERSION: i64 = 9;
const COMMAND_CAPACITY: usize = 128;
/// Output observed this long before it is ingested is not posted.
const STALE_OUTPUT_MS: i64 = 15 * 60 * 1000;
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
    /// For a message steered into a busy target without acknowledgement, what
    /// shows that the target took it; the send stays submitted until then.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proof: Option<DeliveryProof>,
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
    /// Its audio has been transcribed into the body, for a route that asks.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub transcribed: bool,
    /// How many times it was offered to Wakterm under a new admission ID,
    /// because an earlier ID belonged to an agent process that has exited.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub admission_round: u32,
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

impl InboxItem {
    /// The request ID Wakterm admits the message under: its own ID, or one
    /// derived from it for each later round.
    pub fn admission_id(&self) -> EffectId {
        match self.admission_round {
            0 => self.id,
            round => EffectId::named(
                WorkflowId::new(self.id.as_uuid()),
                &format!("admission-{round}"),
            ),
        }
    }
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
    /// The same hash of the message as Claude records it, when it contains
    /// tabs: Claude turns each tab of pasted input into four spaces.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tab_expanded_sha256: Option<String>,
    pub deadline_ms: i64,
    /// Why delivery was unconfirmed, for the sender's notice if no proof comes.
    pub detail: String,
    /// The Wakterm request that typed the message, which Wakterm names on
    /// the input's record; a message steered into a busy turn has none yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl DeliveryProof {
    pub fn input_sha256(text: &str) -> String {
        format!("{:x}", Sha256::digest(text.trim().as_bytes()))
    }

    pub fn tab_expanded_sha256(text: &str) -> Option<String> {
        text.contains('\t')
            .then(|| Self::input_sha256(&text.replace('\t', "    ")))
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

/// One outbox post as the feed reads it.
#[derive(Clone, Debug)]
pub struct FeedPost {
    /// The post's rowid, its position in the feed.
    pub position: i64,
    pub post: OutboxItem,
    pub queued_at_ms: i64,
    /// The request a post about a send's delivery belongs to.
    pub request_id: Option<String>,
}

#[derive(Clone)]
pub struct StoreHandle {
    sender: mpsc::Sender<Command>,
    owner: Arc<Mutex<Option<JoinHandle<()>>>>,
}

enum Command {
    /// Runs one store operation that has no dedicated command.
    Run {
        job: Box<dyn FnOnce(&mut Connection) + Send>,
    },
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

    async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(&mut Connection) -> StoreResult<T> + Send + 'static,
    ) -> StoreResult<T> {
        self.request(|reply| Command::Run {
            job: Box::new(move |connection| send_reply(reply, work(connection))),
        })
        .await
    }

    /// Records that the target of a request owes its sender a reply.
    pub async fn open_reply_debt(&self, debt: ReplyDebt) -> StoreResult<()> {
        self.run(move |connection| replies::open_debt(connection, debt))
            .await
    }

    /// Saves an outbox post as `item`, normally marked as being delivered,
    /// only if it is still in state `from`, so that the outbox worker and a
    /// workflow delivering its own post never both send it. Returns whether
    /// this caller claimed the post.
    pub async fn claim_outbox(
        &self,
        item: OutboxItem,
        from: OutboxState,
        now_ms: i64,
    ) -> StoreResult<bool> {
        self.run(move |connection| {
            Ok(connection.execute(
                "UPDATE outbox SET state = ?2, record_json = ?3, updated_at_ms = ?4
                 WHERE effect_id = ?1 AND state = ?5",
                params![
                    item.id.to_string(),
                    outbox_name(item.state),
                    serde_json::to_string(&item)?,
                    now_ms,
                    outbox_name(from),
                ],
            )? == 1)
        })
        .await
    }

    /// An outbox post by ID, with the time it was queued.
    pub async fn get_outbox(&self, id: EffectId) -> StoreResult<Option<(OutboxItem, i64)>> {
        self.run(move |connection| {
            connection
                .query_row(
                    "SELECT record_json, created_at_ms FROM outbox WHERE effect_id = ?1",
                    params![id.to_string()],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()?
                .map(|(json, queued_at_ms)| Ok((serde_json::from_str(&json)?, queued_at_ms)))
                .transpose()
        })
        .await
    }

    pub async fn outbox_exists(&self, id: EffectId) -> StoreResult<bool> {
        self.run(move |connection| {
            Ok(connection
                .query_row(
                    "SELECT 1 FROM outbox WHERE effect_id = ?1",
                    params![id.to_string()],
                    |_| Ok(()),
                )
                .optional()?
                .is_some())
        })
        .await
    }

    /// Records an endchat profile's agent messages as that route's output and
    /// saves where the log reader stopped, in one transaction.
    pub async fn ingest_endchat_messages(
        &self,
        route_id: RouteId,
        agent: AgentBinding,
        messages: Vec<crate::endchat::Entry>,
        cursor_key: String,
        cursor: String,
        now_ms: i64,
    ) -> StoreResult<EventIngestOutcome> {
        self.run(move |connection| {
            ingest_endchat_messages(
                connection,
                route_id,
                agent,
                &messages,
                &cursor_key,
                &cursor,
                now_ms,
            )
        })
        .await
    }

    pub async fn rooms(&self) -> StoreResult<std::collections::BTreeMap<String, RoomSettings>> {
        self.run(|connection| rooms::rooms(connection)).await
    }

    /// Makes a route a room with these settings, or with `None` an ordinary
    /// route again.
    pub async fn set_room(
        &self,
        route_id: RouteId,
        settings: Option<RoomSettings>,
    ) -> StoreResult<()> {
        self.run(move |connection| rooms::set_room(connection, route_id, settings))
            .await
    }

    /// Adds a message to a room's transcript and queues it for `recipients`.
    pub async fn post_room_message(
        &self,
        route_id: RouteId,
        message: RoomMessage,
        recipients: Vec<String>,
    ) -> StoreResult<RoomMessage> {
        self.run(move |connection| {
            let transaction = connection.transaction()?;
            let posted = rooms::post(&transaction, route_id, message, &recipients)?;
            transaction.commit()?;
            Ok(posted)
        })
        .await
    }

    /// Room messages after `after`, or the last `limit` when `after` is negative.
    pub async fn room_messages(
        &self,
        route_id: RouteId,
        after: i64,
        limit: usize,
    ) -> StoreResult<Vec<RoomMessage>> {
        self.run(move |connection| rooms::messages_after(connection, route_id, after, limit))
            .await
    }

    pub async fn room_people(&self, route_id: RouteId) -> StoreResult<usize> {
        self.run(move |connection| rooms::people(connection, route_id))
            .await
    }

    pub async fn latest_room_message(&self, route_id: RouteId) -> StoreResult<i64> {
        self.run(move |connection| rooms::latest_message_id(connection, route_id))
            .await
    }

    pub async fn pending_room_deliveries(&self) -> StoreResult<Vec<RoomDelivery>> {
        self.run(|connection| rooms::pending_deliveries(connection))
            .await
    }

    pub async fn set_room_delivery(
        &self,
        delivery: RoomDelivery,
        state: String,
    ) -> StoreResult<()> {
        self.run(move |connection| rooms::set_delivery(connection, &delivery, &state))
            .await
    }

    pub async fn reply_debt(&self, request_id: WorkflowId) -> StoreResult<Option<ReplyDebt>> {
        self.run(move |connection| replies::debt(connection, request_id))
            .await
    }

    pub async fn open_reply_debts(&self) -> StoreResult<Vec<ReplyDebt>> {
        self.run(|connection| replies::open_debts(connection)).await
    }

    /// Withdraws a reply debt; returns its earlier state, or `None` when the
    /// request did not ask for a reply.
    pub async fn cancel_reply_debt(
        &self,
        request_id: WorkflowId,
        now_ms: i64,
    ) -> StoreResult<Option<String>> {
        self.run(move |connection| replies::cancel_debt(connection, request_id, now_ms))
            .await
    }

    pub async fn pending_reply_messages(&self) -> StoreResult<Vec<ReplyMessage>> {
        self.run(|connection| replies::pending_messages(connection))
            .await
    }

    pub async fn unreported_reply_tags(&self, agent_id: String) -> StoreResult<Vec<ReplyMessage>> {
        self.run(move |connection| replies::unreported_tags(connection, &agent_id))
            .await
    }

    pub async fn set_reply_message_state(&self, id: EffectId, state: String) -> StoreResult<()> {
        self.run(move |connection| replies::set_message_state(connection, id, &state))
            .await
    }

    pub async fn count_reply_attempt(&self, id: EffectId) -> StoreResult<()> {
        self.run(move |connection| replies::count_attempt(connection, id))
            .await
    }

    pub async fn last_turn_end(
        &self,
        agent_id: String,
        after_sequence: u64,
    ) -> StoreResult<Option<TurnEnd>> {
        self.run(move |connection| replies::last_turn_end(connection, &agent_id, after_sequence))
            .await
    }

    pub async fn record_reply_reminder(
        &self,
        id: EffectId,
        agent_id: String,
        turn_id: String,
        input_sha256: String,
        reported: Vec<EffectId>,
        now_ms: i64,
    ) -> StoreResult<()> {
        self.run(move |connection| {
            replies::record_reminder(
                connection,
                id,
                &agent_id,
                &turn_id,
                &input_sha256,
                &reported,
                now_ms,
            )
        })
        .await
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

    /// Sends steered without acknowledgement, still waiting for the target to
    /// record taking them.
    pub async fn awaiting_send_proof(&self) -> StoreResult<Vec<StoredWorkflow>> {
        self.run(|connection| {
            Ok(workflows_by_state(connection, WorkflowState::Submitted)?
                .into_iter()
                .filter(|record| record.proof.is_some())
                .collect())
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

    /// Whether the target of a send has answered it with a tagged reply or
    /// progress update, which shows it took the message.
    pub async fn request_answered(&self, request_id: WorkflowId) -> StoreResult<bool> {
        self.run(move |connection| {
            Ok(connection
                .query_row(
                    "SELECT 1 FROM reply_messages
                     WHERE kind IN ('final', 'progress')
                       AND json_extract(record_json, '$.request_id') = ?1
                     LIMIT 1",
                    params![request_id.to_string()],
                    |_| Ok(()),
                )
                .optional()?
                .is_some())
        })
        .await
    }

    /// Whether an agent, under any of its processes, recorded input with this
    /// hash since a time.
    pub async fn input_recorded_since(
        &self,
        agent_id: String,
        input_sha256: String,
        since_ms: i64,
    ) -> StoreResult<bool> {
        self.run(move |connection| {
            Ok(connection
                .query_row(
                    "SELECT 1 FROM agent_events
                     WHERE agent_id = ?1 AND created_at_ms >= ?2
                       AND kind IN ('input_accepted', 'turn_started')
                       AND json_extract(record_json, '$.input_sha256') = ?3
                     LIMIT 1",
                    params![agent_id, since_ms, input_sha256],
                    |_| Ok(()),
                )
                .optional()?
                .is_some())
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

    /// The question or approval an agent incarnation is still waiting on: its
    /// latest one, unless the agent has since written, taken input or ended a
    /// turn, or a later process of the agent has reported anything. An answer
    /// continues the turn, so the agent's next message is often the only sign
    /// of it. A message counts by when it was written: one written just before
    /// the question can be recorded after it.
    pub async fn open_approval(
        &self,
        agent_id: String,
        incarnation_id: String,
    ) -> StoreResult<Option<StoredApproval>> {
        self.run(move |connection| {
            let request_id = connection
                .query_row(
                    "SELECT json_extract(record_json, '$.approval.request_id')
                     FROM agent_events AS asked
                     WHERE agent_id = ?1 AND incarnation_id = ?2
                       AND kind = 'approval_requested' AND route_id IS NOT NULL
                       AND NOT EXISTS (
                           SELECT 1 FROM agent_events AS later
                           WHERE later.agent_id = ?1 AND later.sequence > asked.sequence
                             AND (later.incarnation_id != ?2
                                  OR later.kind IN ('input_accepted', 'turn_started', 'turn_final')
                                  OR (later.kind = 'assistant_message'
                                      AND julianday(json_extract(later.record_json, '$.observed_at'))
                                          > julianday(json_extract(asked.record_json, '$.observed_at')))))
                     ORDER BY sequence DESC
                     LIMIT 1",
                    params![agent_id, incarnation_id],
                    |row| row.get::<_, Option<String>>(0),
                )
                .optional()?
                .flatten();
            match request_id {
                Some(request_id) => get_approval(connection, &request_id),
                None => Ok(None),
            }
        })
        .await
    }

    /// Outbox posts in the order they were queued, after a feed position,
    /// with each post's position and queue time, and the latest position. A
    /// position is the row's SQLite rowid, which only grows since posts are
    /// never deleted.
    pub async fn outbox_feed(&self, after: i64, limit: u32) -> StoreResult<(Vec<FeedPost>, i64)> {
        self.run(move |connection| {
            let latest =
                connection.query_row("SELECT coalesce(max(rowid), 0) FROM outbox", [], |row| {
                    row.get::<_, i64>(0)
                })?;
            let mut statement = connection.prepare(
                "SELECT rowid, record_json, created_at_ms, request_id FROM outbox
                 WHERE rowid > ?1 ORDER BY rowid LIMIT ?2",
            )?;
            let rows = statement.query_map(params![after, limit], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })?;
            let mut posts = Vec::new();
            for row in rows {
                let (position, json, queued_at_ms, request_id) = row?;
                posts.push(FeedPost {
                    position,
                    post: serde_json::from_str(&json)?,
                    queued_at_ms,
                    request_id,
                });
            }
            Ok((posts, latest))
        })
        .await
    }

    /// Delivered Telegram posts since a time that still carry approval
    /// buttons.
    pub async fn approval_posts_with_buttons(&self, since_ms: i64) -> StoreResult<Vec<OutboxItem>> {
        self.run(move |connection| {
            let mut statement = connection.prepare(
                "SELECT record_json FROM outbox
                 WHERE channel = 'telegram' AND state = 'delivered' AND created_at_ms >= ?1
                   AND EXISTS (
                       SELECT 1 FROM json_each(record_json, '$.actions')
                       WHERE json_extract(value, '$.id') LIKE 'wakap:%')",
            )?;
            let rows = statement.query_map(params![since_ms], |row| row.get::<_, String>(0))?;
            rows.map(|json| Ok(serde_json::from_str::<OutboxItem>(&json?)?))
                .collect()
        })
        .await
    }

    /// The latest delivered Telegram post of a question form, by the form's
    /// request ID, which its buttons carry as `wakf:REQUEST:...`.
    pub async fn form_post(&self, request_id: String) -> StoreResult<Option<OutboxItem>> {
        self.run(move |connection| {
            connection
                .query_row(
                    "SELECT record_json FROM outbox
                     WHERE channel = 'telegram' AND state = 'delivered'
                       AND EXISTS (
                           SELECT 1 FROM json_each(record_json, '$.actions')
                           WHERE json_extract(value, '$.id') LIKE 'wakf:' || ?1 || ':%')
                     ORDER BY rowid DESC LIMIT 1",
                    params![request_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(|json| Ok(serde_json::from_str::<OutboxItem>(&json)?))
                .transpose()
        })
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
        Command::Run { job } => job(connection),
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
    } else if version < 6 {
        return Err(StoreError::OlderSchema {
            found: version,
            supported: SCHEMA_VERSION,
        });
    }
    if version == 6 {
        let transaction = connection.transaction()?;
        transaction.execute(
            "UPDATE routes
             SET route_json = json_remove(route_json, '$.agent', '$.status')",
            [],
        )?;
        transaction.pragma_update(None, "user_version", 7)?;
        transaction.commit()?;
    }
    if version < 8 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(replies::SCHEMA)?;
        transaction.pragma_update(None, "user_version", 8)?;
        transaction.commit()?;
    }
    if version < 9 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(rooms::SCHEMA)?;
        transaction.pragma_update(None, "user_version", 9)?;
        transaction.commit()?;
    }
    Ok(())
}

fn recover(connection: &mut Connection) -> StoreResult<()> {
    // Wakterm writes a prompt at most once per request ID, so an interrupted
    // admission waits in the busy queue to be asked again under the same ID.
    // An interrupted steer may have been typed and cannot be repeated.
    for (interrupted, recovered) in [
        (
            WorkflowState::AdmissionPrepared,
            WorkflowState::AwaitingTargetIdle,
        ),
        (WorkflowState::Steering, WorkflowState::Indeterminate),
    ] {
        for mut record in workflows_by_state(connection, interrupted)? {
            record.workflow.state = recovered;
            record.updated_at_ms = record.updated_at_ms.saturating_add(1);
            save_workflow(connection, &record, interrupted)?;
        }
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
        // An admission interrupted by a restart is asked again under the same
        // ID, which Wakterm writes at most once.
        "UPDATE inbox SET state = 'pending',
             record_json = json_remove(json_set(record_json, '$.state', 'pending'),
                 '$.proof', '$.receipt')
         WHERE state = 'admission_prepared'",
        [],
    )?;
    connection.execute(
        // A steer interrupted by a restart may have been typed, so it waits
        // for proof like any other unconfirmed delivery.
        "UPDATE inbox SET state = CASE WHEN json_extract(record_json, '$.proof') IS NULL
                 THEN 'indeterminate' ELSE 'awaiting_proof' END,
             record_json = json_set(record_json, '$.state',
                 CASE WHEN json_extract(record_json, '$.proof') IS NULL
                 THEN 'indeterminate' ELSE 'awaiting_proof' END)
         WHERE state = 'steering'",
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
        proof: None,
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

/// The text and buttons that ask the owner an agent's question or approval,
/// a question form showing the answers recorded so far.
pub(crate) fn approval_message(
    approval: &ApprovalRequest,
    form_state: &crate::wakterm::form::FormState,
) -> (String, Vec<crate::domain::OutboxAction>) {
    if crate::wakterm::form::answerable(approval) {
        return (
            crate::wakterm::form::text(approval, form_state),
            crate::wakterm::form::actions(approval, form_state),
        );
    }
    // Only a command approval or a Devin or Muse permission prompt
    // asks to approve something; every other kind, including ones
    // Panetone does not know, is a choice the agent is waiting on.
    let mut body = if matches!(
        approval.kind.as_str(),
        "command_approval" | "devin_permission" | "muse_permission"
    ) {
        "Approval needed".to_string()
    } else {
        "Input needed".to_string()
    };
    if let Some(prompt) = approval.prompt.as_deref() {
        body.push_str("\n\n");
        body.push_str(prompt);
    }
    // A prompt that already names the command, as Devin's
    // does, is not followed by it again.
    if let Some(command) = approval.command.as_deref().filter(|command| {
        !approval
            .prompt
            .as_deref()
            .is_some_and(|prompt| prompt.contains(command))
    }) {
        body.push_str("\n\nCommand:\n");
        body.push_str(command);
    }
    if let Some(reason) = approval.reason.as_deref() {
        body.push_str("\n\nReason: ");
        body.push_str(reason);
    }
    if approval.kind == "user_question_form" {
        body.push_str("\n\nThis form has several questions, so answer it in the agent's pane.");
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
        .iter()
        .map(|choice| crate::domain::OutboxAction {
            id: format!("wakap:{}:{}", approval.request_id, choice.id),
            label: choice.label.clone(),
        })
        .collect();
    (body, actions)
}

/// Records one agent event and projects any visible output or approval it
/// carries to its route's channel.
#[allow(clippy::too_many_arguments)]
fn ingest_event(
    transaction: &Transaction<'_>,
    event: &EventRecord,
    routes: &[Route],
    route_agents: &[RouteAgent],
    preferences: &std::collections::BTreeMap<String, String>,
    rooms: &std::collections::BTreeMap<String, RoomSettings>,
    now_ms: i64,
    outcome: &mut EventIngestOutcome,
) -> StoreResult<()> {
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
        return Ok(());
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
    // A message marked for the room joins it from any turn, and is shown
    // without the marker.
    let (visible_body, room_marked) = match visible_body {
        Some(body) if event.kind == "assistant_message" => match rooms::take_marker(&body) {
            Some(clean) => (Some(clean).filter(|body| !body.is_empty()), true),
            None => (Some(body), false),
        },
        other => (other, false),
    };
    // Output an agent produced long before Panetone saw it is a replay of
    // its history, such as a new incarnation reporting its transcript
    // again; it was shown when it was new. Late output of the agent's latest
    // turn is no replay: an agent's transcript can lag its turn's end.
    if visible_body.is_some()
        && approval.is_none()
        && event
            .observed_at_ms()
            .is_some_and(|observed| now_ms - observed > STALE_OUTPUT_MS)
        && !is_latest_turn(transaction, event)?
    {
        insert_agent_event(transaction, event, route_id, "stale", &record_json, now_ms)?;
        outcome.recorded += 1;
        return Ok(());
    }
    if approval.is_some() || visible_body.is_some() {
        let Some(route) = route else {
            state = "unrouted";
            outcome.unrouted += 1;
            insert_agent_event(transaction, event, None, state, &record_json, now_ms)?;
            outcome.recorded += 1;
            return Ok(());
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
                transaction,
                event,
                Some(route.id),
                state,
                &record_json,
                now_ms,
            )?;
            outcome.recorded += 1;
            return Ok(());
        }
        // A message tagged as a reply settles or reports the debt it
        // names, and is shown without its tag.
        let tagged = match (&approval, visible_body.as_deref()) {
            (None, Some(body)) if event.kind == "assistant_message" => replies::find_tags(body),
            _ => None,
        };
        // Whether the message is a reply: its tag answered a request, or the
        // agent owes one it failed to name. A tag written while nothing is
        // owed is reported to the agent, but its message is posted as if it
        // had no tag.
        let mut answered = false;
        for (index, (kind, named, section)) in tagged
            .iter()
            .flat_map(|tagged| tagged.sections.iter())
            .enumerate()
        {
            let namespace = Uuid::new_v5(
                &Uuid::NAMESPACE_URL,
                b"https://panetone.dev/replies/tagged/v1",
            );
            answered |= replies::record_tag(
                transaction,
                EffectId::new(Uuid::new_v5(
                    &namespace,
                    format!("{}\0{}\0{index}", event.incarnation_id, event.event_id).as_bytes(),
                )),
                &event.agent_id,
                route.id,
                *kind,
                named.as_deref(),
                section,
                now_ms,
            )?;
        }
        // In a room, an agent's message joins the transcript and goes to the
        // room's other agents. A member route's agent joins with a marked
        // message here, and from a room turn with its final message, when
        // the turn ends; its notes along the way stay on its own route.
        if let (Some((room_id, settings)), true, Some(body)) = (
            rooms::room_of(rooms, route.id),
            event.kind == "assistant_message" && approval.is_none(),
            visible_body.as_deref(),
        ) && (room_id == route.id || room_marked)
        {
            let text = tagged.as_ref().map_or(body, |tagged| tagged.clean.as_str());
            post_to_room(
                transaction,
                routes,
                route_agents,
                route,
                route_agent,
                (room_id, settings),
                event,
                text,
                now_ms,
            )?;
        }
        let destination = if approval.is_some() {
            route
                .telegram_topic()
                .map(|topic_id| (ChannelKind::Telegram, topic_id.to_string()))
        } else {
            let preference = preferences.get(&route.id.to_string()).map(String::as_str);
            // Where a turn's output goes follows the message that started
            // it, so a reply to a direct message can never reach the
            // group because a group message arrived meanwhile, and a
            // reply to the group never goes to a direct chat.
            let origin = turn_origin(transaction, event)?;
            let preference = match &origin {
                Some((ChannelKind::Signal, destination))
                    if destination.starts_with(crate::domain::SIGNAL_DIRECT_PREFIX) =>
                {
                    Some(format!("sig:{destination}"))
                }
                Some((ChannelKind::Signal, _))
                    if preference.is_some_and(|value| value.starts_with("sig:direct:")) =>
                {
                    Some("sig".to_owned())
                }
                Some((ChannelKind::Telegram, destination))
                    if preference.is_some_and(|value| value.starts_with("sig:direct:")) =>
                {
                    Some(format!("tg:{destination}"))
                }
                _ => preference.map(str::to_owned),
            };
            route.output_destination(preference.as_deref())
        };
        // A reply never appears in a group whose other members are not
        // its audience; the route's Telegram topic shows it instead.
        let is_reply = tagged.is_some()
            && (answered || replies::owes_reply(transaction, &event.agent_id, route.id)?);
        let destination = match (&tagged, destination) {
            (Some(tagged), _) if tagged.clean.is_empty() => None,
            (Some(_), Some((kind, group))) if is_reply && route.is_shared(kind, &group) => route
                .telegram_topic()
                .map(|topic_id| (ChannelKind::Telegram, topic_id.to_string())),
            (_, destination) => destination,
        };
        let Some((kind, destination)) = destination else {
            state = "unrouted";
            outcome.unrouted += 1;
            insert_agent_event(
                transaction,
                event,
                Some(route.id),
                state,
                &record_json,
                now_ms,
            )?;
            outcome.recorded += 1;
            return Ok(());
        };
        let (body, attachments, actions) = match approval {
            Some(approval) => {
                let (body, actions) =
                    approval_message(&approval, &crate::wakterm::form::FormState::default());
                (body, Vec::new(), actions)
            }
            None => {
                let text = match tagged {
                    Some(tagged) => tagged.clean,
                    None => visible_body.expect("visible output was checked"),
                };
                let (body, attachments) = visible_attachments(text);
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
            part: None,
            owner: None,
            turn_end: None,
        };
        for chunk in crate::domain::chunk_outbox(item) {
            enqueue_outbox(transaction, None, &chunk, now_ms)?;
        }
        state = "projected";
        outcome.visible_outputs += 1;
        if let Some(candidate) = route_agent {
            outcome.remember_last_agent(candidate);
        }
    }
    // A turn's start ends the agent's previous turn; one that never reported
    // its end, as when a mux restart or a crash cut it off, gives a room
    // member's last message from it to the room.
    if event.kind == "turn_started"
        && let Some(route) = route
        && let Some((message, text)) =
            cut_off_room_answer(transaction, rooms, route, event, now_ms)?
    {
        let room = rooms::room_of(rooms, route.id).expect("the answer's room exists");
        post_to_room(
            transaction,
            routes,
            route_agents,
            route,
            route_agent,
            room,
            &message,
            &text,
            now_ms,
        )?;
    }
    if event.kind == "turn_final"
        && let Some(route) = route
    {
        record_turn_end(transaction, event, route, state == "projected", now_ms)?;
        tell_owner_of_silent_turn(transaction, route, event, now_ms)?;
        if let Some(text) = member_room_answer(transaction, rooms, route, event, now_ms)? {
            let room = rooms::room_of(rooms, route.id).expect("the answer's room exists");
            post_to_room(
                transaction,
                routes,
                route_agents,
                route,
                route_agent,
                room,
                event,
                &text,
                now_ms,
            )?;
        }
    }
    insert_agent_event(transaction, event, route_id, state, &record_json, now_ms)?;
    outcome.recorded += 1;
    Ok(())
}

/// Tells the owner when an agent's turn that their own message started ends
/// with nothing posted, as when the model returns an empty answer, so the
/// message does not look ignored. A turn the agent closed with
/// <panetone:no-reply> said what it meant, and an aborted turn is reported
/// as a failure already.
fn tell_owner_of_silent_turn(
    transaction: &Transaction<'_>,
    route: &Route,
    event: &EventRecord,
    now_ms: i64,
) -> StoreResult<()> {
    if event.fields.get("outcome").and_then(|value| value.as_str()) != Some("completed")
        || (event
            .observed_at_ms()
            .is_some_and(|observed| now_ms - observed > STALE_OUTPUT_MS)
            && !is_latest_turn(transaction, event)?)
    {
        return Ok(());
    }
    let Some(started) = turn_start(transaction, event)? else {
        return Ok(());
    };
    let answered = transaction
        .query_row(
            "SELECT 1 FROM agent_events
             WHERE agent_id = ?1 AND incarnation_id = ?2
               AND sequence > ?3 AND sequence < ?4
               AND kind = 'assistant_message' AND state IN ('projected', 'suppressed')
               AND json_extract(record_json, '$.turn_id') = ?5
             LIMIT 1",
            params![
                event.agent_id,
                event.incarnation_id,
                started,
                event.sequence as i64,
                event.fields.get("turn_id").and_then(|value| value.as_str())
            ],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if answered {
        return Ok(());
    }
    // Only a turn the owner's message started: its origin is a channel
    // message Panetone delivered.
    let Some((kind, destination)) = turn_origin(transaction, event)? else {
        return Ok(());
    };
    let Some((kind, destination)) = (if route.is_shared(kind, &destination) {
        route.private_destination()
    } else {
        Some((kind, destination))
    }) else {
        return Ok(());
    };
    let namespace = Uuid::new_v5(&Uuid::NAMESPACE_URL, b"https://panetone.dev/no-reply/v1");
    let item = OutboxItem {
        id: EffectId::new(Uuid::new_v5(
            &namespace,
            format!("{}\0{}", event.incarnation_id, event.event_id).as_bytes(),
        )),
        route_id: Some(route.id),
        sender_harness: None,
        source_agent: None,
        kind,
        destination,
        body: format!(
            "[no reply] {}'s turn ended without an answer to your message. Send it again if it still matters.",
            route.title
        ),
        attachments: Vec::new(),
        actions: Vec::new(),
        state: OutboxState::Pending,
        attempts: 0,
        last_error: None,
        external_receipt: None,
        uncertain_attempts: 0,
        part: None,
        owner: None,
        turn_end: None,
    };
    enqueue_outbox(transaction, None, &item, now_ms)
}

/// The answer a member route's agent gives its room when a room turn ends:
/// the turn's final message, unless it says nothing, was already posted with
/// the room marker, or is a replay of an old turn.
fn member_room_answer(
    transaction: &Transaction<'_>,
    rooms: &std::collections::BTreeMap<String, RoomSettings>,
    route: &Route,
    event: &EventRecord,
    now_ms: i64,
) -> StoreResult<Option<String>> {
    if !rooms::room_of(rooms, route.id).is_some_and(|(room_id, _)| room_id != route.id) {
        return Ok(None);
    }
    // An aborted turn's final carries its failure; its answer is the last
    // message it wrote before it was cut off.
    let text = if event.fields.get("outcome").and_then(|value| value.as_str()) == Some("aborted") {
        let Some(started) = turn_start(transaction, event)? else {
            return Ok(None);
        };
        last_turn_message(transaction, event, started)?.and_then(|message| {
            message
                .fields
                .get("text")
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
    } else {
        event
            .fields
            .get("text")
            .and_then(|value| value.as_str())
            .map(str::to_owned)
    };
    let Some(text) = text
        .map(|text| strip_memory_citations(&text))
        .filter(|text| !text.trim().is_empty() && text.trim() != NO_REPLY)
    else {
        return Ok(None);
    };
    if rooms::take_marker(&text).is_some()
        || (event
            .observed_at_ms()
            .is_some_and(|observed| now_ms - observed > STALE_OUTPUT_MS)
            && !is_latest_turn(transaction, event)?)
        || !rooms::is_room_turn(transaction, event)?
    {
        return Ok(None);
    }
    Ok(Some(
        replies::find_tags(&text).map_or(text, |tagged| tagged.clean),
    ))
}

/// The last message an agent wrote in the turn of `event` before it, from the
/// turn's start at sequence `started`.
fn last_turn_message(
    transaction: &Transaction<'_>,
    event: &EventRecord,
    started: i64,
) -> StoreResult<Option<EventRecord>> {
    transaction
        .query_row(
            "SELECT record_json FROM agent_events
             WHERE agent_id = ?1 AND incarnation_id = ?2
               AND sequence > ?3 AND sequence < ?4
               AND kind = 'assistant_message'
               AND json_extract(record_json, '$.turn_id') = ?5
             ORDER BY sequence DESC LIMIT 1",
            params![
                event.agent_id,
                event.incarnation_id,
                started,
                event.sequence as i64,
                event.fields.get("turn_id").and_then(|value| value.as_str())
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| Ok(serde_json::from_str(&json)?))
        .transpose()
}

/// When a turn starts, the agent's previous turn is over. If that turn was a
/// room member's room turn that never reported its end, as when a mux
/// restart or a crash cut it off, its last message is the member's answer to
/// the room, unless it is old, says nothing, or already joined with the room
/// marker.
fn cut_off_room_answer(
    transaction: &Transaction<'_>,
    rooms: &std::collections::BTreeMap<String, RoomSettings>,
    route: &Route,
    event: &EventRecord,
    now_ms: i64,
) -> StoreResult<Option<(EventRecord, String)>> {
    if !rooms::room_of(rooms, route.id).is_some_and(|(room_id, _)| room_id != route.id) {
        return Ok(None);
    }
    let turn_id = event.fields.get("turn_id").and_then(|value| value.as_str());
    // The agent's previous turn, in any incarnation: a restart brings a new one.
    let Some((started, incarnation_id, previous_turn)) = transaction
        .query_row(
            "SELECT sequence, incarnation_id, json_extract(record_json, '$.turn_id')
             FROM agent_events
             WHERE agent_id = ?1 AND sequence < ?2 AND kind = 'turn_started'
               AND json_extract(record_json, '$.turn_id') IS NOT ?3
             ORDER BY sequence DESC LIMIT 1",
            params![event.agent_id, event.sequence as i64, turn_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?
    else {
        return Ok(None);
    };
    let ended = transaction
        .query_row(
            "SELECT 1 FROM agent_events
             WHERE agent_id = ?1 AND incarnation_id = ?2 AND sequence > ?3 AND sequence < ?4
               AND kind = 'turn_final' AND json_extract(record_json, '$.turn_id') = ?5
             LIMIT 1",
            params![
                event.agent_id,
                incarnation_id,
                started,
                event.sequence as i64,
                previous_turn
            ],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if ended {
        return Ok(None);
    }
    let previous = EventRecord {
        sequence: event.sequence,
        event_id: String::new(),
        kind: "turn_started".into(),
        agent_id: event.agent_id.clone(),
        incarnation_id: incarnation_id.clone(),
        fields: [("turn_id".to_owned(), previous_turn.into())]
            .into_iter()
            .collect(),
    };
    let Some(message) = last_turn_message(transaction, &previous, started)? else {
        return Ok(None);
    };
    let Some(text) = message
        .fields
        .get("text")
        .and_then(|value| value.as_str())
        .map(strip_memory_citations)
        .filter(|text| !text.trim().is_empty() && text.trim() != NO_REPLY)
    else {
        return Ok(None);
    };
    if rooms::take_marker(&text).is_some()
        || message
            .observed_at_ms()
            .is_none_or(|observed| now_ms - observed > STALE_OUTPUT_MS)
        || !rooms::is_room_turn(transaction, &message)?
    {
        return Ok(None);
    }
    let text = replies::find_tags(&text).map_or(text, |tagged| tagged.clean);
    Ok(Some((message, text)))
}

/// Adds an agent's message to its room's transcript, queues it for the room's
/// other agents, and, for a member route's agent, posts it to the room
/// route's channel under the agent's name.
#[allow(clippy::too_many_arguments)]
fn post_to_room(
    transaction: &Transaction<'_>,
    routes: &[Route],
    route_agents: &[RouteAgent],
    route: &Route,
    route_agent: Option<&RouteAgent>,
    (room_id, settings): (RouteId, &RoomSettings),
    event: &EventRecord,
    text: &str,
    now_ms: i64,
) -> StoreResult<()> {
    let text = text
        .lines()
        .filter(|line| !line.trim_start().starts_with("[panetone:attach "))
        .collect::<Vec<_>>()
        .join("\n");
    let text = text.trim();
    if text.is_empty() {
        return Ok(());
    }
    let recipients = if settings.paused {
        Vec::new()
    } else {
        route_agents
            .iter()
            .filter(|candidate| {
                (candidate.route_id == room_id || settings.members.contains(&candidate.route_id))
                    && candidate.agent.agent_id != event.agent_id
            })
            .map(|candidate| candidate.agent.agent_id.clone())
            .collect()
    };
    let harness = route_agent.map(|candidate| candidate.agent.harness.clone());
    rooms::post(
        transaction,
        room_id,
        RoomMessage {
            id: 0,
            kind: "agent".into(),
            speaker: event.agent_id.clone(),
            harness: harness.clone(),
            text: text.to_owned(),
            created_at_ms: now_ms,
        },
        &recipients,
    )?;
    // A member's own channel shows its output as usual; the room's channel
    // shows it too, under the agent's name.
    let Some((kind, destination)) = (room_id != route.id)
        .then(|| routes.iter().find(|candidate| candidate.id == room_id))
        .flatten()
        .and_then(Route::private_destination)
    else {
        return Ok(());
    };
    let names: std::collections::BTreeMap<String, String> =
        get_metadata(transaction, ROOM_NAMES_KEY)?
            .map(|json| serde_json::from_str(&json))
            .transpose()?
            .unwrap_or_default();
    let name = names
        .get(&event.agent_id)
        .cloned()
        .unwrap_or_else(|| route.title.clone());
    let namespace = Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        b"https://panetone.dev/rooms/members/v1",
    );
    let item = OutboxItem {
        id: EffectId::new(Uuid::new_v5(
            &namespace,
            format!("{}\0{}", event.incarnation_id, event.event_id).as_bytes(),
        )),
        route_id: Some(room_id),
        sender_harness: harness,
        source_agent: route_agent.map(|candidate| candidate.agent.clone()),
        kind,
        destination,
        body: format!("{name}: {text}"),
        attachments: Vec::new(),
        actions: Vec::new(),
        state: OutboxState::Pending,
        attempts: 0,
        last_error: None,
        external_receipt: None,
        uncertain_attempts: 0,
        part: None,
        owner: None,
        turn_end: None,
    };
    for chunk in crate::domain::chunk_outbox(item) {
        enqueue_outbox(transaction, None, &chunk, now_ms)?;
    }
    Ok(())
}

/// The outbox ID of the post an agent event is projected to.
fn event_post_id(incarnation_id: &str, event_id: &str) -> EffectId {
    let namespace = Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        b"https://panetone.dev/wakterm/events/v1",
    );
    EffectId::new(Uuid::new_v5(
        &namespace,
        format!("{incarnation_id}\0{event_id}").as_bytes(),
    ))
}

/// Records that an agent's turn ended, for the feed, naming the turn's last
/// posted message. The record goes where that message went, or to the
/// route's private channel, and is stored as delivered so it is never sent.
fn record_turn_end(
    transaction: &Transaction<'_>,
    event: &EventRecord,
    route: &Route,
    // An aborted turn's end is itself posted, as a failure notice, which is
    // then the turn's last message.
    posted_itself: bool,
    now_ms: i64,
) -> StoreResult<()> {
    let Some(turn_id) = event.fields.get("turn_id").and_then(|value| value.as_str()) else {
        return Ok(());
    };
    let last = transaction
        .query_row(
            "SELECT event_id FROM agent_events
             WHERE agent_id = ?1 AND incarnation_id = ?2 AND kind = 'assistant_message'
               AND state = 'projected' AND json_extract(record_json, '$.turn_id') = ?3
             ORDER BY sequence DESC LIMIT 1",
            params![event.agent_id, event.incarnation_id, turn_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let final_post = if posted_itself {
        Some(event_post_id(&event.incarnation_id, &event.event_id))
    } else {
        last.map(|event_id| event_post_id(&event.incarnation_id, &event_id))
    };
    // A long message is posted in parts under IDs derived from its own.
    let posted = match final_post {
        Some(id) => transaction
            .query_row(
                "SELECT record_json FROM outbox WHERE effect_id IN (?1, ?2) LIMIT 1",
                params![id.to_string(), EffectId::chunk(id, 0).to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|json| serde_json::from_str::<OutboxItem>(&json))
            .transpose()?,
        None => None,
    };
    let Some((kind, destination)) = posted
        .map(|post| (post.kind, post.destination))
        .or_else(|| route.private_destination())
    else {
        return Ok(());
    };
    let marker = OutboxItem {
        id: EffectId::named(
            WorkflowId::new(event_post_id(&event.incarnation_id, &event.event_id).as_uuid()),
            "turn-end",
        ),
        route_id: Some(route.id),
        sender_harness: None,
        source_agent: Some(AgentBinding {
            agent_id: event.agent_id.clone(),
            incarnation_id: event.incarnation_id.clone(),
            harness: String::new(),
            pane_id: None,
        }),
        kind,
        destination,
        body: String::new(),
        attachments: Vec::new(),
        actions: Vec::new(),
        state: OutboxState::Delivered,
        attempts: 0,
        last_error: None,
        external_receipt: None,
        uncertain_attempts: 0,
        part: None,
        owner: None,
        turn_end: Some(crate::domain::TurnEndPost { final_post }),
    };
    enqueue_outbox(transaction, None, &marker, now_ms)?;
    Ok(())
}

/// Endchat messages are kept beside Wakterm's events under sequence numbers
/// far above any Wakterm uses, each tagged with its endchat agent.
const ENDCHAT_SEQUENCE_BASE: u64 = 1 << 60;

#[allow(clippy::too_many_arguments)]
fn ingest_endchat_messages(
    connection: &mut Connection,
    route_id: RouteId,
    agent: AgentBinding,
    messages: &[crate::endchat::Entry],
    cursor_key: &str,
    cursor: &str,
    now_ms: i64,
) -> StoreResult<EventIngestOutcome> {
    let transaction = connection.transaction()?;
    let routes = list_routes(&transaction)?;
    let preferences = route_preferences(&transaction)?;
    let rooms = rooms::rooms(&transaction)?;
    let route_agents = [RouteAgent { route_id, agent }];
    let mut outcome = EventIngestOutcome::default();
    let mut count = get_metadata(&transaction, "endchat_event_count")?
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    // A turn is named by the message that started it, so its end and its
    // replies can be matched with its start, as Wakterm events are.
    let turn_key = format!("endchat_turn:{}", route_agents[0].agent.agent_id);
    let mut turn = get_metadata(&transaction, &turn_key)?.unwrap_or_default();
    // Catching up on a log, as after a stall, reads many turns at once; only
    // the last one's late output is still news, the rest is stale replay.
    let last_turn = messages
        .iter()
        .rev()
        .find_map(|entry| match entry {
            crate::endchat::Entry::User { i, date, .. } => Some(format!("endchat-{i}@{date}")),
            _ => None,
        })
        .unwrap_or_else(|| turn.clone());
    // Every profile numbers its log from 0, and a profile's log can be
    // started over, while an event ID must name one event across all agents
    // and all time, so it carries the profile's agent ID and the record's
    // own time.
    let event_id =
        |name: String, date: &str| format!("{}#{name}@{date}", route_agents[0].agent.agent_id);
    for entry in messages {
        let mut fields = serde_json::Map::new();
        let (kind, event_id, date) = match entry {
            crate::endchat::Entry::User { i, text, date } => {
                turn = format!("endchat-{i}@{date}");
                fields.insert(
                    "input_sha256".into(),
                    DeliveryProof::input_sha256(text).into(),
                );
                ("turn_started", event_id(format!("{i}"), date), date)
            }
            crate::endchat::Entry::Agent { i, text, date } => {
                fields.insert("text".into(), text.clone().into());
                ("assistant_message", event_id(format!("{i}"), date), date)
            }
            crate::endchat::Entry::TurnEnd { end, ok, date } => {
                fields.insert(
                    "outcome".into(),
                    if *ok { "completed" } else { "aborted" }.into(),
                );
                ("turn_final", event_id(format!("end-{end}"), date), date)
            }
        };
        // A record read again, as after its log was rewritten, was already
        // recorded.
        if transaction
            .query_row(
                "SELECT 1 FROM agent_events WHERE event_id = ?1 AND incarnation_id = ?2",
                params![event_id, route_agents[0].agent.incarnation_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some()
        {
            continue;
        }
        count += 1;
        fields.insert("observed_at".into(), date.clone().into());
        fields.insert("turn_id".into(), turn.clone().into());
        let event = EventRecord {
            sequence: ENDCHAT_SEQUENCE_BASE + count,
            event_id,
            kind: kind.into(),
            agent_id: route_agents[0].agent.agent_id.clone(),
            incarnation_id: route_agents[0].agent.incarnation_id.clone(),
            fields,
        };
        if kind == "assistant_message"
            && turn != last_turn
            && event
                .observed_at_ms()
                .is_some_and(|observed| now_ms - observed > STALE_OUTPUT_MS)
        {
            insert_agent_event(
                &transaction,
                &event,
                Some(route_id),
                "stale",
                &serde_json::to_string(&event)?,
                now_ms,
            )?;
            outcome.recorded += 1;
            continue;
        }
        ingest_event(
            &transaction,
            &event,
            &routes,
            &route_agents,
            &preferences,
            &rooms,
            now_ms,
            &mut outcome,
        )?;
    }
    set_metadata(&transaction, "endchat_event_count", &count.to_string())?;
    set_metadata(&transaction, &turn_key, &turn)?;
    set_metadata(&transaction, cursor_key, cursor)?;
    transaction.commit()?;
    Ok(outcome)
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
    let rooms = rooms::rooms(&transaction)?;
    let mut outcome = EventIngestOutcome {
        next_after_sequence: next,
        ..EventIngestOutcome::default()
    };

    for event in events {
        ingest_event(
            &transaction,
            event,
            &routes,
            route_agents,
            &preferences,
            &rooms,
            now_ms,
            &mut outcome,
        )?;
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

/// The channel and destination of the inbound message that started the turn
/// an event belongs to, when a channel message started it.
/// The latest start of the turn of an agent's event, by event sequence.
fn turn_start(connection: &Connection, event: &EventRecord) -> StoreResult<Option<i64>> {
    let Some(turn_id) = event.fields.get("turn_id").and_then(|value| value.as_str()) else {
        return Ok(None);
    };
    Ok(connection
        .query_row(
            "SELECT sequence FROM agent_events
             WHERE agent_id = ?1 AND incarnation_id = ?2 AND sequence < ?3
               AND kind = 'turn_started' AND json_extract(record_json, '$.turn_id') = ?4
             ORDER BY sequence DESC LIMIT 1",
            params![
                event.agent_id,
                event.incarnation_id,
                event.sequence as i64,
                turn_id
            ],
            |row| row.get::<_, i64>(0),
        )
        .optional()?)
}

/// The input that started the turn of an agent's event: the first input hash
/// recorded from the turn's latest start. Claude and agy put it on
/// `turn_started`, while Codex records it on the `input_accepted` that follows.
pub(super) fn turn_input_sha256(
    connection: &Connection,
    event: &EventRecord,
) -> StoreResult<Option<String>> {
    let Some(started) = turn_start(connection, event)? else {
        return Ok(None);
    };
    Ok(connection
        .query_row(
            "SELECT json_extract(record_json, '$.input_sha256') FROM agent_events
             WHERE agent_id = ?1 AND incarnation_id = ?2
               AND sequence >= ?3 AND sequence < ?4
               AND kind IN ('turn_started', 'input_accepted')
               AND json_extract(record_json, '$.turn_id') = ?5
               AND json_extract(record_json, '$.input_sha256') IS NOT NULL
             ORDER BY sequence LIMIT 1",
            params![
                event.agent_id,
                event.incarnation_id,
                started,
                event.sequence as i64,
                event.fields.get("turn_id").and_then(|value| value.as_str())
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?)
}

/// The input that started the turn of an agent's event, as Wakterm
/// recorded it: the Panetone request that typed it, when Wakterm names one on
/// the turn's starting `input_accepted`, and its hash. Matching by request is
/// exact; the hash serves input Wakterm names no request for, such as
/// hand-typed input, and events recorded before Wakterm named requests.
pub(super) struct TurnInput {
    pub request_id: Option<String>,
    pub input_sha256: Option<String>,
}

pub(super) fn turn_input(connection: &Connection, event: &EventRecord) -> StoreResult<TurnInput> {
    let request_id = connection
        .query_row(
            "SELECT json_extract(record_json, '$.request_id') FROM agent_events
             WHERE agent_id = ?1 AND incarnation_id = ?2 AND sequence < ?3
               AND kind = 'input_accepted'
               AND json_extract(record_json, '$.input_role') = 'start'
               AND json_extract(record_json, '$.turn_id') = ?4
               AND json_extract(record_json, '$.request_id') IS NOT NULL
             ORDER BY sequence DESC LIMIT 1",
            params![
                event.agent_id,
                event.incarnation_id,
                event.sequence as i64,
                event.fields.get("turn_id").and_then(|value| value.as_str())
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    Ok(TurnInput {
        request_id,
        input_sha256: turn_input_sha256(connection, event)?,
    })
}

fn turn_origin(
    connection: &Connection,
    event: &EventRecord,
) -> StoreResult<Option<(ChannelKind, String)>> {
    let input = turn_input(connection, event)?;
    if input.request_id.is_none() && input.input_sha256.is_none() {
        return Ok(None);
    }
    let origin = connection
        .query_row(
            "SELECT channel, json_extract(record_json, '$.destination') FROM inbox
             WHERE json_extract(record_json, '$.proof.agent_id') = ?1
               AND (json_extract(record_json, '$.proof.request_id') = ?3
                    OR ?2 IN (
                        json_extract(record_json, '$.proof.input_sha256'),
                        json_extract(record_json, '$.proof.tab_expanded_sha256')
                    ))
             ORDER BY created_at_ms DESC LIMIT 1",
            params![event.agent_id, input.input_sha256, input.request_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    Ok(origin.map(|(channel, destination)| {
        let kind = if channel == channel_name(ChannelKind::Signal) {
            ChannelKind::Signal
        } else {
            ChannelKind::Telegram
        };
        (kind, destination)
    }))
}

/// Whether a Signal destination is a group of the route that admits members
/// other than the owner.
fn route_preferences(
    connection: &Connection,
) -> StoreResult<std::collections::BTreeMap<String, String>> {
    let value = get_metadata(connection, "route_output_preferences_v1")?;
    value
        .map(|value| serde_json::from_str(&value).map_err(StoreError::from))
        .transpose()
        .map(Option::unwrap_or_default)
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
                   -- A steered request is answered by message, not by a terminal.
                   AND COALESCE(
                       json_extract(workflow.record_json, '$.response.delivery_state'), ''
                   ) != 'steered'
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

/// Whether an event belongs to the turn its agent most recently started.
fn is_latest_turn(connection: &Connection, event: &EventRecord) -> StoreResult<bool> {
    let Some(turn_id) = event.fields.get("turn_id").and_then(Value::as_str) else {
        return Ok(false);
    };
    Ok(connection
        .query_row(
            "SELECT json_extract(record_json, '$.turn_id') FROM agent_events
             WHERE agent_id = ?1 AND kind = 'turn_started'
             ORDER BY sequence DESC LIMIT 1",
            params![event.agent_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten()
        .is_some_and(|latest| latest == turn_id))
}

fn input_recorded(connection: &Connection, proof: &DeliveryProof) -> StoreResult<bool> {
    Ok(connection
        .query_row(
            "SELECT 1 FROM agent_events
             WHERE agent_id = ?1 AND sequence > ?2
               AND kind IN ('input_accepted', 'turn_started')
               AND (json_extract(record_json, '$.request_id') = ?5
                    OR json_extract(record_json, '$.input_sha256') IN (?3, ?4))
             LIMIT 1",
            params![
                proof.agent_id,
                proof.after_sequence,
                proof.input_sha256,
                proof.tab_expanded_sha256,
                proof.request_id
            ],
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
            // The output goes back where the message came from, such as a
            // reply to a direct message to its sender.
            match channel {
                ChannelKind::Telegram => format!("tg:{}", item.destination),
                ChannelKind::Signal => format!("sig:{}", item.destination),
            },
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
