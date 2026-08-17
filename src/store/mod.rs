use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::domain::{
    AgentBinding, CallbackDelivery, ChannelKind, DeliveryState, EffectId, OutboxItem, OutboxState,
    ReconcileDecision, Route, RouteId, SEMANTIC_HASH_KIND, SendCommand, Workflow, WorkflowId,
    WorkflowState, semantic_request_hash, stored_request_hash_matches,
};
use crate::promotion::{
    EventCursorGap, LegacyDecision, LegacyRecordKind, OperatorAction, OperatorMutation,
    OperatorOutcome, PromotionStatus,
};
use crate::wakterm::{AgentCatalog, EventRecord};

pub const SCHEMA_VERSION: i64 = 5;
const COMMAND_CAPACITY: usize = 128;
const PHASE5_SCHEMA_SQL: &str = "
    CREATE TABLE promotion_state (
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        delivery_hold INTEGER NOT NULL CHECK(delivery_hold IN (0, 1)),
        event_cursor INTEGER,
        updated_at_ms INTEGER NOT NULL
    );
    INSERT INTO promotion_state(singleton, delivery_hold, event_cursor, updated_at_ms)
    VALUES (1, 1, NULL, 0);
    CREATE TABLE route_delivery_policy (
        route_id TEXT PRIMARY KEY REFERENCES routes(route_id),
        enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
        updated_at_ms INTEGER NOT NULL
    );
    CREATE TABLE legacy_dispositions (
        record_kind TEXT NOT NULL,
        record_id TEXT NOT NULL,
        decision TEXT NOT NULL,
        evidence TEXT NOT NULL,
        operation_id TEXT NOT NULL UNIQUE,
        record_json TEXT NOT NULL,
        resolved_at_ms INTEGER NOT NULL,
        PRIMARY KEY(record_kind, record_id)
    );
    CREATE TABLE operator_actions (
        operation_id TEXT PRIMARY KEY,
        action_json TEXT NOT NULL,
        outcome_json TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL
    );
    PRAGMA user_version = 4;
";
const EVENT_SCHEMA_SQL: &str = "
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
    PRAGMA user_version = 5;
";

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
    pub body: String,
    pub state: String,
    pub created_at_ms: i64,
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
    pub legacy_control_requests: u64,
    pub legacy_indeterminate_requests: u64,
    pub legacy_unresolved_returns: u64,
    pub legacy_debate_outbox: u64,
    pub signal_messages: u64,
    pub unrouted_agent_events: u64,
    pub observer_failure_events: u64,
    pub promotion: PromotionStatus,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventIngestOutcome {
    pub recorded: u64,
    pub visible_outputs: u64,
    pub unrouted: u64,
    pub next_after_sequence: u64,
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
        record: StoredWorkflow,
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
    EnqueueOutbox {
        workflow_id: Option<WorkflowId>,
        item: OutboxItem,
        now_ms: i64,
        reply: oneshot::Sender<StoreResult<()>>,
    },
    SaveOutbox {
        item: OutboxItem,
        now_ms: i64,
        reply: oneshot::Sender<StoreResult<()>>,
    },
    PendingOutbox {
        reply: oneshot::Sender<StoreResult<Vec<OutboxItem>>>,
    },
    AcceptInbox {
        item: InboxItem,
        reply: oneshot::Sender<StoreResult<bool>>,
    },
    PendingInbox {
        reply: oneshot::Sender<StoreResult<Vec<InboxItem>>>,
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
    PromotionStatus {
        reply: oneshot::Sender<StoreResult<PromotionStatus>>,
    },
    ApplyOperatorMutation {
        mutation: OperatorMutation,
        now_ms: i64,
        reply: oneshot::Sender<StoreResult<OperatorOutcome>>,
    },
    OperatorReplay {
        operation_id: Uuid,
        intent: Value,
        reply: oneshot::Sender<StoreResult<Option<OperatorOutcome>>>,
    },
    AdvanceEventCursor {
        expected: u64,
        next: u64,
        reply: oneshot::Sender<StoreResult<()>>,
    },
    IngestAgentEvents {
        expected: u64,
        next: u64,
        events: Vec<EventRecord>,
        now_ms: i64,
        reply: oneshot::Sender<StoreResult<EventIngestOutcome>>,
    },
    RecoverEventCursorGap {
        gap: EventCursorGap,
        catalog: AgentCatalog,
        reply: oneshot::Sender<StoreResult<PromotionStatus>>,
    },
    Compact {
        before_ms: i64,
        reply: oneshot::Sender<StoreResult<u64>>,
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
            record,
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

    pub async fn enqueue_outbox(
        &self,
        workflow_id: Option<WorkflowId>,
        item: OutboxItem,
        now_ms: i64,
    ) -> StoreResult<()> {
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

    pub async fn accept_inbox(&self, item: InboxItem) -> StoreResult<bool> {
        self.request(|reply| Command::AcceptInbox { item, reply })
            .await
    }

    pub async fn pending_inbox(&self) -> StoreResult<Vec<InboxItem>> {
        self.request(|reply| Command::PendingInbox { reply }).await
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

    pub async fn promotion_status(&self) -> StoreResult<PromotionStatus> {
        self.request(|reply| Command::PromotionStatus { reply })
            .await
    }

    pub async fn apply_operator_mutation(
        &self,
        mutation: OperatorMutation,
        now_ms: i64,
    ) -> StoreResult<OperatorOutcome> {
        self.request(|reply| Command::ApplyOperatorMutation {
            mutation,
            now_ms,
            reply,
        })
        .await
    }

    pub async fn operator_replay(
        &self,
        operation_id: Uuid,
        intent: Value,
    ) -> StoreResult<Option<OperatorOutcome>> {
        self.request(|reply| Command::OperatorReplay {
            operation_id,
            intent,
            reply,
        })
        .await
    }

    pub async fn advance_event_cursor(&self, expected: u64, next: u64) -> StoreResult<()> {
        self.request(|reply| Command::AdvanceEventCursor {
            expected,
            next,
            reply,
        })
        .await
    }

    pub async fn ingest_agent_events(
        &self,
        expected: u64,
        next: u64,
        events: Vec<EventRecord>,
        now_ms: i64,
    ) -> StoreResult<EventIngestOutcome> {
        self.request(|reply| Command::IngestAgentEvents {
            expected,
            next,
            events,
            now_ms,
            reply,
        })
        .await
    }

    pub async fn recover_event_cursor_gap(
        &self,
        gap: EventCursorGap,
        catalog: AgentCatalog,
    ) -> StoreResult<PromotionStatus> {
        self.request(|reply| Command::RecoverEventCursorGap {
            gap,
            catalog,
            reply,
        })
        .await
    }

    pub async fn compact(&self, before_ms: i64) -> StoreResult<u64> {
        self.request(|reply| Command::Compact { before_ms, reply })
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
        Command::EnqueueOutbox {
            workflow_id,
            item,
            now_ms,
            reply,
        } => send_reply(
            reply,
            enqueue_outbox(connection, workflow_id, &item, now_ms),
        ),
        Command::SaveOutbox {
            item,
            now_ms,
            reply,
        } => send_reply(reply, save_outbox(connection, &item, now_ms)),
        Command::PendingOutbox { reply } => send_reply(reply, pending_outbox(connection)),
        Command::AcceptInbox { item, reply } => send_reply(reply, accept_inbox(connection, &item)),
        Command::PendingInbox { reply } => send_reply(reply, pending_inbox(connection)),
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
        Command::PromotionStatus { reply } => send_reply(reply, promotion_status(connection)),
        Command::ApplyOperatorMutation {
            mutation,
            now_ms,
            reply,
        } => send_reply(
            reply,
            apply_operator_mutation(connection, &mutation, now_ms),
        ),
        Command::OperatorReplay {
            operation_id,
            intent,
            reply,
        } => send_reply(reply, operator_replay(connection, operation_id, &intent)),
        Command::AdvanceEventCursor {
            expected,
            next,
            reply,
        } => send_reply(reply, advance_event_cursor(connection, expected, next)),
        Command::IngestAgentEvents {
            expected,
            next,
            events,
            now_ms,
            reply,
        } => send_reply(
            reply,
            ingest_agent_events(connection, expected, next, &events, now_ms),
        ),
        Command::RecoverEventCursorGap {
            gap,
            catalog,
            reply,
        } => send_reply(reply, recover_event_cursor_gap(connection, &gap, &catalog)),
        Command::Compact { before_ms, reply } => send_reply(reply, compact(connection, before_ms)),
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
             CREATE TABLE legacy_control_requests (
                 request_id TEXT PRIMARY KEY REFERENCES idempotency_tombstones(request_id),
                 state TEXT NOT NULL,
                 source TEXT NOT NULL,
                 target TEXT NOT NULL,
                 record_json TEXT NOT NULL,
                 created_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL
             );
             CREATE TABLE legacy_return_deliveries (
                 request_id TEXT PRIMARY KEY REFERENCES idempotency_tombstones(request_id),
                 state TEXT NOT NULL,
                 agent_state TEXT NOT NULL,
                 mirror_state TEXT NOT NULL,
                 record_json TEXT NOT NULL,
                 created_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL
             );
             CREATE TABLE signal_messages (
                 legacy_id TEXT PRIMARY KEY,
                 group_id TEXT NOT NULL,
                 state TEXT NOT NULL,
                 record_json TEXT NOT NULL,
                 received_at_ms INTEGER NOT NULL
             );
             CREATE INDEX signal_messages_state
                 ON signal_messages(state, received_at_ms);
             CREATE TABLE legacy_debate_outbox (
                 effect_id TEXT PRIMARY KEY,
                 destination TEXT NOT NULL,
                 record_json TEXT NOT NULL,
                 resolution_state TEXT NOT NULL,
                 resolution_json TEXT,
                 created_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL
             );
             CREATE INDEX legacy_debate_resolution
                 ON legacy_debate_outbox(resolution_state, created_at_ms);
             CREATE TABLE promotion_state (
                 singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                 delivery_hold INTEGER NOT NULL CHECK(delivery_hold IN (0, 1)),
                 event_cursor INTEGER,
                 updated_at_ms INTEGER NOT NULL
             );
             INSERT INTO promotion_state(singleton, delivery_hold, event_cursor, updated_at_ms)
             VALUES (1, 1, NULL, 0);
             CREATE TABLE route_delivery_policy (
                 route_id TEXT PRIMARY KEY REFERENCES routes(route_id),
                 enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
                 updated_at_ms INTEGER NOT NULL
             );
             CREATE TABLE legacy_dispositions (
                 record_kind TEXT NOT NULL,
                 record_id TEXT NOT NULL,
                 decision TEXT NOT NULL,
                 evidence TEXT NOT NULL,
                 operation_id TEXT NOT NULL UNIQUE,
                 record_json TEXT NOT NULL,
                 resolved_at_ms INTEGER NOT NULL,
                 PRIMARY KEY(record_kind, record_id)
             );
             CREATE TABLE operator_actions (
                 operation_id TEXT PRIMARY KEY,
                 action_json TEXT NOT NULL,
                 outcome_json TEXT NOT NULL,
                 created_at_ms INTEGER NOT NULL
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
             PRAGMA user_version = 5;",
        )?;
        transaction.commit()?;
    }
    if version == 1 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "ALTER TABLE idempotency_tombstones
                 ADD COLUMN hash_kind TEXT NOT NULL DEFAULT 'semantic_v1';
             CREATE TABLE legacy_control_requests (
                 request_id TEXT PRIMARY KEY REFERENCES idempotency_tombstones(request_id),
                 state TEXT NOT NULL,
                 source TEXT NOT NULL,
                 target TEXT NOT NULL,
                 record_json TEXT NOT NULL,
                 created_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL
             );
             CREATE TABLE legacy_return_deliveries (
                 request_id TEXT PRIMARY KEY REFERENCES idempotency_tombstones(request_id),
                 state TEXT NOT NULL,
                 agent_state TEXT NOT NULL,
                 mirror_state TEXT NOT NULL,
                 record_json TEXT NOT NULL,
                 created_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL
             );
             CREATE TABLE signal_messages (
                 legacy_id TEXT PRIMARY KEY,
                 group_id TEXT NOT NULL,
                 state TEXT NOT NULL,
                 record_json TEXT NOT NULL,
                 received_at_ms INTEGER NOT NULL
             );
             CREATE INDEX signal_messages_state
                 ON signal_messages(state, received_at_ms);
             CREATE TABLE legacy_debate_outbox (
                 effect_id TEXT PRIMARY KEY,
                 destination TEXT NOT NULL,
                 record_json TEXT NOT NULL,
                 resolution_state TEXT NOT NULL,
                 resolution_json TEXT,
                 created_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL
             );
             CREATE INDEX legacy_debate_resolution
                 ON legacy_debate_outbox(resolution_state, created_at_ms);
             INSERT INTO legacy_debate_outbox(
                 effect_id, destination, record_json, resolution_state,
                 resolution_json, created_at_ms, updated_at_ms
             )
             SELECT effect_id, destination, record_json, 'held', NULL,
                    created_at_ms, updated_at_ms
             FROM outbox WHERE channel = 'debate';
             DELETE FROM outbox WHERE channel = 'debate';
             PRAGMA user_version = 3;",
        )?;
        transaction.execute_batch(PHASE5_SCHEMA_SQL)?;
        transaction.execute_batch(EVENT_SCHEMA_SQL)?;
        transaction.commit()?;
    }
    if version == 2 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE legacy_debate_outbox (
                 effect_id TEXT PRIMARY KEY,
                 destination TEXT NOT NULL,
                 record_json TEXT NOT NULL,
                 resolution_state TEXT NOT NULL,
                 resolution_json TEXT,
                 created_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL
             );
             CREATE INDEX legacy_debate_resolution
                 ON legacy_debate_outbox(resolution_state, created_at_ms);
             INSERT INTO legacy_debate_outbox(
                 effect_id, destination, record_json, resolution_state,
                 resolution_json, created_at_ms, updated_at_ms
             )
             SELECT effect_id, destination, record_json, 'held', NULL,
                    created_at_ms, updated_at_ms
             FROM outbox WHERE channel = 'debate';
             DELETE FROM outbox WHERE channel = 'debate';
             PRAGMA user_version = 3;",
        )?;
        transaction.execute_batch(PHASE5_SCHEMA_SQL)?;
        transaction.execute_batch(EVENT_SCHEMA_SQL)?;
        transaction.commit()?;
    }
    if version == 3 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(PHASE5_SCHEMA_SQL)?;
        transaction.execute_batch(EVENT_SCHEMA_SQL)?;
        transaction.commit()?;
    }
    if version == 4 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(EVENT_SCHEMA_SQL)?;
        transaction.commit()?;
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
        "UPDATE outbox SET state = 'pending',
             record_json = json_set(record_json, '$.state', 'pending')
         WHERE state = 'delivering'",
        [],
    )?;
    connection.execute(
        "UPDATE inbox SET state = 'indeterminate',
             record_json = json_set(record_json, '$.state', 'indeterminate')
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
            same_content: stored_request_hash_matches(&hash_kind, &stored_hash, &command),
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

fn promotion_status(connection: &Connection) -> StoreResult<PromotionStatus> {
    let (delivery_hold, event_cursor) = connection.query_row(
        "SELECT delivery_hold, event_cursor FROM promotion_state WHERE singleton = 1",
        [],
        |row| Ok((row.get::<_, bool>(0)?, row.get::<_, Option<u64>>(1)?)),
    )?;
    let mut statement = connection.prepare(
        "SELECT route_id FROM route_delivery_policy WHERE enabled = 1 ORDER BY route_id",
    )?;
    let enabled_routes = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .map(|value| {
            let value = value?;
            Uuid::parse_str(&value).map(RouteId::new).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(PromotionStatus {
        delivery_hold,
        event_cursor,
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
        enabled_routes,
        operator_actions: count(connection, "SELECT COUNT(*) FROM operator_actions")?,
        unresolved_legacy_controls: count(
            connection,
            "SELECT COUNT(*) FROM legacy_control_requests legacy
             LEFT JOIN legacy_dispositions disposition
               ON disposition.record_kind = 'control'
              AND disposition.record_id = legacy.request_id
             WHERE legacy.state = 'indeterminate' AND disposition.record_id IS NULL",
        )?,
        unresolved_legacy_returns: count(
            connection,
            "SELECT COUNT(*) FROM legacy_return_deliveries legacy
             LEFT JOIN legacy_dispositions disposition
               ON disposition.record_kind = 'return'
              AND disposition.record_id = legacy.request_id
             WHERE (legacy.agent_state != 'delivered' OR legacy.mirror_state != 'delivered')
               AND disposition.record_id IS NULL",
        )?,
        held_legacy_debate: count(
            connection,
            "SELECT COUNT(*) FROM legacy_debate_outbox WHERE resolution_state = 'held'",
        )?,
    })
}

fn apply_operator_mutation(
    connection: &mut Connection,
    mutation: &OperatorMutation,
    now_ms: i64,
) -> StoreResult<OperatorOutcome> {
    let operation_id = mutation.operation_id.to_string();
    let action_json = serde_json::to_string(mutation)?;
    if let Some((stored_action, outcome)) = connection
        .query_row(
            "SELECT action_json, outcome_json FROM operator_actions WHERE operation_id = ?1",
            params![operation_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?
    {
        if stored_action != action_json {
            return Err(StoreError::Conflict(format!(
                "operator operation {} was already used with different content",
                mutation.operation_id
            )));
        }
        let mut outcome: OperatorOutcome = serde_json::from_str(&outcome)?;
        outcome.replayed = true;
        return Ok(outcome);
    }

    let transaction = connection.transaction()?;
    let (detail, route) = apply_operator_action(&transaction, mutation, now_ms)?;
    let mut promotion = promotion_status(&transaction)?;
    promotion.operator_actions += 1;
    let outcome = OperatorOutcome {
        operation_id: mutation.operation_id,
        replayed: false,
        detail,
        promotion,
        route,
    };
    transaction.execute(
        "INSERT INTO operator_actions(operation_id, action_json, outcome_json, created_at_ms)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            operation_id,
            action_json,
            serde_json::to_string(&outcome)?,
            now_ms,
        ],
    )?;
    transaction.commit()?;
    Ok(outcome)
}

fn operator_replay(
    connection: &Connection,
    operation_id: Uuid,
    intent: &Value,
) -> StoreResult<Option<OperatorOutcome>> {
    let Some((action_json, outcome_json)) = connection
        .query_row(
            "SELECT action_json, outcome_json FROM operator_actions WHERE operation_id = ?1",
            params![operation_id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?
    else {
        return Ok(None);
    };
    let stored: OperatorMutation = serde_json::from_str(&action_json)?;
    if stored.intent.as_ref() != Some(intent) {
        return Err(StoreError::Conflict(format!(
            "operator operation {operation_id} was already used with different content"
        )));
    }
    let mut outcome: OperatorOutcome = serde_json::from_str(&outcome_json)?;
    outcome.replayed = true;
    Ok(Some(outcome))
}

fn apply_operator_action(
    transaction: &rusqlite::Transaction<'_>,
    mutation: &OperatorMutation,
    now_ms: i64,
) -> StoreResult<(String, Option<Route>)> {
    match &mutation.action {
        OperatorAction::SetDeliveryHold { held } => {
            if !held {
                let (cursor, enabled): (Option<u64>, u64) = transaction.query_row(
                    "SELECT event_cursor,
                        (SELECT COUNT(*) FROM route_delivery_policy WHERE enabled = 1)
                     FROM promotion_state WHERE singleton = 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                if cursor.is_none() || enabled == 0 {
                    return Err(StoreError::Conflict(
                        "delivery cannot be released before an event cursor and canary route are set"
                            .into(),
                    ));
                }
                if get_metadata(transaction, "wakterm_event_cursor_gap_v1")?.is_some() {
                    return Err(StoreError::Conflict(
                        "delivery cannot be released until the Wakterm event cursor gap is acknowledged"
                            .into(),
                    ));
                }
            }
            transaction.execute(
                "UPDATE promotion_state SET delivery_hold = ?1, updated_at_ms = ?2
                 WHERE singleton = 1",
                params![held, now_ms],
            )?;
            Ok((
                if *held {
                    "global delivery hold is active"
                } else {
                    "global delivery hold is released for enabled routes"
                }
                .into(),
                None,
            ))
        }
        OperatorAction::SetRouteEnabled { route_id, enabled } => {
            let route = get_route(transaction, *route_id)?
                .ok_or_else(|| StoreError::Conflict(format!("route {route_id} does not exist")))?;
            if *enabled && route.status != crate::domain::RouteStatus::Available {
                return Err(StoreError::Conflict(format!(
                    "route {:?} is not authoritatively available",
                    route.title
                )));
            }
            transaction.execute(
                "INSERT INTO route_delivery_policy(route_id, enabled, updated_at_ms)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(route_id) DO UPDATE SET
                     enabled = excluded.enabled, updated_at_ms = excluded.updated_at_ms",
                params![route_id.to_string(), enabled, now_ms],
            )?;
            Ok((
                format!(
                    "route {:?} delivery is {}",
                    route.title,
                    if *enabled { "enabled" } else { "held" }
                ),
                Some(route),
            ))
        }
        OperatorAction::InitializeEventCursor { sequence } => {
            let existing: Option<u64> = transaction.query_row(
                "SELECT event_cursor FROM promotion_state WHERE singleton = 1",
                [],
                |row| row.get(0),
            )?;
            if existing.is_some_and(|value| value != *sequence) {
                return Err(StoreError::Conflict(format!(
                    "event cursor is already initialized at {}",
                    existing.expect("existing cursor was checked")
                )));
            }
            transaction.execute(
                "UPDATE promotion_state SET event_cursor = ?1, updated_at_ms = ?2
                 WHERE singleton = 1",
                params![sequence, now_ms],
            )?;
            Ok((format!("event cursor initialized at {sequence}"), None))
        }
        OperatorAction::AcknowledgeEventCursorGap {
            requested_after_sequence,
            evidence,
        } => {
            if evidence.trim().is_empty() {
                return Err(StoreError::Conflict(
                    "cursor-gap acknowledgement requires evidence".into(),
                ));
            }
            let stored =
                get_metadata(transaction, "wakterm_event_cursor_gap_v1")?.ok_or_else(|| {
                    StoreError::Conflict("there is no event cursor gap to acknowledge".into())
                })?;
            let gap: EventCursorGap = serde_json::from_str(&stored)?;
            if gap.requested_after_sequence != *requested_after_sequence {
                return Err(StoreError::Conflict(format!(
                    "event cursor gap began at {}, not {requested_after_sequence}",
                    gap.requested_after_sequence
                )));
            }
            transaction.execute(
                "DELETE FROM metadata WHERE key = 'wakterm_event_cursor_gap_v1'",
                [],
            )?;
            Ok((
                format!(
                    "event cursor gap at {requested_after_sequence} acknowledged with evidence: {}",
                    evidence.trim()
                ),
                None,
            ))
        }
        OperatorAction::InitializeInboundCursor { channel, cursor } => {
            if *channel != ChannelKind::Telegram {
                return Err(StoreError::Conflict(
                    "only Telegram has a caller-initialized inbound cursor".into(),
                ));
            }
            let key = "telegram_update_offset";
            if let Some(existing) = get_metadata(transaction, key)? {
                if existing != cursor.to_string() {
                    return Err(StoreError::Conflict(format!(
                        "Telegram update offset is already initialized at {existing}"
                    )));
                }
            } else {
                set_metadata(transaction, key, &cursor.to_string())?;
            }
            Ok((
                format!("Telegram update offset initialized at {cursor}"),
                None,
            ))
        }
        OperatorAction::ReconcileRoute {
            route_id,
            binding,
            replace_identity,
        } => {
            if binding.pane_id.is_none() {
                return Err(StoreError::Conflict(
                    "route reconciliation requires the fresh ephemeral pane locator".into(),
                ));
            }
            let mut route = get_route(transaction, *route_id)?
                .ok_or_else(|| StoreError::Conflict(format!("route {route_id} does not exist")))?;
            let decision = route.reconcile(Some(binding.clone()));
            let decision =
                if decision == ReconcileDecision::ReconciliationRequired && *replace_identity {
                    route.agent = Some(binding.clone());
                    route.status = crate::domain::RouteStatus::Available;
                    ReconcileDecision::Rebound
                } else {
                    decision
                };
            save_route(transaction, &route, now_ms)?;
            let detail = match decision {
                ReconcileDecision::Unchanged => "route identity was already current",
                ReconcileDecision::Rebound => "route was bound to the fresh Wakterm identity",
                ReconcileDecision::Unavailable => "route remains unavailable",
                ReconcileDecision::ReconciliationRequired => {
                    "route identity changed and requires explicit replacement review"
                }
            };
            Ok((detail.into(), Some(route)))
        }
        OperatorAction::DisposeLegacy {
            record_kind,
            record_id,
            decision,
            evidence,
        } => dispose_legacy(
            transaction,
            mutation.operation_id,
            *record_kind,
            record_id,
            decision,
            evidence,
            now_ms,
        ),
    }
}

fn dispose_legacy(
    transaction: &rusqlite::Transaction<'_>,
    operation_id: Uuid,
    record_kind: LegacyRecordKind,
    record_id: &str,
    decision: &LegacyDecision,
    evidence: &str,
    now_ms: i64,
) -> StoreResult<(String, Option<Route>)> {
    if record_id.trim().is_empty() || evidence.trim().is_empty() || evidence.len() > 4096 {
        return Err(StoreError::Conflict(
            "legacy disposition requires a record ID and concise evidence".into(),
        ));
    }
    let kind = legacy_kind_name(record_kind);
    if transaction
        .query_row(
            "SELECT 1 FROM legacy_dispositions WHERE record_kind = ?1 AND record_id = ?2",
            params![kind, record_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some()
    {
        return Err(StoreError::Conflict(format!(
            "legacy {kind} record {record_id:?} already has a disposition"
        )));
    }

    let route = match record_kind {
        LegacyRecordKind::Control => {
            require_legacy_record(
                transaction,
                "legacy_control_requests",
                "request_id",
                record_id,
            )?;
            reject_debate_mapping(decision)?;
            None
        }
        LegacyRecordKind::Return => {
            require_legacy_record(
                transaction,
                "legacy_return_deliveries",
                "request_id",
                record_id,
            )?;
            reject_debate_mapping(decision)?;
            None
        }
        LegacyRecordKind::Debate => {
            dispose_legacy_debate(transaction, record_id, decision, now_ms)?
        }
    };
    let record_json = serde_json::to_string(&serde_json::json!({
        "record_kind": record_kind,
        "record_id": record_id,
        "decision": decision,
        "evidence": evidence,
    }))?;
    transaction.execute(
        "INSERT INTO legacy_dispositions(
             record_kind, record_id, decision, evidence, operation_id,
             record_json, resolved_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            kind,
            record_id,
            decision_name(decision),
            evidence,
            operation_id.to_string(),
            record_json,
            now_ms,
        ],
    )?;
    Ok((
        format!("legacy {kind} record {record_id:?} received an explicit disposition"),
        route,
    ))
}

fn dispose_legacy_debate(
    transaction: &rusqlite::Transaction<'_>,
    record_id: &str,
    decision: &LegacyDecision,
    now_ms: i64,
) -> StoreResult<Option<Route>> {
    let (destination, record_json): (String, String) = transaction
        .query_row(
            "SELECT destination, record_json FROM legacy_debate_outbox
             WHERE effect_id = ?1 AND resolution_state = 'held'",
            params![record_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or_else(|| {
            StoreError::Conflict(format!(
                "held legacy Debate record {record_id:?} is missing"
            ))
        })?;
    match decision {
        LegacyDecision::MapDebateToSignal {
            route_id,
            expected_legacy_destination,
        } => {
            if expected_legacy_destination != &destination {
                return Err(StoreError::Conflict(
                    "the asserted legacy Debate destination does not match durable state".into(),
                ));
            }
            let route = get_route(transaction, *route_id)?
                .ok_or_else(|| StoreError::Conflict(format!("route {route_id} does not exist")))?;
            let signal_groups = route
                .channels
                .iter()
                .filter_map(|binding| match binding {
                    crate::domain::ChannelBinding::Signal { group_id } => Some(group_id),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let [group_id] = signal_groups.as_slice() else {
                return Err(StoreError::Conflict(
                    "the selected route does not have exactly one Signal binding".into(),
                ));
            };
            let legacy: Value = serde_json::from_str(&record_json)?;
            let body = legacy["chunk"].as_str().ok_or_else(|| {
                StoreError::Conflict("legacy Debate payload has no message body".into())
            })?;
            let sender_harness = legacy["harness"].as_str().ok_or_else(|| {
                StoreError::Conflict("legacy Debate payload has no harness identity".into())
            })?;
            let effect_id = Uuid::parse_str(record_id)
                .map(EffectId::new)
                .map_err(|_| StoreError::Conflict("legacy Debate effect ID is invalid".into()))?;
            let item = OutboxItem {
                id: effect_id,
                route_id: Some(route.id),
                sender_harness: Some(sender_harness.to_owned()),
                kind: ChannelKind::Signal,
                destination: (*group_id).clone(),
                body: body.to_owned(),
                state: OutboxState::Pending,
                attempts: 0,
                last_error: None,
                external_receipt: None,
            };
            enqueue_outbox(transaction, None, &item, now_ms)?;
            transaction.execute(
                "UPDATE legacy_debate_outbox
                 SET resolution_state = 'mapped_to_signal', resolution_json = ?2,
                     updated_at_ms = ?3 WHERE effect_id = ?1",
                params![record_id, serde_json::to_string(&item)?, now_ms],
            )?;
            Ok(Some(route))
        }
        LegacyDecision::NoReplay | LegacyDecision::ExternallyVerified => {
            transaction.execute(
                "UPDATE legacy_debate_outbox
                 SET resolution_state = 'no_replay', resolution_json = ?2,
                     updated_at_ms = ?3 WHERE effect_id = ?1",
                params![record_id, serde_json::to_string(decision)?, now_ms],
            )?;
            Ok(None)
        }
    }
}

fn require_legacy_record(
    connection: &Connection,
    table: &str,
    column: &str,
    record_id: &str,
) -> StoreResult<()> {
    let sql = format!(
        "SELECT 1 FROM {} WHERE {} = ?1",
        quote_identifier(table),
        quote_identifier(column)
    );
    if connection
        .query_row(&sql, params![record_id], |_| Ok(()))
        .optional()?
        .is_none()
    {
        return Err(StoreError::Conflict(format!(
            "legacy record {record_id:?} is missing"
        )));
    }
    Ok(())
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn reject_debate_mapping(decision: &LegacyDecision) -> StoreResult<()> {
    if matches!(decision, LegacyDecision::MapDebateToSignal { .. }) {
        Err(StoreError::Conflict(
            "only a held legacy Debate record can be mapped to Signal".into(),
        ))
    } else {
        Ok(())
    }
}

fn legacy_kind_name(kind: LegacyRecordKind) -> &'static str {
    match kind {
        LegacyRecordKind::Control => "control",
        LegacyRecordKind::Return => "return",
        LegacyRecordKind::Debate => "debate",
    }
}

fn decision_name(decision: &LegacyDecision) -> &'static str {
    match decision {
        LegacyDecision::NoReplay => "no_replay",
        LegacyDecision::ExternallyVerified => "externally_verified",
        LegacyDecision::MapDebateToSignal { .. } => "map_debate_to_signal",
    }
}

fn advance_event_cursor(connection: &Connection, expected: u64, next: u64) -> StoreResult<()> {
    if next < expected {
        return Err(StoreError::Conflict(
            "event cursor cannot move backwards".into(),
        ));
    }
    let changed = connection.execute(
        "UPDATE promotion_state SET event_cursor = ?2
         WHERE singleton = 1 AND event_cursor = ?1",
        params![expected, next],
    )?;
    if changed != 1 {
        return Err(StoreError::Conflict(format!(
            "event cursor was not at expected sequence {expected}"
        )));
    }
    Ok(())
}

fn ingest_agent_events(
    connection: &mut Connection,
    expected: u64,
    next: u64,
    events: &[EventRecord],
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
    let current: Option<u64> = transaction.query_row(
        "SELECT event_cursor FROM promotion_state WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
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

        let matching = routes
            .iter()
            .filter(|route| {
                route.agent.as_ref().is_some_and(|binding| {
                    binding.agent_id == event.agent_id
                        && binding.incarnation_id == event.incarnation_id
                })
            })
            .collect::<Vec<_>>();
        if matching.len() > 1 {
            return Err(StoreError::Conflict(format!(
                "Wakterm event {:?} matches more than one durable route",
                event.event_id
            )));
        }
        let route = matching.first().copied();
        if event.kind == "agent_lifecycle" {
            if let Some(route) = route {
                let mut route = route.clone();
                route.status = match event.fields.get("lifecycle").and_then(Value::as_str) {
                    Some("available") => crate::domain::RouteStatus::Available,
                    Some("unavailable") => crate::domain::RouteStatus::Unavailable,
                    _ => {
                        return Err(StoreError::Conflict(
                            "agent lifecycle event has an invalid lifecycle".into(),
                        ));
                    }
                };
                save_route(&transaction, &route, now_ms)?;
            }
        }

        let mut state = "recorded";
        let route_id = route.map(|route| route.id);
        if matches!(event.kind.as_str(), "assistant_message" | "plan") {
            let Some(route) = route else {
                state = "unrouted";
                outcome.unrouted += 1;
                insert_agent_event(&transaction, event, None, state, &record_json, now_ms)?;
                outcome.recorded += 1;
                continue;
            };
            let Some((kind, destination)) = output_destination(
                route,
                preferences.get(&route.id.to_string()).map(String::as_str),
            ) else {
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
            let text = event
                .fields
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| StoreError::Conflict("visible Wakterm event has no text".into()))?;
            let body = if event.kind == "plan" {
                format!("Plan:\n{text}")
            } else {
                text.to_owned()
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
                sender_harness: route.agent.as_ref().map(|agent| agent.harness.clone()),
                kind,
                destination,
                body,
                state: OutboxState::Pending,
                attempts: 0,
                last_error: None,
                external_receipt: None,
            };
            enqueue_outbox(&transaction, None, &item, now_ms)?;
            state = "projected";
            outcome.visible_outputs += 1;
        }
        insert_agent_event(&transaction, event, route_id, state, &record_json, now_ms)?;
        outcome.recorded += 1;
    }
    let changed = transaction.execute(
        "UPDATE promotion_state SET event_cursor = ?2, updated_at_ms = ?3
         WHERE singleton = 1 AND event_cursor = ?1",
        params![expected, next, now_ms],
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
) -> StoreResult<PromotionStatus> {
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
    let current: Option<u64> = transaction.query_row(
        "SELECT event_cursor FROM promotion_state WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    if current != Some(gap.requested_after_sequence) {
        return Err(StoreError::Conflict(format!(
            "event cursor was not at gap sequence {}",
            gap.requested_after_sequence
        )));
    }
    if get_metadata(&transaction, "wakterm_event_cursor_gap_v1")?.is_some() {
        return Err(StoreError::Conflict(
            "an event cursor gap is already awaiting acknowledgement".into(),
        ));
    }

    let mut routes = list_routes(&transaction)?;
    for route in &mut routes {
        let Some(binding) = route.agent.as_ref() else {
            continue;
        };
        let live = catalog.agents.iter().any(|agent| {
            agent.agent_id == binding.agent_id
                && agent.incarnation_id.as_deref() == Some(binding.incarnation_id.as_str())
                && agent.alive
        });
        route.status = if live {
            crate::domain::RouteStatus::Available
        } else {
            crate::domain::RouteStatus::Unavailable
        };
        save_route(&transaction, route, gap.recorded_at_ms)?;
    }
    set_metadata(
        &transaction,
        "wakterm_event_cursor_gap_v1",
        &serde_json::to_string(gap)?,
    )?;
    transaction.execute(
        "UPDATE promotion_state
         SET delivery_hold = 1, event_cursor = ?1, updated_at_ms = ?2
         WHERE singleton = 1",
        params![gap.fresh_catalog_as_of_sequence, gap.recorded_at_ms],
    )?;
    transaction.commit()?;
    promotion_status(connection)
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
    let value = get_metadata(connection, "migrated_route_preferences_v1")?;
    value
        .map(|value| serde_json::from_str(&value).map_err(StoreError::from))
        .transpose()
        .map(Option::unwrap_or_default)
}

fn output_destination(route: &Route, preference: Option<&str>) -> Option<(ChannelKind, String)> {
    let preferred = match preference {
        Some("tg") => Some(ChannelKind::Telegram),
        Some("sig") => Some(ChannelKind::Signal),
        Some("slack") => Some(ChannelKind::Slack),
        _ => None,
    };
    let telegram_topic = route.channels.iter().find_map(|binding| match binding {
        crate::domain::ChannelBinding::Telegram { topic_id } => Some(*topic_id),
        _ => None,
    });
    let signal_group = route.channels.iter().find_map(|binding| match binding {
        crate::domain::ChannelBinding::Signal { group_id } => Some(group_id.as_str()),
        _ => None,
    });
    let slack_channel = route.channels.iter().find_map(|binding| match binding {
        crate::domain::ChannelBinding::Slack { channel_id } => Some(channel_id.as_str()),
        _ => None,
    });
    crate::domain::select_channel(
        preferred,
        &route.title,
        &crate::domain::ChannelAvailability {
            telegram_topic,
            signal_enabled: signal_group.is_some(),
            signal_group,
            slack_enabled: slack_channel.is_some(),
            slack_channel,
        },
    )
    .map(|selection| (selection.kind, selection.destination))
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
        {
            return Err(StoreError::Conflict(format!(
                "outbox effect {} already differs",
                item.id
            )));
        }
    }
    Ok(())
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
    let mut statement = connection
        .prepare("SELECT record_json FROM outbox WHERE state = 'pending' ORDER BY created_at_ms")?;
    let json = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    json.into_iter()
        .map(|value| serde_json::from_str(&value).map_err(StoreError::from))
        .collect()
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

fn pending_inbox(connection: &Connection) -> StoreResult<Vec<InboxItem>> {
    let mut statement = connection
        .prepare("SELECT record_json FROM inbox WHERE state = 'pending' ORDER BY created_at_ms")?;
    let records = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    records
        .into_iter()
        .map(|record| serde_json::from_str(&record).map_err(StoreError::from))
        .collect()
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
                ChannelKind::Slack => "slack",
            }
            .into(),
        );
        set_metadata(
            &transaction,
            "migrated_route_preferences_v1",
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

fn compact(connection: &mut Connection, before_ms: i64) -> StoreResult<u64> {
    let transaction = connection.transaction()?;
    let removable = {
        let mut statement = transaction.prepare(
            "SELECT w.request_id FROM workflows w
             LEFT JOIN return_deliveries r ON r.request_id = w.request_id
             WHERE w.updated_at_ms < ?1
               AND w.state IN ('completed', 'failed', 'cancelled')
               AND (r.request_id IS NULL OR (
                    r.agent_state = 'delivered' AND r.mirror_state = 'delivered'
               ))",
        )?;
        statement
            .query_map(params![before_ms], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    for request_id in &removable {
        transaction.execute(
            "DELETE FROM return_deliveries WHERE request_id = ?1",
            params![request_id],
        )?;
        transaction.execute(
            "DELETE FROM workflows WHERE request_id = ?1",
            params![request_id],
        )?;
    }
    transaction.commit()?;
    Ok(removable.len() as u64)
}

fn status(connection: &Connection) -> StoreResult<StoreStatus> {
    let promotion = promotion_status(connection)?;
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
        legacy_control_requests: count(connection, "SELECT COUNT(*) FROM legacy_control_requests")?,
        legacy_indeterminate_requests: promotion.unresolved_legacy_controls,
        legacy_unresolved_returns: promotion.unresolved_legacy_returns,
        legacy_debate_outbox: promotion.held_legacy_debate,
        signal_messages: count(connection, "SELECT COUNT(*) FROM signal_messages")?,
        unrouted_agent_events: count(
            connection,
            "SELECT COUNT(*) FROM agent_events WHERE state = 'unrouted'",
        )?,
        observer_failure_events: count(
            connection,
            "SELECT COUNT(*) FROM agent_events WHERE kind = 'observer_failure'",
        )?,
        promotion,
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
