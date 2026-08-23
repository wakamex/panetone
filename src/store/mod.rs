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
    Route, RouteId, SEMANTIC_HASH_KIND, SendCommand, Workflow, WorkflowId, WorkflowState,
    semantic_request_hash,
};
use crate::wakterm::{AgentCatalog, EventRecord};

pub const SCHEMA_VERSION: i64 = 7;
const COMMAND_CAPACITY: usize = 128;

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
    FindOutboxAgent {
        channel: ChannelKind,
        destination: String,
        external_receipt: String,
        reply: oneshot::Sender<StoreResult<Option<AgentBinding>>>,
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
    InitializeEventCursor {
        sequence: u64,
        reply: oneshot::Sender<StoreResult<u64>>,
    },
    RebaselinePassiveOutput {
        sequence: u64,
        reply: oneshot::Sender<StoreResult<u64>>,
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

    pub async fn find_outbox_agent(
        &self,
        channel: ChannelKind,
        destination: String,
        external_receipt: String,
    ) -> StoreResult<Option<AgentBinding>> {
        self.request(|reply| Command::FindOutboxAgent {
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

    pub async fn initialize_event_cursor(&self, sequence: u64) -> StoreResult<u64> {
        self.request(|reply| Command::InitializeEventCursor { sequence, reply })
            .await
    }

    pub async fn rebaseline_passive_output(&self, sequence: u64) -> StoreResult<u64> {
        self.request(|reply| Command::RebaselinePassiveOutput { sequence, reply })
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
        Command::FindOutboxAgent {
            channel,
            destination,
            external_receipt,
            reply,
        } => send_reply(
            reply,
            find_outbox_agent(connection, channel, &destination, &external_receipt),
        ),
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
        Command::InitializeEventCursor { sequence, reply } => {
            send_reply(reply, initialize_event_cursor(connection, sequence))
        }
        Command::RebaselinePassiveOutput { sequence, reply } => {
            send_reply(reply, rebaseline_passive_output(connection, sequence))
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
                sender_harness: route_agent.map(|candidate| candidate.agent.harness.clone()),
                source_agent: route_agent.map(|candidate| candidate.agent.clone()),
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
            if let Some(candidate) = route_agent
                && !outcome
                    .last_agents
                    .iter()
                    .any(|existing| existing.route_id == candidate.route_id)
            {
                outcome.last_agents.push(candidate.clone());
            } else if let Some(candidate) = route_agent
                && let Some(existing) = outcome
                    .last_agents
                    .iter_mut()
                    .find(|existing| existing.route_id == candidate.route_id)
            {
                *existing = candidate.clone();
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
        crate::domain::ChannelBinding::Signal { group_id } => Some(group_id.as_str()),
        _ => None,
    });
    match selected {
        ChannelKind::Telegram => telegram_topic.map(|topic_id| (selected, topic_id.to_string())),
        ChannelKind::Signal => signal_group.map(|group_id| (selected, group_id.into())),
    }
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

fn rebaseline_passive_output(connection: &mut Connection, sequence: u64) -> StoreResult<u64> {
    let transaction = connection.transaction()?;
    let discarded = transaction.execute(
        "DELETE FROM outbox
         WHERE request_id IS NULL AND state IN ('pending', 'delivering')",
        [],
    )?;
    set_metadata(&transaction, "wakterm_event_cursor", &sequence.to_string())?;
    transaction.commit()?;
    Ok(discarded as u64)
}

fn find_outbox_agent(
    connection: &Connection,
    channel: ChannelKind,
    destination: &str,
    external_receipt: &str,
) -> StoreResult<Option<AgentBinding>> {
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
    json.map(|value| {
        serde_json::from_str::<OutboxItem>(&value)
            .map(|item| item.source_agent)
            .map_err(StoreError::from)
    })
    .transpose()
    .map(Option::flatten)
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
