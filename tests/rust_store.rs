use std::fs;

use panetone::domain::{
    AdmissionReceipt, AdmissionStatus, AgentBinding, CallbackDelivery, ChannelBinding, ChannelKind,
    DeliveryState, EffectId, OutboxItem, OutboxState, Route, RouteId, SendCommand, WorkflowId,
    WorkflowState,
};
use panetone::store::{
    ClaimResult, DestinationDelivery, InboxItem, ReturnDelivery, StoreError, StoreHandle,
};
use rusqlite::Connection;
use serde_json::json;
use tempfile::tempdir;
use uuid::Uuid;

fn id(value: u128) -> WorkflowId {
    WorkflowId::new(Uuid::from_u128(value))
}

fn route_id(value: u128) -> RouteId {
    RouteId::new(Uuid::from_u128(value))
}

fn binding(name: &str) -> AgentBinding {
    AgentBinding {
        agent_id: format!("agent-{name}"),
        incarnation_id: format!("incarnation-{name}"),
        harness: "codex".into(),
        pane_id: Some(7),
    }
}

fn command(request_id: WorkflowId, message: &str) -> SendCommand {
    SendCommand {
        id: request_id,
        source: "source".into(),
        target: "target".into(),
        message: message.into(),
        return_final: false,
        timeout_ms: 30_000,
    }
}

async fn claim_new(store: &StoreHandle, request_id: WorkflowId) -> panetone::store::StoredWorkflow {
    let result = store
        .claim(
            command(request_id, "hello"),
            route_id(1),
            route_id(2),
            binding("source"),
            binding("target"),
            100,
        )
        .await
        .unwrap();
    match result {
        ClaimResult::New(record) => record,
        other => panic!("expected a new claim, got {other:?}"),
    }
}

#[tokio::test]
async fn store_is_private_and_rejects_newer_schemas() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    store.shutdown().await.unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    let connection = Connection::open(&path).unwrap();
    let mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .unwrap();
    assert_eq!(mode.to_lowercase(), "wal");
    connection.pragma_update(None, "user_version", 99).unwrap();
    drop(connection);
    assert!(matches!(
        StoreHandle::open(&path),
        Err(StoreError::NewerSchema {
            found: 99,
            supported: 7
        })
    ));
}

#[tokio::test]
async fn version_five_is_rejected() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let connection = Connection::open(&path).unwrap();
    connection.pragma_update(None, "user_version", 5).unwrap();
    drop(connection);

    assert!(matches!(
        StoreHandle::open(&path),
        Err(StoreError::OlderSchema {
            found: 5,
            supported: 7
        })
    ));
}

#[tokio::test]
async fn version_six_routes_drop_persisted_runtime_bindings() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    store.shutdown().await.unwrap();

    let route_id = route_id(6);
    let connection = Connection::open(&path).unwrap();
    connection
        .execute(
            "INSERT INTO routes(route_id, route_json, updated_at_ms)
             VALUES (?1, ?2, 1)",
            rusqlite::params![
                route_id.to_string(),
                json!({
                    "id": route_id,
                    "title": "panetone",
                    "channels": [{"kind": "telegram", "topic_id": 12}],
                    "agent": {
                        "agent_id": "old-agent",
                        "incarnation_id": "old-incarnation",
                        "harness": "codex",
                        "pane_id": 7
                    },
                    "status": "available"
                })
                .to_string()
            ],
        )
        .unwrap();
    connection.pragma_update(None, "user_version", 6).unwrap();
    drop(connection);

    let migrated = StoreHandle::open(&path).unwrap();
    let route = migrated.get_route(route_id).await.unwrap().unwrap();
    assert_eq!(route.title, "panetone");
    assert_eq!(route.agent, None);
    migrated.shutdown().await.unwrap();

    let connection = Connection::open(&path).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 7);
    let route_json: serde_json::Value = serde_json::from_str(
        &connection
            .query_row(
                "SELECT route_json FROM routes WHERE route_id = ?1",
                [route_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
    )
    .unwrap();
    assert!(route_json.get("agent").is_none());
    assert!(route_json.get("status").is_none());
}

#[tokio::test]
async fn unknown_historical_hash_kind_keeps_uuid_reserved() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    store.shutdown().await.unwrap();

    let request_id = id(10);
    let connection = Connection::open(&path).unwrap();
    connection
        .execute(
            "INSERT INTO idempotency_tombstones(
                 request_id, semantic_hash, hash_kind, terminal_state,
                 created_at_ms, completed_at_ms
             ) VALUES (?1, 'historical-hash', 'retired_format', 'completed', 1, 2)",
            [request_id.to_string()],
        )
        .unwrap();
    drop(connection);

    let store = StoreHandle::open(&path).unwrap();
    let claim = store
        .claim(
            command(request_id, "hello"),
            route_id(1),
            route_id(2),
            binding("source"),
            binding("target"),
            100,
        )
        .await
        .unwrap();
    assert!(matches!(
        claim,
        ClaimResult::Tombstone {
            same_content: false,
            ..
        }
    ));
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn busy_queue_and_uncertain_admission_recover_conservatively() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let mut queued = claim_new(&store, id(20)).await;
    queued
        .workflow
        .transition(WorkflowState::AuditPosted)
        .unwrap();
    queued.updated_at_ms += 1;
    store
        .save_workflow(queued.clone(), WorkflowState::Claimed)
        .await
        .unwrap();
    queued
        .workflow
        .transition(WorkflowState::AdmissionPrepared)
        .unwrap();
    queued.updated_at_ms += 1;
    store
        .save_workflow(queued.clone(), WorkflowState::AuditPosted)
        .await
        .unwrap();
    let receipt = AdmissionReceipt {
        request_id: EffectId::target_admission(queued.command.id),
        status: AdmissionStatus::Busy,
        definitive: true,
        prompt_written: Some(false),
        agent_id: Some(queued.workflow.observed_target.agent_id.clone()),
        incarnation_id: Some(queued.workflow.observed_target.incarnation_id.clone()),
        detail: None,
    };
    let observed = queued.workflow.observed_target.clone();
    queued
        .workflow
        .apply_target_receipt(&receipt, &observed)
        .unwrap();
    queued.updated_at_ms += 1;
    store
        .save_workflow(queued.clone(), WorkflowState::AdmissionPrepared)
        .await
        .unwrap();

    let mut uncertain = claim_new(&store, id(21)).await;
    uncertain
        .workflow
        .transition(WorkflowState::AuditPosted)
        .unwrap();
    uncertain.updated_at_ms += 1;
    store
        .save_workflow(uncertain.clone(), WorkflowState::Claimed)
        .await
        .unwrap();
    uncertain
        .workflow
        .transition(WorkflowState::AdmissionPrepared)
        .unwrap();
    uncertain.updated_at_ms += 1;
    store
        .save_workflow(uncertain.clone(), WorkflowState::AuditPosted)
        .await
        .unwrap();
    store.shutdown().await.unwrap();

    let reopened = StoreHandle::open(&path).unwrap();
    assert_eq!(reopened.awaiting_target().await.unwrap().len(), 1);
    assert_eq!(
        reopened
            .get_workflow(id(21))
            .await
            .unwrap()
            .unwrap()
            .workflow
            .state,
        WorkflowState::Indeterminate
    );
    reopened.shutdown().await.unwrap();
}

fn return_record(workflow_id: WorkflowId, state: DeliveryState) -> ReturnDelivery {
    ReturnDelivery {
        workflow_id,
        result: json!({"status": "completed", "message": "done"}),
        agent: CallbackDelivery {
            effect_id: EffectId::callback_admission(workflow_id),
            source: binding("source"),
            state,
            last_error: None,
        },
        mirror: DestinationDelivery {
            effect_id: EffectId::new(Uuid::new_v5(
                &Uuid::NAMESPACE_URL,
                format!("mirror-{workflow_id}").as_bytes(),
            )),
            state: DeliveryState::Pending,
            attempts: 0,
            last_error: None,
            external_receipt: None,
        },
        created_at_ms: 100,
        updated_at_ms: 100,
    }
}

#[tokio::test]
async fn unresolved_terminal_exists_only_after_return_final_submission() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    assert!(!store.has_unresolved_terminal().await.unwrap());

    let request_id = id(29);
    let mut return_command = command(request_id, "return the result");
    return_command.return_final = true;
    return_command.timeout_ms = 0;
    let mut workflow = match store
        .claim(
            return_command,
            route_id(1),
            route_id(2),
            binding("source"),
            binding("target"),
            100,
        )
        .await
        .unwrap()
    {
        ClaimResult::New(record) => record,
        other => panic!("expected a new claim, got {other:?}"),
    };
    assert!(!store.has_unresolved_terminal().await.unwrap());

    for next in [
        WorkflowState::AuditPosted,
        WorkflowState::AdmissionPrepared,
        WorkflowState::Submitted,
    ] {
        let expected = workflow.workflow.state;
        if next == WorkflowState::Submitted {
            workflow.workflow.submitted_target = Some(binding("target"));
        }
        workflow.workflow.transition(next).unwrap();
        workflow.updated_at_ms += 1;
        store
            .save_workflow(workflow.clone(), expected)
            .await
            .unwrap();
    }
    assert!(store.has_unresolved_terminal().await.unwrap());

    store
        .register_return(return_record(request_id, DeliveryState::Pending))
        .await
        .unwrap();
    assert!(!store.has_unresolved_terminal().await.unwrap());
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn callback_uncertainty_and_unresolved_results_survive_restart() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let mut workflow = claim_new(&store, id(30)).await;
    for next in [
        WorkflowState::AuditPosted,
        WorkflowState::AdmissionPrepared,
        WorkflowState::Submitted,
        WorkflowState::Completed,
    ] {
        let expected = workflow.workflow.state;
        workflow.workflow.transition(next).unwrap();
        workflow.updated_at_ms += 1;
        store
            .save_workflow(workflow.clone(), expected)
            .await
            .unwrap();
    }
    let returned = return_record(workflow.command.id, DeliveryState::Delivering);
    store.register_return(returned).await.unwrap();
    store.shutdown().await.unwrap();

    let reopened = StoreHandle::open(&path).unwrap();
    let pending = reopened.pending_returns().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].agent.state, DeliveryState::Indeterminate);
    let status = reopened.status().await.unwrap();
    assert_eq!(status.unresolved_returns, 1);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn routes_outbox_inbox_and_metadata_are_durable_and_deduplicated() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let route = Route {
        id: route_id(40),
        title: "alpha".into(),
        channels: vec![ChannelBinding::Telegram { topic_id: 12 }],
        agent: Some(binding("alpha")),
    };
    store.save_route(route.clone(), 100).await.unwrap();

    let effect = EffectId::new(Uuid::from_u128(41));
    let mut outbox = OutboxItem {
        id: effect,
        route_id: Some(route.id),
        sender_harness: Some("codex".into()),
        source_agent: route.agent.clone(),
        kind: ChannelKind::Telegram,
        destination: "12".into(),
        body: "audit".into(),
        state: OutboxState::Delivering,
        attempts: 1,
        last_error: None,
        external_receipt: None,
    };
    store
        .enqueue_outbox(None, outbox.clone(), 100)
        .await
        .unwrap();
    outbox.body = "different".into();
    assert!(matches!(
        store.enqueue_outbox(None, outbox, 100).await,
        Err(StoreError::Conflict(_))
    ));

    let reply_agent = binding("reply");
    let delivered = OutboxItem {
        id: EffectId::new(Uuid::from_u128(43)),
        route_id: Some(route.id),
        sender_harness: Some("codex".into()),
        source_agent: Some(reply_agent.clone()),
        kind: ChannelKind::Telegram,
        destination: "12".into(),
        body: "agent output".into(),
        state: OutboxState::Delivered,
        attempts: 1,
        last_error: None,
        external_receipt: Some("501".into()),
    };
    store
        .enqueue_outbox(None, delivered.clone(), 100)
        .await
        .unwrap();
    assert_eq!(
        store
            .find_outbox_agent(ChannelKind::Telegram, "12".into(), "501".into())
            .await
            .unwrap(),
        Some(reply_agent)
    );

    let inbox = InboxItem {
        id: EffectId::new(Uuid::from_u128(42)),
        channel: ChannelKind::Signal,
        external_id: "external-1".into(),
        destination: "group-one".into(),
        sender_id: Some("+15551234567".into()),
        sender: Some("alice".into()),
        reply_to_external_id: None,
        body: "hello".into(),
        state: "pending".into(),
        created_at_ms: 100,
    };
    assert!(store.accept_inbox(inbox.clone()).await.unwrap());
    assert!(!store.accept_inbox(inbox).await.unwrap());
    store
        .set_metadata("cursor".into(), "17".into())
        .await
        .unwrap();
    store.shutdown().await.unwrap();

    let reopened = StoreHandle::open(&path).unwrap();
    let persisted_route = reopened.get_route(route.id).await.unwrap().unwrap();
    assert_eq!(persisted_route.id, route.id);
    assert_eq!(persisted_route.title, route.title);
    assert_eq!(persisted_route.channels, route.channels);
    assert_eq!(persisted_route.agent, None);
    let recovered = reopened.pending_outbox().await.unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].state, OutboxState::Pending);
    assert_eq!(
        reopened
            .get_metadata("cursor".into())
            .await
            .unwrap()
            .as_deref(),
        Some("17")
    );
    let status = reopened.status().await.unwrap();
    assert_eq!(status.pending_outbox, 1);
    assert_eq!(status.pending_inbox, 1);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn passive_output_rebaseline_discards_only_unrequested_delivery_work() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let passive = OutboxItem {
        id: EffectId::new(Uuid::from_u128(501)),
        route_id: Some(route_id(50)),
        sender_harness: Some("codex".into()),
        source_agent: Some(binding("passive")),
        kind: ChannelKind::Telegram,
        destination: "12".into(),
        body: "observed while offline".into(),
        state: OutboxState::Pending,
        attempts: 0,
        last_error: None,
        external_receipt: None,
    };
    let requested = OutboxItem {
        id: EffectId::new(Uuid::from_u128(502)),
        body: "explicit workflow delivery".into(),
        ..passive.clone()
    };
    store.enqueue_outbox(None, passive, 100).await.unwrap();
    store
        .enqueue_outbox(Some(id(503)), requested.clone(), 100)
        .await
        .unwrap();
    store.initialize_event_cursor(10).await.unwrap();

    assert_eq!(store.rebaseline_passive_output(99).await.unwrap(), 1);
    assert_eq!(store.pending_outbox().await.unwrap(), vec![requested]);
    assert_eq!(
        store
            .get_metadata("wakterm_event_cursor".into())
            .await
            .unwrap()
            .as_deref(),
        Some("99")
    );
    store.shutdown().await.unwrap();
}
