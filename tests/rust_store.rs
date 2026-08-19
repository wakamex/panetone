use std::fs;

use panetone::domain::{
    AdmissionReceipt, AdmissionStatus, AgentBinding, CallbackDelivery, ChannelBinding, ChannelKind,
    DeliveryState, EffectId, OutboxItem, OutboxState, Route, RouteId, RouteStatus, SendCommand,
    WorkflowId, WorkflowState,
};
use panetone::store::{
    ClaimResult, DestinationDelivery, InboxItem, ReturnDelivery, StoreError, StoreHandle,
};
use rusqlite::{Connection, params};
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
            supported: 5
        })
    ));
}

#[tokio::test]
async fn version_one_store_upgrades_without_changing_native_hash_semantics() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    store.shutdown().await.unwrap();
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "DROP TABLE agent_events;
             DROP TABLE operator_actions;
             DROP TABLE legacy_dispositions;
             DROP TABLE route_delivery_policy;
             DROP TABLE promotion_state;
             DROP TABLE legacy_debate_outbox;
             DROP TABLE signal_messages;
             DROP TABLE legacy_return_deliveries;
             DROP TABLE legacy_control_requests;
             ALTER TABLE idempotency_tombstones DROP COLUMN hash_kind;
             PRAGMA user_version = 1;",
        )
        .unwrap();
    drop(connection);

    let upgraded = StoreHandle::open(&path).unwrap();
    assert_eq!(upgraded.status().await.unwrap().schema_version, 5);
    let request = id(9);
    assert!(matches!(
        upgraded
            .claim(
                command(request, "hello"),
                route_id(1),
                route_id(2),
                binding("source"),
                binding("target"),
                100,
            )
            .await
            .unwrap(),
        ClaimResult::New(_)
    ));
    upgraded.shutdown().await.unwrap();
    let connection = Connection::open(&path).unwrap();
    let hash_kind: String = connection
        .query_row(
            "SELECT hash_kind FROM idempotency_tombstones WHERE request_id = ?1",
            params![request.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(hash_kind, "semantic_v1");
}

#[tokio::test]
async fn version_two_store_holds_legacy_debate_output_instead_of_sending_it() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    store.shutdown().await.unwrap();
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "DROP TABLE agent_events;
             DROP TABLE operator_actions;
             DROP TABLE legacy_dispositions;
             DROP TABLE route_delivery_policy;
             DROP TABLE promotion_state;
             DROP TABLE legacy_debate_outbox;
             INSERT INTO outbox(
                 effect_id, request_id, channel, destination, state, record_json,
                 created_at_ms, updated_at_ms
             ) VALUES (
                 '00000000-0000-4000-8000-000000000099', NULL, 'debate',
                 '-100123', 'pending',
                 '{\"kind\":\"debate\",\"body\":\"legacy\"}', 10, 11
             );
             PRAGMA user_version = 2;",
        )
        .unwrap();
    drop(connection);

    let upgraded = StoreHandle::open(&path).unwrap();
    let status = upgraded.status().await.unwrap();
    assert_eq!(status.schema_version, 5);
    assert_eq!(status.pending_outbox, 0);
    assert_eq!(status.legacy_debate_outbox, 1);
    upgraded.shutdown().await.unwrap();

    let connection = Connection::open(&path).unwrap();
    let held: (String, String) = connection
        .query_row(
            "SELECT destination, resolution_state FROM legacy_debate_outbox",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(held, ("-100123".into(), "held".into()));
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM outbox", [], |row| row
                .get::<_, u64>(0))
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn idempotency_survives_compaction_as_a_permanent_tombstone() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let request_id = id(10);
    let mut record = claim_new(&store, request_id).await;

    assert!(matches!(
        store
            .claim(
                command(request_id, "hello"),
                route_id(1),
                route_id(2),
                binding("source"),
                binding("target"),
                101
            )
            .await
            .unwrap(),
        ClaimResult::Existing(_)
    ));
    assert!(matches!(
        store
            .claim(
                command(request_id, "different"),
                route_id(1),
                route_id(2),
                binding("source"),
                binding("target"),
                101
            )
            .await
            .unwrap(),
        ClaimResult::Conflict { .. }
    ));

    for next in [
        WorkflowState::AuditPosted,
        WorkflowState::AdmissionPrepared,
        WorkflowState::Submitted,
        WorkflowState::Completed,
    ] {
        let expected = record.workflow.state;
        record.workflow.transition(next).unwrap();
        record.updated_at_ms += 1;
        store.save_workflow(record.clone(), expected).await.unwrap();
    }
    assert_eq!(store.compact(1_000).await.unwrap(), 1);
    assert!(matches!(
        store
            .claim(
                command(request_id, "hello"),
                route_id(1),
                route_id(2),
                binding("source"),
                binding("target"),
                2_000
            )
            .await
            .unwrap(),
        ClaimResult::Tombstone {
            same_content: true,
            ..
        }
    ));
    assert!(matches!(
        store
            .claim(
                command(request_id, "different"),
                route_id(1),
                route_id(2),
                binding("source"),
                binding("target"),
                2_000
            )
            .await
            .unwrap(),
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
async fn callback_uncertainty_and_unresolved_results_survive_restart_and_pruning() {
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
    assert_eq!(store.compact(1_000).await.unwrap(), 0);
    store.shutdown().await.unwrap();

    let reopened = StoreHandle::open(&path).unwrap();
    let pending = reopened.pending_returns().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].agent.state, DeliveryState::Indeterminate);
    assert_eq!(reopened.compact(1_000).await.unwrap(), 0);
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
        status: RouteStatus::Available,
    };
    store.save_route(route.clone(), 100).await.unwrap();

    let effect = EffectId::new(Uuid::from_u128(41));
    let mut outbox = OutboxItem {
        id: effect,
        route_id: Some(route.id),
        sender_harness: Some("codex".into()),
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

    let inbox = InboxItem {
        id: EffectId::new(Uuid::from_u128(42)),
        channel: ChannelKind::Signal,
        external_id: "external-1".into(),
        destination: "group-one".into(),
        sender_id: Some("+15551234567".into()),
        sender: Some("alice".into()),
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
    assert_eq!(reopened.get_route(route.id).await.unwrap().unwrap(), route);
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
