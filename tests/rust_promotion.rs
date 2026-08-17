use panetone::domain::{
    AgentBinding, ChannelBinding, ChannelKind, OutboxState, Route, RouteId, RouteStatus,
};
use panetone::promotion::{LegacyDecision, LegacyRecordKind, OperatorAction, OperatorMutation};
use panetone::store::{StoreError, StoreHandle};
use rusqlite::{Connection, params};
use tempfile::tempdir;
use uuid::Uuid;

fn route_id(value: u128) -> RouteId {
    RouteId::new(Uuid::from_u128(value))
}

fn operation(value: u128, action: OperatorAction) -> OperatorMutation {
    OperatorMutation {
        operation_id: Uuid::from_u128(value),
        intent: None,
        action,
    }
}

fn binding() -> AgentBinding {
    AgentBinding {
        agent_id: "agent-alpha".into(),
        incarnation_id: "incarnation-alpha".into(),
        harness: "codex".into(),
        pane_id: Some(41),
    }
}

fn unavailable_signal_route() -> Route {
    Route {
        id: route_id(1),
        title: "alpha".into(),
        channels: vec![ChannelBinding::Signal {
            group_id: "signal-group".into(),
        }],
        agent: None,
        status: RouteStatus::Unavailable,
    }
}

#[tokio::test]
async fn promotion_requires_fresh_route_cursor_and_explicit_release() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let route = unavailable_signal_route();
    store.save_route(route.clone(), 1).await.unwrap();

    let initial = store.promotion_status().await.unwrap();
    assert!(initial.delivery_hold);
    assert_eq!(initial.event_cursor, None);
    assert!(initial.enabled_routes.is_empty());

    let enable = operation(
        1,
        OperatorAction::SetRouteEnabled {
            route_id: route.id,
            enabled: true,
        },
    );
    assert!(matches!(
        store.apply_operator_mutation(enable, 2).await,
        Err(StoreError::Conflict(_))
    ));

    let reconcile = operation(
        2,
        OperatorAction::ReconcileRoute {
            route_id: route.id,
            binding: binding(),
            replace_identity: false,
        },
    );
    let first = store
        .apply_operator_mutation(reconcile.clone(), 3)
        .await
        .unwrap();
    assert!(!first.replayed);
    assert_eq!(first.route.unwrap().status, RouteStatus::Available);
    let replay = store.apply_operator_mutation(reconcile, 99).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.promotion.operator_actions, 1);
    assert!(matches!(
        store
            .apply_operator_mutation(
                operation(2, OperatorAction::SetDeliveryHold { held: true }),
                4,
            )
            .await,
        Err(StoreError::Conflict(_))
    ));

    store
        .apply_operator_mutation(
            operation(
                3,
                OperatorAction::SetRouteEnabled {
                    route_id: route.id,
                    enabled: true,
                },
            ),
            5,
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .apply_operator_mutation(
                operation(4, OperatorAction::SetDeliveryHold { held: false }),
                6,
            )
            .await,
        Err(StoreError::Conflict(_))
    ));

    store
        .apply_operator_mutation(
            operation(5, OperatorAction::InitializeEventCursor { sequence: 100 }),
            7,
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .apply_operator_mutation(
                operation(6, OperatorAction::InitializeEventCursor { sequence: 101 },),
                8,
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
    store
        .apply_operator_mutation(
            operation(7, OperatorAction::SetDeliveryHold { held: false }),
            9,
        )
        .await
        .unwrap();

    let released = store.promotion_status().await.unwrap();
    assert!(released.delivery_allowed(route.id));
    store.advance_event_cursor(100, 108).await.unwrap();
    assert!(matches!(
        store.advance_event_cursor(100, 109).await,
        Err(StoreError::Conflict(_))
    ));
    assert!(matches!(
        store.advance_event_cursor(108, 107).await,
        Err(StoreError::Conflict(_))
    ));
    store.shutdown().await.unwrap();

    let reopened = StoreHandle::open(&path).unwrap();
    let durable = reopened.promotion_status().await.unwrap();
    assert_eq!(durable.event_cursor, Some(108));
    assert!(durable.delivery_allowed(route.id));
    assert_eq!(durable.operator_actions, 4);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn legacy_records_need_audited_dispositions_and_debate_maps_only_to_signal() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let mut route = unavailable_signal_route();
    route.agent = Some(binding());
    route.status = RouteStatus::Available;
    store.save_route(route.clone(), 1).await.unwrap();
    store.shutdown().await.unwrap();

    let control_id = Uuid::from_u128(20).to_string();
    let return_id = Uuid::from_u128(21).to_string();
    let debate_id = Uuid::from_u128(22).to_string();
    let connection = Connection::open(&path).unwrap();
    for request_id in [&control_id, &return_id] {
        connection
            .execute(
                "INSERT INTO idempotency_tombstones(
                    request_id, semantic_hash, hash_kind, terminal_state,
                    created_at_ms, completed_at_ms
                 ) VALUES (?1, 'hash', 'legacy_full_record_v1', 'indeterminate', 1, NULL)",
                params![request_id],
            )
            .unwrap();
    }
    connection
        .execute(
            "INSERT INTO legacy_control_requests(
                request_id, state, source, target, record_json,
                created_at_ms, updated_at_ms
             ) VALUES (?1, 'indeterminate', 'a', 'b', '{}', 1, 1)",
            params![control_id],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO legacy_return_deliveries(
                request_id, state, agent_state, mirror_state, record_json,
                created_at_ms, updated_at_ms
             ) VALUES (?1, 'pending', 'pending', 'pending', '{}', 1, 1)",
            params![return_id],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO legacy_debate_outbox(
                effect_id, destination, record_json, resolution_state,
                resolution_json, created_at_ms, updated_at_ms
             ) VALUES (?1, '-100-old', ?2, 'held', NULL, 1, 1)",
            params![
                debate_id,
                serde_json::json!({"chunk": "legacy debate", "harness": "codex"}).to_string()
            ],
        )
        .unwrap();
    drop(connection);

    let store = StoreHandle::open(&path).unwrap();
    let unresolved = store.promotion_status().await.unwrap();
    assert_eq!(unresolved.unresolved_legacy_controls, 1);
    assert_eq!(unresolved.unresolved_legacy_returns, 1);
    assert_eq!(unresolved.held_legacy_debate, 1);

    store
        .apply_operator_mutation(
            operation(
                20,
                OperatorAction::DisposeLegacy {
                    record_kind: LegacyRecordKind::Control,
                    record_id: control_id.clone(),
                    decision: LegacyDecision::NoReplay,
                    evidence: "operator inspected the legacy request".into(),
                },
            ),
            10,
        )
        .await
        .unwrap();
    store
        .apply_operator_mutation(
            operation(
                21,
                OperatorAction::DisposeLegacy {
                    record_kind: LegacyRecordKind::Return,
                    record_id: return_id,
                    decision: LegacyDecision::ExternallyVerified,
                    evidence: "delivery was verified in the destination".into(),
                },
            ),
            11,
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .apply_operator_mutation(
                operation(
                    22,
                    OperatorAction::DisposeLegacy {
                        record_kind: LegacyRecordKind::Debate,
                        record_id: debate_id.clone(),
                        decision: LegacyDecision::MapDebateToSignal {
                            route_id: route.id,
                            expected_legacy_destination: "wrong".into(),
                        },
                        evidence: "mapping review".into(),
                    },
                ),
                12,
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
    store
        .apply_operator_mutation(
            operation(
                23,
                OperatorAction::DisposeLegacy {
                    record_kind: LegacyRecordKind::Debate,
                    record_id: debate_id,
                    decision: LegacyDecision::MapDebateToSignal {
                        route_id: route.id,
                        expected_legacy_destination: "-100-old".into(),
                    },
                    evidence: "the old Debate chat is this Signal group".into(),
                },
            ),
            13,
        )
        .await
        .unwrap();

    let status = store.promotion_status().await.unwrap();
    assert_eq!(status.unresolved_legacy_controls, 0);
    assert_eq!(status.unresolved_legacy_returns, 0);
    assert_eq!(status.held_legacy_debate, 0);
    let outbox = store.pending_outbox().await.unwrap();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].kind, ChannelKind::Signal);
    assert_eq!(outbox[0].destination, "signal-group");
    assert_eq!(outbox[0].sender_harness.as_deref(), Some("codex"));
    assert_eq!(outbox[0].body, "legacy debate");
    assert_eq!(outbox[0].state, OutboxState::Pending);
    store.shutdown().await.unwrap();
}
