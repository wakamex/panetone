use std::fs;

use panetone::domain::{AgentBinding, ChannelBinding, ChannelKind, Route, RouteId};
use panetone::store::{EventCursorGap, RouteAgent, StoreError, StoreHandle};
use panetone::wakterm::EventRecord;
use rusqlite::Connection;
use tempfile::tempdir;
use uuid::Uuid;

fn route_id() -> RouteId {
    RouteId::new(Uuid::from_u128(50))
}

fn route() -> Route {
    Route {
        id: route_id(),
        title: "zola".into(),
        channels: vec![
            ChannelBinding::Telegram { topic_id: 101 },
            ChannelBinding::Signal {
                group_id: "signal-zola".into(),
            },
        ],
        agent: Some(AgentBinding {
            agent_id: "agent-zola".into(),
            incarnation_id: "incarnation-zola-7".into(),
            harness: "codex".into(),
            pane_id: Some(11),
        }),
    }
}

fn live_agents(route: &Route) -> Vec<RouteAgent> {
    vec![RouteAgent {
        route_id: route.id,
        agent: route.agent.clone().unwrap(),
    }]
}

fn fixture_events() -> Vec<EventRecord> {
    let fixture: serde_json::Value = serde_json::from_str(
        &fs::read_to_string("/code/wakterm/docs/agent-api/v1/golden-fixtures.json").unwrap(),
    )
    .unwrap();
    serde_json::from_value(fixture["event_page"]["events"].clone()).unwrap()
}

#[tokio::test]
async fn event_page_recording_output_projection_and_cursor_advance_are_atomic() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let route = route();
    store.save_route(route.clone(), 1).await.unwrap();
    store
        .set_metadata(
            "route_output_preferences_v1".into(),
            serde_json::json!({route.id.to_string(): "tg"}).to_string(),
        )
        .await
        .unwrap();
    store.initialize_event_cursor(100).await.unwrap();

    let outcome = store
        .ingest_agent_events(100, 107, fixture_events(), live_agents(&route), 3)
        .await
        .unwrap();
    assert_eq!(outcome.recorded, 7);
    assert_eq!(outcome.visible_outputs, 2);
    assert_eq!(outcome.unrouted, 0);
    assert_eq!(outcome.next_after_sequence, 107);
    let status = store.status().await.unwrap();
    assert_eq!(status.event_cursor, Some(107));
    assert_eq!(status.pending_outbox, 2);
    let disposition = store
        .output_disposition(
            "agent-zola".into(),
            "incarnation-zola-7".into(),
            100,
            "Working on café support ✓".into(),
        )
        .await
        .unwrap();
    assert_eq!(disposition.event_cursor, Some(107));
    let routed = disposition.output.unwrap();
    assert_eq!(routed.disposition, "projected");
    assert_eq!(routed.route_id, Some(route.id));
    assert_eq!(routed.event.kind, "assistant_message");
    let output = store.pending_outbox().await.unwrap();
    assert_eq!(output[0].destination, "101");
    assert_eq!(output[0].sender_harness.as_deref(), Some("codex"));
    assert!(output.iter().any(|item| item.body.starts_with("Plan:\n")));
    assert!(
        output
            .iter()
            .any(|item| item.body == "Working on café support ✓")
    );
    assert!(
        output
            .iter()
            .all(|item| item.body != "Completed café support ✓")
    );
    store.shutdown().await.unwrap();

    let connection = Connection::open(&path).unwrap();
    let projected: u64 = connection
        .query_row(
            "SELECT COUNT(*) FROM agent_events WHERE state = 'projected'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(projected, 2);
    drop(connection);
    let reopened = StoreHandle::open(&path).unwrap();
    assert_eq!(reopened.status().await.unwrap().event_cursor, Some(107));
    assert_eq!(reopened.pending_outbox().await.unwrap().len(), 2);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn output_disposition_exposes_unrouted_assistant_output() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let event = fixture_events()
        .into_iter()
        .find(|event| event.kind == "assistant_message")
        .unwrap();
    let expected = event.sequence - 1;
    store.initialize_event_cursor(expected).await.unwrap();
    store
        .ingest_agent_events(expected, event.sequence, vec![event], Vec::new(), 3)
        .await
        .unwrap();

    let snapshot = store
        .output_disposition(
            "agent-zola".into(),
            "incarnation-zola-7".into(),
            expected,
            "Working on café support ✓".into(),
        )
        .await
        .unwrap();
    let output = snapshot.output.unwrap();
    assert_eq!(output.disposition, "unrouted");
    assert_eq!(output.route_id, None);
    assert_eq!(store.status().await.unwrap().unrouted_agent_events, 1);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn event_output_defaults_to_an_available_signal_binding() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let route = route();
    store.save_route(route.clone(), 1).await.unwrap();
    let event = fixture_events()
        .into_iter()
        .find(|event| event.kind == "assistant_message")
        .unwrap();
    let expected = event.sequence - 1;
    store.initialize_event_cursor(expected).await.unwrap();

    store
        .ingest_agent_events(
            expected,
            event.sequence,
            vec![event],
            live_agents(&route),
            3,
        )
        .await
        .unwrap();

    let output = store.pending_outbox().await.unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].kind, ChannelKind::Signal);
    assert_eq!(output[0].destination, "signal-zola");
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_visible_event_rolls_back_event_and_cursor_together() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let route = route();
    store.save_route(route.clone(), 1).await.unwrap();
    store.initialize_event_cursor(100).await.unwrap();
    let mut event = fixture_events()
        .into_iter()
        .find(|event| event.kind == "assistant_message")
        .unwrap();
    event.fields.remove("text");

    assert!(matches!(
        store
            .ingest_agent_events(100, event.sequence, vec![event], live_agents(&route), 3,)
            .await,
        Err(StoreError::Conflict(_))
    ));
    let status = store.status().await.unwrap();
    assert_eq!(status.event_cursor, Some(100));
    assert_eq!(status.pending_outbox, 0);
    store.shutdown().await.unwrap();
    let connection = Connection::open(&path).unwrap();
    let events: u64 = connection
        .query_row("SELECT COUNT(*) FROM agent_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(events, 0);
}

#[tokio::test]
async fn cursor_gap_takes_a_fresh_catalog_baseline_and_remains_visible() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let route = route();
    store.save_route(route.clone(), 1).await.unwrap();
    store.initialize_event_cursor(12).await.unwrap();

    let fixture: serde_json::Value = serde_json::from_str(
        &fs::read_to_string("/code/wakterm/docs/agent-api/v1/golden-fixtures.json").unwrap(),
    )
    .unwrap();
    let catalog = serde_json::from_value(fixture["catalog"].clone()).unwrap();
    store
        .recover_event_cursor_gap(
            EventCursorGap {
                requested_after_sequence: 12,
                oldest_available_sequence: 90,
                latest_sequence: 108,
                recovery_catalog_as_of_sequence: 100,
                fresh_catalog_as_of_sequence: 100,
                recorded_at_ms: 4,
            },
            catalog,
        )
        .await
        .unwrap();
    let status = store.status().await.unwrap();
    assert_eq!(status.event_cursor, Some(100));
    assert_eq!(
        status
            .event_cursor_gap
            .as_ref()
            .unwrap()
            .requested_after_sequence,
        12
    );
    store.shutdown().await.unwrap();
}
