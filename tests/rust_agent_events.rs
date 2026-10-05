use std::fs;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
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
                allow_members: false,
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

fn debate_route() -> Route {
    Route {
        title: "debate".into(),
        ..route()
    }
}

fn live_agents(route: &Route) -> Vec<RouteAgent> {
    vec![RouteAgent {
        route_id: route.id,
        agent: route.agent.clone().unwrap(),
        working_directory: None,
    }]
}

fn fixture_events() -> Vec<EventRecord> {
    let fixture: serde_json::Value = serde_json::from_str(
        &fs::read_to_string("/code/wakterm/docs/agent-api/v1/golden-fixtures.json").unwrap(),
    )
    .unwrap();
    serde_json::from_value(fixture["event_page"]["events"].clone()).unwrap()
}

fn policy_aborted_final() -> EventRecord {
    let fixture: serde_json::Value = serde_json::from_str(
        &fs::read_to_string("/code/wakterm/docs/agent-api/v1/golden-fixtures.json").unwrap(),
    )
    .unwrap();
    serde_json::from_value(fixture["policy_aborted_turn_final"].clone()).unwrap()
}

#[tokio::test]
async fn question_event_routes_buttons_and_preserves_exact_identity() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let route = route();
    store.save_route(route.clone(), 1).await.unwrap();
    store.initialize_event_cursor(100).await.unwrap();
    let event: EventRecord = serde_json::from_value(serde_json::json!({
        "sequence": 101,
        "event_id": "approval-event-101",
        "kind": "approval_requested",
        "agent_id": "agent-zola",
        "incarnation_id": "incarnation-zola-7",
        "turn_id": "turn-1",
        "approval": {
            "schema": "wakterm.agent-approval.v1",
            "kind": "user_question",
            "request_id": "0123456789abcdef01234567",
            "agent_id": "agent-zola",
            "incarnation_id": "incarnation-zola-7",
            "turn_id": "turn-1",
            "item_id": "item-1",
            "observed_at": "2026-09-29T12:00:00Z",
            "prompt": "How should I promote the build?",
            "reason": "Promotion scope",
            "choices": [
                {"id": "option_1", "label": "Build and activate", "description": "Replace the shared runtime."},
                {"id": "option_2", "label": "Hold off", "description": "Keep the current runtime."}
            ]
        }
    }))
    .unwrap();
    store
        .ingest_agent_events(100, 101, vec![event], live_agents(&route), 3)
        .await
        .unwrap();
    let pending = store.pending_outbox().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].kind, ChannelKind::Telegram);
    assert_eq!(pending[0].destination, "101");
    assert!(pending[0].body.contains("How should I promote the build?"));
    assert!(
        pending[0]
            .body
            .contains("1. Build and activate: Replace the shared runtime.")
    );
    assert_eq!(
        pending[0].actions[0].id,
        "wakap:0123456789abcdef01234567:option_1"
    );
    let stored = store
        .get_approval("0123456789abcdef01234567".into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.route_id, route.id);
    assert_eq!(stored.request.agent_id, "agent-zola");
    assert_eq!(stored.request.incarnation_id, "incarnation-zola-7");
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn question_form_is_posted_as_text_for_the_agent_pane() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let route = route();
    store.save_route(route.clone(), 1).await.unwrap();
    store.initialize_event_cursor(100).await.unwrap();
    // The shape Wakterm emits for a Claude form with several questions.
    let prompt = "1. Browser: Which Browser?\n- A: first\n- B\n\n2. Lifetime: Which Lifetime?\n- A: first\n- B";
    let event: EventRecord = serde_json::from_value(serde_json::json!({
        "sequence": 101,
        "event_id": "approval-event-101",
        "kind": "approval_requested",
        "agent_id": "agent-zola",
        "incarnation_id": "incarnation-zola-7",
        "turn_id": "turn-1",
        "approval": {
            "schema": "wakterm.agent-approval.v1",
            "kind": "user_question_form",
            "request_id": "0123456789abcdef01234567",
            "agent_id": "agent-zola",
            "incarnation_id": "incarnation-zola-7",
            "turn_id": "turn-1",
            "item_id": "item-1",
            "observed_at": "2026-10-05T04:44:00Z",
            "prompt": prompt,
            "reason": null,
            "choices": []
        }
    }))
    .unwrap();
    store
        .ingest_agent_events(100, 101, vec![event], live_agents(&route), 3)
        .await
        .unwrap();
    let pending = store.pending_outbox().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert!(pending[0].body.starts_with("Input needed"));
    assert!(pending[0].body.contains(prompt));
    assert!(pending[0].body.contains("answer it in the agent's pane"));
    assert!(pending[0].actions.is_empty());
    assert_eq!(store.status().await.unwrap().event_cursor, Some(101));
    store.shutdown().await.unwrap();
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
async fn aborted_final_detail_is_projected_once_as_a_failure_notice() {
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
    let event = policy_aborted_final();
    store
        .initialize_event_cursor(event.sequence - 1)
        .await
        .unwrap();

    let outcome = store
        .ingest_agent_events(
            event.sequence - 1,
            event.sequence,
            vec![event.clone()],
            live_agents(&route),
            3,
        )
        .await
        .unwrap();

    assert_eq!(outcome.visible_outputs, 1);
    let output = store.pending_outbox().await.unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(
        output[0].body,
        "Turn failed: Codex could not complete this turn because the provider blocked the response under its content policy."
    );
    store.shutdown().await.unwrap();

    let reopened = StoreHandle::open(&path).unwrap();
    assert_eq!(reopened.pending_outbox().await.unwrap().len(), 1);
    assert_eq!(
        reopened.status().await.unwrap().event_cursor,
        Some(event.sequence)
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_turn_ended_without_a_reply_is_not_projected() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let route = route();
    store.save_route(route.clone(), 1).await.unwrap();
    let mut silent = policy_aborted_final();
    silent.fields.insert("reason".into(), "no_reply".into());
    silent
        .fields
        .insert("detail".into(), "Claude went idle without replying.".into());
    store
        .initialize_event_cursor(silent.sequence - 1)
        .await
        .unwrap();

    let outcome = store
        .ingest_agent_events(
            silent.sequence - 1,
            silent.sequence,
            vec![silent.clone()],
            live_agents(&route),
            3,
        )
        .await
        .unwrap();

    assert_eq!(outcome.visible_outputs, 0);
    assert!(store.pending_outbox().await.unwrap().is_empty());
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn completed_final_and_aborted_final_without_detail_are_not_projected() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let route = route();
    store.save_route(route.clone(), 1).await.unwrap();
    let mut aborted = policy_aborted_final();
    aborted.fields.remove("detail");
    let mut completed = aborted.clone();
    completed.sequence += 1;
    completed.event_id = "completed-final".into();
    completed
        .fields
        .insert("outcome".into(), "completed".into());
    completed
        .fields
        .insert("text".into(), "already projected".into());
    store
        .initialize_event_cursor(aborted.sequence - 1)
        .await
        .unwrap();

    let outcome = store
        .ingest_agent_events(
            aborted.sequence - 1,
            completed.sequence,
            vec![aborted, completed.clone()],
            live_agents(&route),
            3,
        )
        .await
        .unwrap();

    assert_eq!(outcome.visible_outputs, 0);
    assert!(store.pending_outbox().await.unwrap().is_empty());
    assert_eq!(
        store.status().await.unwrap().event_cursor,
        Some(completed.sequence)
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn only_debate_suppresses_the_exact_no_reply_disposition() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let debate = debate_route();
    store.save_route(debate.clone(), 1).await.unwrap();
    let mut silent = fixture_events()
        .into_iter()
        .find(|event| event.kind == "assistant_message")
        .unwrap();
    silent
        .fields
        .insert("text".into(), "  <panetone:no-reply>\n".into());
    store
        .initialize_event_cursor(silent.sequence - 1)
        .await
        .unwrap();

    let outcome = store
        .ingest_agent_events(
            silent.sequence - 1,
            silent.sequence,
            vec![silent.clone()],
            live_agents(&debate),
            3,
        )
        .await
        .unwrap();

    assert_eq!(outcome.visible_outputs, 0);
    assert!(store.pending_outbox().await.unwrap().is_empty());
    assert_eq!(outcome.last_agents, live_agents(&debate));
    store.shutdown().await.unwrap();
    let connection = Connection::open(&path).unwrap();
    let state: String = connection
        .query_row(
            "SELECT state FROM agent_events WHERE event_id = ?1",
            [&silent.event_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "suppressed");
}

#[tokio::test]
async fn debate_forwards_normal_output_and_failures_and_other_routes_forward_the_token() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let debate = debate_route();
    store.save_route(debate.clone(), 1).await.unwrap();
    let mut normal = fixture_events()
        .into_iter()
        .find(|event| event.kind == "assistant_message")
        .unwrap();
    normal.fields.insert(
        "text".into(),
        "The token <panetone:no-reply> is not the whole response.".into(),
    );
    let mut failure = policy_aborted_final();
    failure.sequence = normal.sequence + 1;
    failure.event_id = "debate-policy-failure".into();
    store
        .initialize_event_cursor(normal.sequence - 1)
        .await
        .unwrap();

    let outcome = store
        .ingest_agent_events(
            normal.sequence - 1,
            failure.sequence,
            vec![normal, failure],
            live_agents(&debate),
            3,
        )
        .await
        .unwrap();

    assert_eq!(outcome.visible_outputs, 2);
    let bodies = store
        .pending_outbox()
        .await
        .unwrap()
        .into_iter()
        .map(|item| item.body)
        .collect::<Vec<_>>();
    assert!(bodies.iter().any(|body| body.starts_with("The token ")));
    assert!(bodies.iter().any(|body| body.starts_with("Turn failed: ")));
    store.shutdown().await.unwrap();

    let other_store = StoreHandle::open(directory.path().join("other.sqlite3")).unwrap();
    let other = route();
    other_store.save_route(other.clone(), 1).await.unwrap();
    let mut token = fixture_events()
        .into_iter()
        .find(|event| event.kind == "assistant_message")
        .unwrap();
    token
        .fields
        .insert("text".into(), "<panetone:no-reply>".into());
    other_store
        .initialize_event_cursor(token.sequence - 1)
        .await
        .unwrap();
    other_store
        .ingest_agent_events(
            token.sequence - 1,
            token.sequence,
            vec![token],
            live_agents(&other),
            3,
        )
        .await
        .unwrap();
    assert_eq!(
        other_store.pending_outbox().await.unwrap()[0].body,
        "<panetone:no-reply>"
    );
    other_store.shutdown().await.unwrap();
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
async fn standalone_attachment_tags_are_captured_in_order_and_use_the_route_channel() {
    let directory = tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    fs::create_dir(&workspace).unwrap();
    let image = workspace.join("scene image.png");
    fs::write(&image, b"png bytes").unwrap();
    let second_image = workspace.join("detail.jpg");
    fs::write(&second_image, b"jpeg bytes").unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let route = route();
    store.save_route(route.clone(), 1).await.unwrap();
    let mut event = fixture_events()
        .into_iter()
        .find(|event| event.kind == "assistant_message")
        .unwrap();
    let expected = event.sequence - 1;
    event.fields.insert(
        "text".into(),
        format!(
            "Here is the scene.\n\n```text\n[panetone:attach /example/not-a-request.png]\n```\n\n[panetone:attach {}]\n[panetone:attach {}]",
            image.display(),
            second_image.display()
        )
        .into(),
    );
    let mut agents = live_agents(&route);
    agents[0].working_directory = Some(workspace);
    store.initialize_event_cursor(expected).await.unwrap();

    store
        .ingest_agent_events(expected, event.sequence, vec![event], agents, 3)
        .await
        .unwrap();

    let output = store.pending_outbox().await.unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].kind, ChannelKind::Signal);
    assert_eq!(
        output[0].body,
        "Here is the scene.\n\n```text\n[panetone:attach /example/not-a-request.png]\n```"
    );
    assert_eq!(output[0].attachments.len(), 2);
    let first = &output[0].attachments[0];
    assert_eq!(first.file_name, "scene_image.png");
    assert_eq!(first.media_type, "image/png");
    assert_eq!(BASE64.decode(&first.data_base64).unwrap(), b"png bytes");
    let second = &output[0].attachments[1];
    assert_eq!(second.file_name, "detail.jpg");
    assert_eq!(second.media_type, "image/jpeg");
    assert_eq!(BASE64.decode(&second.data_base64).unwrap(), b"jpeg bytes");
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn attachment_tag_cannot_read_outside_the_harness_working_directory() {
    let directory = tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    fs::create_dir(&workspace).unwrap();
    let outside = directory.path().join("private.png");
    fs::write(&outside, b"private").unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let route = route();
    store.save_route(route.clone(), 1).await.unwrap();
    let mut event = fixture_events()
        .into_iter()
        .find(|event| event.kind == "assistant_message")
        .unwrap();
    let expected = event.sequence - 1;
    event.fields.insert(
        "text".into(),
        format!("[panetone:attach {}]", outside.display()).into(),
    );
    let mut agents = live_agents(&route);
    agents[0].working_directory = Some(workspace);
    store.initialize_event_cursor(expected).await.unwrap();

    store
        .ingest_agent_events(expected, event.sequence, vec![event], agents, 3)
        .await
        .unwrap();

    let output = store.pending_outbox().await.unwrap();
    assert_eq!(output.len(), 1);
    assert!(output[0].attachments.is_empty());
    assert_eq!(
        output[0].body,
        "Attachment unavailable: the tagged file is outside the harness working directory"
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn long_telegram_output_is_durably_chunked_before_cursor_advance() {
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
    let mut event = fixture_events()
        .into_iter()
        .find(|event| event.kind == "assistant_message")
        .unwrap();
    let expected = event.sequence - 1;
    let body = "x".repeat(3900) + "tail";
    event.fields.insert("text".into(), body.clone().into());
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
    let chunks = store.pending_outbox().await.unwrap();
    assert_eq!(chunks.len(), 2);
    assert_eq!(
        chunks
            .iter()
            .map(|chunk| chunk.body.as_str())
            .collect::<String>(),
        body
    );
    assert_ne!(chunks[0].id, chunks[1].id);
    let mut delivered = chunks[0].clone();
    delivered.state = panetone::domain::OutboxState::Delivered;
    store.save_outbox(delivered, 4).await.unwrap();
    store.shutdown().await.unwrap();

    let reopened = StoreHandle::open(&path).unwrap();
    let pending = reopened.pending_outbox().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].body, "tail");
    assert_eq!(
        reopened.status().await.unwrap().event_cursor,
        Some(expected + 1)
    );
    reopened.shutdown().await.unwrap();
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
