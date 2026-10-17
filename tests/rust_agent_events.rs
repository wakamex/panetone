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
                account: None,
                direct: false,
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
async fn an_approval_of_an_unknown_kind_is_posted_with_its_choices_as_buttons() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let route = route();
    store.save_route(route.clone(), 1).await.unwrap();
    store.initialize_event_cursor(100).await.unwrap();
    // The shape Wakterm emits for Claude's "Session paused" safeguards menu.
    let event: EventRecord = serde_json::from_value(serde_json::json!({
        "sequence": 101,
        "event_id": "approval-event-101",
        "kind": "approval_requested",
        "agent_id": "agent-zola",
        "incarnation_id": "incarnation-zola-7",
        "turn_id": "",
        "approval": {
            "schema": "wakterm.agent-approval.v1",
            "kind": "claude_refusal_fallback",
            "request_id": "0123456789abcdef01234567",
            "agent_id": "agent-zola",
            "incarnation_id": "incarnation-zola-7",
            "turn_id": "",
            "item_id": "0123456789abcdef01234567",
            "observed_at": "2026-10-11T04:06:12Z",
            "prompt": "Session paused: Opus 5.5's safeguards flagged this session. Details: [cyber]",
            "reason": null,
            "command": null,
            "cwd": null,
            "choices": [
                {"id": "retry_fallback", "label": "Switch to Opus 4.8"},
                {"id": "edit_prompt", "label": "Edit prompt and retry with Opus 5.5"}
            ],
            "questions": []
        }
    }))
    .unwrap();
    store
        .ingest_agent_events(100, 101, vec![event], live_agents(&route), 3)
        .await
        .unwrap();
    let pending = store.pending_outbox().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0].body,
        "Input needed\n\nSession paused: Opus 5.5's safeguards flagged this session. Details: [cyber]"
    );
    assert_eq!(
        pending[0]
            .actions
            .iter()
            .map(|action| (action.id.as_str(), action.label.as_str()))
            .collect::<Vec<_>>(),
        [
            (
                "wakap:0123456789abcdef01234567:retry_fallback",
                "Switch to Opus 4.8"
            ),
            (
                "wakap:0123456789abcdef01234567:edit_prompt",
                "Edit prompt and retry with Opus 5.5"
            ),
        ]
    );
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
async fn the_exact_no_reply_token_is_suppressed_and_remembers_the_agent() {
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
async fn normal_output_and_failures_are_forwarded_and_the_token_is_suppressed_on_every_route() {
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
    assert!(other_store.pending_outbox().await.unwrap().is_empty());
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
    let agents = live_agents(&route);
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
async fn attachment_tags_read_any_readable_file_and_report_missing_ones() {
    let directory = tempdir().unwrap();
    let elsewhere = directory.path().join("elsewhere");
    fs::create_dir(&elsewhere).unwrap();
    let outside = elsewhere.join("report.png");
    fs::write(&outside, b"png bytes").unwrap();
    let missing = directory.path().join("missing.png");
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
            "[panetone:attach {}]\n[panetone:attach {}]",
            outside.display(),
            missing.display()
        )
        .into(),
    );
    let agents = live_agents(&route);
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
        "Attachment unavailable: the tagged file does not exist"
    );
    store.shutdown().await.unwrap();

    // A file anywhere the agent's user can read is attached.
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    store.save_route(route.clone(), 1).await.unwrap();
    let mut event = fixture_events()
        .into_iter()
        .find(|event| event.kind == "assistant_message")
        .unwrap();
    event.fields.insert(
        "text".into(),
        format!("Here.\n[panetone:attach {}]", outside.display()).into(),
    );
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
    assert_eq!(output[0].body, "Here.");
    assert_eq!(output[0].attachments.len(), 1);
    assert_eq!(output[0].attachments[0].file_name, "report.png");
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

/// Projects one assistant message with `text` to `route` and returns the
/// posted bodies.
async fn project_assistant_text(route: Route, text: &str) -> Vec<String> {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    store.save_route(route.clone(), 1).await.unwrap();
    let mut event = fixture_events()
        .into_iter()
        .find(|event| event.kind == "assistant_message")
        .unwrap();
    event.fields.insert("text".into(), text.into());
    store
        .initialize_event_cursor(event.sequence - 1)
        .await
        .unwrap();
    store
        .ingest_agent_events(
            event.sequence - 1,
            event.sequence,
            vec![event],
            live_agents(&route),
            3,
        )
        .await
        .unwrap();
    let bodies = store
        .pending_outbox()
        .await
        .unwrap()
        .into_iter()
        .map(|item| item.body)
        .collect();
    store.shutdown().await.unwrap();
    bodies
}

#[tokio::test]
async fn memory_citation_blocks_are_removed_before_posting() {
    assert_eq!(
        project_assistant_text(
            route(),
            "Here is the answer. <memory-used>a.md ^m-7f3a, b.md</memory-used>\n\nSecond paragraph.\n<memory-used>c.md\n^m-1\n</memory-used>"
        )
        .await,
        ["Here is the answer.\n\nSecond paragraph."]
    );
    // A citation inside fenced code is shown as written.
    let fenced = "Example:\n```\n<memory-used>a.md</memory-used>\n```";
    assert_eq!(project_assistant_text(route(), fenced).await, [fenced]);
    // An unterminated block hides the rest of the message.
    assert_eq!(
        project_assistant_text(route(), "Visible.\n<memory-used>a.md\nhidden").await,
        ["Visible."]
    );
    // A message of nothing but citations is not posted.
    assert!(
        project_assistant_text(route(), "<memory-used>a.md</memory-used>")
            .await
            .is_empty()
    );
    // The debate no-reply token still suppresses a reply that also cites memories.
    assert!(
        project_assistant_text(
            debate_route(),
            "<panetone:no-reply>\n<memory-used>a.md</memory-used>"
        )
        .await
        .is_empty()
    );
}

#[tokio::test]
async fn a_claude_menu_raised_outside_a_turn_is_posted_with_numbered_buttons() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let route = route();
    store.save_route(route.clone(), 1).await.unwrap();
    store.initialize_event_cursor(100).await.unwrap();
    // The shape Wakterm emits for the "Switch model?" menu a `/model` command raises.
    let event: EventRecord = serde_json::from_value(serde_json::json!({
        "sequence": 101,
        "event_id": "approval-event-101",
        "kind": "approval_requested",
        "agent_id": "agent-zola",
        "incarnation_id": "incarnation-zola-7",
        "turn_id": "",
        "approval": {
            "schema": "wakterm.agent-approval.v1",
            "kind": "claude_menu",
            "request_id": "0123456789abcdef01234567",
            "agent_id": "agent-zola",
            "incarnation_id": "incarnation-zola-7",
            "turn_id": "",
            "item_id": "0123456789abcdef01234567",
            "observed_at": "2026-10-12T03:21:00Z",
            "prompt": "Switch model?: Your next response will be slower and use more tokens",
            "reason": null,
            "command": null,
            "cwd": null,
            "choices": [
                {"id": "1", "label": "Yes, switch to Opus 5.5"},
                {"id": "2", "label": "No, go back"}
            ],
            "questions": []
        }
    }))
    .unwrap();
    store
        .ingest_agent_events(100, 101, vec![event], live_agents(&route), 3)
        .await
        .unwrap();
    let pending = store.pending_outbox().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0].body,
        "Input needed\n\nSwitch model?: Your next response will be slower and use more tokens"
    );
    assert_eq!(
        pending[0]
            .actions
            .iter()
            .map(|action| (action.id.as_str(), action.label.as_str()))
            .collect::<Vec<_>>(),
        [
            (
                "wakap:0123456789abcdef01234567:1",
                "Yes, switch to Opus 5.5"
            ),
            ("wakap:0123456789abcdef01234567:2", "No, go back"),
        ]
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn input_recorded_with_its_tabs_expanded_proves_delivery() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let route = route();
    store.save_route(route.clone(), 1).await.unwrap();
    store.initialize_event_cursor(100).await.unwrap();
    let body = "Saved from\thost\n\t12 Oct 2026";
    // Claude records each pasted tab as four spaces.
    let recorded = "Saved from    host\n    12 Oct 2026";
    let event: EventRecord = serde_json::from_value(serde_json::json!({
        "sequence": 101,
        "event_id": "input-event-101",
        "kind": "input_accepted",
        "agent_id": "agent-zola",
        "incarnation_id": "incarnation-zola-7",
        "turn_id": "turn-1",
        "input_sha256": panetone::store::DeliveryProof::input_sha256(recorded),
    }))
    .unwrap();
    store
        .ingest_agent_events(100, 101, vec![event], live_agents(&route), 3)
        .await
        .unwrap();
    let proof = |tab_expanded_sha256| panetone::store::DeliveryProof {
        agent_id: "agent-zola".into(),
        after_sequence: 100,
        input_sha256: panetone::store::DeliveryProof::input_sha256(body),
        tab_expanded_sha256,
        deadline_ms: i64::MAX,
        detail: String::new(),
    };

    assert!(!store.input_recorded(proof(None)).await.unwrap());
    assert!(
        store
            .input_recorded(proof(panetone::store::DeliveryProof::tab_expanded_sha256(
                body
            )))
            .await
            .unwrap()
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn output_after_a_direct_message_returns_to_its_sender() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let mut route = route();
    for binding in &mut route.channels {
        if let ChannelBinding::Signal { direct, .. } = binding {
            *direct = true;
        }
    }
    store.save_route(route.clone(), 1).await.unwrap();
    let mut item = panetone::store::InboxItem {
        id: panetone::domain::EffectId::new(Uuid::from_u128(901)),
        channel: ChannelKind::Signal,
        external_id: "owner:1".into(),
        destination: "direct:+15551111".into(),
        sender_id: Some("+15551111".into()),
        sender: Some("Mihai".into()),
        reply_to_external_id: None,
        body: "[direct message] start".into(),
        state: "pending".into(),
        created_at_ms: 1,
        receipt: None,
        steering_acknowledged: None,
        proof: None,
    };
    store.accept_inbox(item.clone()).await.unwrap();
    item.state = "delivered".into();
    store
        .save_inbox(item, "pending", Some((route.id, ChannelKind::Signal)))
        .await
        .unwrap();
    store.initialize_event_cursor(100).await.unwrap();

    store
        .ingest_agent_events(100, 107, fixture_events(), live_agents(&route), 3)
        .await
        .unwrap();
    let pending = store.pending_outbox().await.unwrap();
    assert!(!pending.is_empty());
    for output in &pending {
        assert_eq!(output.kind, ChannelKind::Signal);
        assert_eq!(output.destination, "direct:+15551111");
    }
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_turns_output_goes_where_the_message_that_started_it_came_from() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let mut route = route();
    for binding in &mut route.channels {
        if let ChannelBinding::Signal { direct, .. } = binding {
            *direct = true;
        }
    }
    store.save_route(route.clone(), 1).await.unwrap();
    store.initialize_event_cursor(100).await.unwrap();
    let deliver = |id: u128, destination: &'static str, body: &'static str| {
        let store = store.clone();
        let route_id = route.id;
        async move {
            let mut item = panetone::store::InboxItem {
                id: panetone::domain::EffectId::new(Uuid::from_u128(id)),
                channel: ChannelKind::Signal,
                external_id: format!("owner:{id}"),
                destination: destination.into(),
                sender_id: Some("+15551111".into()),
                sender: Some("Mihai".into()),
                reply_to_external_id: None,
                body: body.into(),
                state: "pending".into(),
                created_at_ms: id as i64,
                receipt: None,
                steering_acknowledged: None,
                proof: None,
            };
            store.accept_inbox(item.clone()).await.unwrap();
            item.state = "delivered".into();
            item.proof = Some(panetone::store::DeliveryProof {
                agent_id: "agent-zola".into(),
                after_sequence: 100,
                input_sha256: panetone::store::DeliveryProof::input_sha256(body),
                tab_expanded_sha256: None,
                deadline_ms: i64::MAX,
                detail: String::new(),
            });
            store
                .save_inbox(item, "pending", Some((route_id, ChannelKind::Signal)))
                .await
                .unwrap();
        }
    };
    let event = |sequence: u64, kind: &str, turn: &str, field: &str, value: &str| -> EventRecord {
        serde_json::from_value(serde_json::json!({
            "sequence": sequence,
            "event_id": format!("event-{sequence}"),
            "kind": kind,
            "agent_id": "agent-zola",
            "incarnation_id": "incarnation-zola-7",
            "turn_id": turn,
            "observed_at": "2026-10-13T18:08:50Z",
            field: value,
        }))
        .unwrap()
    };
    let hash = panetone::store::DeliveryProof::input_sha256;

    // A direct message starts a turn, and a group message arrives before the
    // reply is posted, so the route's last channel is the group again.
    deliver(1, "direct:uuid-owner", "[direct message] is this private?").await;
    deliver(2, "signal-zola", "Melissa says: hello").await;
    store
        .ingest_agent_events(
            100,
            104,
            vec![
                event(
                    101,
                    "turn_started",
                    "turn-direct",
                    "input_sha256",
                    &hash("[direct message] is this private?"),
                ),
                event(
                    102,
                    "assistant_message",
                    "turn-direct",
                    "text",
                    "Yes, only you see this.",
                ),
                event(
                    103,
                    "turn_started",
                    "turn-group",
                    "input_sha256",
                    &hash("Melissa says: hello"),
                ),
                event(
                    104,
                    "assistant_message",
                    "turn-group",
                    "text",
                    "Hello Melissa.",
                ),
            ],
            live_agents(&route),
            3,
        )
        .await
        .unwrap();
    let destinations = |pending: Vec<panetone::domain::OutboxItem>| {
        pending
            .into_iter()
            .map(|item| (item.body, item.destination))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        destinations(store.pending_outbox().await.unwrap()),
        [
            (
                "Yes, only you see this.".to_owned(),
                "direct:uuid-owner".to_owned()
            ),
            ("Hello Melissa.".to_owned(), "signal-zola".to_owned()),
        ]
    );

    // The reverse order: the group's reply is posted after a direct message
    // made the direct chat the route's last channel.
    deliver(3, "signal-zola", "Melissa says: again").await;
    deliver(4, "direct:uuid-owner", "[direct message] start").await;
    store
        .ingest_agent_events(
            104,
            106,
            vec![
                event(
                    105,
                    "turn_started",
                    "turn-group-2",
                    "input_sha256",
                    &hash("Melissa says: again"),
                ),
                event(
                    106,
                    "assistant_message",
                    "turn-group-2",
                    "text",
                    "Again, hello.",
                ),
            ],
            live_agents(&route),
            3,
        )
        .await
        .unwrap();
    let pending = destinations(store.pending_outbox().await.unwrap());
    assert_eq!(
        pending.last().unwrap(),
        &("Again, hello.".to_owned(), "signal-zola".to_owned())
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_tagged_message_settles_the_reply_its_agent_owes() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let mut route = route();
    for binding in &mut route.channels {
        if let ChannelBinding::Signal { allow_members, .. } = binding {
            *allow_members = true;
        }
    }
    store.save_route(route.clone(), 1).await.unwrap();
    // The route's output goes to its shared Signal group.
    store
        .set_metadata(
            "route_output_preferences_v1".into(),
            serde_json::json!({route.id.to_string(): "sig"}).to_string(),
        )
        .await
        .unwrap();
    store.initialize_event_cursor(100).await.unwrap();
    let debt = |id: u128| panetone::store::ReplyDebt {
        request_id: panetone::domain::WorkflowId::new(Uuid::from_u128(id)),
        source: "asker".into(),
        source_route_id: RouteId::new(Uuid::from_u128(77)),
        source_agent: route.agent.clone().unwrap(),
        target: "zola".into(),
        target_route_id: route.id,
        target_agent_id: "agent-zola".into(),
        summary: "run the trials".into(),
        after_sequence: 0,
        created_at_ms: 1,
        state: "open".into(),
    };
    store
        .open_reply_debt(debt(0xaaaa_aaaa_u128 << 96))
        .await
        .unwrap();
    store
        .open_reply_debt(debt(0xbbbb_bbbb_u128 << 96))
        .await
        .unwrap();
    let message = |sequence: u64, text: &str| -> EventRecord {
        serde_json::from_value(serde_json::json!({
            "sequence": sequence,
            "event_id": format!("event-{sequence}"),
            "kind": "assistant_message",
            "agent_id": "agent-zola",
            "incarnation_id": "incarnation-zola-7",
            "turn_id": "turn-1",
            "observed_at": "2026-10-16T12:00:00Z",
            "text": text,
        }))
        .unwrap()
    };
    let first = Uuid::from_u128(0xaaaa_aaaa_u128 << 96).to_string();

    store
        .ingest_agent_events(
            100,
            102,
            vec![
                // Two replies are owed, so a bare tag names neither.
                message(101, "Done.\n[panetone:reply]"),
                message(
                    102,
                    &format!("All 23 trials passed.\n[panetone:reply {}]", &first[..8]),
                ),
            ],
            live_agents(&route),
            3,
        )
        .await
        .unwrap();

    let open = store.open_reply_debts().await.unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(
        open[0].request_id.to_string(),
        Uuid::from_u128(0xbbbb_bbbb_u128 << 96).to_string()
    );
    let pending = store.pending_reply_messages().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].kind, "final");
    assert_eq!(pending[0].body, "All 23 trials passed.");
    let invalid = store
        .unreported_reply_tags("agent-zola".into())
        .await
        .unwrap();
    assert_eq!(invalid.len(), 1);
    assert!(invalid[0].body.contains("several replies are owed"));
    // Shown without the tag, in the Telegram topic instead of the shared group.
    let posts = store.pending_outbox().await.unwrap();
    assert_eq!(posts.len(), 2);
    for post in &posts {
        assert_eq!(post.kind, ChannelKind::Telegram);
        assert!(!post.body.contains("[panetone:"));
    }
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_turn_a_reminder_started_is_not_followed_by_another_reminder() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let route = route();
    store.save_route(route.clone(), 1).await.unwrap();
    store.initialize_event_cursor(100).await.unwrap();
    let event = |sequence: u64, kind: &str, turn: &str, hash: Option<&str>| -> EventRecord {
        let mut value = serde_json::json!({
            "sequence": sequence,
            "event_id": format!("event-{sequence}"),
            "kind": kind,
            "agent_id": "agent-zola",
            "incarnation_id": "incarnation-zola-7",
            "turn_id": turn,
            "observed_at": "2026-10-16T12:00:00Z",
        });
        if let Some(hash) = hash {
            value["input_sha256"] = hash.into();
        }
        serde_json::from_value(value).unwrap()
    };
    let reminder_hash = panetone::store::DeliveryProof::input_sha256("[Panetone reply reminder]");
    store
        .ingest_agent_events(
            100,
            102,
            vec![
                event(101, "turn_started", "turn-1", Some("other")),
                event(102, "turn_final", "turn-1", None),
            ],
            live_agents(&route),
            3,
        )
        .await
        .unwrap();
    let ended = store
        .last_turn_end("agent-zola".into(), 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ended.turn_id, "turn-1");
    assert!(!ended.started_by_reminder && !ended.reminded && !ended.superseded);

    store
        .record_reply_reminder(
            panetone::domain::EffectId::new(Uuid::from_u128(5)),
            "agent-zola".into(),
            "turn-1".into(),
            reminder_hash.clone(),
            Vec::new(),
            2,
        )
        .await
        .unwrap();
    assert!(
        store
            .last_turn_end("agent-zola".into(), 100)
            .await
            .unwrap()
            .unwrap()
            .reminded
    );
    store
        .ingest_agent_events(
            102,
            104,
            vec![
                event(103, "turn_started", "turn-2", Some(&reminder_hash)),
                event(104, "turn_final", "turn-2", None),
            ],
            live_agents(&route),
            3,
        )
        .await
        .unwrap();
    let ended = store
        .last_turn_end("agent-zola".into(), 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ended.turn_id, "turn-2");
    assert!(ended.started_by_reminder);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn output_observed_long_before_ingest_is_a_replay_and_is_not_posted() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let route = route();
    store.save_route(route.clone(), 1).await.unwrap();
    store.initialize_event_cursor(100).await.unwrap();
    let message = |sequence: u64, observed_at: &str, text: &str| -> EventRecord {
        serde_json::from_value(serde_json::json!({
            "sequence": sequence,
            "event_id": format!("event-{sequence}"),
            "kind": "assistant_message",
            "agent_id": "agent-zola",
            "incarnation_id": "incarnation-zola-7",
            "turn_id": "turn-1",
            "observed_at": observed_at,
            "text": text,
        }))
        .unwrap()
    };
    // 2026-10-16T22:19:30Z
    let now_ms = 1_792_189_170_000;

    let outcome = store
        .ingest_agent_events(
            100,
            102,
            vec![
                message(
                    101,
                    "2026-10-13T20:51:57.889Z",
                    "an answer from three days ago",
                ),
                message(102, "2026-10-16T22:19:20.000Z", "a new answer"),
            ],
            live_agents(&route),
            now_ms,
        )
        .await
        .unwrap();
    assert_eq!(outcome.recorded, 2);
    let posts = store.pending_outbox().await.unwrap();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].body, "a new answer");
    store.shutdown().await.unwrap();
}
