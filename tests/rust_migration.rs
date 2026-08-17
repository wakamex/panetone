use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::process::Command;

use panetone::domain::{
    AgentBinding, ChannelBinding, PYTHON_CONTROL_HASH_KIND, Route, RouteId, RouteStatus,
    SendCommand, WorkflowId, legacy_python_request_hashes,
};
use panetone::migration::{MigrationError, MigrationOptions, migrate};
use panetone::store::{ClaimResult, StoreHandle};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use tempfile::TempDir;
use uuid::Uuid;

const LEGACY_FIXTURE: &str = "tests/fixtures/legacy-state/representative.json";

struct LegacySources {
    directory: TempDir,
    options: MigrationOptions,
    command: SendCommand,
}

fn setup_sources() -> LegacySources {
    let directory = tempfile::tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let fixture: Value = serde_json::from_slice(&fs::read(LEGACY_FIXTURE).unwrap()).unwrap();
    let state = directory.path().join("state.json");
    let pending = directory.path().join("pending_sends.json");
    fs::write(&state, serde_json::to_vec(&fixture["state_json"]).unwrap()).unwrap();
    fs::write(
        &pending,
        serde_json::to_vec(&fixture["pending_json"]).unwrap(),
    )
    .unwrap();
    fs::set_permissions(&state, fs::Permissions::from_mode(0o600)).unwrap();
    fs::set_permissions(&pending, fs::Permissions::from_mode(0o600)).unwrap();

    let command = SendCommand {
        id: WorkflowId::new(Uuid::parse_str("77777777-7777-4777-8777-777777777771").unwrap()),
        source: "alpha".into(),
        target: "temporarily-absent".into(),
        message: "legacy request one".into(),
        return_final: false,
        timeout_ms: 0,
    };
    let control = directory.path().join("control-journal.sqlite3");
    create_control_journal(&control, &command);
    let signal = directory.path().join("signal.sqlite3");
    create_signal_database(&signal, &fixture["signal_rows"]);
    let output = directory.path().join("migration-bundle");
    let options = MigrationOptions {
        state,
        pending,
        control_journal: control,
        signal_database: Some(signal),
        legacy_control_socket: directory.path().join("control.sock"),
        output,
    };
    LegacySources {
        directory,
        options,
        command,
    }
}

fn create_control_journal(path: &Path, first: &SendCommand) {
    let connection = Connection::open(path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE control_request (
                 request_id TEXT PRIMARY KEY,
                 request_hash TEXT NOT NULL,
                 source TEXT NOT NULL,
                 target TEXT NOT NULL,
                 state TEXT NOT NULL,
                 progress_json TEXT,
                 response_json TEXT,
                 created_at REAL NOT NULL,
                 updated_at REAL NOT NULL
             );
             CREATE TABLE return_delivery (
                 request_id TEXT PRIMARY KEY REFERENCES control_request(request_id),
                 source_json TEXT NOT NULL,
                 target_json TEXT NOT NULL,
                 state TEXT NOT NULL,
                 result_json TEXT,
                 agent_state TEXT NOT NULL,
                 telegram_state TEXT NOT NULL,
                 last_error TEXT,
                 created_at REAL NOT NULL,
                 updated_at REAL NOT NULL
             );
             CREATE TABLE control_meta (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );",
        )
        .unwrap();
    let requests = [
        (
            first.clone(),
            "in_progress",
            Some(r#"{"stage":"delivering"}"#),
            None,
        ),
        (
            command(0x72, "legacy request two"),
            "succeeded",
            None,
            Some(r#"{"schema":"panetone.control.v1","ok":true}"#),
        ),
        (
            command(0x73, "legacy request three"),
            "failed",
            None,
            Some(r#"{"schema":"panetone.control.v1","ok":false}"#),
        ),
        (
            command(0x74, "legacy request four"),
            "indeterminate",
            Some(r#"{"stage":"delivering"}"#),
            Some(r#"{"schema":"panetone.control.v1","ok":false}"#),
        ),
    ];
    for (index, (command, state, progress, response)) in requests.iter().enumerate() {
        connection
            .execute(
                "INSERT INTO control_request(
                     request_id, request_hash, source, target, state, progress_json,
                     response_json, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    command.id.to_string(),
                    legacy_python_request_hashes(command)[0],
                    command.source,
                    command.target,
                    state,
                    progress,
                    response,
                    1_786_932_000.0 + index as f64,
                    1_786_932_100.0 + index as f64,
                ],
            )
            .unwrap();
    }
    let returned = command(0x72, "legacy request two");
    connection
        .execute(
            "INSERT INTO return_delivery(
                 request_id, source_json, target_json, state, result_json,
                 agent_state, telegram_state, last_error, created_at, updated_at
             ) VALUES (?1, ?2, ?3, 'terminal', ?4, 'delivered', 'delivering', ?5, ?6, ?7)",
            params![
                returned.id.to_string(),
                r#"{"title":"alpha","harness":"codex","pane_id":11}"#,
                r#"{"title":"temporarily-absent","harness":"codex","pane_id":12}"#,
                r#"{"request_id":"77777777-7777-4777-8777-777777777772","state":"completed","final_message":"done"}"#,
                "legacy error with unknown destination attribution",
                1_786_932_200.0,
                1_786_932_201.0,
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO control_meta(key, value) VALUES ('wakterm_event_cursor', '41')",
            [],
        )
        .unwrap();
}

fn command(suffix: u128, message: &str) -> SendCommand {
    let text = format!("77777777-7777-4777-8777-7777777777{suffix:02x}");
    SendCommand {
        id: WorkflowId::new(Uuid::parse_str(&text).unwrap()),
        source: "alpha".into(),
        target: "temporarily-absent".into(),
        message: message.into(),
        return_final: false,
        timeout_ms: 0,
    }
}

fn create_signal_database(path: &Path, rows: &Value) {
    let connection = Connection::open(path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE signal_messages (
                 id INTEGER PRIMARY KEY,
                 group_id TEXT NOT NULL,
                 envelope_timestamp INTEGER,
                 received_at INTEGER NOT NULL,
                 sender_id TEXT NOT NULL,
                 sender_number TEXT NOT NULL,
                 sender_name TEXT NOT NULL,
                 text TEXT NOT NULL,
                 formatted_text TEXT NOT NULL,
                 data_json TEXT NOT NULL,
                 direction TEXT NOT NULL DEFAULT 'incoming',
                 accepted INTEGER NOT NULL,
                 is_command INTEGER NOT NULL,
                 is_mention INTEGER NOT NULL,
                 delivered_at INTEGER,
                 UNIQUE (group_id, sender_id, envelope_timestamp)
             );",
        )
        .unwrap();
    for row in rows.as_array().unwrap() {
        connection
            .execute(
                "INSERT INTO signal_messages(
                     id, group_id, envelope_timestamp, received_at, sender_id,
                     sender_number, sender_name, text, formatted_text, data_json,
                     direction, accepted, is_command, is_mention, delivered_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                params![
                    row["id"].as_i64(),
                    row["group_id"].as_str(),
                    row["envelope_timestamp"].as_i64(),
                    row["received_at"].as_i64(),
                    row["sender_id"].as_str(),
                    row["sender_number"].as_str(),
                    row["sender_name"].as_str(),
                    row["text"].as_str(),
                    row["formatted_text"].as_str(),
                    row["data_json"].as_str(),
                    row["direction"].as_str(),
                    row["accepted"].as_i64(),
                    row["is_command"].as_i64(),
                    row["is_mention"].as_i64(),
                    row["delivered_at"].as_i64(),
                ],
            )
            .unwrap();
    }
}

fn binding(name: &str) -> AgentBinding {
    AgentBinding {
        agent_id: format!("agent-{name}"),
        incarnation_id: format!("incarnation-{name}"),
        harness: "codex".into(),
        pane_id: None,
    }
}

fn route_id(value: u128) -> RouteId {
    RouteId::new(Uuid::from_u128(value))
}

#[tokio::test]
async fn migration_is_copy_only_idempotent_and_preserves_conservative_state() {
    let sources = setup_sources();
    let original_state = fs::read(&sources.options.state).unwrap();
    let original_pending = fs::read(&sources.options.pending).unwrap();
    let first = migrate(&sources.options).unwrap();
    assert!(!first.reused);
    assert_eq!(first.manifest.target_schema_version, 5);
    assert_eq!(first.manifest.counts["routes"], 3);
    assert_eq!(first.manifest.counts["pending_outbox"], 2);
    assert_eq!(first.manifest.counts["legacy_debate_outbox_held"], 1);
    assert_eq!(first.manifest.counts["signal_messages"], 4);
    assert_eq!(first.manifest.counts["pending_signal_inbox"], 2);
    assert_eq!(first.manifest.counts["legacy_control_requests"], 4);
    assert_eq!(first.manifest.counts["legacy_indeterminate_requests"], 2);
    assert_eq!(first.manifest.counts["legacy_unresolved_returns"], 1);
    assert!(
        first
            .manifest
            .warnings
            .iter()
            .any(|warning| warning.contains("collaboration tab IDs"))
    );
    assert!(
        first
            .manifest
            .warnings
            .iter()
            .any(|warning| warning.contains("Slack preference")
                && warning.contains("not deliverable"))
    );
    assert_eq!(fs::read(&sources.options.state).unwrap(), original_state);
    assert_eq!(
        fs::read(&sources.options.pending).unwrap(),
        original_pending
    );
    assert_eq!(
        fs::read(sources.options.output.join("legacy/state.json")).unwrap(),
        original_state
    );
    assert_eq!(
        fs::metadata(&sources.options.output)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    for relative in [
        "panetone.sqlite3",
        "migration-manifest.json",
        "legacy/state.json",
        "legacy/pending_sends.json",
        "legacy/control-journal.sqlite3",
        "legacy/signal.sqlite3",
    ] {
        assert_eq!(
            fs::metadata(sources.options.output.join(relative))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600,
            "unexpected mode for {relative}"
        );
    }

    let database = sources.options.output.join("panetone.sqlite3");
    let connection = Connection::open(&database).unwrap();
    let hash_kind: String = connection
        .query_row(
            "SELECT hash_kind FROM idempotency_tombstones WHERE request_id = ?1",
            params![sources.command.id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(hash_kind, PYTHON_CONTROL_HASH_KIND);
    let migrated_return: (String, String) = connection
        .query_row(
            "SELECT agent_state, mirror_state FROM legacy_return_deliveries",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        migrated_return,
        ("delivered".into(), "indeterminate".into())
    );
    let held_debate: (String, String) = connection
        .query_row(
            "SELECT destination, resolution_state FROM legacy_debate_outbox",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(held_debate, ("-100123".into(), "held".into()));
    let routes = connection
        .prepare("SELECT route_json FROM routes ORDER BY route_id")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .map(|value| serde_json::from_str::<Route>(&value).unwrap())
        .collect::<Vec<_>>();
    let alpha = routes.iter().find(|route| route.title == "alpha").unwrap();
    assert_eq!(alpha.status, RouteStatus::Unavailable);
    assert!(alpha.agent.is_none());
    assert!(
        alpha
            .channels
            .contains(&ChannelBinding::Telegram { topic_id: 101 })
    );
    assert!(alpha.channels.contains(&ChannelBinding::Signal {
        group_id: "signal-alpha".into()
    }));
    assert!(
        connection
            .query_row(
                "SELECT value FROM metadata WHERE key = 'wakterm_event_cursor'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .unwrap()
            .is_none()
    );
    drop(connection);

    let store = StoreHandle::open(&database).unwrap();
    let same = store
        .claim(
            sources.command.clone(),
            route_id(1),
            route_id(2),
            binding("source"),
            binding("target"),
            200,
        )
        .await
        .unwrap();
    assert!(matches!(
        same,
        ClaimResult::Tombstone {
            same_content: true,
            state
        } if state == "indeterminate"
    ));
    let mut different = sources.command.clone();
    different.message.push_str(" changed");
    assert!(matches!(
        store
            .claim(
                different,
                route_id(1),
                route_id(2),
                binding("source"),
                binding("target"),
                201,
            )
            .await
            .unwrap(),
        ClaimResult::Tombstone {
            same_content: false,
            ..
        }
    ));
    let status = store.status().await.unwrap();
    assert_eq!(status.legacy_control_requests, 4);
    assert_eq!(status.legacy_indeterminate_requests, 2);
    assert_eq!(status.legacy_unresolved_returns, 1);
    assert_eq!(status.pending_outbox, 2);
    assert_eq!(status.legacy_debate_outbox, 1);
    assert_eq!(status.pending_inbox, 2);
    store.shutdown().await.unwrap();

    let second = migrate(&sources.options).unwrap();
    assert!(second.reused);
    assert_eq!(second.manifest, first.manifest);
}

#[test]
fn migration_rejects_pending_slack_delivery_without_replaying_or_discarding_it() {
    let sources = setup_sources();
    let mut pending: Value =
        serde_json::from_slice(&fs::read(&sources.options.pending).unwrap()).unwrap();
    pending["items"].as_array_mut().unwrap().push(json!({
        "id": "removed-slack-delivery",
        "kind": "slack",
        "target": "C012345",
        "chunk": "must not be replayed",
        "pane_id": 14,
        "harness": "opencode",
        "route_title": ""
    }));
    fs::write(
        &sources.options.pending,
        serde_json::to_vec(&pending).unwrap(),
    )
    .unwrap();
    fs::set_permissions(&sources.options.pending, fs::Permissions::from_mode(0o600)).unwrap();

    let error = migrate(&sources.options).unwrap_err();
    assert!(matches!(
        error,
        MigrationError::RemovedSlackPending(ref id) if id == "removed-slack-delivery"
    ));
    assert!(!sources.options.output.exists());
    let saved: Value =
        serde_json::from_slice(&fs::read(&sources.options.pending).unwrap()).unwrap();
    assert_eq!(saved["items"].as_array().unwrap().len(), 4);
}

#[test]
fn repeat_migrations_match_and_snapshot_restores_to_python_inputs() {
    let sources = setup_sources();
    let first = migrate(&sources.options).unwrap();
    let mut other = sources.options.clone();
    other.output = sources.directory.path().join("migration-bundle-two");
    let second = migrate(&other).unwrap();
    assert_eq!(first.manifest, second.manifest);

    let restore = sources.directory.path().join("python-restore");
    fs::create_dir(&restore).unwrap();
    fs::set_permissions(&restore, fs::Permissions::from_mode(0o700)).unwrap();
    for name in [
        "state.json",
        "pending_sends.json",
        "control-journal.sqlite3",
        "signal.sqlite3",
    ] {
        fs::copy(
            sources.options.output.join("legacy").join(name),
            restore.join(name),
        )
        .unwrap();
    }
    assert_eq!(
        fs::read(restore.join("state.json")).unwrap(),
        fs::read(&sources.options.state).unwrap()
    );
    assert_eq!(
        fs::read(restore.join("pending_sends.json")).unwrap(),
        fs::read(&sources.options.pending).unwrap()
    );
    let script = r#"
import json, sqlite3, sys
from pathlib import Path
from panetone_control import ControlJournal
root = Path(sys.argv[1])
state = json.loads((root / 'state.json').read_text())
pending = json.loads((root / 'pending_sends.json').read_text())
journal = ControlJournal(root / 'control-journal.sqlite3')
row = journal.get('77777777-7777-4777-8777-777777777771')
with sqlite3.connect(root / 'signal.sqlite3') as db:
    signal_count = db.execute('select count(*) from signal_messages').fetchone()[0]
assert state['telegram_topics']['alpha'] == 101
assert pending['schema'] == 'panetone.delivery-state.v2'
assert row['state'] == 'in_progress'
assert signal_count == 3
"#;
    let output = Command::new("python3")
        .args(["-c", script, restore.to_str().unwrap()])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Python restore check failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn live_service_changed_source_and_malformed_state_fail_closed() {
    let mut sources = setup_sources();
    let listener = UnixListener::bind(&sources.options.legacy_control_socket).unwrap();
    assert!(matches!(
        migrate(&sources.options),
        Err(MigrationError::LegacyServiceRunning(_))
    ));
    assert!(!sources.options.output.exists());
    drop(listener);
    fs::remove_file(&sources.options.legacy_control_socket).unwrap();

    migrate(&sources.options).unwrap();
    let mut state: Value =
        serde_json::from_slice(&fs::read(&sources.options.state).unwrap()).unwrap();
    state["telegram_topics"]["ALPHA"] = json!(999);
    fs::write(&sources.options.state, serde_json::to_vec(&state).unwrap()).unwrap();
    assert!(matches!(
        migrate(&sources.options),
        Err(MigrationError::SourceChanged(_))
    ));

    sources.options.output = sources.directory.path().join("malformed-output");
    assert!(matches!(
        migrate(&sources.options),
        Err(MigrationError::Malformed(_))
    ));
    assert!(!sources.options.output.exists());
}

#[test]
fn migration_cli_returns_a_structured_reusable_bundle_acknowledgement() {
    let sources = setup_sources();
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_panetone"))
            .args([
                "migrate",
                "--state",
                sources.options.state.to_str().unwrap(),
                "--pending",
                sources.options.pending.to_str().unwrap(),
                "--control-journal",
                sources.options.control_journal.to_str().unwrap(),
                "--signal-database",
                sources
                    .options
                    .signal_database
                    .as_ref()
                    .unwrap()
                    .to_str()
                    .unwrap(),
                "--legacy-control-socket",
                sources.options.legacy_control_socket.to_str().unwrap(),
                "--output",
                sources.options.output.to_str().unwrap(),
            ])
            .output()
            .unwrap()
    };
    let first = run();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first: Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(first["ok"], true);
    assert_eq!(first["reused"], false);
    assert_eq!(first["manifest"]["counts"]["pending_outbox"], 2);
    assert_eq!(first["manifest"]["counts"]["legacy_debate_outbox_held"], 1);
    let second = run();
    assert!(second.status.success());
    let second: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(second["reused"], true);
}

#[test]
fn legacy_hash_compatibility_matches_every_python_default_shape() {
    let command = SendCommand {
        id: WorkflowId::new(Uuid::nil()),
        source: "café".into(),
        target: "target".into(),
        message: "legacy hash ✓".into(),
        return_final: false,
        timeout_ms: 0,
    };
    let variants = [
        json!({"from":"café","to":"target","message":"legacy hash ✓"}),
        json!({"from":"café","to":"target","message":"legacy hash ✓","return_final":false}),
        json!({"from":"café","to":"target","message":"legacy hash ✓","timeout_ms":0}),
        json!({"from":"café","to":"target","message":"legacy hash ✓","return_final":false,"timeout_ms":0}),
    ];
    let expected = legacy_python_request_hashes(&command);
    assert_eq!(expected.len(), variants.len());
    let script = r#"
import sys
from panetone_control import parse_request, request_hash
print(request_hash(parse_request(sys.argv[1].encode())))
"#;
    for params in variants {
        let request = json!({
            "schema": "panetone.control.v1",
            "id": Uuid::nil().to_string(),
            "method": "send",
            "params": params,
        });
        let output = Command::new("python3")
            .args(["-c", script, &serde_json::to_string(&request).unwrap()])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .unwrap();
        assert!(output.status.success());
        let python_hash = String::from_utf8(output.stdout).unwrap();
        assert!(expected.iter().any(|hash| hash == python_hash.trim()));
    }
}
