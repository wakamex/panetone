use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use panetone::channels::{RealChannels, TelegramClient};
use panetone::domain::{
    AgentBinding, ChannelBinding, Route, RouteId, RouteStatus, SendCommand, WorkflowId,
    WorkflowState,
};
use panetone::service::OfflineService;
use panetone::service::ServiceError;
use panetone::store::StoreHandle;
use panetone::wakterm::WaktermCli;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use uuid::Uuid;

fn route(id: u128, title: &str, agent: AgentBinding, topic_id: i64) -> Route {
    Route {
        id: RouteId::new(Uuid::from_u128(id)),
        title: title.into(),
        channels: vec![ChannelBinding::Telegram { topic_id }],
        agent: Some(agent),
        status: RouteStatus::Available,
    }
}

async fn recording_telegram(
    order_log: std::path::PathBuf,
) -> (String, tokio::task::JoinHandle<Vec<serde_json::Value>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut captured = Vec::new();
        for message_id in [701, 702] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            let header_end = loop {
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0);
                received.extend_from_slice(&chunk[..read]);
                if let Some(position) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    break position + 4;
                }
            };
            let header = String::from_utf8(received[..header_end].to_vec()).unwrap();
            let content_length = header
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap();
            while received.len() - header_end < content_length {
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0);
                received.extend_from_slice(&chunk[..read]);
            }
            let body =
                serde_json::from_slice(&received[header_end..header_end + content_length]).unwrap();
            captured.push(body);
            let mut order = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&order_log)
                .unwrap();
            std::io::Write::write_all(&mut order, b"telegram\n").unwrap();
            let body = format!("{{\"ok\":true,\"result\":{{\"message_id\":{message_id}}}}}");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
        captured
    });
    (format!("http://{address}"), server)
}

#[tokio::test]
async fn real_adapters_execute_the_durable_workflow_once_in_audit_first_order() {
    let directory = tempdir().unwrap();
    let order_log = directory.path().join("order.log");
    let script = directory.path().join("wakterm-fake");
    fs::write(
        &script,
        format!(
            r#"#!/bin/bash
set -euo pipefail
operation="$*"
if [[ "$operation" == *"agent capabilities"* ]]; then
  printf '%s\n' '{{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}}'
elif [[ "$operation" == *"agent catalog"* ]]; then
  printf '%s\n' '{{"schema":"wakterm.agent-api.v1","agents":[{{"agent_id":"agent-target","incarnation_id":"target-incarnation-1","pane_id":22,"name":"display-name-does-not-match-route","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-17T00:00:00Z"}}]}}'
elif [[ "$operation" == *"agent admit"* ]]; then
  prompt=$(cat)
  [[ "$prompt" == *"From: Source (codex)"* ]]
  [[ "$prompt" == *"To: Target (codex)"* ]]
  [[ "$operation" == *"agent-target --exact-agent-id --incarnation target-incarnation-1"* ]]
  request_id=""
  while (($#)); do
    if [[ "$1" == "--request-id" ]]; then request_id=$2; break; fi
    shift
  done
  printf 'wakterm\n' >> '{}'
  printf '{{"schema":"wakterm.agent-api.v1","request_id":"%s","status":"accepted","definitive":true,"prompt_written":true,"agent_id":"agent-target","incarnation_id":"target-incarnation-1","return_final":false,"request":null,"detail":null}}\n' "$request_id"
else
  exit 9
fi
"#,
            order_log.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
    let wakterm = WaktermCli::new(
        &script,
        directory.path().join("dev-mux.sock"),
        Duration::from_secs(2),
    );
    let target_binding = wakterm.resolve_stable_binding(22, || Ok(())).await.unwrap();
    let source_binding = AgentBinding {
        agent_id: "agent-source".into(),
        incarnation_id: "source-incarnation-1".into(),
        harness: "codex".into(),
        pane_id: Some(11),
    };
    let source = route(1, "Source", source_binding, 101);
    let target = route(2, "Target", target_binding, 202);

    let (telegram_base, telegram_server) = recording_telegram(order_log.clone()).await;
    let channels = RealChannels {
        telegram: Some(
            TelegramClient::new(telegram_base, "fake-token", -1001, Duration::from_secs(2))
                .unwrap(),
        ),
        ..RealChannels::default()
    };
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let service = OfflineService::new_real(store.clone(), wakterm, channels);
    let command = SendCommand {
        id: WorkflowId::new(Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap()),
        source: "Source".into(),
        target: "Target".into(),
        message: "perform the review".into(),
        return_final: false,
        timeout_ms: 0,
    };
    let first = service
        .submit(command.clone(), &source, &target, 100)
        .await
        .unwrap();
    assert!(first.submitted);
    let replay = service
        .submit(command.clone(), &source, &target, 101)
        .await
        .unwrap();
    assert_eq!(replay, first);

    let telegram = telegram_server.await.unwrap();
    assert_eq!(telegram.len(), 2);
    assert!(
        telegram[0]["text"]
            .as_str()
            .unwrap()
            .starts_with("[pending]")
    );
    assert!(
        telegram[1]["text"]
            .as_str()
            .unwrap()
            .starts_with("[submitted]")
    );
    assert_eq!(
        fs::read_to_string(&order_log).unwrap(),
        "telegram\nwakterm\ntelegram\n"
    );
    let workflow = store.get_workflow(command.id).await.unwrap().unwrap();
    assert_eq!(workflow.workflow.state, WorkflowState::Completed);
    assert_eq!(store.status().await.unwrap().failed_outbox, 0);
    drop(service);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn lost_wakterm_response_becomes_durable_and_visibly_indeterminate() {
    let directory = tempdir().unwrap();
    let order_log = directory.path().join("order.log");
    let script = directory.path().join("wakterm-timeout");
    fs::write(
        &script,
        r#"#!/bin/bash
set -euo pipefail
if [[ "$*" == *"agent capabilities"* ]]; then
  printf '%s\n' '{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}'
elif [[ "$*" == *"agent catalog"* ]]; then
  printf '%s\n' '{"schema":"wakterm.agent-api.v1","agents":[{"agent_id":"agent-target","incarnation_id":"target-incarnation-1","pane_id":22,"name":"target","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-17T00:00:00Z"}]}'
else
  cat >/dev/null
  while :; do :; done
fi
"#,
    )
    .unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
    let wakterm = WaktermCli::new(
        &script,
        directory.path().join("dev-mux.sock"),
        Duration::from_millis(30),
    );
    let target_binding = wakterm.resolve_stable_binding(22, || Ok(())).await.unwrap();
    let source = route(
        1,
        "Source",
        AgentBinding {
            agent_id: "agent-source".into(),
            incarnation_id: "source-incarnation-1".into(),
            harness: "codex".into(),
            pane_id: Some(11),
        },
        101,
    );
    let target = route(2, "Target", target_binding, 202);
    let (telegram_base, telegram_server) = recording_telegram(order_log).await;
    let channels = RealChannels {
        telegram: Some(
            TelegramClient::new(telegram_base, "fake-token", -1001, Duration::from_secs(2))
                .unwrap(),
        ),
        ..RealChannels::default()
    };
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let service = OfflineService::new_real(store.clone(), wakterm, channels);
    let command = SendCommand {
        id: WorkflowId::new(Uuid::from_u128(99)),
        source: "Source".into(),
        target: "Target".into(),
        message: "perform the review".into(),
        return_final: false,
        timeout_ms: 0,
    };
    assert!(matches!(
        service.submit(command.clone(), &source, &target, 100).await,
        Err(ServiceError::Adapter(_))
    ));
    let telegram = telegram_server.await.unwrap();
    assert_eq!(telegram.len(), 2);
    assert!(
        telegram[1]["text"]
            .as_str()
            .unwrap()
            .contains("DELIVERY INDETERMINATE")
    );
    assert_eq!(
        store
            .get_workflow(command.id)
            .await
            .unwrap()
            .unwrap()
            .workflow
            .state,
        WorkflowState::Indeterminate
    );
    drop(service);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejected_audit_is_durable_visible_and_prevents_prompt_submission() {
    let directory = tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let admission_log = directory.path().join("admission.log");
    let script = directory.path().join("wakterm-fake");
    fs::write(
        &script,
        format!(
            r#"#!/bin/bash
set -euo pipefail
if [[ "$*" == *"agent capabilities"* ]]; then
  printf '%s\n' '{{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}}'
elif [[ "$*" == *"agent catalog"* ]]; then
  printf '%s\n' '{{"schema":"wakterm.agent-api.v1","agents":[{{"agent_id":"agent-target","incarnation_id":"target-incarnation-1","pane_id":22,"name":"target","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-17T00:00:00Z"}}]}}'
else
  printf 'unexpected admission\n' >> '{}'
  exit 9
fi
"#,
            admission_log.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
    let wakterm = WaktermCli::new(
        &script,
        directory.path().join("dev-mux.sock"),
        Duration::from_secs(2),
    );
    let target_binding = wakterm.resolve_stable_binding(22, || Ok(())).await.unwrap();
    let source = route(
        1,
        "Source",
        AgentBinding {
            agent_id: "agent-source".into(),
            incarnation_id: "source-incarnation-1".into(),
            harness: "codex".into(),
            pane_id: Some(11),
        },
        101,
    );
    let target = route(2, "Target", target_binding, 202);

    let response = b"HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: 83\r\n\r\n{\"ok\":false,\"error_code\":429,\"description\":\"retry\",\"parameters\":{\"retry_after\":17}}";
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let telegram_server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).await.unwrap();
        stream.write_all(response).await.unwrap();
    });
    let channels = RealChannels {
        telegram: Some(
            TelegramClient::new(
                format!("http://{address}"),
                "fake-token",
                -1001,
                Duration::from_secs(2),
            )
            .unwrap(),
        ),
        ..RealChannels::default()
    };
    let journal = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&journal).unwrap();
    let service = OfflineService::new_real(store.clone(), wakterm, channels);
    let command = SendCommand {
        id: WorkflowId::new(Uuid::from_u128(100)),
        source: "Source".into(),
        target: "Target".into(),
        message: "must remain audit-first".into(),
        return_final: false,
        timeout_ms: 0,
    };
    assert!(matches!(
        service.submit(command.clone(), &source, &target, 100).await,
        Err(ServiceError::AuditFailed(_))
    ));
    telegram_server.await.unwrap();
    assert!(!admission_log.exists());
    let status = store.status().await.unwrap();
    assert_eq!(status.failed_workflows, 1);
    assert_eq!(status.failed_outbox, 1);
    assert_eq!(
        store
            .get_workflow(command.id)
            .await
            .unwrap()
            .unwrap()
            .workflow
            .state,
        WorkflowState::Failed
    );
    drop(service);
    store.shutdown().await.unwrap();

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_panetone"))
        .args([
            "doctor",
            "--socket",
            directory.path().join("control.sock").to_str().unwrap(),
            "--journal",
            journal.to_str().unwrap(),
            "--wakterm-fixture",
            "/code/wakterm/docs/agent-api/v1/golden-fixtures.json",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["checks"]["store"]["ok"], false);
    assert_eq!(report["checks"]["store"]["status"]["failed_workflows"], 1);
    assert_eq!(report["checks"]["store"]["status"]["failed_outbox"], 1);
}
