use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use panetone::domain::{AdmissionStatus, AgentBinding, EffectId};
use panetone::wakterm::{EventRead, WaktermCli, WaktermCliError};
use tempfile::tempdir;
use uuid::Uuid;

fn fake_cli(script_body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let directory = tempdir().unwrap();
    let path = directory.path().join("wakterm-fake");
    fs::write(
        &path,
        format!("#!/bin/bash\nset -euo pipefail\n{script_body}\n"),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    (directory, path)
}

fn binding() -> AgentBinding {
    AgentBinding {
        agent_id: "agent-zola".into(),
        incarnation_id: "incarnation-zola-7".into(),
        harness: "codex".into(),
        pane_id: Some(9),
    }
}

#[tokio::test]
async fn real_cli_boundary_negotiates_joins_admits_and_resumes_terminals() {
    let (_directory, binary) = fake_cli(
        r#"
operation="$*"
if [[ "$operation" == *"agent capabilities"* ]]; then
  printf '%s\n' '{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1","codex_output_shadow.experimental.v1"]}'
elif [[ "$operation" == *"agent catalog"* ]]; then
  printf '%s\n' '{"schema":"wakterm.agent-api.v1","agents":[{"agent_id":"agent-zola","incarnation_id":"incarnation-zola-7","pane_id":9,"name":"renamed display","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-17T00:00:00Z"}]}'
elif [[ "$operation" == *"list --format json"* ]]; then
  printf '%s\n' '[{"pane_id":9,"tab_id":4,"window_id":2,"tab_title":"","effective_title":"route title"}]'
elif [[ "$operation" == *"agent events"* ]]; then
  printf '%s\n' '{"schema":"wakterm.agent-events.v1","status":"ok","requested_after_sequence":100,"oldest_available_sequence":90,"latest_sequence":101,"next_after_sequence":101,"events":[{"sequence":101,"event_id":"event-101","kind":"assistant_message","agent_id":"agent-zola","incarnation_id":"incarnation-zola-7","turn_id":"turn-1","observed_at":"2026-08-17T00:00:00Z","text":"done"}]}'
elif [[ "$operation" == *"agent admit"* ]]; then
  prompt=$(cat)
  [[ "$operation" == *"agent-zola --exact-agent-id --incarnation incarnation-zola-7"* ]]
  [[ "$operation" == *"--return-final --final-timeout-ms 9000"* ]]
  [[ "$prompt" == "exact prompt bytes" ]]
  request_id=""
  while (($#)); do
    if [[ "$1" == "--request-id" ]]; then
      request_id=$2
      break
    fi
    shift
  done
  printf '{"schema":"wakterm.agent-api.v1","request_id":"%s","status":"accepted","definitive":true,"prompt_written":true,"agent_id":"agent-zola","incarnation_id":"incarnation-zola-7","return_final":true,"request":null,"detail":null}\n' "$request_id"
elif [[ "$operation" == *"agent request watch"* ]]; then
  [[ "$operation" == *"--after 40 --once"* ]]
  printf '%s\n' '{"request_id":"11111111-1111-4111-8111-111111111111","target_agent_id":"agent-zola","state":"completed","final_message":"done","detail":null,"terminal_event_sequence":41}'
else
  echo "unexpected fake invocation" >&2
  exit 9
fi
"#,
    );
    let socket = tempdir().unwrap().path().join("mux.sock");
    let cli = WaktermCli::new(&binary, &socket, Duration::from_secs(2));

    let capabilities = cli.capabilities().await.unwrap();
    assert!(capabilities.general_event_consumer_enabled());
    let route_observations = AtomicUsize::new(0);
    let resolved = cli
        .resolve_stable_binding(9, || {
            route_observations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(route_observations.load(Ordering::SeqCst), 1);
    assert_eq!(resolved, binding());
    assert_eq!(
        cli.resolve_route_binding("ROUTE TITLE", None)
            .await
            .unwrap(),
        binding()
    );
    assert!(matches!(
        cli.event_page(100, 10).await.unwrap(),
        EventRead::Events {
            next_after_sequence: 101,
            ..
        }
    ));

    let request_id =
        EffectId::new(Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap());
    let receipt = cli
        .admit(request_id, &resolved, "exact prompt bytes", true, 9_000)
        .await
        .unwrap();
    assert_eq!(receipt.status, AdmissionStatus::Accepted);
    assert_eq!(receipt.request_id, request_id);
    assert_eq!(receipt.prompt_written, Some(true));

    let events = cli.terminal_events(40).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].terminal_event_sequence, 41);
    assert_eq!(events[0].final_message.as_deref(), Some("done"));
}

#[tokio::test]
async fn effective_route_keeps_multiple_agent_panes_and_prefers_the_requested_one() {
    let (_directory, binary) = fake_cli(
        r#"
operation="$*"
if [[ "$operation" == *"agent capabilities"* ]]; then
  printf '%s\n' '{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1"]}'
elif [[ "$operation" == *"agent catalog"* ]]; then
  printf '%s\n' '{"schema":"wakterm.agent-api.v1","as_of_event_sequence":12,"agents":[{"agent_id":"agent-first","incarnation_id":"inc-first","pane_id":4,"name":"first","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-17T00:00:00Z"},{"agent_id":"agent-second","incarnation_id":"inc-second","pane_id":9,"name":"second","harness":"claude","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-17T00:00:00Z"}]}'
elif [[ "$operation" == *"list --format json"* ]]; then
  printf '%s\n' '[{"pane_id":9,"tab_id":3,"window_id":1,"effective_title":"panetone"},{"pane_id":4,"tab_id":3,"window_id":1,"effective_title":"panetone"}]'
else
  exit 9
fi
"#,
    );
    let cli = WaktermCli::new(binary, "/tmp/non-production.sock", Duration::from_secs(2));
    let live = cli.live_routes().await.unwrap();
    let route = live.route("PANETONE").unwrap();
    assert_eq!(route.agents.len(), 2);
    assert_eq!(route.select(None).unwrap().pane_id, Some(4));

    let preferred = AgentBinding {
        agent_id: "agent-second".into(),
        incarnation_id: "older-incarnation".into(),
        harness: "claude".into(),
        pane_id: Some(9),
    };
    assert_eq!(
        route.select(Some(&preferred)).unwrap().agent_id,
        "agent-second"
    );
}

#[tokio::test]
async fn real_cli_boundary_preserves_structured_observer_failure() {
    let (_directory, binary) = fake_cli(
        r#"
if [[ "$*" == *"agent capabilities"* ]]; then
  printf '%s\n' '{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}'
  exit 0
fi
cat >/dev/null
request_id=""
while (($#)); do
  if [[ "$1" == "--request-id" ]]; then request_id=$2; break; fi
  shift
done
printf '{"schema":"wakterm.agent-api.v1","request_id":"%s","status":"observer_failure","definitive":true,"prompt_written":false,"agent_id":"agent-zola","incarnation_id":"incarnation-zola-7","return_final":true,"request":null,"detail":"observer cursor is unavailable"}\n' "$request_id"
"#,
    );
    let cli = WaktermCli::new(binary, "/tmp/non-production.sock", Duration::from_secs(2));
    let receipt = cli
        .admit(EffectId::random(), &binding(), "prompt", true, 0)
        .await
        .unwrap();
    assert_eq!(receipt.status, AdmissionStatus::ObserverFailure);
    assert!(receipt.definitive);
    assert_eq!(receipt.prompt_written, Some(false));
}

#[tokio::test]
async fn real_cli_boundary_bounds_time_and_output() {
    let (_directory, sleeping) = fake_cli("while :; do :; done");
    let cli = WaktermCli::new(
        sleeping,
        "/tmp/non-production.sock",
        Duration::from_millis(50),
    );
    assert!(matches!(cli.catalog().await, Err(WaktermCliError::Timeout)));

    let (_directory, noisy) = fake_cli("yes x | head -c 1100000");
    let cli = WaktermCli::new(noisy, "/tmp/non-production.sock", Duration::from_secs(2));
    assert!(matches!(
        cli.catalog().await,
        Err(WaktermCliError::OutputTooLarge(1_048_576))
    ));
}

#[tokio::test]
async fn real_cli_boundary_rejects_incompatible_or_incomplete_capabilities() {
    let (_directory, binary) = fake_cli(
        r#"printf '%s\n' '{"schema":"wakterm.agent-api.v1","api_major":2,"capabilities":[]}'"#,
    );
    let cli = WaktermCli::new(binary, "/tmp/non-production.sock", Duration::from_secs(2));
    let result = cli.capabilities().await;
    assert!(
        matches!(
            result,
            Err(WaktermCliError::IncompatibleApi { major: 2, .. })
        ),
        "unexpected result: {result:?}"
    );

    let (_directory, binary) = fake_cli(
        r#"printf '%s\n' '{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1"]}'"#,
    );
    let cli = WaktermCli::new(binary, "/tmp/non-production.sock", Duration::from_secs(2));
    assert!(matches!(
        cli.capabilities().await,
        Err(WaktermCliError::MissingCapability("prompt_admission.v1"))
    ));
}

#[tokio::test]
async fn configured_development_mux_matches_the_current_contract() {
    let (Ok(binary), Ok(socket)) = (
        std::env::var("PANETONE_TEST_WAKTERM_BIN"),
        std::env::var("PANETONE_TEST_WAKTERM_SOCKET"),
    ) else {
        return;
    };
    assert!(
        socket.contains("panetone-wakterm-dev"),
        "the live adapter test refuses a production-looking mux socket"
    );
    let cli = WaktermCli::new(binary, socket, Duration::from_secs(5));
    assert!(cli.version().await.unwrap().starts_with("wakterm "));
    let capabilities = cli.capabilities().await.unwrap();
    assert!(capabilities.general_event_consumer_enabled());
    let catalog = cli.catalog().await.unwrap();
    assert_eq!(catalog.schema, "wakterm.agent-api.v1");
}
