use std::sync::Arc;

use panetone::channels::RecordingChannels;
use panetone::domain::{
    AdmissionStatus, AgentBinding, ChannelBinding, ChannelKind, DeliveryState, Route, RouteId,
    RouteStatus, SendCommand, WorkflowId, WorkflowState,
};
use panetone::service::{FaultInjector, FaultPoint, OfflineService, ServiceError};
use panetone::store::StoreHandle;
use panetone::wakterm::EventRead;
use panetone::wakterm::{FakeWakterm, ProfileKind, TerminalResult, WaktermContract};
use tempfile::tempdir;
use uuid::Uuid;

fn contract() -> WaktermContract {
    WaktermContract::from_golden_json(
        &std::fs::read_to_string("/code/wakterm/docs/agent-api/v1/golden-fixtures.json").unwrap(),
        ProfileKind::Current,
    )
    .unwrap()
}

fn binding(name: &str, incarnation: &str, pane_id: u64) -> AgentBinding {
    AgentBinding {
        agent_id: format!("agent-{name}"),
        incarnation_id: incarnation.into(),
        harness: "codex".into(),
        pane_id: Some(pane_id),
    }
}

fn route(value: u128, title: &str, binding: AgentBinding, channel: ChannelBinding) -> Route {
    Route {
        id: RouteId::new(Uuid::from_u128(value)),
        title: title.into(),
        channels: vec![channel],
        agent: Some(binding),
        status: RouteStatus::Available,
    }
}

fn command(value: u128, return_final: bool) -> SendCommand {
    SendCommand {
        id: WorkflowId::new(Uuid::from_u128(value)),
        source: "source".into(),
        target: "target".into(),
        message: "do the work".into(),
        return_final,
        timeout_ms: 0,
    }
}

fn routes() -> (Route, Route) {
    (
        route(
            1,
            "source",
            binding("source", "source-incarnation-1", 1),
            ChannelBinding::Slack {
                channel_id: "source-channel".into(),
            },
        ),
        route(
            2,
            "target",
            binding("target", "target-incarnation-1", 2),
            ChannelBinding::Telegram { topic_id: 200 },
        ),
    )
}

#[tokio::test]
async fn audit_is_visible_before_exact_target_admission() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let faults = Arc::new(FaultInjector::default());
    let service = OfflineService::with_faults(
        store.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
        faults.clone(),
    );
    let (source, target) = routes();
    let ack = service
        .submit(command(10, false), &source, &target, 100)
        .await
        .unwrap();
    assert!(ack.submitted);
    assert_eq!(service.channels().calls().len(), 2);
    assert!(service.channels().calls()[0].body.starts_with("[pending]"));
    assert_eq!(service.wakterm().calls().len(), 1);
    assert_eq!(
        service.wakterm().calls()[0].binding.incarnation_id,
        "target-incarnation-1"
    );
    let hits = faults.hits();
    assert!(
        hits.iter()
            .position(|hit| *hit == FaultPoint::AfterAuditEffect)
            < hits
                .iter()
                .position(|hit| *hit == FaultPoint::AfterPromptEffect)
    );
    drop(service);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn audit_failure_fails_closed_without_a_prompt_effect() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let channels = RecordingChannels::default();
    channels.fail(ChannelKind::Telegram);
    let service = OfflineService::new(store.clone(), FakeWakterm::new(contract()), channels);
    let (source, target) = routes();
    let request = command(11, false);
    assert!(matches!(
        service.submit(request.clone(), &source, &target, 100).await,
        Err(ServiceError::AuditFailed(_))
    ));
    assert!(service.wakterm().calls().is_empty());
    assert_eq!(
        store
            .get_workflow(request.id)
            .await
            .unwrap()
            .unwrap()
            .workflow
            .state,
        WorkflowState::Failed
    );
    drop(service);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn definitive_target_failure_adds_a_linked_visible_failure_annotation() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let wakterm = FakeWakterm::new(contract());
    wakterm.script_receipts([AdmissionStatus::Unavailable]);
    let service = OfflineService::new(store.clone(), wakterm, RecordingChannels::default());
    let (source, target) = routes();
    let request = command(111, false);
    assert!(matches!(
        service.submit(request, &source, &target, 100).await,
        Err(ServiceError::AdmissionFailed(WorkflowState::Failed))
    ));
    let calls = service.channels().calls();
    assert_eq!(calls.len(), 2);
    assert!(calls[0].body.starts_with("[pending]"));
    assert!(calls[1].body.contains("DELIVERY FAILED"));
    drop(service);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn busy_target_queues_durably_and_re_resolves_the_same_route_before_retry() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let wakterm = FakeWakterm::new(contract());
    wakterm.script_receipts([AdmissionStatus::Busy]);
    let service = OfflineService::new(store.clone(), wakterm, RecordingChannels::default());
    let (source, mut target) = routes();
    let request = command(12, false);
    let queued = service
        .submit(request.clone(), &source, &target, 100)
        .await
        .unwrap();
    assert_eq!(queued.delivery_state, "queued");
    assert!(!queued.submitted);
    assert!(service.wakterm().calls().is_empty());
    drop(service);
    store.shutdown().await.unwrap();

    let reopened = StoreHandle::open(&path).unwrap();
    target.agent = Some(binding("replacement", "target-incarnation-2", 88));
    let service = OfflineService::new(
        reopened.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
    );
    let submitted = service
        .retry_busy_target(request.id, &target, 200)
        .await
        .unwrap();
    assert!(submitted.submitted);
    let calls = service.wakterm().calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].request_id.as_uuid(), request.id.as_uuid());
    assert_eq!(calls[0].binding.agent_id, "agent-replacement");
    assert_eq!(
        reopened
            .get_workflow(request.id)
            .await
            .unwrap()
            .unwrap()
            .workflow
            .submitted_target
            .unwrap()
            .incarnation_id,
        "target-incarnation-2"
    );
    drop(service);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn callback_busy_is_queued_with_one_visible_mirror_and_the_same_callback_id() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let wakterm = FakeWakterm::new(contract());
    wakterm.script_receipts([
        AdmissionStatus::Accepted,
        AdmissionStatus::Busy,
        AdmissionStatus::Accepted,
    ]);
    let service = OfflineService::new(store.clone(), wakterm, RecordingChannels::default());
    let (source, target) = routes();
    let request = command(13, true);
    service
        .submit(request.clone(), &source, &target, 100)
        .await
        .unwrap();
    let terminal = TerminalResult {
        workflow_id: request.id,
        source: source.agent.clone().unwrap(),
        target: target.agent.clone().unwrap(),
        status: "completed".into(),
        message: "finished safely".into(),
    };
    let returned = service
        .accept_terminal(terminal, &source, 200)
        .await
        .unwrap();
    assert_eq!(returned.agent.state, DeliveryState::Pending);
    assert_eq!(returned.mirror.state, DeliveryState::Delivered);
    assert_eq!(service.channels().calls().len(), 3);
    assert!(
        service.channels().calls()[2]
            .body
            .contains("[Panetone asynchronous final return]")
    );
    let callback_id = returned.agent.effect_id;
    let delivered = service.retry_pending_return(request.id, 300).await.unwrap();
    assert_eq!(delivered.agent.state, DeliveryState::Delivered);
    assert_eq!(delivered.agent.effect_id, callback_id);
    assert_eq!(service.channels().calls().len(), 3);
    assert_eq!(service.wakterm().calls().len(), 2);
    assert_eq!(service.wakterm().calls()[1].request_id, callback_id);
    drop(service);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_source_agent_keeps_the_result_visible_and_durable() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let wakterm = FakeWakterm::new(contract());
    wakterm.script_receipts([AdmissionStatus::Accepted, AdmissionStatus::Unavailable]);
    let service = OfflineService::new(store.clone(), wakterm, RecordingChannels::default());
    let (mut source, target) = routes();
    let request = command(14, true);
    service
        .submit(request.clone(), &source, &target, 100)
        .await
        .unwrap();
    let terminal = TerminalResult {
        workflow_id: request.id,
        source: source.agent.clone().unwrap(),
        target: target.agent.clone().unwrap(),
        status: "completed".into(),
        message: "source has gone away".into(),
    };
    source.status = RouteStatus::Unavailable;
    source.agent = None;
    let returned = service
        .accept_terminal(terminal, &source, 200)
        .await
        .unwrap();
    assert_eq!(returned.agent.state, DeliveryState::Failed);
    assert_eq!(returned.mirror.state, DeliveryState::Delivered);
    assert_eq!(store.status().await.unwrap().unresolved_returns, 1);
    assert!(
        service.channels().calls()[2]
            .body
            .contains("source has gone away")
    );
    drop(service);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn unrelated_terminal_identity_is_rejected_without_a_callback_or_mirror() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let service = OfflineService::new(
        store.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
    );
    let (source, target) = routes();
    let request = command(15, true);
    service
        .submit(request.clone(), &source, &target, 100)
        .await
        .unwrap();
    let mut wrong_target = target.agent.clone().unwrap();
    wrong_target.incarnation_id = "unrelated-turn-owner".into();
    let terminal = TerminalResult {
        workflow_id: request.id,
        source: source.agent.clone().unwrap(),
        target: wrong_target,
        status: "completed".into(),
        message: "wrong final".into(),
    };
    assert!(matches!(
        service.accept_terminal(terminal, &source, 200).await,
        Err(ServiceError::TerminalIdentity)
    ));
    assert_eq!(service.channels().calls().len(), 2);
    assert_eq!(service.wakterm().calls().len(), 1);
    drop(service);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn crash_after_prompt_effect_recovers_indeterminate_without_redelivery() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let faults = Arc::new(FaultInjector::default());
    faults.arm(FaultPoint::AfterPromptEffect);
    let service = OfflineService::with_faults(
        store.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
        faults,
    );
    let (source, target) = routes();
    let request = command(16, false);
    assert!(matches!(
        service.submit(request.clone(), &source, &target, 100).await,
        Err(ServiceError::Injected(FaultPoint::AfterPromptEffect))
    ));
    assert_eq!(service.wakterm().calls().len(), 1);
    drop(service);
    store.shutdown().await.unwrap();

    let reopened = StoreHandle::open(&path).unwrap();
    assert_eq!(
        reopened
            .get_workflow(request.id)
            .await
            .unwrap()
            .unwrap()
            .workflow
            .state,
        WorkflowState::Indeterminate
    );
    let replacement = OfflineService::new(
        reopened.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
    );
    assert!(matches!(
        replacement.submit(request, &source, &target, 200).await,
        Err(ServiceError::AdmissionFailed(WorkflowState::Indeterminate))
    ));
    assert!(replacement.wakterm().calls().is_empty());
    drop(replacement);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn crash_after_audit_effect_replays_only_the_at_least_once_audit() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let faults = Arc::new(FaultInjector::default());
    faults.arm(FaultPoint::AfterAuditEffect);
    let service = OfflineService::with_faults(
        store.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
        faults,
    );
    let (source, target) = routes();
    let request = command(161, false);
    assert!(matches!(
        service.submit(request.clone(), &source, &target, 100).await,
        Err(ServiceError::Injected(FaultPoint::AfterAuditEffect))
    ));
    assert_eq!(service.channels().calls().len(), 1);
    assert!(service.wakterm().calls().is_empty());
    drop(service);
    store.shutdown().await.unwrap();

    let reopened = StoreHandle::open(&path).unwrap();
    let replacement = OfflineService::new(
        reopened.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
    );
    let ack = replacement
        .submit(request, &source, &target, 200)
        .await
        .unwrap();
    assert!(ack.submitted);
    assert_eq!(replacement.wakterm().calls().len(), 1);
    assert_eq!(replacement.channels().calls().len(), 2);
    drop(replacement);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn crash_after_durable_acceptance_finishes_without_a_second_prompt() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let faults = Arc::new(FaultInjector::default());
    faults.arm(FaultPoint::AfterReceiptCheckpoint);
    let service = OfflineService::with_faults(
        store.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
        faults,
    );
    let (source, target) = routes();
    let request = command(162, false);
    assert!(matches!(
        service.submit(request.clone(), &source, &target, 100).await,
        Err(ServiceError::Injected(FaultPoint::AfterReceiptCheckpoint))
    ));
    assert_eq!(service.wakterm().calls().len(), 1);
    drop(service);
    store.shutdown().await.unwrap();

    let reopened = StoreHandle::open(&path).unwrap();
    let replacement = OfflineService::new(
        reopened.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
    );
    let ack = replacement
        .submit(request, &source, &target, 200)
        .await
        .unwrap();
    assert!(ack.submitted);
    assert!(replacement.wakterm().calls().is_empty());
    assert_eq!(replacement.channels().calls().len(), 1);
    drop(replacement);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn crash_before_the_busy_queue_checkpoint_is_conservatively_indeterminate() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let faults = Arc::new(FaultInjector::default());
    faults.arm(FaultPoint::AfterPromptEffect);
    let wakterm = FakeWakterm::new(contract());
    wakterm.script_receipts([AdmissionStatus::Busy]);
    let service =
        OfflineService::with_faults(store.clone(), wakterm, RecordingChannels::default(), faults);
    let (source, target) = routes();
    let request = command(163, false);
    assert!(matches!(
        service.submit(request.clone(), &source, &target, 100).await,
        Err(ServiceError::Injected(FaultPoint::AfterPromptEffect))
    ));
    drop(service);
    store.shutdown().await.unwrap();

    let reopened = StoreHandle::open(&path).unwrap();
    assert_eq!(
        reopened
            .get_workflow(request.id)
            .await
            .unwrap()
            .unwrap()
            .workflow
            .state,
        WorkflowState::Indeterminate
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn crash_after_callback_effect_recovers_callback_indeterminate_without_retry() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let faults = Arc::new(FaultInjector::default());
    let service = OfflineService::with_faults(
        store.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
        faults.clone(),
    );
    let (source, target) = routes();
    let request = command(17, true);
    service
        .submit(request.clone(), &source, &target, 100)
        .await
        .unwrap();
    faults.arm(FaultPoint::AfterCallbackEffect);
    let terminal = TerminalResult {
        workflow_id: request.id,
        source: source.agent.clone().unwrap(),
        target: target.agent.clone().unwrap(),
        status: "completed".into(),
        message: "done before crash".into(),
    };
    assert!(matches!(
        service.accept_terminal(terminal, &source, 200).await,
        Err(ServiceError::Injected(FaultPoint::AfterCallbackEffect))
    ));
    drop(service);
    store.shutdown().await.unwrap();

    let reopened = StoreHandle::open(&path).unwrap();
    assert_eq!(reopened.status().await.unwrap().unresolved_returns, 1);
    assert!(reopened.pending_returns().await.unwrap().is_empty());
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn replayed_terminal_after_completion_has_no_duplicate_fanout_effects() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let service = OfflineService::new(
        store.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
    );
    let (source, target) = routes();
    let request = command(18, true);
    service
        .submit(request.clone(), &source, &target, 100)
        .await
        .unwrap();
    let terminal = TerminalResult {
        workflow_id: request.id,
        source: source.agent.clone().unwrap(),
        target: target.agent.clone().unwrap(),
        status: "completed".into(),
        message: "replay-safe result".into(),
    };
    service
        .accept_terminal(terminal.clone(), &source, 200)
        .await
        .unwrap();
    let channel_effects = service.channels().calls().len();
    let prompt_effects = service.wakterm().calls().len();
    let returned = service
        .accept_terminal(terminal, &source, 300)
        .await
        .unwrap();
    assert_eq!(returned.agent.state, DeliveryState::Delivered);
    assert_eq!(returned.mirror.state, DeliveryState::Delivered);
    assert_eq!(service.channels().calls().len(), channel_effects);
    assert_eq!(service.wakterm().calls().len(), prompt_effects);
    drop(service);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn replayed_terminal_resumes_after_the_durable_return_checkpoint() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let faults = Arc::new(FaultInjector::default());
    faults.arm(FaultPoint::AfterReturnPersist);
    let service = OfflineService::with_faults(
        store.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
        faults,
    );
    let (source, target) = routes();
    let request = command(19, true);
    service
        .submit(request.clone(), &source, &target, 100)
        .await
        .unwrap();
    let terminal = TerminalResult {
        workflow_id: request.id,
        source: source.agent.clone().unwrap(),
        target: target.agent.clone().unwrap(),
        status: "completed".into(),
        message: "resume fanout".into(),
    };
    assert!(matches!(
        service
            .accept_terminal(terminal.clone(), &source, 200)
            .await,
        Err(ServiceError::Injected(FaultPoint::AfterReturnPersist))
    ));
    drop(service);
    store.shutdown().await.unwrap();

    let reopened = StoreHandle::open(&path).unwrap();
    let replacement = OfflineService::new(
        reopened.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
    );
    let returned = replacement
        .accept_terminal(terminal, &source, 300)
        .await
        .unwrap();
    assert_eq!(returned.agent.state, DeliveryState::Delivered);
    assert_eq!(returned.mirror.state, DeliveryState::Delivered);
    assert_eq!(replacement.channels().calls().len(), 1);
    assert_eq!(replacement.wakterm().calls().len(), 1);
    drop(replacement);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn mirror_acceptance_crash_replays_only_the_at_least_once_destination() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&path).unwrap();
    let faults = Arc::new(FaultInjector::default());
    faults.arm(FaultPoint::AfterMirrorEffect);
    let service = OfflineService::with_faults(
        store.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
        faults,
    );
    let (source, target) = routes();
    let request = command(20, true);
    service
        .submit(request.clone(), &source, &target, 100)
        .await
        .unwrap();
    let terminal = TerminalResult {
        workflow_id: request.id,
        source: source.agent.clone().unwrap(),
        target: target.agent.clone().unwrap(),
        status: "completed".into(),
        message: "at least once mirror".into(),
    };
    assert!(matches!(
        service
            .accept_terminal(terminal.clone(), &source, 200)
            .await,
        Err(ServiceError::Injected(FaultPoint::AfterMirrorEffect))
    ));
    assert_eq!(service.wakterm().calls().len(), 1);
    drop(service);
    store.shutdown().await.unwrap();

    let reopened = StoreHandle::open(&path).unwrap();
    let replacement = OfflineService::new(
        reopened.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
    );
    let returned = replacement
        .accept_terminal(terminal, &source, 300)
        .await
        .unwrap();
    assert_eq!(returned.agent.state, DeliveryState::Delivered);
    assert_eq!(returned.mirror.state, DeliveryState::Delivered);
    assert_eq!(replacement.channels().calls().len(), 1);
    assert_eq!(replacement.wakterm().calls().len(), 1);
    drop(replacement);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn fixture_event_cursor_advances_only_after_a_valid_future_page() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let future = WaktermContract::from_golden_json(
        &std::fs::read_to_string("/code/wakterm/docs/agent-api/v1/golden-fixtures.json").unwrap(),
        ProfileKind::FutureEvents,
    )
    .unwrap();
    let service = OfflineService::new(
        store.clone(),
        FakeWakterm::new(future),
        RecordingChannels::default(),
    );
    let page = service.consume_fixture_events(100).await.unwrap();
    assert!(matches!(
        page,
        EventRead::Events {
            next_after_sequence: 108,
            ..
        }
    ));
    assert_eq!(
        store
            .get_metadata("wakterm_event_cursor".into())
            .await
            .unwrap()
            .as_deref(),
        Some("108")
    );
    assert!(matches!(
        service.consume_fixture_events(12).await.unwrap(),
        EventRead::CursorTooOld { .. }
    ));
    assert_eq!(
        store
            .get_metadata("wakterm_event_cursor".into())
            .await
            .unwrap()
            .as_deref(),
        Some("108")
    );
    drop(service);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn current_profile_never_creates_a_general_event_cursor() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let service = OfflineService::new(
        store.clone(),
        FakeWakterm::new(contract()),
        RecordingChannels::default(),
    );
    assert_eq!(
        service.consume_fixture_events(0).await.unwrap(),
        EventRead::Unsupported
    );
    assert_eq!(
        store
            .get_metadata("wakterm_event_cursor".into())
            .await
            .unwrap(),
        None
    );
    drop(service);
    store.shutdown().await.unwrap();
}
