use panetone::channels::RecordingChannels;
use panetone::domain::{
    AdmissionStatus, AgentBinding, ChannelKind, EffectId, OutboxItem, OutboxState, SendCommand,
    WorkflowId,
};
use panetone::wakterm::{
    ContractError, EventRead, FakeWakterm, ProfileKind, WaktermContract, join_catalog_binding,
};
use serde_json::Value;
use uuid::Uuid;

fn golden() -> String {
    std::fs::read_to_string("/code/wakterm/docs/agent-api/v1/golden-fixtures.json").unwrap()
}

#[test]
fn current_and_future_profiles_negotiate_distinct_event_behavior() {
    let current = WaktermContract::from_golden_json(&golden(), ProfileKind::Current).unwrap();
    assert!(!current.general_event_consumer_enabled());
    assert_eq!(current.read_events(0).unwrap(), EventRead::Unsupported);

    let future = WaktermContract::from_golden_json(&golden(), ProfileKind::FutureEvents).unwrap();
    assert!(future.general_event_consumer_enabled());
    match future.read_events(100).unwrap() {
        EventRead::Events {
            events,
            next_after_sequence,
        } => {
            assert_eq!(events.len(), 8);
            assert_eq!(events.first().unwrap().sequence, 101);
            assert_eq!(events.last().unwrap().sequence, 108);
            assert_eq!(next_after_sequence, 108);
        }
        other => panic!("expected fixture events, got {other:?}"),
    }
    assert!(matches!(
        future.read_events(12).unwrap(),
        EventRead::CursorTooOld {
            requested_after_sequence: 12,
            oldest_available_sequence: 90,
            latest_sequence: 108
        }
    ));
}

#[test]
fn incompatible_versions_and_unknown_future_events_fail_closed() {
    let mut fixture: Value = serde_json::from_str(&golden()).unwrap();
    fixture["current_capabilities"]["api_major"] = 2.into();
    assert!(matches!(
        WaktermContract::from_golden_json(&fixture.to_string(), ProfileKind::Current),
        Err(ContractError::IncompatibleMajor(2))
    ));

    let mut fixture: Value = serde_json::from_str(&golden()).unwrap();
    fixture["event_page"]["events"][0]["kind"] = "future_unknown".into();
    assert!(matches!(
        WaktermContract::from_golden_json(&fixture.to_string(), ProfileKind::FutureEvents),
        Err(ContractError::Invalid("unknown event kind"))
    ));
}

#[test]
fn observer_not_ready_is_a_definitive_no_write_failure() {
    let fixture: Value = serde_json::from_str(&golden()).unwrap();
    let receipt = &fixture["admission_receipts"]["observer_failure"];
    assert_eq!(receipt["status"], "observer_failure");
    assert_eq!(receipt["definitive"], true);
    assert_eq!(receipt["prompt_written"], false);
    assert!(
        receipt["detail"]
            .as_str()
            .unwrap()
            .contains("observer cursor")
    );
}

#[test]
fn pane_is_only_a_fresh_catalog_join_and_names_are_not_identity() {
    let contract = WaktermContract::from_golden_json(&golden(), ProfileKind::Current).unwrap();
    let before = contract.catalog.clone();
    let mut after = before.clone();
    after.agents[0].name = "renamed display title".into();
    let binding = join_catalog_binding(9, &before, &after).unwrap();
    assert_eq!(binding.agent_id, "agent-zola");
    assert_eq!(binding.incarnation_id, "incarnation-zola-7");
    assert_eq!(binding.pane_id, Some(9));

    after.agents[0].agent_id = "replacement-agent".into();
    assert!(matches!(
        join_catalog_binding(9, &before, &after),
        Err(ContractError::UnstableCatalog)
    ));
}

#[test]
fn fake_wakterm_uses_exact_identity_stable_ids_and_labeled_envelopes() {
    let contract = WaktermContract::from_golden_json(&golden(), ProfileKind::Current).unwrap();
    let fake = FakeWakterm::new(contract);
    fake.script_receipts([AdmissionStatus::Busy, AdmissionStatus::Accepted]);
    let binding = AgentBinding {
        agent_id: "agent-zola".into(),
        incarnation_id: "incarnation-zola-7".into(),
        harness: "codex".into(),
        pane_id: Some(999),
    };
    let workflow_id = WorkflowId::new(Uuid::from_u128(1));
    let effect_id = EffectId::target_admission(workflow_id);
    let busy = fake.admit(effect_id, &binding, "prompt".into(), false);
    assert_eq!(busy.status, AdmissionStatus::Busy);
    assert_eq!(busy.prompt_written, Some(false));
    let accepted = fake.admit(effect_id, &binding, "prompt".into(), false);
    assert_eq!(accepted.status, AdmissionStatus::Accepted);
    assert_eq!(fake.calls().len(), 1);
    assert_eq!(fake.calls()[0].binding.agent_id, "agent-zola");
    assert_eq!(fake.calls()[0].binding.incarnation_id, "incarnation-zola-7");

    let envelope = FakeWakterm::envelope(
        &SendCommand {
            id: workflow_id,
            source: "ufopedia".into(),
            target: "wakterm".into(),
            message: "review this".into(),
            return_final: true,
            timeout_ms: 0,
        },
        "codex",
        "codex",
    );
    assert!(envelope.contains("From: ufopedia (codex)"));
    assert!(envelope.contains("To: wakterm (codex)"));
    assert!(envelope.contains(&workflow_id.to_string()));
    assert!(envelope.contains("asynchronous final callback"));
}

fn item(index: u128, kind: ChannelKind) -> OutboxItem {
    OutboxItem {
        id: EffectId::new(Uuid::from_u128(index)),
        kind,
        destination: format!("destination-{index}"),
        body: format!("message-{index}"),
        state: OutboxState::Pending,
        attempts: 0,
        last_error: None,
        external_receipt: None,
    }
}

#[test]
fn recording_channels_cover_all_transports_and_do_not_hide_at_least_once_replays() {
    let channels = RecordingChannels::default();
    for (index, kind) in [
        ChannelKind::Telegram,
        ChannelKind::Signal,
        ChannelKind::Slack,
        ChannelKind::Debate,
    ]
    .into_iter()
    .enumerate()
    {
        let item = item(index as u128 + 1, kind);
        channels.send(&item).unwrap();
        channels.send(&item).unwrap();
    }
    assert_eq!(channels.calls().len(), 8);

    channels.fail(ChannelKind::Telegram);
    assert!(channels.send(&item(10, ChannelKind::Telegram)).is_err());
    channels.restore(ChannelKind::Telegram);
    assert!(channels.send(&item(10, ChannelKind::Telegram)).is_ok());
}
