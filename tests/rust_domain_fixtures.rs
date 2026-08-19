use panetone::domain::{
    AgentBinding, ChannelAvailability, ChannelBinding, ChannelKind, EffectId, LiveRoute,
    OutboxItem, OutboxState, ReconcileDecision, Route, RouteError, RouteId, RouteStatus,
    chunk_lines, chunk_utf16, fair_retry_indices, normalize_signal_group_id, resolve_live_route,
    select_channel,
};
use serde_json::Value;
use uuid::Uuid;

fn fixture(path: &str) -> Value {
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

#[test]
fn route_resolution_matches_frozen_fixture() {
    let data = fixture("tests/fixtures/routes/control-resolution.json");
    for case in data["cases"].as_array().unwrap() {
        let routes = case["tabs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|route| LiveRoute {
                title: route["title"].as_str().unwrap().into(),
                tab_id: route["tab_id"].as_u64().unwrap(),
                pane_id: route["pane_id"].as_u64(),
                harness: route["harness"].as_str().map(str::to_owned),
                topic_id: route["topic_id"].as_i64(),
            })
            .collect::<Vec<_>>();
        let actual = resolve_live_route(case["lookup"].as_str().unwrap(), &routes);
        if let Some(expected) = case.get("expected") {
            let route = actual.unwrap();
            assert_eq!(route.title, expected["title"].as_str().unwrap());
            assert_eq!(route.tab_id, expected["tab_id"].as_u64().unwrap());
            assert_eq!(route.pane_id, expected["pane_id"].as_u64());
            assert_eq!(route.harness.as_deref(), expected["harness"].as_str());
            assert_eq!(route.topic_id, expected["topic_id"].as_i64());
        } else {
            let expected = case["error"].as_str().unwrap();
            let code = match actual.unwrap_err() {
                RouteError::NotFound => "route_not_found",
                RouteError::Ambiguous => "route_ambiguous",
                RouteError::Unavailable => "route_unavailable",
            };
            assert_eq!(code, expected, "case {}", case["name"]);
        }
    }
}

fn binding(value: &Value) -> AgentBinding {
    AgentBinding {
        agent_id: value["agent_id"].as_str().unwrap().into(),
        incarnation_id: value["incarnation"].as_str().unwrap().into(),
        harness: "codex".into(),
        pane_id: value["pane_id"].as_u64(),
    }
}

#[test]
fn route_lifecycle_matches_frozen_fixture() {
    let data = fixture("tests/fixtures/routes/control-resolution.json");
    for case in data["lifecycle_cases"].as_array().unwrap() {
        let mut route = Route {
            id: RouteId::new(Uuid::parse_str(case["route_id"].as_str().unwrap()).unwrap()),
            title: case["name"].as_str().unwrap().into(),
            channels: vec![ChannelBinding::Telegram {
                topic_id: case["channel_binding"]["topic_id"].as_i64().unwrap(),
            }],
            agent: Some(binding(&case["before"])),
            status: RouteStatus::Available,
        };
        let observed = case["after"].as_object().map(|_| binding(&case["after"]));
        let actual = route.reconcile(observed);
        let expected = match case["expected"].as_str().unwrap() {
            "route_unavailable" => ReconcileDecision::Unavailable,
            "reconciliation_required" => ReconcileDecision::ReconciliationRequired,
            "rebound" => ReconcileDecision::Rebound,
            other => panic!("unknown fixture decision {other}"),
        };
        assert_eq!(actual, expected);
        assert_eq!(route.channels.len(), 1);
    }
}

#[test]
fn channel_routing_and_formatting_match_frozen_fixture() {
    let data = fixture("tests/fixtures/channels/routing.json");
    for case in data["route_cases"].as_array().unwrap() {
        if case["source"].as_str() == Some("debate") {
            continue;
        }
        let source = match case["source"].as_str() {
            Some("tg") => Some(ChannelKind::Telegram),
            Some("sig") => Some(ChannelKind::Signal),
            None => None,
            Some(other) => panic!("unknown source {other}"),
        };
        let availability = ChannelAvailability {
            telegram_topic: case["telegram_topic"].as_i64(),
            signal_enabled: case["signal_enabled"].as_bool().unwrap(),
            signal_group: case["signal_group"].as_str(),
        };
        let actual = select_channel(source, "Alpha", &availability).map(|selection| {
            let kind = match selection.kind {
                ChannelKind::Telegram => "tg",
                ChannelKind::Signal => "sig",
            };
            serde_json::json!([kind, selection.destination, selection.route_title])
        });
        let expected = case
            .get("expected")
            .filter(|value| !value.is_null())
            .map(|value| {
                let mut value = value.clone();
                value[1] = Value::String(match &value[1] {
                    Value::String(text) => text.clone(),
                    number => number.to_string(),
                });
                value
            });
        assert_eq!(actual, expected, "case {}", case["name"]);
    }
    assert!(serde_json::from_str::<ChannelKind>(r#""debate""#).is_err());

    let formats = &data["format_cases"];
    for case in formats["signal_group_ids"].as_array().unwrap() {
        assert_eq!(
            normalize_signal_group_id(case["input"].as_str()),
            case["expected"].as_str().unwrap()
        );
    }
    assert_eq!(
        chunk_lines(
            formats["line_chunks"]["input"].as_str().unwrap(),
            formats["line_chunks"]["limit"].as_u64().unwrap() as usize,
        ),
        formats["line_chunks"]["expected"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        chunk_utf16(
            formats["utf16_chunks"]["input"].as_str().unwrap(),
            formats["utf16_chunks"]["limit"].as_u64().unwrap() as usize,
        ),
        formats["utf16_chunks"]["expected"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>()
    );
}

#[test]
fn retry_selection_is_fair_per_destination() {
    let data = fixture("tests/fixtures/channels/routing.json");
    let source = data["retry_fairness"]["items"].as_array().unwrap();
    let items = source
        .iter()
        .enumerate()
        .map(|(index, item)| OutboxItem {
            id: EffectId::new(Uuid::from_u128(index as u128 + 1)),
            route_id: None,
            sender_harness: item[4].as_str().map(str::to_owned),
            kind: match item[0].as_str().unwrap() {
                "tg" => ChannelKind::Telegram,
                "sig" => ChannelKind::Signal,
                other => panic!("unknown channel {other}"),
            },
            destination: match &item[1] {
                Value::String(value) => value.clone(),
                value => value.to_string(),
            },
            body: item[2].as_str().unwrap().into(),
            state: OutboxState::Pending,
            attempts: 0,
            last_error: None,
            external_receipt: None,
        })
        .collect::<Vec<_>>();
    let actual = fair_retry_indices(&items)
        .into_iter()
        .map(|index| source[index][6].as_str().unwrap())
        .collect::<Vec<_>>();
    let expected = data["retry_fairness"]["expected_attempt_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}
