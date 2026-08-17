use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::channels::{RealChannels, TelegramPoller};
use crate::control::{
    CONTROL_SCHEMA, ControlHandler, ControlRequest, ControlResponse, SendParams, error_response,
    success_response,
};
use crate::domain::{
    AdmissionStatus, ChannelBinding, ChannelKind, OutboxState, Route, RouteStatus, WorkflowId,
};
use crate::promotion::{
    EventCursorGap, LegacyDecision, LegacyRecordKind, OperatorAction, OperatorMutation,
};
use crate::store::{InboxItem, StoreHandle};
use crate::supervisor::SupervisorHandle;
use crate::wakterm::{EventRead, TerminalResult, WaktermCli};

use super::{OfflineService, ServiceError};

pub struct ProductionService {
    store: StoreHandle,
    wakterm: WaktermCli,
    channels: RealChannels,
    telegram_poller: Option<TelegramPoller>,
    workflows: Arc<OfflineService>,
    effects: RwLock<()>,
    health: SupervisorHandle,
    capabilities: Vec<String>,
    control_socket: PathBuf,
    started_at: Instant,
}

#[derive(Debug, Deserialize)]
struct HoldParams {
    held: bool,
}

#[derive(Debug, Deserialize)]
struct RoutePolicyParams {
    route: String,
    enabled: bool,
}

#[derive(Debug, Deserialize)]
struct CursorParams {
    sequence: u64,
}

#[derive(Debug, Deserialize)]
struct CursorGapAckParams {
    requested_after_sequence: u64,
    evidence: String,
}

#[derive(Debug, Deserialize)]
struct InboundCursorParams {
    channel: ChannelKind,
    cursor: u64,
}

#[derive(Debug, Deserialize)]
struct ReconcileParams {
    route: String,
    #[serde(default)]
    replace_identity: bool,
}

#[derive(Debug, Deserialize)]
struct DisposeLegacyParams {
    record_kind: LegacyRecordKind,
    record_id: String,
    decision: String,
    evidence: String,
    route: Option<String>,
    expected_legacy_destination: Option<String>,
}

impl ProductionService {
    pub fn new(
        store: StoreHandle,
        wakterm: WaktermCli,
        channels: RealChannels,
        telegram_poller: Option<TelegramPoller>,
        health: SupervisorHandle,
        capabilities: Vec<String>,
        control_socket: PathBuf,
    ) -> Self {
        let workflows = Arc::new(OfflineService::new_real(
            store.clone(),
            wakterm.clone(),
            channels.clone(),
        ));
        Self {
            store,
            wakterm,
            channels,
            telegram_poller,
            workflows,
            effects: RwLock::new(()),
            health,
            capabilities,
            control_socket,
            started_at: Instant::now(),
        }
    }

    pub async fn event_once(&self) -> Result<usize, String> {
        let _guard = self.effects.read().await;
        let promotion = self.store.promotion_status().await.map_err(error_string)?;
        if promotion.delivery_hold {
            return Ok(0);
        }
        let Some(mut cursor) = promotion.event_cursor else {
            return Err("Wakterm event cursor has not been initialized".into());
        };
        let mut recorded = 0usize;
        loop {
            match self
                .wakterm
                .event_page(cursor, 100)
                .await
                .map_err(error_string)?
            {
                EventRead::Events {
                    events,
                    next_after_sequence,
                    latest_sequence,
                } => {
                    let outcome = self
                        .store
                        .ingest_agent_events(cursor, next_after_sequence, events, now_ms())
                        .await
                        .map_err(error_string)?;
                    recorded += outcome.recorded as usize;
                    cursor = next_after_sequence;
                    if cursor >= latest_sequence {
                        return Ok(recorded);
                    }
                }
                EventRead::CursorTooOld {
                    requested_after_sequence,
                    oldest_available_sequence,
                    latest_sequence,
                    catalog_as_of_sequence,
                } => {
                    let catalog = self.wakterm.catalog().await.map_err(error_string)?;
                    let fresh_catalog_as_of_sequence = catalog.as_of_event_sequence;
                    self.store
                        .recover_event_cursor_gap(
                            EventCursorGap {
                                requested_after_sequence,
                                oldest_available_sequence,
                                latest_sequence,
                                recovery_catalog_as_of_sequence: catalog_as_of_sequence,
                                fresh_catalog_as_of_sequence,
                                recorded_at_ms: now_ms(),
                            },
                            catalog,
                        )
                        .await
                        .map_err(error_string)?;
                    return Err(format!(
                        "Wakterm event cursor {requested_after_sequence} was older than retained sequence {oldest_available_sequence}; delivery is held at fresh catalog cursor {fresh_catalog_as_of_sequence} until the explicit gap is reviewed and acknowledged"
                    ));
                }
                EventRead::Unsupported => {
                    return Err("Wakterm event_stream.v1 is unavailable".into());
                }
            }
        }
    }

    pub async fn outbox_once(&self) -> Result<usize, String> {
        let _guard = self.effects.read().await;
        let promotion = self.store.promotion_status().await.map_err(error_string)?;
        if promotion.delivery_hold {
            return Ok(0);
        }
        let mut attempted = 0;
        for mut item in self.store.pending_outbox().await.map_err(error_string)? {
            if item
                .route_id
                .is_none_or(|route_id| !promotion.delivery_allowed(route_id))
            {
                continue;
            }
            let expected_attempts = item.attempts;
            item.state = OutboxState::Delivering;
            item.attempts += 1;
            self.store
                .save_outbox(item.clone(), now_ms())
                .await
                .map_err(error_string)?;
            attempted += 1;
            match self.channels.send(&item).await {
                Ok(receipt) => {
                    item.state = OutboxState::Delivered;
                    item.external_receipt = Some(receipt.external_id);
                    item.last_error = None;
                }
                Err(error) => {
                    item.state = if error.retryable() {
                        OutboxState::Pending
                    } else {
                        OutboxState::Failed
                    };
                    item.last_error = Some(error.to_string());
                }
            }
            if item.attempts != expected_attempts + 1 {
                return Err("outbox attempt accounting changed unexpectedly".into());
            }
            self.store
                .save_outbox(item, now_ms())
                .await
                .map_err(error_string)?;
        }
        Ok(attempted)
    }

    pub async fn busy_once(&self) -> Result<usize, String> {
        let _guard = self.effects.read().await;
        let promotion = self.store.promotion_status().await.map_err(error_string)?;
        if promotion.delivery_hold {
            return Ok(0);
        }
        let mut attempted = 0;
        for workflow in self.store.awaiting_target().await.map_err(error_string)? {
            if !promotion.delivery_allowed(workflow.workflow.target_route_id) {
                continue;
            }
            let Some(route) = self
                .store
                .get_route(workflow.workflow.target_route_id)
                .await
                .map_err(error_string)?
            else {
                continue;
            };
            attempted += 1;
            match self
                .workflows
                .retry_busy_target(workflow.command.id, &route, now_ms())
                .await
            {
                Ok(_) | Err(ServiceError::RouteUnavailable(_)) => {}
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(attempted)
    }

    pub async fn terminal_once(&self) -> Result<usize, String> {
        let _guard = self.effects.read().await;
        let promotion = self.store.promotion_status().await.map_err(error_string)?;
        if promotion.delivery_hold {
            return Ok(0);
        }
        let cursor = self
            .store
            .get_metadata("wakterm_return_cursor".into())
            .await
            .map_err(error_string)?
            .as_deref()
            .unwrap_or("0")
            .parse::<u64>()
            .map_err(|_| "stored Wakterm return cursor is invalid".to_string())?;
        let terminals = self
            .wakterm
            .terminal_events(cursor)
            .await
            .map_err(error_string)?;
        let mut processed = 0;
        let mut next = cursor;
        for terminal in terminals {
            if terminal.terminal_event_sequence <= next {
                return Err("Wakterm return terminal sequences are not increasing".into());
            }
            let request_id = Uuid::parse_str(&terminal.request_id)
                .map(WorkflowId::new)
                .map_err(|_| "Wakterm returned an invalid request UUID".to_string())?;
            if let Some(workflow) = self
                .store
                .get_workflow(request_id)
                .await
                .map_err(error_string)?
            {
                let target = workflow.workflow.submitted_target.clone().ok_or_else(|| {
                    "return terminal workflow has no submitted target".to_string()
                })?;
                if target.agent_id != terminal.target_agent_id {
                    return Err("Wakterm return terminal target identity changed".into());
                }
                let source_route = self
                    .store
                    .get_route(workflow.workflow.source_route_id)
                    .await
                    .map_err(error_string)?
                    .ok_or_else(|| "return terminal source route is missing".to_string())?;
                self.workflows
                    .persist_terminal(
                        TerminalResult {
                            workflow_id: request_id,
                            source: workflow.workflow.observed_source,
                            target,
                            status: terminal.state,
                            message: terminal
                                .final_message
                                .or(terminal.detail)
                                .unwrap_or_default(),
                        },
                        &source_route,
                        now_ms(),
                    )
                    .await
                    .map_err(error_string)?;
            }
            next = terminal.terminal_event_sequence;
            self.store
                .set_metadata("wakterm_return_cursor".into(), next.to_string())
                .await
                .map_err(error_string)?;
            processed += 1;
        }
        Ok(processed)
    }

    pub async fn pending_return_once(&self) -> Result<usize, String> {
        let _guard = self.effects.read().await;
        let promotion = self.store.promotion_status().await.map_err(error_string)?;
        if promotion.delivery_hold {
            return Ok(0);
        }
        let pending = self.store.pending_returns().await.map_err(error_string)?;
        let mut attempted = 0;
        for returned in pending {
            let workflow = self
                .store
                .get_workflow(returned.workflow_id)
                .await
                .map_err(error_string)?
                .ok_or_else(|| "pending return workflow is missing".to_string())?;
            if !promotion.delivery_allowed(workflow.workflow.source_route_id) {
                continue;
            }
            let route = self
                .store
                .get_route(workflow.workflow.source_route_id)
                .await
                .map_err(error_string)?
                .ok_or_else(|| "pending return source route is missing".to_string())?;
            attempted += 1;
            match self
                .workflows
                .deliver_pending_return(returned.workflow_id, &route, now_ms())
                .await
            {
                Ok(_) | Err(ServiceError::RouteUnavailable(_)) => {}
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(attempted)
    }

    pub async fn inbox_once(&self) -> Result<usize, String> {
        let _guard = self.effects.read().await;
        let promotion = self.store.promotion_status().await.map_err(error_string)?;
        if promotion.delivery_hold {
            return Ok(0);
        }
        let routes = self.store.list_routes().await.map_err(error_string)?;
        let mut attempted = 0;
        for mut item in self.store.pending_inbox().await.map_err(error_string)? {
            let matching = routes
                .iter()
                .filter(|route| route_matches_inbox(route, &item))
                .collect::<Vec<_>>();
            let [route] = matching.as_slice() else {
                continue;
            };
            if !promotion.delivery_allowed(route.id) || route.status != RouteStatus::Available {
                continue;
            }
            let Some(binding) = route.agent.as_ref() else {
                continue;
            };
            attempted += 1;
            item.state = "admission_prepared".into();
            self.store
                .save_inbox(item.clone(), "pending", None)
                .await
                .map_err(error_string)?;
            let prompt = inbound_envelope(&item, route);
            let receipt = match self
                .wakterm
                .admit(item.id, binding, &prompt, false, 0)
                .await
            {
                Ok(receipt) => receipt,
                Err(error) => {
                    item.state = "indeterminate".into();
                    self.store
                        .save_inbox(item, "admission_prepared", None)
                        .await
                        .map_err(error_string)?;
                    return Err(error.to_string());
                }
            };
            receipt.validate(item.id, binding).map_err(error_string)?;
            item.state = match receipt.status {
                AdmissionStatus::Accepted => "delivered",
                AdmissionStatus::Indeterminate => "indeterminate",
                _ => "pending",
            }
            .into();
            let preference = (item.state == "delivered").then_some((route.id, item.channel));
            self.store
                .save_inbox(item, "admission_prepared", preference)
                .await
                .map_err(error_string)?;
        }
        Ok(attempted)
    }

    async fn handle_send(&self, request: ControlRequest, params: SendParams) -> ControlResponse {
        let id = request.id;
        if params.timeout_ms != 0 {
            return error_response(
                id,
                "invalid_params",
                "asynchronous final callbacks do not expire; params.timeout_ms must be zero",
                None,
            );
        }
        if params.source.is_empty() || params.target.is_empty() || params.message.is_empty() {
            return error_response(
                id,
                "invalid_request",
                "send requires non-empty from, to, and message fields",
                None,
            );
        }
        let _guard = self.effects.read().await;
        let promotion = match self.store.promotion_status().await {
            Ok(status) => status,
            Err(error) => return internal(id, error),
        };
        let routes = match self.store.list_routes().await {
            Ok(routes) => routes,
            Err(error) => return internal(id, error),
        };
        let source = match exact_route(&routes, &params.source) {
            Ok(route) => route,
            Err(response) => return route_error(id, "source", response),
        };
        let target = match exact_route(&routes, &params.target) {
            Ok(route) => route,
            Err(response) => return route_error(id, "target", response),
        };
        if !promotion.delivery_allowed(target.id)
            || (params.return_final && !promotion.delivery_allowed(source.id))
        {
            return error_response(
                id,
                "delivery_held",
                "delivery is held globally or for a requested route",
                Some(json!({"promotion": promotion})),
            );
        }
        match self
            .workflows
            .submit(params.into_command(id), source, target, now_ms())
            .await
        {
            Ok(ack) => success_response(id, serde_json::to_value(ack).expect("ack serializes")),
            Err(ServiceError::IdempotencyConflict) => error_response(
                id,
                "idempotency_conflict",
                "the request ID was already used with different content",
                None,
            ),
            Err(ServiceError::Expired(state)) => error_response(
                id,
                "request_expired",
                "the request payload expired but its idempotency key remains reserved",
                Some(json!({"state": state})),
            ),
            Err(error) => error_response(
                id,
                "delivery_failed",
                error.to_string(),
                Some(json!({"durable": true})),
            ),
        }
    }

    async fn handle_operator(&self, request: ControlRequest) -> ControlResponse {
        let id = request.id;
        let intent = json!({"method": request.method.clone(), "params": request.params.clone()});
        match self.store.operator_replay(id, intent.clone()).await {
            Ok(Some(outcome)) => {
                return success_response(
                    id,
                    serde_json::to_value(outcome).expect("operator outcome serializes"),
                );
            }
            Ok(None) => {}
            Err(error) => {
                return error_response(id, "operator_conflict", error.to_string(), None);
            }
        }
        let result = match request.method.as_str() {
            "set_delivery_hold" => {
                let params = match serde_json::from_value::<HoldParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => return invalid(id, error),
                };
                let _guard = self.effects.write().await;
                self.apply(
                    id,
                    OperatorAction::SetDeliveryHold { held: params.held },
                    intent.clone(),
                )
                .await
            }
            "set_route_enabled" => {
                let params = match serde_json::from_value::<RoutePolicyParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => return invalid(id, error),
                };
                let route = match self.resolve_route(&params.route).await {
                    Ok(route) => route,
                    Err(error) => return route_error(id, "route", error),
                };
                self.apply(
                    id,
                    OperatorAction::SetRouteEnabled {
                        route_id: route.id,
                        enabled: params.enabled,
                    },
                    intent.clone(),
                )
                .await
            }
            "initialize_event_cursor" => {
                let params = match serde_json::from_value::<CursorParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => return invalid(id, error),
                };
                self.apply(
                    id,
                    OperatorAction::InitializeEventCursor {
                        sequence: params.sequence,
                    },
                    intent.clone(),
                )
                .await
            }
            "baseline_event_cursor" => {
                if !request.params.is_null() {
                    return error_response(
                        id,
                        "invalid_request",
                        "Wakterm event baseline takes no parameters",
                        None,
                    );
                }
                let _guard = self.effects.write().await;
                let catalog = match self.wakterm.catalog().await {
                    Ok(catalog) => catalog,
                    Err(error) => {
                        return error_response(id, "wakterm_unavailable", error.to_string(), None);
                    }
                };
                self.apply(
                    id,
                    OperatorAction::InitializeEventCursor {
                        sequence: catalog.as_of_event_sequence,
                    },
                    intent.clone(),
                )
                .await
            }
            "acknowledge_event_cursor_gap" => {
                let params = match serde_json::from_value::<CursorGapAckParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => return invalid(id, error),
                };
                let _guard = self.effects.write().await;
                self.apply(
                    id,
                    OperatorAction::AcknowledgeEventCursorGap {
                        requested_after_sequence: params.requested_after_sequence,
                        evidence: params.evidence,
                    },
                    intent.clone(),
                )
                .await
            }
            "initialize_inbound_cursor" => {
                let params = match serde_json::from_value::<InboundCursorParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => return invalid(id, error),
                };
                self.apply(
                    id,
                    OperatorAction::InitializeInboundCursor {
                        channel: params.channel,
                        cursor: params.cursor,
                    },
                    intent.clone(),
                )
                .await
            }
            "baseline_telegram_cursor" => {
                if !request.params.is_null() {
                    return error_response(
                        id,
                        "invalid_request",
                        "Telegram baseline takes no parameters",
                        None,
                    );
                }
                let Some(poller) = self.telegram_poller.as_ref() else {
                    return error_response(
                        id,
                        "channel_unavailable",
                        "Telegram is not configured",
                        None,
                    );
                };
                let _guard = self.effects.write().await;
                let batch = match poller.poll(-1, 0).await {
                    Ok(batch) => batch,
                    Err(error) => {
                        return error_response(id, "channel_unavailable", error.to_string(), None);
                    }
                };
                self.apply(
                    id,
                    OperatorAction::InitializeInboundCursor {
                        channel: ChannelKind::Telegram,
                        cursor: batch.next_offset.max(0) as u64,
                    },
                    intent.clone(),
                )
                .await
            }
            "reconcile_route" => {
                let params = match serde_json::from_value::<ReconcileParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => return invalid(id, error),
                };
                let route = match self.resolve_route(&params.route).await {
                    Ok(route) => route,
                    Err(error) => return route_error(id, "route", error),
                };
                let binding = match self.wakterm.resolve_route_binding(&route.title).await {
                    Ok(binding) => binding,
                    Err(error) => {
                        return error_response(
                            id,
                            "route_reconciliation_failed",
                            error.to_string(),
                            None,
                        );
                    }
                };
                self.apply(
                    id,
                    OperatorAction::ReconcileRoute {
                        route_id: route.id,
                        binding,
                        replace_identity: params.replace_identity,
                    },
                    intent.clone(),
                )
                .await
            }
            "dispose_legacy" => {
                let params = match serde_json::from_value::<DisposeLegacyParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => return invalid(id, error),
                };
                let decision = match params.decision.as_str() {
                    "no_replay" => LegacyDecision::NoReplay,
                    "externally_verified" => LegacyDecision::ExternallyVerified,
                    "map_debate_to_signal" => {
                        let Some(route_name) = params.route else {
                            return error_response(
                                id,
                                "invalid_request",
                                "Signal mapping requires a route",
                                None,
                            );
                        };
                        let route = match self.resolve_route(&route_name).await {
                            Ok(route) => route,
                            Err(error) => return route_error(id, "route", error),
                        };
                        let Some(destination) = params.expected_legacy_destination else {
                            return error_response(
                                id,
                                "invalid_request",
                                "Signal mapping requires the expected legacy destination",
                                None,
                            );
                        };
                        LegacyDecision::MapDebateToSignal {
                            route_id: route.id,
                            expected_legacy_destination: destination,
                        }
                    }
                    _ => {
                        return error_response(
                            id,
                            "invalid_request",
                            "unknown legacy disposition",
                            None,
                        );
                    }
                };
                self.apply(
                    id,
                    OperatorAction::DisposeLegacy {
                        record_kind: params.record_kind,
                        record_id: params.record_id,
                        decision,
                        evidence: params.evidence,
                    },
                    intent.clone(),
                )
                .await
            }
            _ => unreachable!("operator method was prefiltered"),
        };
        match result {
            Ok(outcome) => success_response(id, serde_json::to_value(outcome).expect("serializes")),
            Err(error) => error_response(id, "operator_conflict", error.to_string(), None),
        }
    }

    async fn apply(
        &self,
        operation_id: Uuid,
        action: OperatorAction,
        intent: Value,
    ) -> Result<crate::promotion::OperatorOutcome, crate::store::StoreError> {
        self.store
            .apply_operator_mutation(
                OperatorMutation {
                    operation_id,
                    intent: Some(intent),
                    action,
                },
                now_ms(),
            )
            .await
    }

    async fn resolve_route(&self, name: &str) -> Result<Route, &'static str> {
        let routes = self.store.list_routes().await.map_err(|_| "store_failed")?;
        exact_route(&routes, name).cloned()
    }

    async fn status_response(&self, id: Uuid) -> ControlResponse {
        match self.store.status().await {
            Ok(status) => success_response(
                id,
                json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "mode": "production",
                    "uptime_ms": self.started_at.elapsed().as_millis() as u64,
                    "store": status,
                    "wakterm": {
                        "capabilities": self.capabilities,
                        "general_event_consumer": self.capabilities.iter().any(|value| value == "event_stream.v1"),
                        "socket": self.wakterm.socket(),
                    },
                    "channels": {
                        "telegram": self.channels.telegram.is_some() || !self.channels.telegram_by_harness.is_empty(),
                        "signal": self.channels.signal.is_some(),
                        "slack": self.channels.slack.is_some(),
                    },
                    "control": {"path": self.control_socket},
                    "tasks": self.health.snapshot(),
                }),
            ),
            Err(error) => internal(id, error),
        }
    }
}

impl ControlHandler for ProductionService {
    fn handle(
        &self,
        request: ControlRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ControlResponse> + Send + '_>> {
        Box::pin(async move {
            if request.schema != CONTROL_SCHEMA {
                return error_response(
                    request.id,
                    "unsupported_schema",
                    "only panetone.control.v1 is supported",
                    None,
                );
            }
            match request.method.as_str() {
                "status" => self.status_response(request.id).await,
                "send" => match serde_json::from_value::<SendParams>(request.params.clone()) {
                    Ok(params) => self.handle_send(request, params).await,
                    Err(error) => invalid(request.id, error),
                },
                "set_delivery_hold"
                | "set_route_enabled"
                | "initialize_event_cursor"
                | "baseline_event_cursor"
                | "acknowledge_event_cursor_gap"
                | "initialize_inbound_cursor"
                | "baseline_telegram_cursor"
                | "reconcile_route"
                | "dispose_legacy" => self.handle_operator(request).await,
                _ => error_response(
                    request.id,
                    "unknown_method",
                    "the requested control method is not supported",
                    None,
                ),
            }
        })
    }
}

fn exact_route<'a>(routes: &'a [Route], name: &str) -> Result<&'a Route, &'static str> {
    let matches = routes
        .iter()
        .filter(|route| route.title.eq_ignore_ascii_case(name))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Err("not_found"),
        [route] => Ok(route),
        _ => Err("ambiguous"),
    }
}

fn route_matches_inbox(route: &Route, item: &InboxItem) -> bool {
    route
        .channels
        .iter()
        .any(|binding| match (binding, item.channel) {
            (ChannelBinding::Telegram { topic_id }, ChannelKind::Telegram) => {
                topic_id.to_string() == item.destination
            }
            (ChannelBinding::Signal { group_id }, ChannelKind::Signal) => {
                group_id.trim_end_matches('=') == item.destination.trim_end_matches('=')
            }
            (ChannelBinding::Slack { channel_id }, ChannelKind::Slack) => {
                channel_id == &item.destination
            }
            _ => false,
        })
}

fn inbound_envelope(item: &InboxItem, route: &Route) -> String {
    format!(
        "[Panetone channel message]\nChannel: {:?}\nRoute: {}\nFrom: {}\nMessage ID: {}\n\n{}",
        item.channel,
        route.title,
        item.sender.as_deref().unwrap_or("unknown"),
        item.external_id,
        item.body
    )
}

fn route_error(id: Uuid, role: &str, detail: &'static str) -> ControlResponse {
    error_response(
        id,
        format!("{role}_route_{detail}"),
        format!("{role} route is {detail}"),
        None,
    )
}

fn invalid(id: Uuid, error: serde_json::Error) -> ControlResponse {
    error_response(id, "invalid_request", error.to_string(), None)
}

fn internal(id: Uuid, error: impl std::fmt::Display) -> ControlResponse {
    error_response(id, "internal_error", error.to_string(), None)
}

fn error_string(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    use tempfile::tempdir;

    use super::*;
    use crate::domain::{AgentBinding, ChannelBinding, RouteId};

    fn mutation(action: OperatorAction) -> OperatorMutation {
        OperatorMutation {
            operation_id: Uuid::new_v4(),
            intent: None,
            action,
        }
    }

    #[tokio::test]
    async fn event_worker_drains_every_page_to_the_advertised_head() {
        let directory = tempdir().unwrap();
        let log = directory.path().join("events.log");
        let binary = directory.path().join("wakterm-fake");
        fs::write(
            &binary,
            format!(
                r#"#!/bin/bash
set -euo pipefail
operation="$*"
if [[ "$operation" == *"agent capabilities"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}}'
elif [[ "$operation" == *"agent events"*"--after 100"* ]]; then
  echo 100 >> '{}'
  echo '{{"schema":"wakterm.agent-events.v1","status":"ok","requested_after_sequence":100,"oldest_available_sequence":1,"latest_sequence":102,"next_after_sequence":101,"events":[{{"sequence":101,"event_id":"event-101","kind":"turn_started","agent_id":"agent-route","incarnation_id":"inc-route","turn_id":"turn-1"}}]}}'
elif [[ "$operation" == *"agent events"*"--after 101"* ]]; then
  echo 101 >> '{}'
  echo '{{"schema":"wakterm.agent-events.v1","status":"ok","requested_after_sequence":101,"oldest_available_sequence":1,"latest_sequence":102,"next_after_sequence":102,"events":[{{"sequence":102,"event_id":"event-102","kind":"turn_state_changed","agent_id":"agent-route","incarnation_id":"inc-route","turn_id":"turn-1","turn_state":"waiting_on_user"}}]}}'
else
  echo "unexpected fake invocation: $operation" >&2
  exit 9
fi
"#,
                log.display(),
                log.display(),
            ),
        )
        .unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o700)).unwrap();

        let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
        let route = Route {
            id: RouteId::new(Uuid::new_v4()),
            title: "route".into(),
            channels: vec![ChannelBinding::Telegram { topic_id: 10 }],
            agent: Some(AgentBinding {
                agent_id: "agent-route".into(),
                incarnation_id: "inc-route".into(),
                harness: "codex".into(),
                pane_id: Some(1),
            }),
            status: RouteStatus::Available,
        };
        store.save_route(route.clone(), 1).await.unwrap();
        for action in [
            OperatorAction::InitializeEventCursor { sequence: 100 },
            OperatorAction::SetRouteEnabled {
                route_id: route.id,
                enabled: true,
            },
            OperatorAction::SetDeliveryHold { held: false },
        ] {
            store
                .apply_operator_mutation(mutation(action), 2)
                .await
                .unwrap();
        }
        let service = ProductionService::new(
            store.clone(),
            WaktermCli::new(
                binary,
                directory.path().join("mux.sock"),
                Duration::from_secs(2),
            ),
            RealChannels::default(),
            None,
            SupervisorHandle::default(),
            vec!["event_stream.v1".into()],
            directory.path().join("control.sock"),
        );

        assert_eq!(service.event_once().await.unwrap(), 2);
        assert_eq!(
            store.promotion_status().await.unwrap().event_cursor,
            Some(102)
        );
        assert_eq!(fs::read_to_string(log).unwrap(), "100\n101\n");
        drop(service);
        store.shutdown().await.unwrap();
    }
}
