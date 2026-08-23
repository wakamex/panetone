use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use serde_json::json;
use uuid::Uuid;

use crate::channels::RealChannels;
use crate::control::{
    CONTROL_SCHEMA, ControlHandler, ControlRequest, ControlResponse, OutputDispositionParams,
    RouteEnsureParams, RouteInspectParams, SendParams, error_response, success_response,
};
use crate::domain::{
    AdmissionStatus, AgentBinding, ChannelBinding, ChannelKind, OutboxState, Route, RouteId,
    WorkflowId,
};
use crate::store::{EventCursorGap, InboxItem, RouteAgent, StoreHandle};
use crate::supervisor::SupervisorHandle;
use crate::wakterm::{EventRead, LiveRouteSnapshot, TerminalResult, WaktermCli, WaktermCliError};

use super::{OfflineService, ServiceError};

pub struct ProductionService {
    store: StoreHandle,
    wakterm: WaktermCli,
    channels: RealChannels,
    workflows: Arc<OfflineService>,
    health: SupervisorHandle,
    capabilities: Vec<String>,
    control_socket: PathBuf,
    started_at: Instant,
    last_agents: tokio::sync::RwLock<HashMap<RouteId, AgentBinding>>,
    route_changes: tokio::sync::Mutex<()>,
}

impl ProductionService {
    pub fn new(
        store: StoreHandle,
        wakterm: WaktermCli,
        channels: RealChannels,
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
            workflows,
            health,
            capabilities,
            control_socket,
            started_at: Instant::now(),
            last_agents: tokio::sync::RwLock::new(HashMap::new()),
            route_changes: tokio::sync::Mutex::new(()),
        }
    }

    pub async fn event_once(&self) -> Result<usize, String> {
        let Some(mut cursor) = self
            .store
            .get_metadata("wakterm_event_cursor".into())
            .await
            .map_err(error_string)?
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|_| "stored Wakterm event cursor is invalid".to_string())?
        else {
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
                    let route_agents = if events
                        .iter()
                        .any(|event| matches!(event.kind.as_str(), "assistant_message" | "plan"))
                    {
                        let routes = self.store.list_routes().await.map_err(error_string)?;
                        let live = self.wakterm.live_routes().await.map_err(error_string)?;
                        project_live_routes(&routes, &live)?
                    } else {
                        Vec::new()
                    };
                    let outcome = self
                        .store
                        .ingest_agent_events(
                            cursor,
                            next_after_sequence,
                            events,
                            route_agents,
                            now_ms(),
                        )
                        .await
                        .map_err(error_string)?;
                    if !outcome.last_agents.is_empty() {
                        let mut last_agents = self.last_agents.write().await;
                        for last in outcome.last_agents {
                            last_agents.insert(last.route_id, last.agent);
                        }
                    }
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
                        "Wakterm event cursor {requested_after_sequence} was older than retained sequence {oldest_available_sequence}; the cursor advanced to fresh catalog sequence {fresh_catalog_as_of_sequence}"
                    ));
                }
                EventRead::Unsupported => {
                    return Err("Wakterm event_stream.v1 is unavailable".into());
                }
            }
        }
    }

    pub async fn outbox_once(&self) -> Result<usize, String> {
        let Some(mut item) = self
            .store
            .pending_outbox()
            .await
            .map_err(error_string)?
            .into_iter()
            .next()
        else {
            return Ok(0);
        };
        let expected_attempts = item.attempts;
        item.state = OutboxState::Delivering;
        item.attempts += 1;
        self.store
            .save_outbox(item.clone(), now_ms())
            .await
            .map_err(error_string)?;
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
        Ok(1)
    }

    pub async fn busy_once(&self) -> Result<usize, String> {
        let workflows = self.store.awaiting_target().await.map_err(error_string)?;
        if workflows.is_empty() {
            return Ok(0);
        }
        let live = self.wakterm.live_routes().await.map_err(error_string)?;
        let mut attempted = 0;
        for workflow in workflows {
            let Some(route) = self
                .store
                .get_route(workflow.workflow.target_route_id)
                .await
                .map_err(error_string)?
            else {
                continue;
            };
            let preferred = self.last_agents.read().await.get(&route.id).cloned();
            let Some(binding) = resolve_live(&live, &route, preferred.as_ref())? else {
                continue;
            };
            let live_route = route.with_agent(binding);
            attempted += 1;
            match self
                .workflows
                .retry_busy_target(workflow.command.id, &live_route, now_ms())
                .await
            {
                Ok(_) | Err(ServiceError::RouteUnavailable(_)) => {}
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(attempted)
    }

    pub async fn terminal_once(&self) -> Result<usize, String> {
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
        let pending = self.store.pending_returns().await.map_err(error_string)?;
        if pending.is_empty() {
            return Ok(0);
        }
        let live = self.wakterm.live_routes().await.map_err(error_string)?;
        let mut attempted = 0;
        for returned in pending {
            let workflow = self
                .store
                .get_workflow(returned.workflow_id)
                .await
                .map_err(error_string)?
                .ok_or_else(|| "pending return workflow is missing".to_string())?;
            let route = self
                .store
                .get_route(workflow.workflow.source_route_id)
                .await
                .map_err(error_string)?
                .ok_or_else(|| "pending return source route is missing".to_string())?;
            let preferred = self.last_agents.read().await.get(&route.id).cloned();
            let live_route = match resolve_live(&live, &route, preferred.as_ref())? {
                Some(binding) => route.with_agent(binding),
                None => route,
            };
            attempted += 1;
            match self
                .workflows
                .deliver_pending_return(returned.workflow_id, &live_route, now_ms())
                .await
            {
                Ok(_) | Err(ServiceError::RouteUnavailable(_)) => {}
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(attempted)
    }

    pub async fn inbox_once(&self) -> Result<usize, String> {
        let pending = self.store.pending_inbox().await.map_err(error_string)?;
        if pending.is_empty() {
            return Ok(0);
        }
        let routes = self.store.list_routes().await.map_err(error_string)?;
        let live = self.wakterm.live_routes().await.map_err(error_string)?;
        let mut attempted = 0;
        for mut item in pending {
            let matching = routes
                .iter()
                .filter(|route| route_matches_inbox(route, &item))
                .collect::<Vec<_>>();
            let [route] = matching.as_slice() else {
                continue;
            };
            let reply_agent = match item.reply_to_external_id.as_ref() {
                Some(receipt) => self
                    .store
                    .find_outbox_agent(item.channel, item.destination.clone(), receipt.clone())
                    .await
                    .map_err(error_string)?,
                None => None,
            };
            let last_agent = self.last_agents.read().await.get(&route.id).cloned();
            let preferred = reply_agent.as_ref().or(last_agent.as_ref());
            let Some(binding) = resolve_live(&live, route, preferred)? else {
                continue;
            };
            attempted += 1;
            item.state = "admission_prepared".into();
            self.store
                .save_inbox(item.clone(), "pending", None)
                .await
                .map_err(error_string)?;
            let receipt = match self
                .wakterm
                .admit(item.id, &binding, &item.body, false, 0)
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
            receipt.validate(item.id, &binding).map_err(error_string)?;
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
        let live = match self.wakterm.live_routes().await {
            Ok(live) => live,
            Err(error) => {
                return error_response(id, "route_resolution_failed", error.to_string(), None);
            }
        };
        let last_agents = self.last_agents.read().await;
        let source_binding = match resolve_live(&live, source, last_agents.get(&source.id)) {
            Ok(Some(binding)) => binding,
            Ok(None) => return route_unavailable(id, "source", source),
            Err(error) => return error_response(id, "route_resolution_failed", error, None),
        };
        let target_binding = match resolve_live(&live, target, last_agents.get(&target.id)) {
            Ok(Some(binding)) => binding,
            Ok(None) => return route_unavailable(id, "target", target),
            Err(error) => return error_response(id, "route_resolution_failed", error, None),
        };
        drop(last_agents);
        let source = source.with_agent(source_binding);
        let target = target.with_agent(target_binding);
        match self
            .workflows
            .submit(params.into_command(id), &source, &target, now_ms())
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
                    },
                    "control": {"path": self.control_socket},
                    "tasks": self.health.snapshot(),
                }),
            ),
            Err(error) => internal(id, error),
        }
    }

    async fn handle_route_inspect(&self, id: Uuid, params: RouteInspectParams) -> ControlResponse {
        let routes = match self.store.list_routes().await {
            Ok(routes) => routes,
            Err(error) => return internal(id, error),
        };
        let route = match exact_route(&routes, &params.title) {
            Ok(route) => route,
            Err(detail) => return route_error(id, "", detail),
        };
        self.route_response(id, route, false, false, None).await
    }

    async fn handle_route_ensure(&self, id: Uuid, params: RouteEnsureParams) -> ControlResponse {
        if params.title.is_empty()
            || params.title.trim() != params.title
            || params.title.chars().count() > 128
            || params
                .telegram_topic_id
                .is_some_and(|topic_id| topic_id <= 0)
        {
            return error_response(
                id,
                "invalid_params",
                "route title must be 1 to 128 characters without surrounding whitespace, and a Telegram topic ID must be positive",
                None,
            );
        }
        let _change = self.route_changes.lock().await;
        let routes = match self.store.list_routes().await {
            Ok(routes) => routes,
            Err(error) => return internal(id, error),
        };
        let mut route = match exact_route(&routes, &params.title) {
            Ok(route) => route.clone(),
            Err("not_found") => {
                let live = match self.wakterm.live_routes().await {
                    Ok(live) => live,
                    Err(error) => {
                        return error_response(
                            id,
                            "route_resolution_failed",
                            error.to_string(),
                            None,
                        );
                    }
                };
                match live.route(&params.title) {
                    Ok(_) => {}
                    Err(
                        WaktermCliError::RouteNotFound(_) | WaktermCliError::RouteUnavailable(_),
                    ) => {
                        return error_response(
                            id,
                            "route_unavailable",
                            format!(
                                "route {:?} cannot be created without one live Wakterm agent tab",
                                params.title
                            ),
                            None,
                        );
                    }
                    Err(WaktermCliError::RouteAmbiguous(_)) => {
                        return error_response(
                            id,
                            "route_ambiguous",
                            format!(
                                "more than one live Wakterm tab matches route {:?}",
                                params.title
                            ),
                            None,
                        );
                    }
                    Err(error) => {
                        return error_response(
                            id,
                            "route_resolution_failed",
                            error.to_string(),
                            None,
                        );
                    }
                }
                let topic_id = match params.telegram_topic_id {
                    Some(topic_id) => topic_id,
                    None => match self.channels.create_telegram_topic(&params.title).await {
                        Ok(topic_id) => topic_id,
                        Err(error) => {
                            return error_response(
                                id,
                                "route_binding_failed",
                                error.to_string(),
                                None,
                            );
                        }
                    },
                };
                let route = Route {
                    id: RouteId::random(),
                    title: params.title,
                    channels: vec![ChannelBinding::Telegram { topic_id }],
                    agent: None,
                };
                if let Err(error) = self.store.save_route(route.clone(), now_ms()).await {
                    return internal(id, error);
                }
                return self
                    .route_response(id, &route, true, true, Some(live))
                    .await;
            }
            Err(detail) => return route_error(id, "", detail),
        };

        let existing_topic = route.channels.iter().find_map(|binding| match binding {
            ChannelBinding::Telegram { topic_id } => Some(*topic_id),
            _ => None,
        });
        if let (Some(existing), Some(requested)) = (existing_topic, params.telegram_topic_id)
            && existing != requested
        {
            return error_response(
                id,
                "route_binding_conflict",
                format!(
                    "route {:?} is already bound to Telegram topic {existing}",
                    route.title
                ),
                Some(json!({"telegram_topic_id": existing})),
            );
        }
        let mut binding_created = false;
        if existing_topic.is_none() {
            let topic_id = match params.telegram_topic_id {
                Some(topic_id) => topic_id,
                None => match self.channels.create_telegram_topic(&route.title).await {
                    Ok(topic_id) => topic_id,
                    Err(error) => {
                        return error_response(id, "route_binding_failed", error.to_string(), None);
                    }
                },
            };
            route.channels.push(ChannelBinding::Telegram { topic_id });
            if let Err(error) = self.store.save_route(route.clone(), now_ms()).await {
                return internal(id, error);
            }
            binding_created = true;
        }
        self.route_response(id, &route, false, binding_created, None)
            .await
    }

    async fn route_response(
        &self,
        id: Uuid,
        route: &Route,
        created: bool,
        binding_created: bool,
        live: Option<LiveRouteSnapshot>,
    ) -> ControlResponse {
        let live = match live {
            Some(live) => live,
            None => match self.wakterm.live_routes().await {
                Ok(live) => live,
                Err(error) => {
                    return error_response(id, "route_resolution_failed", error.to_string(), None);
                }
            },
        };
        let live = match live.route(&route.title) {
            Ok(live) => json!({"status": "available", "agents": live.agents}),
            Err(WaktermCliError::RouteNotFound(_) | WaktermCliError::RouteUnavailable(_)) => {
                json!({"status": "unavailable", "agents": []})
            }
            Err(WaktermCliError::RouteAmbiguous(_)) => {
                return error_response(
                    id,
                    "route_ambiguous",
                    format!(
                        "more than one live Wakterm tab matches route {:?}",
                        route.title
                    ),
                    None,
                );
            }
            Err(error) => {
                return error_response(id, "route_resolution_failed", error.to_string(), None);
            }
        };
        let event_cursor = match self.store.get_metadata("wakterm_event_cursor".into()).await {
            Ok(Some(value)) => match value.parse::<u64>() {
                Ok(value) => value,
                Err(_) => return internal(id, "stored Wakterm event cursor is invalid"),
            },
            Ok(None) => return internal(id, "Wakterm event cursor is unavailable"),
            Err(error) => return internal(id, error),
        };
        success_response(
            id,
            json!({
                "created": created,
                "binding_created": binding_created,
                "route": route,
                "live": live,
                "event_cursor": event_cursor,
            }),
        )
    }

    async fn handle_output_disposition(
        &self,
        id: Uuid,
        params: OutputDispositionParams,
    ) -> ControlResponse {
        if params.route.is_empty()
            || params.agent_id.is_empty()
            || params.incarnation_id.is_empty()
            || params.expected_text.is_empty()
        {
            return error_response(
                id,
                "invalid_params",
                "output disposition requires route, agent_id, incarnation_id, and expected_text",
                None,
            );
        }
        let routes = match self.store.list_routes().await {
            Ok(routes) => routes,
            Err(error) => return internal(id, error),
        };
        let route = match exact_route(&routes, &params.route) {
            Ok(route) => route,
            Err(detail) => return route_error(id, "", detail),
        };
        let snapshot = match self
            .store
            .output_disposition(
                params.agent_id,
                params.incarnation_id,
                params.after_sequence,
                params.expected_text,
            )
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => return internal(id, error),
        };
        let Some(output) = snapshot.output else {
            return success_response(
                id,
                json!({
                    "disposition": "pending",
                    "event_cursor": snapshot.event_cursor,
                    "route": route,
                }),
            );
        };
        let disposition = if output.disposition == "projected" && output.route_id == Some(route.id)
        {
            "projected"
        } else if output.disposition == "unrouted" {
            "unrouted"
        } else if output.route_id.is_some() && output.route_id != Some(route.id) {
            "misrouted"
        } else {
            output.disposition.as_str()
        };
        let actual_route = output
            .route_id
            .and_then(|route_id| routes.iter().find(|candidate| candidate.id == route_id));
        success_response(
            id,
            json!({
                "disposition": disposition,
                "event_cursor": snapshot.event_cursor,
                "route": route,
                "actual_route": actual_route,
                "event": output.event,
            }),
        )
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
                "route.inspect" => {
                    match serde_json::from_value::<RouteInspectParams>(request.params) {
                        Ok(params) => self.handle_route_inspect(request.id, params).await,
                        Err(error) => invalid(request.id, error),
                    }
                }
                "route.ensure" => {
                    match serde_json::from_value::<RouteEnsureParams>(request.params) {
                        Ok(params) => self.handle_route_ensure(request.id, params).await,
                        Err(error) => invalid(request.id, error),
                    }
                }
                "output.disposition" => {
                    match serde_json::from_value::<OutputDispositionParams>(request.params) {
                        Ok(params) => self.handle_output_disposition(request.id, params).await,
                        Err(error) => invalid(request.id, error),
                    }
                }
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

fn resolve_live(
    live: &LiveRouteSnapshot,
    route: &Route,
    preferred: Option<&AgentBinding>,
) -> Result<Option<AgentBinding>, String> {
    match live.resolve(&route.title, preferred) {
        Ok(binding) => Ok(Some(binding)),
        Err(WaktermCliError::RouteNotFound(_) | WaktermCliError::RouteUnavailable(_)) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

fn project_live_routes(
    routes: &[Route],
    live: &LiveRouteSnapshot,
) -> Result<Vec<RouteAgent>, String> {
    let mut projected = Vec::<RouteAgent>::new();
    for route in routes {
        let live_route = match live.route(&route.title) {
            Ok(live_route) => live_route,
            Err(WaktermCliError::RouteNotFound(_) | WaktermCliError::RouteUnavailable(_)) => {
                continue;
            }
            Err(error) => return Err(error.to_string()),
        };
        for agent in &live_route.agents {
            if projected.iter().any(|candidate| {
                candidate.route_id != route.id
                    && candidate.agent.agent_id == agent.agent_id
                    && candidate.agent.incarnation_id == agent.incarnation_id
            }) {
                return Err(format!(
                    "live Wakterm agent {} matches more than one Panetone route",
                    agent.agent_id
                ));
            }
            projected.push(RouteAgent {
                route_id: route.id,
                agent: agent.clone(),
            });
        }
    }
    Ok(projected)
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
            _ => false,
        })
}

fn route_error(id: Uuid, role: &str, detail: &'static str) -> ControlResponse {
    let prefix = if role.is_empty() {
        String::new()
    } else {
        format!("{role}_")
    };
    error_response(
        id,
        format!("{prefix}route_{detail}"),
        if role.is_empty() {
            format!("route is {detail}")
        } else {
            format!("{role} route is {detail}")
        },
        None,
    )
}

fn route_unavailable(id: Uuid, role: &str, route: &Route) -> ControlResponse {
    error_response(
        id,
        format!("{role}_route_unavailable"),
        format!("{role} route {:?} has no live agent pane", route.title),
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
    use crate::domain::{ChannelBinding, EffectId, RouteId};
    use crate::store::InboxItem;

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
elif [[ "$operation" == *"agent catalog"* ]]; then
  echo catalog >> '{}'
  echo '{{"schema":"wakterm.agent-api.v1","as_of_event_sequence":102,"agents":[{{"agent_id":"agent-route","incarnation_id":"inc-route","pane_id":1,"name":"route_codex","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-17T00:00:00Z"}}]}}'
elif [[ "$operation" == *"list --format json"* ]]; then
  echo list >> '{}'
  echo '[{{"pane_id":1,"tab_id":2,"window_id":3,"effective_title":"route"}}]'
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
            agent: None,
        };
        store.save_route(route.clone(), 1).await.unwrap();
        store.initialize_event_cursor(100).await.unwrap();
        let service = ProductionService::new(
            store.clone(),
            WaktermCli::new(
                binary,
                directory.path().join("mux.sock"),
                Duration::from_secs(2),
            ),
            RealChannels::default(),
            SupervisorHandle::default(),
            vec!["event_stream.v1".into()],
            directory.path().join("control.sock"),
        );

        assert_eq!(service.event_once().await.unwrap(), 2);
        assert_eq!(store.status().await.unwrap().event_cursor, Some(102));
        assert_eq!(fs::read_to_string(log).unwrap(), "100\n101\n");
        drop(service);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn authorized_channel_input_reaches_wakterm_without_transport_markup() {
        let directory = tempdir().unwrap();
        let prompt = directory.path().join("prompt.txt");
        let binary = directory.path().join("wakterm-fake");
        fs::write(
            &binary,
            format!(
                r#"#!/bin/bash
set -euo pipefail
operation="$*"
if [[ "$operation" == *"agent capabilities"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}}'
elif [[ "$operation" == *"agent catalog"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","as_of_event_sequence":10,"agents":[{{"agent_id":"agent-route","incarnation_id":"inc-route","pane_id":1,"name":"route","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-17T00:00:00Z"}}]}}'
elif [[ "$operation" == *"list --format json"* ]]; then
  echo '[{{"pane_id":1,"tab_id":2,"window_id":3,"effective_title":"route"}}]'
elif [[ "$operation" == *"agent admit"* ]]; then
  cat > '{}'
  echo '{{"schema":"wakterm.agent-api.v1","request_id":"00000000-0000-0000-0000-00000000007b","status":"accepted","definitive":true,"prompt_written":true,"agent_id":"agent-route","incarnation_id":"inc-route","detail":null}}'
else
  echo "unexpected fake invocation: $operation" >&2
  exit 9
fi
"#,
                prompt.display(),
            ),
        )
        .unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o700)).unwrap();

        let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
        store
            .save_route(
                Route {
                    id: RouteId::new(Uuid::new_v4()),
                    title: "route".into(),
                    channels: vec![ChannelBinding::Telegram { topic_id: 10 }],
                    agent: None,
                },
                1,
            )
            .await
            .unwrap();
        let body = "did the computer restart?\n\nAnswer directly.";
        store
            .accept_inbox(InboxItem {
                id: EffectId::new(Uuid::from_u128(123)),
                channel: ChannelKind::Telegram,
                external_id: "update-1".into(),
                destination: "10".into(),
                sender_id: Some("42".into()),
                sender: Some("Mihai".into()),
                reply_to_external_id: None,
                body: body.into(),
                state: "pending".into(),
                created_at_ms: 1,
            })
            .await
            .unwrap();
        let service = ProductionService::new(
            store.clone(),
            WaktermCli::new(
                binary,
                directory.path().join("mux.sock"),
                Duration::from_secs(2),
            ),
            RealChannels::default(),
            SupervisorHandle::default(),
            vec!["event_stream.v1".into()],
            directory.path().join("control.sock"),
        );

        assert_eq!(service.inbox_once().await.unwrap(), 1);
        assert_eq!(fs::read_to_string(prompt).unwrap(), body);
        assert_eq!(store.status().await.unwrap().pending_inbox, 0);
        drop(service);
        store.shutdown().await.unwrap();
    }
}
