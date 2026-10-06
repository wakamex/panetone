use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::channels::{InboundMessage, TelegramFormTap};
use crate::channels::{RealChannels, TelegramApprovalResponse};
use crate::control::{
    CONTROL_SCHEMA, ControlHandler, ControlRequest, ControlResponse, OutputDispositionParams,
    RouteEnsureParams, RouteInspectParams, SendParams, error_response, success_response,
};
use crate::domain::{
    AdmissionStatus, AgentBinding, ChannelBinding, ChannelKind, EffectId, OutboxAction, OutboxItem,
    OutboxState, Route, RouteId, WorkflowId, WorkflowState,
};
use crate::store::{EventCursorGap, InboxItem, RouteAgent, StoreHandle, StoredApproval};
use crate::supervisor::SupervisorHandle;
use crate::wakterm::form::{self, FormAction, FormState};
use crate::wakterm::{EventRead, LiveRouteSnapshot, TerminalResult, WaktermCli, WaktermCliError};

use super::offline::channel_destination;
use super::{OfflineService, ServiceError};

/// Uncertain attempts after which an outbox item is left indeterminate
/// instead of risking more duplicate posts.
const MAX_UNCERTAIN_ATTEMPTS: u32 = 3;

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
    /// Agent problems seen on the previous route health pass. A problem is
    /// reported only when it persists across two passes, because a starting or
    /// restored agent is briefly unregistered or unobserved.
    health_candidates: tokio::sync::Mutex<BTreeSet<String>>,
}

/// A question form message to update after an answer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FormUpdate {
    /// The short confirmation shown to the user.
    pub toast: String,
    pub message_id: i64,
    pub text: String,
    pub actions: Vec<OutboxAction>,
}

/// Route health metadata key: reported agent problems by problem key.
const ROUTE_HEALTH_KEY: &str = "route_health";

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ReportedProblem {
    route_id: RouteId,
    route_title: String,
    description: String,
}

impl ProductionService {
    pub async fn resolve_telegram_approval(
        &self,
        response: &TelegramApprovalResponse,
    ) -> Result<String, String> {
        let stored = self
            .store
            .get_approval(response.request_id.clone())
            .await
            .map_err(error_string)?
            .ok_or_else(|| "approval request is unknown or expired".to_string())?;
        let route = self
            .store
            .get_route(stored.route_id)
            .await
            .map_err(error_string)?
            .ok_or_else(|| "approval route no longer exists".to_string())?;
        let correct_topic = route.channels.iter().any(|binding| {
            matches!(binding, ChannelBinding::Telegram { topic_id } if topic_id.to_string() == response.destination)
        });
        if !correct_topic {
            return Err("approval button does not belong to this topic".into());
        }
        let choice = stored
            .request
            .choices
            .iter()
            .find(|choice| choice.id == response.choice_id)
            .ok_or_else(|| "approval choice is no longer available".to_string())?;
        self.wakterm
            .resolve_approval(
                &stored.request.request_id,
                &stored.request.agent_id,
                &stored.request.incarnation_id,
                &choice.id,
                None,
            )
            .await
            .map_err(error_string)?;
        Ok(choice.label.clone())
    }

    /// Applies a tap on a question form message and returns the confirmation
    /// to show, with the form's updated text and buttons.
    pub async fn answer_form_tap(&self, tap: &TelegramFormTap) -> Result<FormUpdate, String> {
        let stored = self.form_request(&tap.request_id, &tap.destination).await?;
        let mut state = self.form_state(&tap.request_id).await?;
        if state.closed.is_some() {
            return Err("this form was already answered".into());
        }
        let toast = match tap.action {
            FormAction::Option { question, option } => {
                form::tap(&stored.request, &mut state, question, option)?
            }
            FormAction::Submit => {
                let answers = form::answers_json(&state);
                self.close_form(&stored, &mut state, "submit", Some(&answers), "Submitted.")
                    .await?
            }
            FormAction::Chat => {
                self.close_form(
                    &stored,
                    &mut state,
                    "chat",
                    None,
                    "Sent back to the agent to chat about.",
                )
                .await?
            }
            FormAction::Cancel => {
                self.close_form(&stored, &mut state, "cancel", None, "Cancelled.")
                    .await?
            }
        };
        self.save_form_state(&tap.request_id, &state).await?;
        Ok(FormUpdate {
            toast,
            message_id: tap.message_id,
            text: form::text(&stored.request, &state),
            actions: form::actions(&stored.request, &state),
        })
    }

    /// Records a reply to a question form message as a typed answer. Returns
    /// `None` when the message does not reply to an open form.
    pub async fn answer_form_reply(
        &self,
        message: &InboundMessage,
    ) -> Result<Option<FormUpdate>, String> {
        let Some(reply_to) = message.reply_to_external_id.clone() else {
            return Ok(None);
        };
        let Some(posted) = self
            .store
            .find_delivered_outbox(
                message.channel,
                message.destination.clone(),
                reply_to.clone(),
            )
            .await
            .map_err(error_string)?
        else {
            return Ok(None);
        };
        let Some((request_id, _)) = posted
            .actions
            .first()
            .and_then(|action| form::parse_callback(&action.id))
        else {
            return Ok(None);
        };
        let message_id = reply_to
            .parse()
            .map_err(|_| "the form message id is invalid".to_string())?;
        let stored = self.form_request(&request_id, &message.destination).await?;
        let mut state = self.form_state(&request_id).await?;
        if state.closed.is_some() {
            return Err("this form was already answered".into());
        }
        let toast = match form::parse_reply(&message.body) {
            Some((question, text)) => {
                form::type_answer(&stored.request, &mut state, question, text)?
            }
            None => {
                "Start a typed answer with its question number, for example \"1: your answer\"."
                    .into()
            }
        };
        self.save_form_state(&request_id, &state).await?;
        Ok(Some(FormUpdate {
            toast,
            message_id,
            text: form::text(&stored.request, &state),
            actions: form::actions(&stored.request, &state),
        }))
    }

    async fn form_request(
        &self,
        request_id: &str,
        destination: &str,
    ) -> Result<StoredApproval, String> {
        let stored = self
            .store
            .get_approval(request_id.to_string())
            .await
            .map_err(error_string)?
            .ok_or_else(|| "this form is unknown or expired".to_string())?;
        let route = self
            .store
            .get_route(stored.route_id)
            .await
            .map_err(error_string)?
            .ok_or_else(|| "this form's route no longer exists".to_string())?;
        let in_topic = route.channels.iter().any(|binding| {
            matches!(binding, ChannelBinding::Telegram { topic_id } if topic_id.to_string() == destination)
        });
        if !in_topic || !form::answerable(&stored.request) {
            return Err("this form does not belong to this topic".into());
        }
        Ok(stored)
    }

    async fn form_state(&self, request_id: &str) -> Result<FormState, String> {
        match self
            .store
            .get_metadata(format!("form:{request_id}"))
            .await
            .map_err(error_string)?
        {
            Some(json) => serde_json::from_str(&json).map_err(error_string),
            None => Ok(FormState::default()),
        }
    }

    async fn save_form_state(&self, request_id: &str, state: &FormState) -> Result<(), String> {
        self.store
            .set_metadata(
                format!("form:{request_id}"),
                serde_json::to_string(state).map_err(error_string)?,
            )
            .await
            .map_err(error_string)
    }

    /// Resolves the form through Wakterm. A refusal, such as Wakterm stopping
    /// before submitting, leaves the form open and is shown to the user.
    async fn close_form(
        &self,
        stored: &StoredApproval,
        state: &mut FormState,
        choice: &str,
        answers: Option<&str>,
        closed: &str,
    ) -> Result<String, String> {
        self.wakterm
            .resolve_approval(
                &stored.request.request_id,
                &stored.request.agent_id,
                &stored.request.incarnation_id,
                choice,
                answers,
            )
            .await
            .map_err(error_string)?;
        state.closed = Some(closed.to_string());
        Ok(closed.to_string())
    }

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
            health_candidates: tokio::sync::Mutex::new(BTreeSet::new()),
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
                    let route_agents = if events.iter().any(|event| {
                        event.kind == "agent_lifecycle"
                            || event.kind == "approval_requested"
                            || event.visible_output_body().is_ok_and(|body| body.is_some())
                    }) {
                        let live = self.wakterm.live_routes().await.map_err(error_string)?;
                        self.reconcile_live_routes(&live).await?;
                        let routes = self.store.list_routes().await.map_err(error_string)?;
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

    pub async fn reconcile_live_routes(&self, live: &LiveRouteSnapshot) -> Result<usize, String> {
        if self.channels.telegram.is_none() {
            return Ok(0);
        }
        let _change = self.route_changes.lock().await;
        let mut routes = self.store.list_routes().await.map_err(error_string)?;
        let mut created = 0;
        for live_route in live.routes() {
            if live_route.agents.is_empty() {
                continue;
            }
            if !valid_route_title(&live_route.title) {
                tracing::warn!(
                    title = %live_route.title,
                    "live Wakterm title cannot be used as a Panetone route"
                );
                continue;
            }
            match exact_route(&routes, &live_route.title) {
                Ok(_) => continue,
                Err("not_found") => {}
                Err(detail) => {
                    return Err(format!(
                        "cannot reconcile live Wakterm route {:?}: {detail}",
                        live_route.title
                    ));
                }
            }
            let topic_id = self
                .channels
                .create_telegram_topic(&live_route.title)
                .await
                .map_err(error_string)?;
            let route = telegram_route(live_route.title.clone(), topic_id);
            self.store
                .save_route(route.clone(), now_ms())
                .await
                .map_err(error_string)?;
            tracing::info!(
                title = %route.title,
                topic_id,
                "created route for live Wakterm agent"
            );
            routes.push(route);
            created += 1;
        }
        Ok(created)
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
        let attempt = OutboxItem {
            body: item.attempt_body(),
            ..item.clone()
        };
        match self.channels.send(&attempt).await {
            Ok(receipt) => {
                item.state = OutboxState::Delivered;
                item.external_receipt = Some(receipt.external_id);
                item.last_error = None;
            }
            Err(error) => {
                if error.may_have_delivered() {
                    item.uncertain_attempts += 1;
                }
                tracing::warn!(
                    effect_id = %item.id,
                    channel = ?item.kind,
                    attempts = item.attempts,
                    may_have_posted = error.may_have_delivered(),
                    error = %error,
                    "channel post failed"
                );
                item.state = if !error.retryable() {
                    OutboxState::Failed
                } else if item.uncertain_attempts >= MAX_UNCERTAIN_ATTEMPTS {
                    // Each further try risks another visible duplicate.
                    OutboxState::Indeterminate
                } else {
                    OutboxState::Pending
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
            // Keep the agent chosen when the message was sent; select falls
            // back to that agent's replacement only if it is gone.
            let Some(binding) =
                resolve_live(&live, &route, Some(&workflow.workflow.observed_target))?
            else {
                continue;
            };
            let live_route = route.with_agent(binding);
            attempted += 1;
            match self
                .workflows
                .retry_busy_target(workflow.command.id, &live_route, now_ms())
                .await
            {
                Ok(_)
                | Err(ServiceError::RouteUnavailable(_))
                | Err(ServiceError::AdmissionFailed(
                    WorkflowState::Failed | WorkflowState::Indeterminate,
                ))
                | Err(ServiceError::AdmissionRejected {
                    state: WorkflowState::Failed | WorkflowState::Indeterminate,
                    ..
                }) => {}
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(attempted)
    }

    pub async fn terminal_once(&self) -> Result<usize, String> {
        if !self
            .store
            .has_unresolved_terminal()
            .await
            .map_err(error_string)?
        {
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
                if let Some(target) = workflow.workflow.submitted_target.clone() {
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
                } else if workflow.workflow.state == WorkflowState::AdmissionPrepared {
                    tracing::debug!(
                        request_id = %request_id,
                        terminal_event_sequence = terminal.terminal_event_sequence,
                        "deferring return terminal until admission state is durable"
                    );
                    break;
                } else {
                    tracing::warn!(
                        request_id = %request_id,
                        terminal_event_sequence = terminal.terminal_event_sequence,
                        workflow_state = ?workflow.workflow.state,
                        "ignoring return terminal for a workflow that was never submitted"
                    );
                }
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
            let live_route = if let Some(binding) = live.current_agent(&returned.agent.source) {
                route.with_agent(binding)
            } else {
                let preferred = self.last_agents.read().await.get(&route.id).cloned();
                match resolve_live(&live, &route, preferred.as_ref())? {
                    Some(binding) => route.with_agent(binding),
                    None => route,
                }
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
                    self.notify_unconfirmed(&item, route, &error.to_string())
                        .await?;
                    item.state = "indeterminate".into();
                    self.store
                        .save_inbox(item, "admission_prepared", None)
                        .await
                        .map_err(error_string)?;
                    return Err(error.to_string());
                }
            };
            receipt.validate(item.id, &binding).map_err(error_string)?;
            item.receipt = Some(receipt.clone());
            item.state = match receipt.status {
                AdmissionStatus::Accepted => "delivered",
                AdmissionStatus::Busy => {
                    let steering = match self.wakterm.steer(&binding, &item.body).await {
                        Ok(steering) => steering,
                        // Nothing was written; a later pass retries.
                        Err(WaktermCliError::TargetBlocked(_)) => {
                            item.state = "pending".into();
                            item.receipt = None;
                            self.store
                                .save_inbox(item, "admission_prepared", None)
                                .await
                                .map_err(error_string)?;
                            continue;
                        }
                        Err(error) => {
                            self.notify_unconfirmed(&item, route, &error.to_string())
                                .await?;
                            item.state = "indeterminate".into();
                            self.store
                                .save_inbox(item, "admission_prepared", None)
                                .await
                                .map_err(error_string)?;
                            return Err(error.to_string());
                        }
                    };
                    item.steering_acknowledged = Some(steering.acknowledged());
                    if steering.acknowledged() {
                        "delivered"
                    } else {
                        tracing::warn!(
                            agent_id = %binding.agent_id,
                            pane_id = ?binding.pane_id,
                            "Wakterm submitted channel steering without observer acknowledgement"
                        );
                        self.notify_unconfirmed(
                            &item,
                            route,
                            "the message was typed into the busy agent's turn, but the agent did not acknowledge it",
                        )
                        .await?;
                        "indeterminate"
                    }
                }
                AdmissionStatus::Indeterminate => {
                    let detail = receipt
                        .detail
                        .as_deref()
                        .unwrap_or("Wakterm could not confirm the agent received it");
                    self.notify_unconfirmed(&item, route, detail).await?;
                    "indeterminate"
                }
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

    /// Tells the sender's chat that an inbound message may not have reached the
    /// agent. The text may still be sitting in the pane, so the notice asks the
    /// user to check there rather than resending.
    async fn notify_unconfirmed(
        &self,
        item: &InboxItem,
        route: &Route,
        detail: &str,
    ) -> Result<(), String> {
        let namespace = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            b"https://panetone.dev/inbox/unconfirmed/v1",
        );
        let notice = OutboxItem {
            id: EffectId::new(Uuid::new_v5(&namespace, item.id.to_string().as_bytes())),
            route_id: Some(route.id),
            sender_harness: None,
            source_agent: None,
            kind: item.channel,
            destination: item.destination.clone(),
            body: format!(
                "[unconfirmed] {} may not have received your message: {detail}. \
                 Check the pane before resending; the text may still be there.",
                route.title
            ),
            attachments: Vec::new(),
            actions: Vec::new(),
            state: OutboxState::Pending,
            attempts: 0,
            last_error: None,
            external_receipt: None,
            uncertain_attempts: 0,
        };
        self.store
            .enqueue_outbox(None, notice, now_ms())
            .await
            .map(|_| ())
            .map_err(error_string)
    }

    /// Posts a notice in a route's channel when one of its agents becomes
    /// unusable for Panetone, and again when that problem clears.
    pub async fn route_health_once(&self) -> Result<usize, String> {
        let problems = self.wakterm.agent_problems().await.map_err(error_string)?;
        let routes = self.store.list_routes().await.map_err(error_string)?;
        let current = problems
            .into_iter()
            .filter_map(|problem| {
                let route = routes
                    .iter()
                    .find(|route| route.title.eq_ignore_ascii_case(&problem.title))?;
                Some((
                    problem.key(),
                    ReportedProblem {
                        route_id: route.id,
                        route_title: route.title.clone(),
                        description: problem.description(),
                    },
                ))
            })
            .collect::<BTreeMap<_, _>>();
        let confirmed = {
            let mut candidates = self.health_candidates.lock().await;
            let confirmed = current
                .iter()
                .filter(|(key, _)| candidates.contains(*key))
                .map(|(key, problem)| (key.clone(), problem.clone()))
                .collect::<BTreeMap<_, _>>();
            *candidates = current.keys().cloned().collect();
            confirmed
        };
        let mut reported = self.reported_problems().await?;
        let mut posted = 0;
        for (key, problem) in &confirmed {
            if !reported.contains_key(key) {
                self.post_route_health(key, problem, "Agent problem", now_ms())
                    .await?;
                reported.insert(key.clone(), problem.clone());
                posted += 1;
            }
        }
        let cleared = reported
            .keys()
            .filter(|key| !current.contains_key(*key))
            .cloned()
            .collect::<Vec<_>>();
        for key in cleared {
            if let Some(problem) = reported.remove(&key) {
                self.post_route_health(&key, &problem, "Resolved", now_ms())
                    .await?;
                posted += 1;
            }
        }
        self.store
            .set_metadata(
                ROUTE_HEALTH_KEY.into(),
                serde_json::to_string(&reported).map_err(error_string)?,
            )
            .await
            .map_err(error_string)?;
        Ok(posted)
    }

    async fn reported_problems(&self) -> Result<BTreeMap<String, ReportedProblem>, String> {
        match self
            .store
            .get_metadata(ROUTE_HEALTH_KEY.into())
            .await
            .map_err(error_string)?
        {
            Some(json) => serde_json::from_str(&json).map_err(error_string),
            None => Ok(BTreeMap::new()),
        }
    }

    async fn post_route_health(
        &self,
        key: &str,
        problem: &ReportedProblem,
        label: &str,
        now_ms: i64,
    ) -> Result<(), String> {
        let Some(route) = self
            .store
            .get_route(problem.route_id)
            .await
            .map_err(error_string)?
        else {
            return Ok(());
        };
        let Ok((kind, destination)) = channel_destination(&route) else {
            return Ok(());
        };
        let namespace = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            b"https://panetone.dev/route-health/v1",
        );
        let notice = OutboxItem {
            id: EffectId::new(Uuid::new_v5(
                &namespace,
                format!("{key}\0{label}\0{now_ms}").as_bytes(),
            )),
            route_id: Some(route.id),
            sender_harness: None,
            source_agent: None,
            kind,
            destination,
            body: format!("[{label}] {}", problem.description),
            attachments: Vec::new(),
            actions: Vec::new(),
            state: OutboxState::Pending,
            attempts: 0,
            last_error: None,
            external_receipt: None,
            uncertain_attempts: 0,
        };
        self.store
            .enqueue_outbox(None, notice, now_ms)
            .await
            .map(|_| ())
            .map_err(error_string)
    }

    async fn handle_send(&self, request: ControlRequest, params: SendParams) -> ControlResponse {
        let id = request.id;
        if params.steer && params.return_final {
            return error_response(
                id,
                "invalid_params",
                "active-turn steering cannot request a correlated final callback",
                None,
            );
        }
        if params.timeout_ms != 0 {
            return error_response(
                id,
                "invalid_params",
                "asynchronous final callbacks do not expire; params.timeout_ms must be zero",
                None,
            );
        }
        if params
            .source
            .as_ref()
            .is_some_and(|source| source.is_empty())
            || params.target.is_empty()
            || params.message.is_empty()
            || (params.source.is_none()
                && params.source_pane_id.is_none()
                && params.source_agent.is_none())
        {
            return error_response(
                id,
                "invalid_request",
                "send requires non-empty to and message fields plus from, a Wakterm source pane, or a source agent",
                None,
            );
        }
        if params.source_pane_id.is_some() && params.source_agent.is_some() {
            return error_response(
                id,
                "invalid_request",
                "send accepts a Wakterm source pane or a source agent, not both",
                None,
            );
        }
        let routes = match self.store.list_routes().await {
            Ok(routes) => routes,
            Err(error) => return internal(id, error),
        };
        let live = match self.wakterm.live_routes().await {
            Ok(live) => live,
            Err(error) => {
                return error_response(id, "route_resolution_failed", error.to_string(), None);
            }
        };
        // `to` names a route or one live agent by its Wakterm name. A route
        // title must not leave the choice between several agents to a guess.
        let (target, named_target) = match exact_route(&routes, &params.target) {
            Ok(route) => {
                if let Ok(live_route) = live.route(&route.title)
                    && live_route.agents.len() > 1
                {
                    let names = live_route
                        .agents
                        .iter()
                        .map(|agent| live.agent_name(agent).unwrap_or(&agent.agent_id))
                        .collect::<Vec<_>>();
                    return error_response(
                        id,
                        "target_route_has_several_agents",
                        format!(
                            "target route {} has {} agents; send to one by name: {}",
                            route.title,
                            names.len(),
                            names.join(", ")
                        ),
                        Some(json!({"agents": names})),
                    );
                }
                (route, None)
            }
            Err("not_found") => {
                let Some((live_route, binding)) = live.agent_named(&params.target) else {
                    return route_error(id, "target", "not_found");
                };
                match exact_route(&routes, &live_route.title) {
                    Ok(route) => (route, Some(binding)),
                    Err(response) => return route_error(id, "target", response),
                }
            }
            Err(response) => return route_error(id, "target", response),
        };
        let last_agents = self.last_agents.read().await;
        let source_agent = match (params.source_pane_id, &params.source_agent) {
            (Some(pane_id), _) => live.source_in_pane(pane_id),
            (None, Some(agent)) => live.source_agent(&agent.agent_id, &agent.incarnation_id),
            (None, None) => None,
        };
        if source_agent.is_none() && params.source.is_none() {
            if let Some(pane_id) = params.source_pane_id {
                return error_response(
                    id,
                    "source_pane_unavailable",
                    format!("source pane {pane_id} has no live Wakterm agent"),
                    Some(json!({"pane_id": pane_id})),
                );
            }
            if let Some(agent) = &params.source_agent {
                return error_response(
                    id,
                    "source_agent_unavailable",
                    format!(
                        "source agent {} incarnation {} is not live in a Wakterm route",
                        agent.agent_id, agent.incarnation_id
                    ),
                    Some(json!({
                        "agent_id": agent.agent_id,
                        "incarnation_id": agent.incarnation_id,
                    })),
                );
            }
        }
        let (source, source_binding) = match source_agent {
            Some((pane_route, binding)) => {
                let source_title = params.source.as_deref().unwrap_or(&pane_route.title);
                let source = match exact_route(&routes, source_title) {
                    Ok(route) => route,
                    Err(response) => return route_error(id, "source", response),
                };
                (source, binding)
            }
            None => {
                let source_title = params
                    .source
                    .as_deref()
                    .expect("a source route is required without a live source agent");
                let source = match exact_route(&routes, source_title) {
                    Ok(route) => route,
                    Err(response) => return route_error(id, "source", response),
                };
                let binding = match resolve_live(&live, source, last_agents.get(&source.id)) {
                    Ok(Some(binding)) => binding,
                    Ok(None) => return route_unavailable(id, "source", source),
                    Err(error) => {
                        return error_response(id, "route_resolution_failed", error, None);
                    }
                };
                (source, binding)
            }
        };
        let target_binding = match named_target {
            Some(binding) => binding,
            None => match resolve_live(&live, target, last_agents.get(&target.id)) {
                Ok(Some(binding)) => binding,
                Ok(None) => return route_unavailable(id, "target", target),
                Err(error) => return error_response(id, "route_resolution_failed", error, None),
            },
        };
        drop(last_agents);
        let command_source = source.title.clone();
        let source = source.with_agent(source_binding);
        let target = target.with_agent(target_binding);
        match self
            .workflows
            .submit(
                params.into_command(id, command_source),
                &source,
                &target,
                now_ms(),
            )
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
        let degraded_routes = match self.reported_problems().await {
            Ok(reported) => reported
                .into_values()
                .map(
                    |problem| json!({"route": problem.route_title, "problem": problem.description}),
                )
                .collect::<Vec<_>>(),
            Err(error) => return internal(id, error),
        };
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
                    "degraded_routes": degraded_routes,
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

    async fn handle_route_list(&self, id: Uuid) -> ControlResponse {
        let routes = match self.store.list_routes().await {
            Ok(routes) => routes,
            Err(error) => return internal(id, error),
        };
        let live = match self.wakterm.live_routes().await {
            Ok(live) => live,
            Err(error) => {
                return error_response(id, "route_resolution_failed", error.to_string(), None);
            }
        };
        let mut listed = Vec::with_capacity(routes.len());
        for route in routes {
            match live.route(&route.title) {
                Ok(live_route) => listed.push(json!({
                    "title": route.title,
                    "available": true,
                    "agents": live_route.agents.iter().map(|agent| {
                        let mut entry = json!(agent);
                        entry["name"] = json!(live.agent_name(agent));
                        entry
                    }).collect::<Vec<_>>(),
                })),
                Err(WaktermCliError::RouteNotFound(_) | WaktermCliError::RouteUnavailable(_)) => {
                    listed.push(json!({
                        "title": route.title,
                        "available": false,
                        "agents": [],
                    }));
                }
                Err(error) => {
                    return error_response(id, "route_resolution_failed", error.to_string(), None);
                }
            }
        }
        listed.sort_by_cached_key(|entry| {
            entry["title"]
                .as_str()
                .unwrap_or_default()
                .to_ascii_lowercase()
        });
        success_response(id, json!({"routes": listed}))
    }

    async fn handle_route_ensure(&self, id: Uuid, params: RouteEnsureParams) -> ControlResponse {
        if !valid_route_title(&params.title)
            || params
                .telegram_topic_id
                .is_some_and(|topic_id| topic_id <= 0)
            || params
                .signal_group_id
                .as_deref()
                .is_some_and(|group_id| group_id.trim().is_empty())
            || (params.signal_allow_members && params.signal_group_id.is_none())
        {
            return error_response(
                id,
                "invalid_params",
                "route title must be 1 to 128 characters without surrounding whitespace, a Telegram topic ID must be positive, and Signal member access requires a non-empty group ID",
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
                                "route {:?} cannot be created without a live Wakterm agent",
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
                let mut route = telegram_route(params.title, topic_id);
                ensure_signal_binding(
                    &mut route,
                    params.signal_group_id.as_deref(),
                    params.signal_allow_members,
                )
                .expect("a new route has no conflicting Signal binding");
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
            binding_created = true;
        }
        match ensure_signal_binding(
            &mut route,
            params.signal_group_id.as_deref(),
            params.signal_allow_members,
        ) {
            Ok(changed) => binding_created |= changed,
            Err(existing) => {
                return error_response(
                    id,
                    "route_binding_conflict",
                    format!(
                        "route {:?} is already bound to a different Signal group",
                        route.title
                    ),
                    Some(json!({"signal_group_id": existing})),
                );
            }
        }
        if binding_created && let Err(error) = self.store.save_route(route.clone(), now_ms()).await
        {
            return internal(id, error);
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
                "route.list" => self.handle_route_list(request.id).await,
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

fn ensure_signal_binding(
    route: &mut Route,
    requested_group_id: Option<&str>,
    allow_members: bool,
) -> Result<bool, String> {
    let Some(requested_group_id) = requested_group_id else {
        return Ok(false);
    };
    if let Some((group_id, existing_allow_members)) =
        route.channels.iter_mut().find_map(|binding| match binding {
            ChannelBinding::Signal {
                group_id,
                allow_members,
            } => Some((group_id, allow_members)),
            ChannelBinding::Telegram { .. } => None,
        })
    {
        if group_id.trim_end_matches('=') != requested_group_id.trim_end_matches('=') {
            return Err(group_id.clone());
        }
        if allow_members && !*existing_allow_members {
            *existing_allow_members = true;
            return Ok(true);
        }
        return Ok(false);
    }
    route.channels.push(ChannelBinding::Signal {
        group_id: requested_group_id.to_owned(),
        allow_members,
    });
    Ok(true)
}

fn valid_route_title(title: &str) -> bool {
    !title.is_empty() && title.trim() == title && title.chars().count() <= 128
}

fn telegram_route(title: String, topic_id: i64) -> Route {
    Route {
        id: RouteId::random(),
        title,
        channels: vec![ChannelBinding::Telegram { topic_id }],
        agent: None,
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
                working_directory: live_route.working_directory(agent).map(Path::to_path_buf),
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
            (ChannelBinding::Signal { group_id, .. }, ChannelKind::Signal) => {
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
    use std::time::Duration;

    use tempfile::tempdir;

    use super::*;
    use crate::channels::RecordingChannels;
    use crate::domain::{ChannelBinding, EffectId, RouteId, SendCommand};
    use crate::service::{FaultInjector, FaultPoint};
    use crate::store::InboxItem;
    use crate::wakterm::{FakeWakterm, ProfileKind, WaktermContract};

    fn current_contract() -> WaktermContract {
        WaktermContract::from_golden_json(
            &fs::read_to_string("/code/wakterm/docs/agent-api/v1/golden-fixtures.json").unwrap(),
            ProfileKind::Current,
        )
        .unwrap()
    }

    /// How the fake signal-cli answers one send.
    #[derive(Clone, Copy)]
    enum SignalReply {
        Posted,
        Error(&'static str),
        /// Closes the connection after reading the request, as if signal-cli
        /// failed after it may have posted.
        HangUp,
    }

    /// Serves signal-cli JSON-RPC sends from a script of replies. Records
    /// each posted body.
    fn scripted_signal(
        socket: &std::path::Path,
        script: Vec<SignalReply>,
    ) -> tokio::task::JoinHandle<Vec<String>> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::UnixListener::bind(socket).unwrap();
        tokio::spawn(async move {
            let mut posted = Vec::new();
            for response in script {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let request: serde_json::Value = serde_json::from_str(&line).unwrap();
                posted.push(request["params"]["message"].as_str().unwrap().to_owned());
                let reply = match response {
                    SignalReply::Posted => {
                        json!({"jsonrpc": "2.0", "id": request["id"], "result": {"timestamp": 1}})
                    }
                    SignalReply::Error(message) => json!({"jsonrpc": "2.0", "id": request["id"],
                        "error": {"code": -32603, "message": message}}),
                    SignalReply::HangUp => continue,
                };
                reader
                    .get_mut()
                    .write_all(format!("{reply}\n").as_bytes())
                    .await
                    .unwrap();
            }
            posted
        })
    }

    async fn signal_outbox_service(
        directory: &std::path::Path,
        socket: &std::path::Path,
    ) -> (StoreHandle, ProductionService, EffectId) {
        let store = StoreHandle::open(directory.join("state.sqlite3")).unwrap();
        let id = EffectId::new(Uuid::from_u128(77));
        store
            .enqueue_outbox(
                None,
                OutboxItem {
                    id,
                    route_id: None,
                    sender_harness: None,
                    source_agent: None,
                    kind: ChannelKind::Signal,
                    destination: "group-one".into(),
                    body: "reply".into(),
                    attachments: Vec::new(),
                    actions: Vec::new(),
                    state: OutboxState::Pending,
                    attempts: 0,
                    last_error: None,
                    external_receipt: None,
                    uncertain_attempts: 0,
                },
                1,
            )
            .await
            .unwrap();
        let channels = RealChannels {
            signal: Some(crate::channels::SignalClient::new(
                socket,
                "+15550000",
                Duration::from_secs(2),
            )),
            ..RealChannels::default()
        };
        let service = ProductionService::new(
            store.clone(),
            WaktermCli::new(
                "/bin/false",
                directory.join("mux.sock"),
                Duration::from_secs(2),
            ),
            channels,
            SupervisorHandle::default(),
            Vec::new(),
            directory.join("control.sock"),
        );
        (store, service, id)
    }

    const INACTIVE: &str =
        "Failed to send message: the connection was closed (ChatServiceInactiveException)";

    #[tokio::test]
    async fn outbox_labels_a_retry_after_an_attempt_that_may_have_posted() {
        let directory = tempdir().unwrap();
        let socket = directory.path().join("signal.sock");
        // Unreachable first: nothing can have been posted, so no label yet.
        let (store, service, id) = signal_outbox_service(directory.path(), &socket).await;
        assert_eq!(service.outbox_once().await.unwrap(), 1);
        let server = scripted_signal(&socket, vec![SignalReply::HangUp, SignalReply::Posted]);
        assert_eq!(service.outbox_once().await.unwrap(), 1);
        assert_eq!(service.outbox_once().await.unwrap(), 1);
        assert_eq!(server.await.unwrap(), ["reply", "[resent] reply"]);

        let item = store
            .pending_outbox()
            .await
            .unwrap()
            .into_iter()
            .find(|item| item.id == id);
        assert!(item.is_none(), "the item was delivered");
        assert_eq!(store.status().await.unwrap().failed_outbox, 0);
        drop(service);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_retry_after_signals_closed_server_connection_is_unlabeled() {
        let directory = tempdir().unwrap();
        let socket = directory.path().join("signal.sock");
        let (store, service, _) = signal_outbox_service(directory.path(), &socket).await;
        let server = scripted_signal(
            &socket,
            vec![SignalReply::Error(INACTIVE), SignalReply::Posted],
        );
        assert_eq!(service.outbox_once().await.unwrap(), 1);
        assert_eq!(service.outbox_once().await.unwrap(), 1);
        assert_eq!(server.await.unwrap(), ["reply", "reply"]);
        drop(service);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn outbox_stops_retrying_after_three_attempts_that_may_have_posted() {
        let directory = tempdir().unwrap();
        let socket = directory.path().join("signal.sock");
        let (store, service, _) = signal_outbox_service(directory.path(), &socket).await;
        let server = scripted_signal(&socket, vec![SignalReply::HangUp; 3]);
        for _ in 0..3 {
            assert_eq!(service.outbox_once().await.unwrap(), 1);
        }
        assert_eq!(service.outbox_once().await.unwrap(), 0);
        assert_eq!(server.await.unwrap().len(), 3);
        let status = store.status().await.unwrap();
        assert_eq!(status.indeterminate_outbox, 1);
        assert_eq!(status.pending_outbox, 0);
        drop(service);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_send_interrupted_by_restart_is_labeled_when_retried() {
        let directory = tempdir().unwrap();
        let socket = directory.path().join("signal.sock");
        let (store, service, id) = signal_outbox_service(directory.path(), &socket).await;
        let mut item = store.pending_outbox().await.unwrap().remove(0);
        item.state = OutboxState::Delivering;
        store.save_outbox(item, 2).await.unwrap();
        drop(service);
        store.shutdown().await.unwrap();

        let reopened = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
        let item = reopened.pending_outbox().await.unwrap().remove(0);
        assert_eq!(item.id, id);
        assert_eq!(item.uncertain_attempts, 1);
        assert_eq!(item.attempt_body(), "[resent] reply");
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn route_health_reports_a_persistent_agent_problem_once_and_its_resolution() {
        let directory = tempdir().unwrap();
        let agents = directory.path().join("agents.json");
        fs::copy("tests/fixtures/agent_list.json", &agents).unwrap();
        let binary = directory.path().join("wakterm-fake");
        fs::write(
            &binary,
            format!(
                r#"#!/bin/bash
set -euo pipefail
operation="$*"
if [[ "$operation" == *"agent list --format json"* ]]; then
  cat '{}'
elif [[ "$operation" == *"list --format json"* ]]; then
  echo '[{{"pane_id":1,"tab_id":1,"window_id":1,"effective_title":"other"}},{{"pane_id":3,"tab_id":2,"window_id":1,"effective_title":"route"}}]'
else
  echo "unexpected fake invocation: $operation" >&2
  exit 9
fi
"#,
                agents.display()
            ),
        )
        .unwrap();
        crate::test_support::seal_executable(&binary);
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
        let service = ProductionService::new(
            store.clone(),
            WaktermCli::new(
                binary,
                directory.path().join("mux.sock"),
                Duration::from_secs(2),
            ),
            RealChannels::default(),
            SupervisorHandle::default(),
            Vec::new(),
            directory.path().join("control.sock"),
        );

        // plain_claude in pane 3 is unobserved; the first sighting is only a candidate.
        assert_eq!(service.route_health_once().await.unwrap(), 0);
        assert_eq!(service.route_health_once().await.unwrap(), 1);
        assert_eq!(service.route_health_once().await.unwrap(), 0);
        let notices = store.pending_outbox().await.unwrap();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].destination, "10");
        assert!(
            notices[0]
                .body
                .starts_with("[Agent problem] Wakterm cannot read the output of plain_claude")
        );
        assert_eq!(service.reported_problems().await.unwrap().len(), 1);

        // The agent becomes readable.
        let healthy = fs::read_to_string(&agents)
            .unwrap()
            .replace("\"PlainPty\"", "\"ObservedPty\"");
        fs::write(&agents, healthy).unwrap();
        assert_eq!(service.route_health_once().await.unwrap(), 1);
        let notices = store.pending_outbox().await.unwrap();
        assert!(
            notices[1]
                .body
                .starts_with("[Resolved] Wakterm cannot read")
        );
        assert!(service.reported_problems().await.unwrap().is_empty());
        drop(service);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_question_form_is_answered_by_taps_and_a_reply_then_submitted() {
        let directory = tempdir().unwrap();
        let calls = directory.path().join("approval-args.log");
        let binary = directory.path().join("wakterm-fake");
        fs::write(
            &binary,
            format!(
                r#"#!/bin/bash
set -euo pipefail
operation="$*"
if [[ "$operation" == *"agent capabilities"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1","approval_control.v1","question_form_answers.v1"]}}'
elif [[ "$operation" == *"agent approval"* ]]; then
  printf '%s\n' "$@" > '{}'
  choice=""; previous=""
  for argument in "$@"; do
    if [[ "$previous" == "--choice" ]]; then choice="$argument"; fi
    previous="$argument"
  done
  echo '{{"schema":"wakterm.agent-approval.v1","request_id":"0123456789abcdef01234567","agent_id":"agent-route","incarnation_id":"inc-route","choice_id":"'"$choice"'","resolved":true}}'
else
  echo "unexpected fake invocation: $operation" >&2
  exit 9
fi
"#,
                calls.display()
            ),
        )
        .unwrap();
        crate::test_support::seal_executable(&binary);
        let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
        let route = Route {
            id: RouteId::new(Uuid::new_v4()),
            title: "route".into(),
            channels: vec![ChannelBinding::Telegram { topic_id: 10 }],
            agent: None,
        };
        store.save_route(route.clone(), 1).await.unwrap();
        store.initialize_event_cursor(100).await.unwrap();
        // A form in the shape Wakterm documents for question_form_answers.v1.
        let event: crate::wakterm::EventRecord = serde_json::from_value(json!({
            "sequence": 101,
            "event_id": "form-event-101",
            "kind": "approval_requested",
            "agent_id": "agent-route",
            "incarnation_id": "inc-route",
            "observed_at": "2026-10-05T04:44:00Z",
            "approval": {
                "schema": "wakterm.agent-approval.v1",
                "kind": "user_question_form",
                "request_id": "0123456789abcdef01234567",
                "agent_id": "agent-route",
                "incarnation_id": "inc-route",
                "turn_id": "turn-1",
                "item_id": "item-1",
                "observed_at": "2026-10-05T04:44:00Z",
                "prompt": "1. Browser: Which browser?\n- Chrome\n- Edge\n\n2. Lifetime: How to start?\n- Browser\n- Worker",
                "reason": null,
                "command": null,
                "cwd": null,
                "choices": [],
                "questions": [
                    {"index": 0, "header": "Browser", "question": "Which browser?", "multi_select": false,
                     "options": [{"id": "option_1", "label": "Chrome"}, {"id": "option_2", "label": "Edge"}]},
                    {"index": 1, "header": "Lifetime", "question": "How to start?", "multi_select": false,
                     "options": [{"id": "option_1", "label": "Browser"}, {"id": "option_2", "label": "Worker"}]}
                ]
            }
        }))
        .unwrap();
        let agent = AgentBinding {
            agent_id: "agent-route".into(),
            incarnation_id: "inc-route".into(),
            harness: "claude".into(),
            pane_id: Some(1),
        };
        store
            .ingest_agent_events(
                100,
                101,
                vec![event],
                vec![RouteAgent {
                    route_id: route.id,
                    agent,
                    working_directory: None,
                }],
                2,
            )
            .await
            .unwrap();
        let mut posted = store.pending_outbox().await.unwrap().remove(0);
        assert!(
            posted.body.contains(
                "1. Browser: Which browser?\n1) Chrome\n2) Edge\nAnswer: not answered yet"
            )
        );
        assert_eq!(posted.actions.len(), 7);
        posted.state = OutboxState::Delivered;
        posted.external_receipt = Some("555".into());
        store.save_outbox(posted, 3).await.unwrap();

        let service = ProductionService::new(
            store.clone(),
            WaktermCli::new(
                binary,
                directory.path().join("mux.sock"),
                Duration::from_secs(2),
            ),
            RealChannels::default(),
            SupervisorHandle::default(),
            Vec::new(),
            directory.path().join("control.sock"),
        );
        let tap = |action| TelegramFormTap {
            query_id: "query".into(),
            destination: "10".into(),
            sender_id: "42".into(),
            message_id: 555,
            request_id: "0123456789abcdef01234567".into(),
            action,
        };
        let update = service
            .answer_form_tap(&tap(FormAction::Option {
                question: 1,
                option: 1,
            }))
            .await
            .unwrap();
        assert_eq!(update.toast, "Lifetime: Browser");
        assert_eq!(update.message_id, 555);
        assert!(update.text.contains("Answer: Browser"));

        let reply = |body: &str, reply_to: &str| InboundMessage {
            channel: ChannelKind::Telegram,
            external_id: "update-1".into(),
            destination: "10".into(),
            sender_id: Some("42".into()),
            sender: None,
            reply_to_external_id: Some(reply_to.into()),
            body: body.into(),
        };
        // A reply to anything else is an ordinary prompt.
        assert!(
            service
                .answer_form_reply(&reply("hello", "10"))
                .await
                .unwrap()
                .is_none()
        );
        let update = service
            .answer_form_reply(&reply("1: new chrome profile", "555"))
            .await
            .unwrap()
            .unwrap();
        assert!(update.text.contains("Answer: \"new chrome profile\""));

        let update = service
            .answer_form_tap(&tap(FormAction::Submit))
            .await
            .unwrap();
        assert_eq!(update.toast, "Submitted.");
        assert!(update.actions.is_empty());
        let args = fs::read_to_string(&calls).unwrap();
        assert!(args.contains("--choice\nsubmit\n--answers\n"));
        assert!(args.contains(
            r#"[{"question":0,"text":"new chrome profile"},{"choices":["option_1"],"question":1}]"#
        ));
        assert_eq!(
            service
                .answer_form_tap(&tap(FormAction::Cancel))
                .await
                .unwrap_err(),
            "this form was already answered"
        );
        drop(service);
        store.shutdown().await.unwrap();
    }

    #[test]
    fn signal_binding_member_policy_is_explicit_and_monotonic() {
        let mut route = telegram_route("inquisition".into(), 42);
        assert_eq!(
            ensure_signal_binding(&mut route, Some("group-id=="), true),
            Ok(true)
        );
        assert!(matches!(
            route.channels.last(),
            Some(ChannelBinding::Signal {
                group_id,
                allow_members: true,
            }) if group_id == "group-id=="
        ));
        assert_eq!(
            ensure_signal_binding(&mut route, Some("group-id"), false),
            Ok(false)
        );
        assert_eq!(
            ensure_signal_binding(&mut route, Some("other-group"), true),
            Err("group-id==".into())
        );
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
        crate::test_support::seal_executable(&binary);

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
    async fn separate_tabs_share_output_and_the_latest_output_agent_receives_input() {
        let directory = tempdir().unwrap();
        let binary = directory.path().join("wakterm-fake");
        let admission = directory.path().join("admission.txt");
        fs::write(
            &binary,
            format!(
                r#"#!/bin/bash
set -euo pipefail
operation="$*"
if [[ "$operation" == *"agent capabilities"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}}'
elif [[ "$operation" == *"agent catalog"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","as_of_event_sequence":101,"agents":[{{"agent_id":"agent-first","incarnation_id":"inc-first","pane_id":1,"name":"first","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-17T00:00:00Z"}},{{"agent_id":"agent-second","incarnation_id":"inc-second","pane_id":2,"name":"second","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-17T00:00:00Z"}}]}}'
elif [[ "$operation" == *"list --format json"* ]]; then
  echo '[{{"pane_id":1,"tab_id":2,"window_id":3,"effective_title":"route"}},{{"pane_id":2,"tab_id":9,"window_id":4,"effective_title":"ROUTE"}}]'
elif [[ "$operation" == *"agent events"*"--after 100"* ]]; then
  echo '{{"schema":"wakterm.agent-events.v1","status":"ok","requested_after_sequence":100,"oldest_available_sequence":1,"latest_sequence":101,"next_after_sequence":101,"events":[{{"sequence":101,"event_id":"event-101","kind":"assistant_message","agent_id":"agent-second","incarnation_id":"inc-second","text":"second tab output"}}]}}'
elif [[ "$operation" == *"agent admit"* ]]; then
  cat >/dev/null
  echo "$operation" > '{}'
  echo '{{"schema":"wakterm.agent-api.v1","request_id":"00000000-0000-0000-0000-00000000007d","status":"accepted","definitive":true,"prompt_written":true,"agent_id":"agent-second","incarnation_id":"inc-second","detail":null}}'
else
  echo "unexpected fake invocation: $operation" >&2
  exit 9
fi
"#,
                admission.display(),
            ),
        )
        .unwrap();
        crate::test_support::seal_executable(&binary);

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

        assert_eq!(service.event_once().await.unwrap(), 1);
        assert_eq!(store.status().await.unwrap().event_cursor, Some(101));
        let output = store.pending_outbox().await.unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].destination, "10");
        assert_eq!(output[0].body, "second tab output");
        assert_eq!(
            service.last_agents.read().await.get(&route.id),
            Some(&AgentBinding {
                agent_id: "agent-second".into(),
                incarnation_id: "inc-second".into(),
                harness: "codex".into(),
                pane_id: Some(2),
            })
        );
        store
            .accept_inbox(InboxItem {
                id: EffectId::new(Uuid::from_u128(125)),
                channel: ChannelKind::Telegram,
                external_id: "update-2".into(),
                destination: "10".into(),
                sender_id: Some("42".into()),
                sender: Some("Mihai".into()),
                reply_to_external_id: None,
                body: "continue the active work".into(),
                state: "pending".into(),
                created_at_ms: 2,
                receipt: None,
                steering_acknowledged: None,
            })
            .await
            .unwrap();
        assert_eq!(service.inbox_once().await.unwrap(), 1);
        let admission = fs::read_to_string(admission).unwrap();
        assert!(admission.contains("agent-second --exact-agent-id --incarnation inc-second"));
        drop(service);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn terminal_worker_does_not_call_wakterm_without_return_work() {
        let directory = tempdir().unwrap();
        let log = directory.path().join("terminal.log");
        let binary = directory.path().join("wakterm-fake");
        fs::write(
            &binary,
            format!(
                "#!/bin/bash\nprintf '%s\\n' \"$*\" >> '{}'\nexit 9\n",
                log.display()
            ),
        )
        .unwrap();
        crate::test_support::seal_executable(&binary);
        let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
        let service = ProductionService::new(
            store.clone(),
            WaktermCli::new(
                binary,
                directory.path().join("mux.sock"),
                Duration::from_secs(2),
            ),
            RealChannels::default(),
            SupervisorHandle::default(),
            vec!["return_request_terminal_stream.v1".into()],
            directory.path().join("control.sock"),
        );

        assert_eq!(service.terminal_once().await.unwrap(), 0);
        assert!(!log.exists());
        drop(service);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn terminal_worker_defers_in_flight_work_then_skips_it_after_failure() {
        let directory = tempdir().unwrap();
        let binary = directory.path().join("wakterm-fake");
        let bad_id = WorkflowId::new(Uuid::from_u128(126));
        let good_id = WorkflowId::new(Uuid::from_u128(127));
        fs::write(
            &binary,
            format!(
                r#"#!/bin/bash
set -euo pipefail
operation="$*"
if [[ "$operation" == *"agent capabilities"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1"]}}'
elif [[ "$operation" == *"agent request watch"* ]]; then
  echo '{{"request_id":"{bad_id}","target_agent_id":"agent-target","state":"indeterminate","final_message":null,"detail":"registration was abandoned","terminal_event_sequence":69}}'
  echo '{{"request_id":"{good_id}","target_agent_id":"agent-target","state":"completed","final_message":"finished","detail":null,"terminal_event_sequence":70}}'
else
  echo "unexpected fake invocation: $operation" >&2
  exit 9
fi
"#,
            ),
        )
        .unwrap();
        crate::test_support::seal_executable(&binary);

        let source = Route {
            id: RouteId::new(Uuid::from_u128(10)),
            title: "source".into(),
            channels: vec![ChannelBinding::Signal {
                group_id: "source-group".into(),
                allow_members: false,
            }],
            agent: Some(AgentBinding {
                agent_id: "agent-source".into(),
                incarnation_id: "inc-source".into(),
                harness: "codex".into(),
                pane_id: Some(1),
            }),
        };
        let target = Route {
            id: RouteId::new(Uuid::from_u128(11)),
            title: "target".into(),
            channels: vec![ChannelBinding::Telegram { topic_id: 20 }],
            agent: Some(AgentBinding {
                agent_id: "agent-target".into(),
                incarnation_id: "inc-target".into(),
                harness: "codex".into(),
                pane_id: Some(2),
            }),
        };
        let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
        store.save_route(source.clone(), 1).await.unwrap();
        store.save_route(target.clone(), 1).await.unwrap();
        store
            .set_metadata("wakterm_return_cursor".into(), "68".into())
            .await
            .unwrap();

        let faults = Arc::new(FaultInjector::default());
        faults.arm(FaultPoint::AfterAdmissionPrepared);
        let setup = OfflineService::with_faults(
            store.clone(),
            FakeWakterm::new(current_contract()),
            RecordingChannels::default(),
            faults,
        );
        setup.wakterm().script_receipts([AdmissionStatus::Accepted]);
        let command = |id| SendCommand {
            id,
            source: source.title.clone(),
            target: target.title.clone(),
            message: "do the work".into(),
            return_final: true,
            steer: false,
            timeout_ms: 0,
        };
        assert!(matches!(
            setup.submit(command(bad_id), &source, &target, 2).await,
            Err(ServiceError::Injected(FaultPoint::AfterAdmissionPrepared))
        ));
        assert!(
            setup
                .submit(command(good_id), &source, &target, 3)
                .await
                .unwrap()
                .submitted
        );
        drop(setup);

        let service = ProductionService::new(
            store.clone(),
            WaktermCli::new(
                binary,
                directory.path().join("mux.sock"),
                Duration::from_secs(2),
            ),
            RealChannels::default(),
            SupervisorHandle::default(),
            vec!["return_request_terminal_stream.v1".into()],
            directory.path().join("control.sock"),
        );

        assert_eq!(service.terminal_once().await.unwrap(), 0);
        assert_eq!(
            store
                .get_metadata("wakterm_return_cursor".into())
                .await
                .unwrap()
                .as_deref(),
            Some("68")
        );
        let mut failed = store.get_workflow(bad_id).await.unwrap().unwrap();
        failed.workflow.transition(WorkflowState::Failed).unwrap();
        failed.updated_at_ms += 1;
        store
            .save_workflow(failed, WorkflowState::AdmissionPrepared)
            .await
            .unwrap();

        assert_eq!(service.terminal_once().await.unwrap(), 2);
        assert_eq!(
            store
                .get_metadata("wakterm_return_cursor".into())
                .await
                .unwrap()
                .as_deref(),
            Some("70")
        );
        assert!(store.get_return(bad_id).await.unwrap().is_none());
        assert!(store.get_return(good_id).await.unwrap().is_some());
        drop(service);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn definitive_busy_target_failure_does_not_stop_the_batch() {
        let directory = tempdir().unwrap();
        let binary = directory.path().join("wakterm-fake");
        fs::write(
            &binary,
            r#"#!/bin/bash
set -euo pipefail
operation="$*"
if [[ "$operation" == *"agent capabilities"* ]]; then
  printf '%s\n' '{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}'
elif [[ "$operation" == *"agent catalog"* ]]; then
  printf '%s\n' '{"schema":"wakterm.agent-api.v1","as_of_event_sequence":10,"agents":[{"agent_id":"agent-failure","incarnation_id":"inc-failure","pane_id":11,"name":"failure","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-27T00:00:00Z"},{"agent_id":"agent-success","incarnation_id":"inc-success","pane_id":12,"name":"success","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-27T00:00:00Z"}]}'
elif [[ "$operation" == *"list --format json"* ]]; then
  printf '%s\n' '[{"pane_id":11,"tab_id":11,"window_id":1,"effective_title":"failure"},{"pane_id":12,"tab_id":12,"window_id":1,"effective_title":"success"}]'
else
  echo "unexpected fake invocation: $operation" >&2
  exit 9
fi
"#,
        )
        .unwrap();
        crate::test_support::seal_executable(&binary);

        let source = Route {
            id: RouteId::new(Uuid::from_u128(900)),
            title: "source".into(),
            channels: vec![ChannelBinding::Signal {
                group_id: "source-group".into(),
                allow_members: false,
            }],
            agent: Some(AgentBinding {
                agent_id: "agent-source".into(),
                incarnation_id: "inc-source".into(),
                harness: "codex".into(),
                pane_id: Some(1),
            }),
        };
        let target = |id: u128, title: &str, pane_id: u64| Route {
            id: RouteId::new(Uuid::from_u128(id)),
            title: title.into(),
            channels: vec![ChannelBinding::Telegram {
                topic_id: pane_id as i64,
            }],
            agent: Some(AgentBinding {
                agent_id: format!("agent-{title}"),
                incarnation_id: format!("inc-{title}"),
                harness: "codex".into(),
                pane_id: Some(pane_id),
            }),
        };
        let failed_target = target(901, "failure", 11);
        let submitted_target = target(902, "success", 12);
        let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
        store.save_route(failed_target.clone(), 1).await.unwrap();
        store.save_route(submitted_target.clone(), 1).await.unwrap();
        let workflows = Arc::new(OfflineService::new(
            store.clone(),
            FakeWakterm::new(current_contract()),
            RecordingChannels::default(),
        ));
        workflows.wakterm().script_receipts([
            AdmissionStatus::Busy,
            AdmissionStatus::Busy,
            AdmissionStatus::ObserverFailure,
            AdmissionStatus::Accepted,
        ]);
        let command = |id, target: &Route| SendCommand {
            id: WorkflowId::new(Uuid::from_u128(id)),
            source: source.title.clone(),
            target: target.title.clone(),
            message: format!("request {id}"),
            return_final: false,
            steer: false,
            timeout_ms: 0,
        };
        let failed_id = WorkflowId::new(Uuid::from_u128(903));
        let submitted_id = WorkflowId::new(Uuid::from_u128(904));
        workflows
            .submit(command(903, &failed_target), &source, &failed_target, 10)
            .await
            .unwrap();
        workflows
            .submit(
                command(904, &submitted_target),
                &source,
                &submitted_target,
                20,
            )
            .await
            .unwrap();
        let service = ProductionService {
            store: store.clone(),
            wakterm: WaktermCli::new(
                binary,
                directory.path().join("mux.sock"),
                Duration::from_secs(2),
            ),
            channels: RealChannels::default(),
            workflows: workflows.clone(),
            health: SupervisorHandle::default(),
            capabilities: vec!["event_stream.v1".into()],
            control_socket: directory.path().join("control.sock"),
            started_at: Instant::now(),
            last_agents: tokio::sync::RwLock::new(HashMap::new()),
            route_changes: tokio::sync::Mutex::new(()),
            health_candidates: tokio::sync::Mutex::new(BTreeSet::new()),
        };

        assert_eq!(service.busy_once().await.unwrap(), 2);
        assert_eq!(
            store
                .get_workflow(failed_id)
                .await
                .unwrap()
                .unwrap()
                .workflow
                .state,
            WorkflowState::Failed
        );
        assert_eq!(
            store
                .get_workflow(submitted_id)
                .await
                .unwrap()
                .unwrap()
                .workflow
                .state,
            WorkflowState::Completed
        );
        let channel_calls = workflows.channels().calls();
        assert_eq!(channel_calls.len(), 6);
        assert!(channel_calls[4].body.starts_with("[delivery-failed]"));
        assert!(channel_calls[5].body.starts_with("[submitted]"));
        assert_eq!(store.status().await.unwrap().awaiting_target_idle, 0);
        drop(service);
        drop(workflows);
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
        crate::test_support::seal_executable(&binary);

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
                receipt: None,
                steering_acknowledged: None,
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
        assert!(store.pending_outbox().await.unwrap().is_empty());
        drop(service);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unconfirmed_channel_input_keeps_the_receipt_and_tells_the_sender() {
        let directory = tempdir().unwrap();
        let binary = directory.path().join("wakterm-fake");
        fs::write(
            &binary,
            r#"#!/bin/bash
set -euo pipefail
operation="$*"
if [[ "$operation" == *"agent capabilities"* ]]; then
  echo '{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}'
elif [[ "$operation" == *"agent catalog"* ]]; then
  echo '{"schema":"wakterm.agent-api.v1","as_of_event_sequence":10,"agents":[{"agent_id":"agent-route","incarnation_id":"inc-route","pane_id":1,"name":"route","harness":"claude","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-10-02T00:00:00Z"}]}'
elif [[ "$operation" == *"list --format json"* ]]; then
  echo '[{"pane_id":1,"tab_id":2,"window_id":3,"effective_title":"route"}]'
elif [[ "$operation" == *"agent admit"* ]]; then
  cat >/dev/null
  echo '{"schema":"wakterm.agent-api.v1","request_id":"00000000-0000-0000-0000-00000000007e","status":"indeterminate","definitive":false,"prompt_written":true,"agent_id":"agent-route","incarnation_id":"inc-route","detail":"prompt was written, but the agent did not start a turn within 15 s"}'
else
  echo "unexpected fake invocation: $operation" >&2
  exit 9
fi
"#,
        )
        .unwrap();
        crate::test_support::seal_executable(&binary);
        let database = directory.path().join("state.sqlite3");
        let store = StoreHandle::open(&database).unwrap();
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
        store
            .accept_inbox(InboxItem {
                id: EffectId::new(Uuid::from_u128(126)),
                channel: ChannelKind::Telegram,
                external_id: "update-3".into(),
                destination: "10".into(),
                sender_id: Some("42".into()),
                sender: Some("Mihai".into()),
                reply_to_external_id: None,
                body: "I just woke up. summarize.".into(),
                state: "pending".into(),
                created_at_ms: 1,
                receipt: None,
                steering_acknowledged: None,
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
        let notices = store.pending_outbox().await.unwrap();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].destination, "10");
        assert!(
            notices[0]
                .body
                .starts_with("[unconfirmed] route may not have received")
        );
        assert!(notices[0].body.contains("did not start a turn within 15 s"));
        // The item is settled rather than retried, so no second notice follows.
        assert_eq!(store.status().await.unwrap().pending_inbox, 0);
        assert_eq!(service.inbox_once().await.unwrap(), 0);
        drop(service);
        store.shutdown().await.unwrap();

        let record: String = rusqlite::Connection::open(&database)
            .unwrap()
            .query_row("SELECT record_json FROM inbox", [], |row| row.get(0))
            .unwrap();
        let record: serde_json::Value = serde_json::from_str(&record).unwrap();
        assert_eq!(record["state"], "indeterminate");
        assert_eq!(record["receipt"]["status"], "indeterminate");
        assert_eq!(record["receipt"]["prompt_written"], true);
    }

    #[tokio::test]
    async fn channel_input_waits_while_the_target_cannot_take_input() {
        let directory = tempdir().unwrap();
        let binary = directory.path().join("wakterm-fake");
        fs::write(
            &binary,
            r#"#!/bin/bash
set -euo pipefail
operation="$*"
if [[ "$operation" == *"agent capabilities"* ]]; then
  echo '{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}'
elif [[ "$operation" == *"agent catalog"* ]]; then
  echo '{"schema":"wakterm.agent-api.v1","as_of_event_sequence":10,"agents":[{"agent_id":"agent-route","incarnation_id":"inc-route","pane_id":1,"name":"route","harness":"claude","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-10-02T00:00:00Z"}]}'
elif [[ "$operation" == *"list --format json"* ]]; then
  echo '[{"pane_id":1,"tab_id":2,"window_id":3,"effective_title":"route"}]'
elif [[ "$operation" == *"agent admit"* ]]; then
  cat >/dev/null
  echo '{"schema":"wakterm.agent-api.v1","request_id":"00000000-0000-0000-0000-00000000007e","status":"busy","definitive":true,"prompt_written":false,"agent_id":"agent-route","incarnation_id":"inc-route","detail":"the target is waiting for dialog open"}'
elif [[ "$operation" == *"agent send agent-route"* ]]; then
  cat >/dev/null
  echo '{"agent_id":"agent-route","agent_name":"route","pane_id":1,"transport":"ObservedPty","submitted":false,"acknowledgement":{"kind":"not_requested","acknowledged":false,"latency_ms":null,"session_path":null,"detail":null},"refusal":{"reason":"input_blocked","detail":"the target is waiting for dialog open"}}'
else
  echo "unexpected fake invocation: $operation" >&2
  exit 9
fi
"#,
        )
        .unwrap();
        crate::test_support::seal_executable(&binary);
        let database = directory.path().join("state.sqlite3");
        let store = StoreHandle::open(&database).unwrap();
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
        store
            .accept_inbox(InboxItem {
                id: EffectId::new(Uuid::from_u128(126)),
                channel: ChannelKind::Telegram,
                external_id: "update-3".into(),
                destination: "10".into(),
                sender_id: Some("42".into()),
                sender: Some("Mihai".into()),
                reply_to_external_id: None,
                body: "I just woke up. summarize.".into(),
                state: "pending".into(),
                created_at_ms: 1,
                receipt: None,
                steering_acknowledged: None,
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
        assert!(store.pending_outbox().await.unwrap().is_empty());
        assert_eq!(store.status().await.unwrap().pending_inbox, 1);
        // Still blocked on the next pass: retried again, still no notice.
        assert_eq!(service.inbox_once().await.unwrap(), 1);
        assert!(store.pending_outbox().await.unwrap().is_empty());
        assert_eq!(store.status().await.unwrap().pending_inbox, 1);
        drop(service);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn busy_channel_input_steers_the_active_turn_instead_of_waiting() {
        let directory = tempdir().unwrap();
        let calls = directory.path().join("calls.log");
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
  echo '{{"schema":"wakterm.agent-api.v1","as_of_event_sequence":10,"agents":[{{"agent_id":"agent-route","incarnation_id":"inc-route","pane_id":1,"name":"route","harness":"codex","status":"busy","turn_state":"waiting_on_agent","alive":true,"observed_at":"2026-08-17T00:00:00Z"}}]}}'
elif [[ "$operation" == *"list --format json"* ]]; then
  echo '[{{"pane_id":1,"tab_id":2,"window_id":3,"effective_title":"route"}}]'
elif [[ "$operation" == *"agent admit"* ]]; then
  cat >/dev/null
  echo admit >> '{}'
  echo '{{"schema":"wakterm.agent-api.v1","request_id":"00000000-0000-0000-0000-00000000007c","status":"busy","definitive":true,"prompt_written":false,"agent_id":"agent-route","incarnation_id":"inc-route","detail":"target is busy"}}'
elif [[ "$operation" == *"agent send agent-route"* ]]; then
  cat > '{}'
  echo steer >> '{}'
  echo '{{"agent_id":"agent-route","agent_name":"route","pane_id":1,"transport":"observed_pty","submitted":true,"acknowledgement":{{"kind":"session_observer","acknowledged":true,"latency_ms":10,"session_path":"/tmp/session","detail":null}}}}'
else
  echo "unexpected fake invocation: $operation" >&2
  exit 9
fi
"#,
                calls.display(),
                prompt.display(),
                calls.display(),
            ),
        )
        .unwrap();
        crate::test_support::seal_executable(&binary);

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
        let body = "change the active plan now";
        store
            .accept_inbox(InboxItem {
                id: EffectId::new(Uuid::from_u128(124)),
                channel: ChannelKind::Telegram,
                external_id: "update-2".into(),
                destination: "10".into(),
                sender_id: Some("42".into()),
                sender: Some("Mihai".into()),
                reply_to_external_id: None,
                body: body.into(),
                state: "pending".into(),
                created_at_ms: 2,
                receipt: None,
                steering_acknowledged: None,
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
        assert_eq!(fs::read_to_string(calls).unwrap(), "admit\nsteer\n");
        assert_eq!(store.status().await.unwrap().pending_inbox, 0);
        drop(service);
        store.shutdown().await.unwrap();
    }
}
