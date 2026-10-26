use std::collections::VecDeque;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::domain::{
    AdmissionReceipt, AdmissionStatus, AgentBinding, EffectId, SendCommand, WorkflowId,
    strip_invisible,
};

use super::{AgentCatalog, ContractError, EventRead, WaktermContract};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionCall {
    pub request_id: EffectId,
    pub binding: AgentBinding,
    pub prompt: String,
    pub return_final: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SteeringCall {
    pub binding: AgentBinding,
    pub prompt: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TerminalResult {
    pub workflow_id: WorkflowId,
    pub source: AgentBinding,
    pub target: AgentBinding,
    pub status: String,
    pub message: String,
}

pub struct FakeWakterm {
    contract: WaktermContract,
    receipts: Mutex<VecDeque<AdmissionStatus>>,
    calls: Mutex<Vec<AdmissionCall>>,
    steering_calls: Mutex<Vec<SteeringCall>>,
    /// Steers go unacknowledged, as when the agent records no input in time.
    steering_unacknowledged: std::sync::atomic::AtomicBool,
    /// An indeterminate admission reports its prompt as written, as when the
    /// agent starts just after Wakterm stops watching.
    indeterminate_written: std::sync::atomic::AtomicBool,
}

impl FakeWakterm {
    pub fn new(contract: WaktermContract) -> Self {
        Self {
            contract,
            receipts: Mutex::new(VecDeque::new()),
            calls: Mutex::new(Vec::new()),
            steering_calls: Mutex::new(Vec::new()),
            steering_unacknowledged: std::sync::atomic::AtomicBool::new(false),
            indeterminate_written: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn write_indeterminate_prompts(&self) {
        self.indeterminate_written
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn leave_steering_unacknowledged(&self) {
        self.steering_unacknowledged
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn capabilities(&self) -> &std::collections::BTreeSet<String> {
        &self.contract.capabilities
    }

    pub fn catalog(&self) -> AgentCatalog {
        self.contract.catalog.clone()
    }

    pub fn read_events(&self, after_sequence: u64) -> Result<EventRead, ContractError> {
        self.contract.read_events(after_sequence)
    }

    pub fn script_receipts(&self, receipts: impl IntoIterator<Item = AdmissionStatus>) {
        self.receipts
            .lock()
            .expect("fake receipt lock is healthy")
            .extend(receipts);
    }

    pub fn admit(
        &self,
        request_id: EffectId,
        binding: &AgentBinding,
        prompt: String,
        return_final: bool,
    ) -> AdmissionReceipt {
        let status = self
            .receipts
            .lock()
            .expect("fake receipt lock is healthy")
            .pop_front()
            .unwrap_or(AdmissionStatus::Accepted);
        if !matches!(status, AdmissionStatus::Busy) {
            self.calls
                .lock()
                .expect("fake call lock is healthy")
                .push(AdmissionCall {
                    request_id,
                    binding: binding.clone(),
                    prompt,
                    return_final,
                });
        }
        AdmissionReceipt {
            request_id,
            status,
            definitive: status != AdmissionStatus::Indeterminate,
            prompt_written: match status {
                AdmissionStatus::Accepted => Some(true),
                AdmissionStatus::Indeterminate => self
                    .indeterminate_written
                    .load(std::sync::atomic::Ordering::SeqCst)
                    .then_some(true),
                _ => Some(false),
            },
            agent_id: Some(binding.agent_id.clone()),
            incarnation_id: Some(binding.incarnation_id.clone()),
            detail: (status != AdmissionStatus::Accepted).then(|| format!("fake {status:?}")),
        }
    }

    pub fn calls(&self) -> Vec<AdmissionCall> {
        self.calls
            .lock()
            .expect("fake call lock is healthy")
            .clone()
    }

    pub fn steer(&self, binding: &AgentBinding, prompt: String) -> bool {
        self.steering_calls
            .lock()
            .expect("fake steering lock is healthy")
            .push(SteeringCall {
                binding: binding.clone(),
                prompt,
            });
        !self
            .steering_unacknowledged
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn steering_calls(&self) -> Vec<SteeringCall> {
        self.steering_calls
            .lock()
            .expect("fake steering lock is healthy")
            .clone()
    }

    pub fn envelope(command: &SendCommand, source_harness: &str, target_harness: &str) -> String {
        let requested;
        let reply = if command.reply {
            requested = format!(
                "reply requested. Answer when you have the answer, in this turn or a later one: put [panetone:reply] on its own line, outside code, in the message that holds the answer, or [panetone:reply {}] if you owe several replies. [panetone:progress] sends an update and keeps the request open. A reply that you cannot do it counts as an answer. Your final response does not reach the sender",
                &command.id.to_string()[..8]
            );
            requested.as_str()
        } else if command.return_final {
            "asynchronous final callback"
        } else if source_harness == "external" {
            // The sender is outside Wakterm, often a script that has already
            // exited, and cannot be messaged back.
            "one-way from outside Wakterm; no reply is expected. Your final response goes only to this route's chat, and the sender cannot be sent a message"
        } else {
            "one-way"
        };
        format!(
            "[Panetone cross-agent message]\nFrom: {} ({source_harness})\nTo: {} ({target_harness})\nRequest ID: {}\nReply mode: {reply}\n\n{}",
            command.source,
            command.target,
            command.id,
            strip_invisible(&command.message)
        )
    }
}
