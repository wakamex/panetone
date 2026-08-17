use std::collections::VecDeque;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::domain::{
    AdmissionReceipt, AdmissionStatus, AgentBinding, EffectId, SendCommand, WorkflowId,
};

use super::{AgentCatalog, WaktermContract};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionCall {
    pub request_id: EffectId,
    pub binding: AgentBinding,
    pub prompt: String,
    pub return_final: bool,
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
    terminal: Mutex<VecDeque<TerminalResult>>,
}

impl FakeWakterm {
    pub fn new(contract: WaktermContract) -> Self {
        Self {
            contract,
            receipts: Mutex::new(VecDeque::new()),
            calls: Mutex::new(Vec::new()),
            terminal: Mutex::new(VecDeque::new()),
        }
    }

    pub fn capabilities(&self) -> &std::collections::BTreeSet<String> {
        &self.contract.capabilities
    }

    pub fn catalog(&self) -> AgentCatalog {
        self.contract.catalog.clone()
    }

    pub fn general_event_consumer_enabled(&self) -> bool {
        self.contract.general_event_consumer_enabled()
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
                AdmissionStatus::Indeterminate => None,
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

    pub fn push_terminal(&self, result: TerminalResult) {
        self.terminal
            .lock()
            .expect("fake terminal lock is healthy")
            .push_back(result);
    }

    pub fn next_terminal(&self) -> Option<TerminalResult> {
        self.terminal
            .lock()
            .expect("fake terminal lock is healthy")
            .pop_front()
    }

    pub fn envelope(command: &SendCommand, source_harness: &str, target_harness: &str) -> String {
        let reply = if command.return_final {
            "asynchronous final callback"
        } else {
            "one-way"
        };
        format!(
            "[Panetone cross-agent message]\nFrom: {} ({source_harness})\nTo: {} ({target_harness})\nRequest ID: {}\nReply mode: {reply}\n\n{}",
            command.source, command.target, command.id, command.message
        )
    }
}
