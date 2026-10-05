//! Answering a Claude question form from Telegram: one message lists every
//! question with an option button each, the user taps or types answers, and
//! Submit resolves the whole form through Wakterm.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::json;

use super::contract::ApprovalRequest;
use crate::domain::OutboxAction;

const CALLBACK_PREFIX: &str = "wakf";

/// Whether this approval is a form Panetone can answer with buttons.
pub fn answerable(request: &ApprovalRequest) -> bool {
    request.kind == "user_question_form" && !request.questions.is_empty()
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct FormState {
    /// Answers by question index.
    pub answers: BTreeMap<usize, FormAnswer>,
    /// How the form was closed, once it was.
    pub closed: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FormAnswer {
    Choices(Vec<String>),
    Text(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FormAction {
    /// Tap option `option` (1-based) of question `question` (0-based).
    Option {
        question: usize,
        option: usize,
    },
    Submit,
    Chat,
    Cancel,
}

/// Parses the button data `wakf:<request_id>:<action>`.
pub fn parse_callback(data: &str) -> Option<(String, FormAction)> {
    let mut parts = data.split(':');
    if parts.next()? != CALLBACK_PREFIX {
        return None;
    }
    let request_id = parts.next()?;
    if request_id.len() != 24 || !request_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let action = match (parts.next()?, parts.next()) {
        ("submit", None) => FormAction::Submit,
        ("chat", None) => FormAction::Chat,
        ("cancel", None) => FormAction::Cancel,
        (question, Some(option)) => FormAction::Option {
            question: question.parse().ok()?,
            option: option.parse().ok()?,
        },
        _ => return None,
    };
    parts
        .next()
        .is_none()
        .then(|| (request_id.to_string(), action))
}

/// Parses a typed answer such as `2: ten days`, returning the 0-based question
/// index and the text.
pub fn parse_reply(body: &str) -> Option<(usize, String)> {
    let body = body.trim();
    let digits = body.chars().take_while(char::is_ascii_digit).count();
    let number = body[..digits].parse::<usize>().ok()?.checked_sub(1)?;
    let text = body[digits..]
        .trim_start()
        .trim_start_matches([':', '.', ')', '-'])
        .trim();
    (!text.is_empty()).then(|| (number, text.to_string()))
}

/// Records a tapped option: a single-choice question takes the option, a
/// multi-select question toggles it. Returns a short confirmation.
pub fn tap(
    request: &ApprovalRequest,
    state: &mut FormState,
    question: usize,
    option: usize,
) -> Result<String, String> {
    let asked = request
        .questions
        .get(question)
        .ok_or("that question is not in this form")?;
    let choice = asked
        .options
        .get(option.wrapping_sub(1))
        .ok_or("that option is not in this question")?;
    if asked.multi_select {
        let mut chosen = match state.answers.remove(&question) {
            Some(FormAnswer::Choices(chosen)) => chosen,
            _ => Vec::new(),
        };
        if let Some(position) = chosen.iter().position(|id| *id == choice.id) {
            chosen.remove(position);
        } else {
            chosen.push(choice.id.clone());
        }
        if !chosen.is_empty() {
            state.answers.insert(question, FormAnswer::Choices(chosen));
        }
    } else {
        state
            .answers
            .insert(question, FormAnswer::Choices(vec![choice.id.clone()]));
    }
    Ok(format!("{}: {}", asked.header, choice.label))
}

/// Records a typed answer for a single-choice question.
pub fn type_answer(
    request: &ApprovalRequest,
    state: &mut FormState,
    question: usize,
    text: String,
) -> Result<String, String> {
    let asked = request
        .questions
        .get(question)
        .ok_or("that question is not in this form")?;
    if asked.multi_select {
        return Err(format!(
            "{} takes several choices, so tap its options instead of typing",
            asked.header
        ));
    }
    state.answers.insert(question, FormAnswer::Text(text));
    Ok(format!("{}: typed answer recorded", asked.header))
}

/// The `--answers` JSON Wakterm expects for `--choice submit`.
pub fn answers_json(state: &FormState) -> String {
    let answers = state
        .answers
        .iter()
        .map(|(question, answer)| match answer {
            FormAnswer::Choices(choices) => json!({"question": question, "choices": choices}),
            FormAnswer::Text(text) => json!({"question": question, "text": text}),
        })
        .collect::<Vec<_>>();
    serde_json::Value::Array(answers).to_string()
}

/// The form message text, showing current answers.
pub fn text(request: &ApprovalRequest, state: &FormState) -> String {
    let mut body = String::from("Input needed");
    for asked in &request.questions {
        body.push_str(&format!(
            "\n\n{}. {}: {}{}",
            asked.index + 1,
            asked.header,
            asked.question,
            if asked.multi_select {
                " (choose any)"
            } else {
                ""
            }
        ));
        for (number, option) in asked.options.iter().enumerate() {
            body.push_str(&format!("\n{}) {}", number + 1, option.label));
            if let Some(description) = option.description.as_deref() {
                body.push_str(&format!(": {description}"));
            }
        }
        let answer = match state.answers.get(&asked.index) {
            Some(FormAnswer::Choices(chosen)) => asked
                .options
                .iter()
                .filter(|option| chosen.contains(&option.id))
                .map(|option| option.label.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            Some(FormAnswer::Text(text)) => format!("\"{text}\""),
            None => "not answered yet".into(),
        };
        body.push_str(&format!("\nAnswer: {answer}"));
    }
    match &state.closed {
        Some(closed) => body.push_str(&format!("\n\n{closed}")),
        None => body.push_str(
            "\n\nTap an option for each question, then Submit. To type an answer instead, reply to this message with the question number and your text, for example \"1: your answer\".",
        ),
    }
    body
}

/// The form buttons: one per option, marked when chosen, then Submit, chat
/// and cancel. A closed form has none.
pub fn actions(request: &ApprovalRequest, state: &FormState) -> Vec<OutboxAction> {
    if state.closed.is_some() {
        return Vec::new();
    }
    let callback = |action: &str| format!("{CALLBACK_PREFIX}:{}:{action}", request.request_id);
    let mut actions = Vec::new();
    for asked in &request.questions {
        let chosen = match state.answers.get(&asked.index) {
            Some(FormAnswer::Choices(chosen)) => chosen.as_slice(),
            _ => &[],
        };
        for (number, option) in asked.options.iter().enumerate() {
            let mark = if chosen.contains(&option.id) {
                "✓ "
            } else {
                ""
            };
            actions.push(OutboxAction {
                id: callback(&format!("{}:{}", asked.index, number + 1)),
                label: format!("{}. {mark}{}", asked.index + 1, option.label),
            });
        }
    }
    for (action, label) in [
        ("submit", "Submit"),
        ("chat", "Chat about this"),
        ("cancel", "Cancel"),
    ] {
        actions.push(OutboxAction {
            id: callback(action),
            label: label.into(),
        });
    }
    actions
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A form in the shape Wakterm documents for `question_form_answers.v1`.
    fn form() -> ApprovalRequest {
        serde_json::from_value(json!({
            "schema": "wakterm.agent-approval.v1",
            "kind": "user_question_form",
            "request_id": "0123456789abcdef01234567",
            "agent_id": "agent",
            "incarnation_id": "incarnation",
            "turn_id": "turn",
            "item_id": "item",
            "observed_at": "2026-10-05T04:44:00Z",
            "prompt": "1. Browser: Which browser?",
            "reason": null,
            "command": null,
            "cwd": null,
            "choices": [],
            "questions": [
                {"index": 0, "header": "Browser", "question": "Which browser?", "multi_select": false,
                 "options": [{"id": "option_1", "label": "Chrome", "description": "Google Chrome."},
                             {"id": "option_2", "label": "Edge", "description": null}]},
                {"index": 1, "header": "Tools", "question": "Which tools?", "multi_select": true,
                 "options": [{"id": "option_1", "label": "A"}, {"id": "option_2", "label": "B"}]}
            ]
        }))
        .unwrap()
    }

    #[test]
    fn callbacks_and_replies_parse_and_reject_malformed_data() {
        let request = "0123456789abcdef01234567";
        assert_eq!(
            parse_callback(&format!("wakf:{request}:1:2")),
            Some((
                request.into(),
                FormAction::Option {
                    question: 1,
                    option: 2
                }
            ))
        );
        assert_eq!(
            parse_callback(&format!("wakf:{request}:submit")),
            Some((request.into(), FormAction::Submit))
        );
        assert_eq!(parse_callback("wakf:short:submit"), None);
        assert_eq!(parse_callback(&format!("wakap:{request}:submit")), None);
        assert_eq!(parse_callback(&format!("wakf:{request}:1:2:3")), None);
        assert!(
            actions(&form(), &FormState::default())
                .iter()
                .all(|action| action.id.len() <= 64)
        );

        assert_eq!(
            parse_reply("1: new chrome profile"),
            Some((0, "new chrome profile".into()))
        );
        assert_eq!(parse_reply(" 2. ten days "), Some((1, "ten days".into())));
        assert_eq!(parse_reply("no number"), None);
        assert_eq!(parse_reply("0: zero"), None);
        assert_eq!(parse_reply("1:"), None);
    }

    #[test]
    fn taps_and_typed_answers_build_the_wakterm_answers() {
        let request = form();
        let mut state = FormState::default();
        tap(&request, &mut state, 0, 1).unwrap();
        tap(&request, &mut state, 0, 2).unwrap();
        tap(&request, &mut state, 1, 1).unwrap();
        tap(&request, &mut state, 1, 2).unwrap();
        tap(&request, &mut state, 1, 1).unwrap();
        assert_eq!(
            answers_json(&state),
            r#"[{"choices":["option_2"],"question":0},{"choices":["option_2"],"question":1}]"#
        );
        type_answer(&request, &mut state, 0, "Firefox".into()).unwrap();
        assert!(type_answer(&request, &mut state, 1, "both".into()).is_err());
        assert!(tap(&request, &mut state, 2, 1).is_err());
        assert!(tap(&request, &mut state, 0, 3).is_err());
        assert_eq!(
            answers_json(&state),
            r#"[{"question":0,"text":"Firefox"},{"choices":["option_2"],"question":1}]"#
        );

        let shown = text(&request, &state);
        assert!(shown.contains(
            "1. Browser: Which browser?\n1) Chrome: Google Chrome.\n2) Edge\nAnswer: \"Firefox\""
        ));
        assert!(shown.contains("2. Tools: Which tools? (choose any)"));
        let buttons = actions(&request, &state);
        assert_eq!(buttons.len(), 7);
        assert_eq!(buttons[3].label, "2. ✓ B");
        assert_eq!(buttons[4].id, "wakf:0123456789abcdef01234567:submit");

        state.closed = Some("Submitted.".into());
        assert!(actions(&request, &state).is_empty());
        assert!(text(&request, &state).ends_with("Submitted."));
    }
}
