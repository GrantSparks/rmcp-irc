//! Putting a person in front of one destructive IRC mutation.
//!
//! `irc.kick` and `irc.message.redact` are the two tools whose effect is
//! visible to other people and cannot be taken back: a removed member and a
//! removed message. Some deployments want a human to approve each one; most do
//! not, and a gateway that always asked would be unusable headlessly. So the
//! gate is configuration (`mcp.confirm_destructive`, off by default) and the
//! question is an MRTR round trip on the call itself.
//!
//! Two properties make the gate worth anything, and both are enforced in
//! [`crate::mcp::service`] rather than here:
//!
//! 1. **Nothing is applied before the answer.** The confirmation is settled
//!    before the IRC command is written, so a decline, an expiry, a forged
//!    state, or a client that never retries all leave the channel untouched.
//! 2. **A request that cannot be asked is refused, not waved through.** The
//!    setting exists because somebody decided a model may not do this alone;
//!    proceeding when there is nobody to ask would quietly delete the policy.
//!
//! The summary a caller confirms is built from the already-validated arguments
//! and sealed with them, so the action described in the question is exactly the
//! action the retry performs.

use rmcp::{
    ErrorData as McpError,
    model::{ElicitationSchema, InputRequests, InputResponses},
};
use serde::{Deserialize, Serialize};

use crate::mcp::mrtr::{FormAnswer, bool_field, form_elicitation, read_form_answer};

/// Key the confirmation question is filed under within one MRTR round.
///
/// The client echoes it back as the key of its response, so it is part of the
/// wire contract between the two rounds and must not drift.
pub const CONFIRMATION_INPUT: &str = "destructive_confirmation";

/// What one confirmation exchange remembers between its rounds.
///
/// The rendered action, so redemption can check that the state belongs to the
/// question that was actually shown. The sealed binding already covers the
/// caller and the call's arguments; this makes the *description* a person read
/// part of what is verified, rather than something re-derived on trust.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct PendingConfirmation {
    /// Exact action summary the caller was shown.
    pub action: String,
}

impl PendingConfirmation {
    /// Describe the action about to be confirmed.
    pub fn for_action(action: impl Into<String>) -> Self {
        Self {
            action: action.into(),
        }
    }

    /// Whether this state still describes the action in front of us.
    pub fn matches(&self, action: &str) -> bool {
        self.action == action
    }
}

/// What a client sent back for the confirmation question.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    /// No response carried this round's key, or the box was left unfilled.
    Missing,
    /// The caller declined, cancelled, or answered "no".
    Refused,
    /// The caller confirmed.
    Confirmed,
}

/// Read the client's answer to the confirmation question.
///
/// # Errors
///
/// A response that is not an elicitation result at all is malformed, which the
/// specification says is an ordinary protocol error rather than another round.
pub fn read_answer(responses: Option<&InputResponses>) -> Result<Answer, McpError> {
    Ok(match read_form_answer(responses, CONFIRMATION_INPUT)? {
        FormAnswer::Missing => Answer::Missing,
        FormAnswer::Declined => Answer::Refused,
        // An accepted form is not by itself a confirmation: the field is what
        // the person answered, and its absence is an unanswered question rather
        // than an implied yes.
        FormAnswer::Accepted(content) => match bool_field(&content, "confirm") {
            Some(true) => Answer::Confirmed,
            Some(false) => Answer::Refused,
            None => Answer::Missing,
        },
    })
}

/// Build the form that asks a caller to confirm one exact action.
///
/// Form mode only, and only for a client that declared it: sending an input
/// request a client never said it could answer is a protocol violation, not a
/// graceful degradation.
pub fn confirmation_requests(action: &str) -> Result<InputRequests, McpError> {
    let schema = ElicitationSchema::builder()
        .required_bool_with("confirm", |field| {
            field
                .title("Confirm")
                .description("Apply this action. Answering no leaves everything unchanged.")
        })
        .description("Confirmation for one destructive IRC mutation.")
        .build()
        .map_err(|error| McpError::internal_error(error, None))?;

    Ok(InputRequests::from([(
        CONFIRMATION_INPUT.to_owned(),
        form_elicitation(
            format!("This gateway requires confirmation before: {action}"),
            schema,
        ),
    )]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answered(content: serde_json::Value) -> InputResponses {
        InputResponses::from([(
            CONFIRMATION_INPUT.to_owned(),
            serde_json::json!({ "action": "accept", "content": content }),
        )])
    }

    #[test]
    fn only_an_explicit_yes_confirms() {
        assert_eq!(
            read_answer(Some(&answered(serde_json::json!({ "confirm": true })))).expect("answer"),
            Answer::Confirmed
        );
        assert_eq!(
            read_answer(Some(&answered(serde_json::json!({ "confirm": false })))).expect("answer"),
            Answer::Refused
        );
        assert_eq!(
            read_answer(Some(&answered(serde_json::json!({})))).expect("answer"),
            Answer::Missing,
            "an unfilled box is a question still waiting, never an implied yes"
        );
        assert_eq!(read_answer(None).expect("answer"), Answer::Missing);
        for refusal in ["decline", "cancel"] {
            assert_eq!(
                read_answer(Some(&InputResponses::from([(
                    CONFIRMATION_INPUT.to_owned(),
                    serde_json::json!({ "action": refusal }),
                )])))
                .expect("answer"),
                Answer::Refused
            );
        }
    }

    #[test]
    fn the_form_states_the_exact_action_and_asks_one_boolean() {
        let action = "kick Prometheus from #forge (reason: repeated flooding)";
        let requests = confirmation_requests(action).expect("form");
        let wire = serde_json::to_value(&requests).expect("serialize");
        let request = &wire[CONFIRMATION_INPUT];
        assert_eq!(request["method"], "elicitation/create");
        assert_eq!(request["params"]["mode"], "form");
        let schema = &request["params"]["requestedSchema"];
        assert_eq!(schema["properties"]["confirm"]["type"], "boolean");
        assert_eq!(schema["required"], serde_json::json!(["confirm"]));
        assert!(
            request["params"]["message"]
                .as_str()
                .expect("message")
                .contains(action),
            "a person can only approve what the question actually describes: {request}"
        );
    }

    #[test]
    fn a_state_only_matches_the_action_it_was_minted_for() {
        let pending = PendingConfirmation::for_action("kick Prometheus from #forge");
        assert!(pending.matches("kick Prometheus from #forge"));
        assert!(!pending.matches("kick Prometheus from #other"));
    }
}
