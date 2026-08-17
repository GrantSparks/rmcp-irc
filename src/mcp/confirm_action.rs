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
//!
//! A third property needs state, which is why [`RedeemedConfirmations`] lives
//! here: **one approval applies one action**. A sealed confirmation is an HMAC
//! over the caller, the arguments, and a deadline, and nothing in that says how
//! often it may be presented — so without a record of what has been spent, a
//! single human "yes" re-executes the same kick for the rest of its
//! time-to-live, which is exactly the authority the deployment withheld from the
//! model.

use std::{collections::VecDeque, sync::Mutex, time::Instant};

use rmcp::{
    ErrorData as McpError,
    model::{ElicitationSchema, InputRequests, InputResponses},
};
use serde::{Deserialize, Serialize};

use crate::mcp::mrtr::{
    FormAnswer, REQUEST_STATE_TTL, bool_field, form_elicitation, read_form_answer,
};

/// Confirmations retained at once, an upper bound on what this costs.
///
/// A record lives for [`REQUEST_STATE_TTL`], and every one of them is a
/// separate action a person approved inside that window, so the ceiling is far
/// above any human-paced deployment. Past it the oldest record is dropped —
/// deliberately, rather than refusing further confirmations: turning memory
/// pressure into an outage of the gated tools would be the worse failure, and
/// reaching this at all means something other than a person is answering.
const MAX_REDEEMED: usize = 1_024;

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

/// The confirmations this process has already acted on.
///
/// ## Why this exists
///
/// A `requestState` is integrity-protected and short-lived, and both of those
/// are about *who* may redeem it and *for how long* — neither says *how many
/// times*. For a question whose answer is "yes, remove that person", once is
/// the whole point: a client that repeats the identical call with the identical
/// answer would otherwise kick again, and again, for two minutes, on one
/// person's approval.
///
/// ## Why only this flow
///
/// Single-use is deliberately **not** a property of request state in general.
/// The other exchanges answer a question about what an operation should do — a
/// nickname, a channel key, a destination — and re-running one is at worst a
/// repeated attempt at something the caller asked for; the DCC acceptance path
/// re-opens its own state *inside* the task it creates, so making states
/// single-use would break it outright. What is special here is that the answer
/// is an authorization rather than an argument, and an authorization that can
/// be replayed is not one. So the ledger is consumed in exactly one place: the
/// moment a confirmed destructive mutation is about to be written.
///
/// Records are process-local and expire with the states they describe, which
/// needs no more retention than that: a record can only be useful while the
/// state it names is still redeemable at all.
#[derive(Default)]
pub struct RedeemedConfirmations {
    spent: Mutex<VecDeque<Spent>>,
}

/// One confirmation that has already been acted on, and when.
struct Spent {
    /// The sealed state exactly as the client presented it.
    ///
    /// Kept whole rather than digested: it already ends in the codec's own
    /// authentication tag, so it is its own collision-resistant identifier,
    /// while a short digest could only make two distinct approvals collide into
    /// one and refuse a legitimate confirmation.
    sealed: String,
    redeemed_at: Instant,
}

impl std::fmt::Debug for RedeemedConfirmations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The records are live request states; only how many there are is safe
        // to print inside a debug of the whole gateway.
        formatter
            .debug_struct("RedeemedConfirmations")
            .field("spent", &self.lock().len())
            .finish_non_exhaustive()
    }
}

impl RedeemedConfirmations {
    /// Spend one confirmation, reporting whether it had already been spent.
    ///
    /// Returns `true` exactly once per sealed state — the caller may apply the
    /// action — and `false` for every later presentation of the same one.
    pub fn redeem(&self, sealed: &str) -> bool {
        self.redeem_at(sealed, Instant::now())
    }

    /// The same decision at an explicit instant, so retention is testable
    /// without waiting out the time-to-live.
    fn redeem_at(&self, sealed: &str, now: Instant) -> bool {
        let mut spent = self.lock();
        // A record outliving the state it names could only refuse a
        // confirmation that can no longer be redeemed anyway.
        while spent
            .front()
            .is_some_and(|oldest| now.duration_since(oldest.redeemed_at) >= REQUEST_STATE_TTL)
        {
            spent.pop_front();
        }
        if spent.iter().any(|entry| entry.sealed == sealed) {
            return false;
        }
        if spent.len() >= MAX_REDEEMED {
            spent.pop_front();
        }
        spent.push_back(Spent {
            sealed: sealed.to_owned(),
            redeemed_at: now,
        });
        true
    }

    /// Take the ledger, recovering from a panic in another holder.
    ///
    /// Nothing under this lock can leave a record half-written, so a poisoned
    /// lock means an unrelated panic — and refusing every later confirmation
    /// over it would turn that into a gate nobody can pass.
    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Spent>> {
        self.spent.lock().unwrap_or_else(|poisoned| {
            self.spent.clear_poison();
            poisoned.into_inner()
        })
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

    #[test]
    fn one_approval_is_spent_by_the_first_action_it_applies() {
        let spent = RedeemedConfirmations::default();
        assert!(spent.redeem("rs1.approved"), "the first use is the answer");
        for _ in 0..3 {
            assert!(
                !spent.redeem("rs1.approved"),
                "a replayed approval must not apply the action a second time"
            );
        }
        assert!(
            spent.redeem("rs1.approved-again"),
            "a fresh confirmation is unaffected by another one being spent"
        );
    }

    #[test]
    fn a_record_is_retired_with_the_state_it_describes() {
        // A record that outlived its state would only refuse a confirmation
        // that has expired anyway, and one retired early would let the
        // approval be replayed for the rest of its window.
        let spent = RedeemedConfirmations::default();
        let approved = Instant::now();
        assert!(spent.redeem_at("rs1.approved", approved));
        assert!(!spent.redeem_at("rs1.approved", approved + REQUEST_STATE_TTL / 2));
        assert!(spent.redeem_at("rs1.approved", approved + REQUEST_STATE_TTL));
    }

    #[test]
    fn the_ledger_holds_a_bounded_number_of_records() {
        let spent = RedeemedConfirmations::default();
        let now = Instant::now();
        for index in 0..MAX_REDEEMED + 8 {
            assert!(spent.redeem_at(&format!("rs1.{index}"), now));
        }
        assert_eq!(spent.lock().len(), MAX_REDEEMED);
        assert!(
            !spent.redeem_at(&format!("rs1.{}", MAX_REDEEMED + 7), now),
            "the newest approvals are the ones still protected from replay"
        );
    }
}
