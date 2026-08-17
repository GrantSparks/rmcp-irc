//! Asking a caller for the key one channel wants.
//!
//! `ERR_BADCHANNELKEY` (475) is the narrowest possible failure: the channel
//! exists, the guest may reach it, and the only missing thing is a secret the
//! server will not describe. Every other join rejection — invite-only, banned,
//! full, throttled — is a decision about *the guest* that no answer from the
//! caller would change, so only 475 becomes a question here.
//!
//! No configuration gates it. The exchange happens strictly inside a join the
//! caller already asked for, asks for exactly the argument the tool already
//! accepts, is only ever offered to a request that declared it can answer a
//! form, and can be declined — so a flag would be a second, less discoverable
//! way of saying "do not declare elicitation".
//!
//! One honest limitation belongs to the caller: MRTR form mode has no secret
//! field. A channel key is an ordinary string property, and the host renders it
//! as such.

use rmcp::{
    ErrorData as McpError,
    model::{ElicitationSchema, InputRequests, InputResponses},
};
use serde::{Deserialize, Serialize};

use crate::{
    irc::correlation::{CommandOutcome, CommandResult},
    mcp::mrtr::{FormAnswer, form_elicitation, read_form_answer, text_field},
};

/// Key the channel-key question is filed under within one MRTR round.
///
/// The client echoes it back as the key of its response, so it is part of the
/// wire contract between the two rounds and must not drift.
pub const CHANNEL_KEY_INPUT: &str = "channel_key";

/// `ERR_BADCHANNELKEY`: the channel wants a key this join did not carry.
const ERR_BADCHANNELKEY: u16 = 475;

/// What one `irc.join` exchange remembers between its rounds.
///
/// The channel is re-checked on redemption even though the sealed binding
/// already covers the call's arguments, so a state can only ever be spent on
/// the join whose refusal the caller was actually shown.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct PendingJoin {
    /// Channel the key was asked about, case-preserved.
    pub channel: String,
}

impl PendingJoin {
    /// Whether this state still describes the join in front of us.
    pub fn matches(&self, channel: &str) -> bool {
        self.channel == channel
    }
}

/// What a client sent back for the channel-key question.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    /// No response carried this round's key.
    Missing,
    /// The caller refused to supply one, or cancelled.
    Declined,
    /// The caller supplied a key.
    Chosen(String),
}

/// Read the client's answer to the channel-key question.
///
/// # Errors
///
/// A response that is not an elicitation result at all is malformed, which the
/// specification says is an ordinary protocol error rather than another round.
pub fn read_answer(responses: Option<&InputResponses>) -> Result<Answer, McpError> {
    Ok(match read_form_answer(responses, CHANNEL_KEY_INPUT)? {
        FormAnswer::Missing => Answer::Missing,
        FormAnswer::Declined => Answer::Declined,
        // An accepted form left blank has answered nothing: re-issuing the JOIN
        // with an empty key would just collect the same 475.
        FormAnswer::Accepted(content) => match text_field(&content, "key") {
            Some(key) => Answer::Chosen(key),
            None => Answer::Missing,
        },
    })
}

/// Whether this join failed *only* because it carried no key.
///
/// Read from the correlated replies rather than from the outcome, which
/// collapses every error numeric into one rejection. The raw numeric is
/// retained there precisely so a caller — here, the tool itself — can tell
/// which refusal it was.
pub fn needs_key(result: &CommandResult, key_supplied: bool) -> bool {
    // A key that was already supplied and still refused is a wrong key, not a
    // missing one. Asking again would invite the caller to guess, and the
    // structured rejection is the honest answer.
    !key_supplied
        && result.outcome == CommandOutcome::Rejected
        && result
            .replies
            .iter()
            .any(|reply| reply.numeric() == Some(ERR_BADCHANNELKEY))
}

/// Build the form that asks for one channel's key.
///
/// Form mode only, and only for a client that declared it: sending an input
/// request a client never said it could answer is a protocol violation, not a
/// graceful degradation.
pub fn key_requests(channel: &str, detail: Option<&str>) -> Result<InputRequests, McpError> {
    let schema = ElicitationSchema::builder()
        .required_string_with("key", |field| {
            field
                .title("Channel key")
                .description("Key this channel requires. Sent as the JOIN key parameter.")
        })
        .description("Key for one keyed IRC channel.")
        .build()
        .map_err(|error| McpError::internal_error(error, None))?;

    let detail = detail.map_or_else(String::new, |detail| format!(" The server said: {detail}."));
    Ok(InputRequests::from([(
        CHANNEL_KEY_INPUT.to_owned(),
        form_elicitation(
            format!(
                "{channel} needs a key and the join did not carry one.{detail} Supply the key to \
                 join, or decline to leave the channel unjoined."
            ),
            schema,
        ),
    )]))
}

/// The server's explanation of a rejection, when it gave one.
pub fn rejection_detail(result: &CommandResult) -> Option<String> {
    result
        .replies
        .iter()
        .find(|reply| reply.numeric() == Some(ERR_BADCHANNELKEY))
        .and_then(|reply| reply.trailing.clone())
        .filter(|detail| !detail.is_empty())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{agent::AgentId, irc::correlation::CommandId, irc::wire::WireMessage};

    use super::*;

    fn result(outcome: CommandOutcome, replies: &[&str]) -> CommandResult {
        CommandResult {
            command_id: CommandId::new(),
            agent_id: AgentId::new(),
            command: "JOIN".into(),
            outcome,
            written: true,
            acknowledged: false,
            retriable: false,
            label: None,
            replies: replies
                .iter()
                .map(|line| {
                    WireMessage::parse(Bytes::copy_from_slice(line.as_bytes())).expect("wire")
                })
                .collect(),
            semantic_result: None,
            warnings: Vec::new(),
            first_event_cursor: None,
        }
    }

    #[test]
    fn only_a_missing_key_becomes_a_question() {
        let refused = result(
            CommandOutcome::Rejected,
            &[":fake 475 guest #locked :Cannot join channel (+k)"],
        );
        assert!(needs_key(&refused, false));
        assert_eq!(
            rejection_detail(&refused).as_deref(),
            Some("Cannot join channel (+k)")
        );

        assert!(
            !needs_key(&refused, true),
            "a key that was supplied and refused is wrong, not missing"
        );
        for other in [
            ":fake 473 guest #locked :Cannot join channel (+i)",
            ":fake 474 guest #locked :Cannot join channel (+b)",
            ":fake 471 guest #locked :Cannot join channel (+l)",
        ] {
            assert!(
                !needs_key(&result(CommandOutcome::Rejected, &[other]), false),
                "{other} is a decision about the guest, which no key changes"
            );
        }
        assert!(
            !needs_key(
                &result(CommandOutcome::Completed, &[":guest!u@h JOIN #locked"]),
                false
            ),
            "a successful join is never asked about"
        );
        assert_eq!(
            rejection_detail(&result(CommandOutcome::Rejected, &[])),
            None
        );
    }

    #[test]
    fn the_form_names_the_channel_and_asks_for_one_string() {
        let requests = key_requests("#locked", Some("Cannot join channel (+k)")).expect("form");
        let wire = serde_json::to_value(&requests).expect("serialize");
        let request = &wire[CHANNEL_KEY_INPUT];
        assert_eq!(request["method"], "elicitation/create");
        assert_eq!(request["params"]["mode"], "form");
        let schema = &request["params"]["requestedSchema"];
        assert_eq!(schema["properties"]["key"]["type"], "string");
        assert_eq!(schema["required"], serde_json::json!(["key"]));
        let message = request["params"]["message"].as_str().expect("message");
        assert!(message.contains("#locked"), "{message}");
        assert!(message.contains("Cannot join channel (+k)"), "{message}");
    }

    #[test]
    fn a_blank_key_is_not_an_answer() {
        assert_eq!(
            read_answer(Some(&InputResponses::from([(
                CHANNEL_KEY_INPUT.to_owned(),
                serde_json::json!({ "action": "accept", "content": { "key": "" } }),
            )])))
            .expect("answer"),
            Answer::Missing
        );
        assert_eq!(
            read_answer(Some(&InputResponses::from([(
                CHANNEL_KEY_INPUT.to_owned(),
                serde_json::json!({ "action": "accept", "content": { "key": "sesame" } }),
            )])))
            .expect("answer"),
            Answer::Chosen("sesame".into())
        );
        assert_eq!(
            read_answer(Some(&InputResponses::from([(
                CHANNEL_KEY_INPUT.to_owned(),
                serde_json::json!({ "action": "cancel" }),
            )])))
            .expect("answer"),
            Answer::Declined
        );
    }

    #[test]
    fn a_state_only_matches_the_channel_it_was_minted_for() {
        let pending = PendingJoin {
            channel: "#locked".into(),
        };
        assert!(pending.matches("#locked"));
        assert!(!pending.matches("#other"));
    }
}
