//! Asking a caller which nickname one guest should register with.
//!
//! Nickname acquisition is the one part of registration a server can refuse for
//! a reason the caller can fix instantly: the name is taken. The two headless
//! policies answer it without asking — `suffix` invents `Athena_2`, `fail`
//! gives up — and both are right for a deployment with nobody watching. Neither
//! is right when there *is* somebody watching and the identity matters, which is
//! the whole point of a mythological guest name: a silently suffixed nickname is
//! a different agent to everyone in the channel.
//!
//! `elicit` is that third policy. The attempt is abandoned cleanly — no actor,
//! no handle, no half-registered connection — and the tool returns the question
//! instead of a result. The client answers, retries the same call, and the
//! gateway makes a *fresh* registration attempt with the chosen name.
//!
//! This module is the question and the answer, kept free of gateway access so
//! it can be read and tested as what it is: how one refusal becomes a form.

use rmcp::{
    ErrorData as McpError,
    model::{ElicitationSchema, InputRequests, InputResponses},
};
use serde::{Deserialize, Serialize};

use crate::{
    irc::registration::{NickConflictPolicy, Nickname, NicknamePlan},
    mcp::mrtr::{FormAnswer, form_elicitation, read_form_answer, text_field},
};

/// Key the nickname question is filed under within one MRTR round.
///
/// The client echoes it back as the key of its response, so it is part of the
/// wire contract between the two rounds and must not drift.
pub const NICKNAME_INPUT: &str = "connect_nickname";

/// What one `irc.connect` exchange remembers between its rounds.
///
/// Only the candidates the server refused. Everything that decides what the
/// retry will *do* is already bound into the sealed state as the originating
/// arguments, and the chosen name arrives in the answer, so this carries
/// nothing but what the next question would have to say if it were asked again.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct PendingNickname {
    /// Candidates the server refused, in the order they were tried.
    pub attempted: Vec<String>,
    /// The server's own explanation of the last refusal.
    pub detail: String,
}

/// What a client sent back for the nickname question.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    /// No response carried this round's key.
    Missing,
    /// The caller refused to choose, or cancelled.
    Declined,
    /// The caller named one.
    Chosen(String),
}

/// Read the client's answer to the nickname question.
///
/// # Errors
///
/// A response that is not an elicitation result at all is malformed, which the
/// specification says is an ordinary protocol error rather than another round.
pub fn read_answer(responses: Option<&InputResponses>) -> Result<Answer, McpError> {
    Ok(match read_form_answer(responses, NICKNAME_INPUT)? {
        FormAnswer::Missing => Answer::Missing,
        FormAnswer::Declined => Answer::Declined,
        // An accepted form with the field left blank has answered nothing, so
        // it is treated as an unanswered round and asked again rather than
        // being turned into a registration attempt for the empty name.
        FormAnswer::Accepted(content) => match text_field(&content, "nickname") {
            Some(nickname) => Answer::Chosen(nickname),
            None => Answer::Missing,
        },
    })
}

/// Names a `suffix` policy would have tried, minus the ones already refused.
///
/// Offered as suggestions rather than as the answer: they are exactly what the
/// headless policy would have chosen, so a caller who wants that behavior can
/// have it in one click, and a caller who wants a different name is not pushed
/// toward a number.
pub fn suggestions(requested: &Nickname, attempted: &[String], limit: usize) -> Vec<String> {
    // No advertised NICKLEN exists yet — a refused registration never reached
    // ISUPPORT — so nothing is truncated here and the server stays
    // authoritative about length, exactly as it is for the first attempt.
    NicknamePlan::new(requested, &[], NickConflictPolicy::Suffix, None, limit)
        .candidates()
        .iter()
        .map(Nickname::as_str)
        .filter(|candidate| {
            !attempted
                .iter()
                .any(|tried| tried.eq_ignore_ascii_case(candidate))
        })
        .map(str::to_owned)
        .collect()
}

/// Build the form that asks which nickname to register instead.
///
/// A plain string field, not an enum: the caller is choosing an identity, and a
/// closed list of server-generated substitutes is precisely what this policy
/// exists to avoid. Form mode has no way to offer suggestions *and* free entry
/// in one control, so the suggestions live in the message and the most likely
/// one is the field's default.
///
/// Form mode only, and only for a client that declared it: sending an input
/// request a client never said it could answer is a protocol violation, not a
/// graceful degradation.
pub fn nickname_requests(
    pending: &PendingNickname,
    suggestions: &[String],
) -> Result<InputRequests, McpError> {
    let schema = ElicitationSchema::builder()
        .required_string_with("nickname", |field| {
            let field = field
                .title("Nickname")
                .description("Nickname to register this guest with.");
            match suggestions.first() {
                Some(first) => field.with_default(first.clone()),
                None => field,
            }
        })
        .description("Nickname for one IRC guest whose requested names were refused.")
        .build()
        .map_err(|error| McpError::internal_error(error, None))?;

    Ok(InputRequests::from([(
        NICKNAME_INPUT.to_owned(),
        form_elicitation(message(pending, suggestions), schema),
    )]))
}

/// Phrase the question in terms of what the server actually refused.
fn message(pending: &PendingNickname, suggestions: &[String]) -> String {
    let refused = if pending.attempted.is_empty() {
        "The IRC server refused the requested nickname".to_owned()
    } else {
        format!("The IRC server refused {}", pending.attempted.join(", "))
    };
    let suggested = if suggestions.is_empty() {
        String::new()
    } else {
        format!(" Suggestions: {}.", suggestions.join(", "))
    };
    format!(
        "{refused} ({}). Choose a nickname to register instead; the connection is retried from \
         the start with it.{suggested}",
        pending.detail
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nick(value: &str) -> Nickname {
        Nickname::new(value).expect("valid nickname")
    }

    fn pending() -> PendingNickname {
        PendingNickname {
            attempted: vec!["Athena".into()],
            detail: "Nickname is already in use".into(),
        }
    }

    #[test]
    fn suggestions_are_the_names_the_headless_policy_would_have_taken() {
        assert_eq!(
            suggestions(&nick("Athena"), &[], 3),
            vec!["Athena", "Athena_2", "Athena_3"]
        );
    }

    #[test]
    fn a_name_the_server_already_refused_is_never_suggested() {
        // Including under a different case: IRC nicknames are compared
        // case-insensitively, so proposing `athena_2` after `Athena_2` was
        // taken would be proposing the same collision again.
        assert_eq!(
            suggestions(&nick("Athena"), &["Athena".into(), "athena_2".into()], 4),
            vec!["Athena_3", "Athena_4"]
        );
    }

    #[test]
    fn the_form_asks_for_one_free_text_nickname_and_proposes_the_obvious_one() {
        let suggested = suggestions(&nick("Athena"), &["Athena".into()], 3);
        let requests = nickname_requests(&pending(), &suggested).expect("form");
        let wire = serde_json::to_value(&requests).expect("serialize");
        let request = &wire[NICKNAME_INPUT];

        assert_eq!(request["method"], "elicitation/create");
        assert_eq!(request["params"]["mode"], "form");
        let schema = &request["params"]["requestedSchema"];
        assert_eq!(schema["properties"]["nickname"]["type"], "string");
        assert!(
            schema["properties"]["nickname"]["enum"].is_null(),
            "a caller choosing an identity must not be confined to a generated list: {schema}"
        );
        assert_eq!(schema["properties"]["nickname"]["default"], "Athena_2");
        assert_eq!(schema["required"], serde_json::json!(["nickname"]));

        let message = request["params"]["message"]
            .as_str()
            .expect("a message")
            .to_owned();
        assert!(message.contains("Athena"), "{message}");
        assert!(
            message.contains("already in use"),
            "the server's own words are what tell a caller why: {message}"
        );
        assert!(message.contains("Athena_2"), "{message}");
    }

    #[test]
    fn a_blank_answer_is_not_a_nickname() {
        for content in [
            serde_json::json!({ "nickname": "   " }),
            serde_json::json!({}),
        ] {
            assert_eq!(
                read_answer(Some(&InputResponses::from([(
                    NICKNAME_INPUT.to_owned(),
                    serde_json::json!({ "action": "accept", "content": content }),
                )])))
                .expect("answer"),
                Answer::Missing,
                "an empty field must be asked again, not registered"
            );
        }
        assert_eq!(
            read_answer(Some(&InputResponses::from([(
                NICKNAME_INPUT.to_owned(),
                serde_json::json!({ "action": "accept", "content": { "nickname": " Hestia " } }),
            )])))
            .expect("answer"),
            Answer::Chosen("Hestia".into())
        );
        assert_eq!(
            read_answer(Some(&InputResponses::from([(
                NICKNAME_INPUT.to_owned(),
                serde_json::json!({ "action": "decline" }),
            )])))
            .expect("answer"),
            Answer::Declined
        );
        assert_eq!(read_answer(None).expect("answer"), Answer::Missing);
    }
}
