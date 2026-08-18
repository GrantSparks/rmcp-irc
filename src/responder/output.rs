//! Structured-output schema and the final local IRC policy gate.

use std::collections::HashSet;

use anyhow::{bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Maximum replies one model turn can request.
pub const MAX_ACTIONS: usize = 8;
/// Conservative payload cap below the gateway's negotiated IRC line budget.
pub const MAX_TEXT_BYTES: usize = 350;

/// One final IRC `PRIVMSG`; the same shape validates mid-turn `irc.send` calls.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplyAction {
    /// Channel allowlisted by the operator, or a private sender from this batch.
    pub target: String,
    /// One short IRC line.
    pub text: String,
}

/// Complete structured response accepted from Codex.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplyEnvelope {
    /// Ordered IRC replies. An empty list is valid after bootstrap.
    pub actions: Vec<ReplyAction>,
}

/// JSON Schema passed to every `turn/start`.
pub fn output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "actions": {
                "type": "array",
                "maxItems": MAX_ACTIONS,
                "items": {
                    "type": "object",
                    "properties": {
                        "target": {"type": "string", "minLength": 1},
                        "text": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": MAX_TEXT_BYTES
                        }
                    },
                    "required": ["target", "text"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["actions"],
        "additionalProperties": false
    })
}

/// Decode and enforce every rule that is intentionally outside model control.
pub fn validate_response(
    text: &str,
    allowed_channels: &[String],
    private_senders: &HashSet<String>,
    bootstrap: bool,
) -> anyhow::Result<Vec<ReplyAction>> {
    let response: ReplyEnvelope = serde_json::from_str(text)
        .map_err(|error| anyhow::anyhow!("invalid JSON output: {error}"))?;
    ensure!(
        response.actions.len() <= MAX_ACTIONS,
        "at most {MAX_ACTIONS} actions are allowed"
    );

    let mut seen = HashSet::new();
    let mut has_hello = false;
    for action in &response.actions {
        if action.text.is_empty() || action.text.trim().is_empty() {
            bail!("reply text must not be empty");
        }
        if action
            .text
            .bytes()
            .any(|byte| matches!(byte, b'\r' | b'\n' | b'\0'))
        {
            bail!("reply text contains an IRC control delimiter");
        }
        ensure!(
            action.text.len() <= MAX_TEXT_BYTES,
            "reply text is {} UTF-8 bytes; maximum is {MAX_TEXT_BYTES}",
            action.text.len()
        );
        ensure!(
            !action.target.is_empty()
                && !action
                    .target
                    .bytes()
                    .any(|byte| matches!(byte, b'\r' | b'\n' | b'\0' | b' ' | b',')),
            "reply target is not one IRC target"
        );

        let target_key = action.target.to_ascii_lowercase();
        let target_allowed = if is_channel(&action.target) {
            allowed_channels
                .iter()
                .any(|channel| channel.eq_ignore_ascii_case(&action.target))
        } else {
            private_senders
                .iter()
                .any(|sender| sender.eq_ignore_ascii_case(&action.target))
        };
        ensure!(
            target_allowed,
            "reply target {:?} is not allowed",
            action.target
        );
        ensure!(
            seen.insert((target_key, action.text.clone())),
            "duplicate reply action"
        );

        if action.target.eq_ignore_ascii_case("#control")
            && action
                .text
                .split_whitespace()
                .next()
                .is_some_and(|word| word.eq_ignore_ascii_case("hello"))
        {
            has_hello = true;
        }
    }
    if bootstrap && !has_hello {
        bail!("the bootstrap response must include a #control line beginning with `hello`");
    }
    Ok(response.actions)
}

fn is_channel(target: &str) -> bool {
    target
        .as_bytes()
        .first()
        .is_some_and(|byte| matches!(byte, b'#' | b'&' | b'+' | b'!'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn validate(value: Value) -> anyhow::Result<Vec<ReplyAction>> {
        validate_response(
            &value.to_string(),
            &["#control".into()],
            &HashSet::from(["grant".into()]),
            false,
        )
    }

    #[test]
    fn accepts_allowlisted_channels_and_batch_private_senders() {
        let actions = validate(json!({"actions": [
            {"target": "#CONTROL", "text": "status still watching"},
            {"target": "Grant", "text": "acknowledged"}
        ]}))
        .expect("valid replies");
        assert_eq!(actions.len(), 2);
    }

    #[test]
    fn schema_advertises_the_conservative_text_limit() {
        assert_eq!(
            output_schema()["properties"]["actions"]["items"]["properties"]["text"]["maxLength"],
            MAX_TEXT_BYTES
        );
    }

    #[test]
    fn rejects_target_escalation_and_spoofed_dm() {
        assert!(validate(json!({"actions": [{"target": "#other", "text": "x"}]})).is_err());
        assert!(validate(json!({"actions": [{"target": "nobody", "text": "x"}]})).is_err());
    }

    #[test]
    fn rejects_controls_utf8_byte_overflow_duplicates_and_excess() {
        assert!(validate(json!({"actions": [{"target": "#control", "text": "a\nb"}]})).is_err());
        assert!(
            validate(json!({"actions": [{"target": "#control", "text": "é".repeat(176)}]}))
                .is_err()
        );
        assert!(
            validate(json!({"actions": [
                {"target": "#control", "text": "same"},
                {"target": "#CONTROL", "text": "same"}
            ]}))
            .is_err()
        );
        let actions: Vec<_> = (0..=MAX_ACTIONS)
            .map(|index| json!({"target": "#control", "text": format!("{index}")}))
            .collect();
        assert!(validate(json!({"actions": actions})).is_err());
    }

    #[test]
    fn rejects_extra_operations_and_requires_bootstrap_hello() {
        assert!(
            validate(json!({"actions": [{
                "target": "#control", "text": "hello", "kind": "notice"
            }]}))
            .is_err()
        );
        assert!(
            validate_response(
                r#"{"actions":[]}"#,
                &["#control".into()],
                &HashSet::new(),
                true
            )
            .is_err()
        );
        assert!(
            validate_response(
                r##"{"actions":[{"target":"#control","text":"hello Nabu - responder online"}]}"##,
                &["#control".into()],
                &HashSet::new(),
                true
            )
            .is_ok()
        );
    }
}
