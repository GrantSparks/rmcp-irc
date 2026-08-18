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
///
/// This strict form also gates mid-turn `irc.send`, where an overlong line is
/// an error the model sees immediately and can correct within the turn.
pub fn validate_response(
    text: &str,
    allowed_channels: &[String],
    private_senders: &HashSet<String>,
    bootstrap: bool,
) -> anyhow::Result<Vec<ReplyAction>> {
    let response: ReplyEnvelope = serde_json::from_str(text)
        .map_err(|error| anyhow::anyhow!("invalid JSON output: {error}"))?;
    validate_actions(
        &response.actions,
        allowed_channels,
        private_senders,
        bootstrap,
    )?;
    Ok(response.actions)
}

/// Final-turn variant: overlong lines split at word boundaries into follow-on
/// messages instead of being rejected.
///
/// The output schema's `maxLength` counts characters while IRC's limit is
/// bytes, and a model cannot reliably count UTF-8 bytes — an em-dash-rich
/// reply can satisfy the schema, fail the byte gate, and fail it again on the
/// corrective attempt, which is terminal degradation. Splitting is what the
/// coordination protocol asks for anyway: send a second message rather than
/// one long one.
pub fn validate_final_response(
    text: &str,
    allowed_channels: &[String],
    private_senders: &HashSet<String>,
    bootstrap: bool,
) -> anyhow::Result<Vec<ReplyAction>> {
    let response: ReplyEnvelope = serde_json::from_str(text)
        .map_err(|error| anyhow::anyhow!("invalid JSON output: {error}"))?;
    let actions = split_overlong(response.actions);
    validate_actions(&actions, allowed_channels, private_senders, bootstrap)?;
    Ok(actions)
}

fn validate_actions(
    actions: &[ReplyAction],
    allowed_channels: &[String],
    private_senders: &HashSet<String>,
    bootstrap: bool,
) -> anyhow::Result<()> {
    ensure!(
        actions.len() <= MAX_ACTIONS,
        "at most {MAX_ACTIONS} actions are allowed"
    );

    let mut seen = HashSet::new();
    let mut has_hello = false;
    for action in actions {
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
    Ok(())
}

fn is_channel(target: &str) -> bool {
    target
        .as_bytes()
        .first()
        .is_some_and(|byte| matches!(byte, b'#' | b'&' | b'+' | b'!'))
}

fn split_overlong(actions: Vec<ReplyAction>) -> Vec<ReplyAction> {
    let mut normalized = Vec::new();
    for action in actions {
        if action.text.len() <= MAX_TEXT_BYTES {
            normalized.push(action);
            continue;
        }
        for text in split_line(&action.text) {
            normalized.push(ReplyAction {
                target: action.target.clone(),
                text,
            });
        }
    }
    if normalized.len() > MAX_ACTIONS {
        // A visible ellipsis beats either silent loss or terminal degradation.
        tracing::warn!(
            lines = normalized.len(),
            "split replies exceeded the action budget; truncating with an ellipsis"
        );
        normalized.truncate(MAX_ACTIONS);
        if let Some(last) = normalized.last_mut() {
            let cut = floor_char_boundary(&last.text, MAX_TEXT_BYTES - '…'.len_utf8());
            last.text = format!("{}…", last.text[..cut].trim_end());
        }
    }
    normalized
}

/// Split one overlong text into whole-word lines of at most `MAX_TEXT_BYTES`.
fn split_line(text: &str) -> Vec<String> {
    let mut lines = Vec::new();
    let mut rest = text.trim();
    while rest.len() > MAX_TEXT_BYTES {
        let window = &rest[..floor_char_boundary(rest, MAX_TEXT_BYTES)];
        // `rest` starts non-whitespace, so a found break index is never 0.
        let cut = window
            .char_indices()
            .rev()
            .find(|(_, character)| character.is_whitespace())
            .map_or(window.len(), |(index, _)| index);
        let chunk = rest[..cut].trim_end();
        if !chunk.is_empty() {
            lines.push(chunk.to_owned());
        }
        rest = rest[cut..].trim_start();
    }
    if !rest.is_empty() {
        lines.push(rest.to_owned());
    }
    lines
}

/// Largest char-boundary index not exceeding `max_bytes`.
fn floor_char_boundary(text: &str, max_bytes: usize) -> usize {
    let mut end = max_bytes.min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    end
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
    fn final_responses_split_overlong_lines_where_midturn_sends_reject() {
        // Snotra's fatal shape: em-dash-rich text a few BYTES over the cap
        // while comfortably under it in characters.
        let text = format!("start — {} — end", "word ".repeat(67).trim_end());
        assert!(text.len() > MAX_TEXT_BYTES && text.chars().count() < MAX_TEXT_BYTES);
        let value = json!({"actions": [{"target": "#control", "text": text}]});

        assert!(validate(value.clone()).is_err());

        let actions = validate_final_response(
            &value.to_string(),
            &["#control".into()],
            &HashSet::new(),
            false,
        )
        .expect("split instead of rejecting");
        assert_eq!(actions.len(), 2);
        for action in &actions {
            assert_eq!(action.target, "#control");
            assert!(action.text.len() <= MAX_TEXT_BYTES);
            assert!(!action.text.starts_with(' ') && !action.text.ends_with(' '));
        }
        assert!(actions[0].text.starts_with("start —"));
        assert!(actions[1].text.ends_with("end"));
    }

    #[test]
    fn an_unbreakable_word_splits_at_character_boundaries() {
        let text = "é".repeat(300);
        assert_eq!(text.len(), 600);
        let value = json!({"actions": [{"target": "#control", "text": text}]});
        let actions = validate_final_response(
            &value.to_string(),
            &["#control".into()],
            &HashSet::new(),
            false,
        )
        .expect("hard split");
        assert_eq!(actions.len(), 2);
        assert!(
            actions
                .iter()
                .all(|action| action.text.len() <= MAX_TEXT_BYTES)
        );
        assert_eq!(
            actions
                .iter()
                .map(|action| action.text.len())
                .sum::<usize>(),
            600
        );
    }

    #[test]
    fn splitting_beyond_the_action_budget_truncates_with_an_ellipsis() {
        let actions: Vec<_> = (0..MAX_ACTIONS)
            .map(|index| json!({"target": "#control", "text": format!("word{index} ").repeat(80)}))
            .collect();
        let actions = validate_final_response(
            &json!({"actions": actions}).to_string(),
            &["#control".into()],
            &HashSet::new(),
            false,
        )
        .expect("truncate to the budget");
        assert_eq!(actions.len(), MAX_ACTIONS);
        assert!(
            actions
                .iter()
                .all(|action| action.text.len() <= MAX_TEXT_BYTES)
        );
        assert!(actions.last().expect("last line").text.ends_with('…'));
    }

    #[test]
    fn a_split_bootstrap_hello_still_satisfies_the_hello_requirement() {
        let text = format!("hello Nechtan — {}", "word ".repeat(80));
        let actions = validate_final_response(
            &json!({"actions": [{"target": "#control", "text": text}]}).to_string(),
            &["#control".into()],
            &HashSet::new(),
            true,
        )
        .expect("split hello remains a hello");
        assert!(actions[0].text.starts_with("hello"));
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
