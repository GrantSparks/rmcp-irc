//! The model names its own IRC identity before that identity exists.
//!
//! The coordination protocol makes nickname choice the agent's: a figure
//! reflecting its mood and goals, deliberately obscure. IRC registration
//! needs the nickname before the server will say anything, and this adapter
//! only thinks when a turn runs, so a fresh profile gets one read-only
//! naming turn in its persistent thread before the IRC guest is created.
//! Operator-pinned candidates always lead, and the built-in pool is only the
//! fallback when the model cannot produce usable candidates.

use std::time::Duration;

use anyhow::bail;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    RunConfig,
    app_server::AppServer,
    required_thread,
    state::{ResponderState, StateStore},
};

/// Choosing a name may include a brief look around, never repository work.
const NAMING_TURN_TIMEOUT: Duration = Duration::from_secs(300);

/// Conservative portable nickname bound; servers may allow more.
const MAX_NICKNAME_BYTES: usize = 30;

/// Obscure figures across mythological traditions, per the coordination
/// protocol's advice to skip the famous first-thought names. Only a fallback:
/// the model's own naming turn comes first.
pub const DEFAULT_NICKNAME_POOL: &[&str] = &[
    "Ratatoskr",
    "Vedrfolnir",
    "Hoenir",
    "Tapio",
    "Mielikki",
    "Ilmarinen",
    "Airmed",
    "Morvran",
    "Ogma",
    "Tefnut",
    "Heqet",
    "Serqet",
    "Lugalbanda",
    "Ninshubur",
    "Saranyu",
    "Matarisvan",
    "Zhinu",
    "Leizi",
    "Okuninushi",
    "Sukunabikona",
    "Iktomi",
    "Wisakedjak",
    "Amarok",
    "Bochica",
];

/// Complete structured response accepted from the naming turn.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NamingEnvelope {
    /// Exactly three candidates ordered by preference.
    candidates: Vec<String>,
}

/// Ensure exactly three registration candidates, letting the model choose.
///
/// Operator-pinned candidates always lead. A profile that already chose for
/// itself reuses that persisted choice, and an established IRC identity never
/// re-runs naming for mere backup slots. Only a fresh identity with unclaimed
/// slots gets the naming turn; its outcome is persisted before registration
/// so a crash cannot ask the model to become someone else.
pub async fn resolve_candidates(
    app: &mut AppServer,
    config: &mut RunConfig,
    store: &StateStore,
    state: &mut ResponderState,
    shutdown: &CancellationToken,
) -> anyhow::Result<()> {
    if config.nickname_candidates.len() < 3 && !state.chosen_candidates.is_empty() {
        merge_candidates(&mut config.nickname_candidates, &state.chosen_candidates);
    }
    if config.nickname_candidates.len() < 3
        && state.accepted_nickname.is_none()
        && state.chosen_candidates.is_empty()
    {
        match self_chosen_candidates(app, state, &config.nickname_candidates, shutdown).await {
            Ok(chosen) => {
                merge_candidates(&mut config.nickname_candidates, &chosen);
                state.chosen_candidates = config.nickname_candidates.clone();
                store.save(state)?;
            }
            Err(error) if shutdown.is_cancelled() => return Err(error),
            Err(error) => {
                tracing::warn!(
                    %error,
                    "model naming turn failed; drawing nickname candidates from the built-in pool"
                );
            }
        }
    }
    fill_candidates_from_pool(&mut config.nickname_candidates);
    Ok(())
}

async fn self_chosen_candidates(
    app: &mut AppServer,
    state: &ResponderState,
    pinned: &[String],
    shutdown: &CancellationToken,
) -> anyhow::Result<Vec<String>> {
    let thread_id = required_thread(state)?.to_owned();
    let mut failure: Option<String> = None;
    for _ in 0..2 {
        let prompt = match &failure {
            Some(failure) => format!(
                "Your naming response was rejected locally: {failure}. Do not call tools. Return only a corrected JSON object matching the output schema."
            ),
            None => naming_prompt(pinned),
        };
        let text = app
            .run_turn(
                &thread_id,
                prompt,
                NAMING_TURN_TIMEOUT,
                naming_schema(),
                None,
                shutdown,
            )
            .await?;
        match validate_candidates(&text) {
            Ok(candidates) => return Ok(candidates),
            Err(error) => failure = Some(error.to_string()),
        }
    }
    bail!(
        "naming output failed validation twice: {}",
        failure.unwrap_or_else(|| "unknown failure".into())
    )
}

fn naming_prompt(pinned: &[String]) -> String {
    let mut prompt = String::from(
        "IRC is not connected yet, so the irc.send tool is unavailable this turn. Before the responder registers your IRC identity, choose the nickname you will hold on the coordination network: a figure from Western, Eastern, Nordic, or Indigenous mythology - or the modern mythologies of Star Trek and Star Wars - that reflects your mood and your goals in this repository. You may briefly inspect the workspace read-only to ground the choice, but do no other repository work. Other agents reason like you: skip the famous name you think of first and prefer obscure figures from different traditions. The server may reject taken names, so provide exactly three distinct candidates ordered by preference. Each candidate must start with a letter and contain only ASCII letters, digits, hyphens, or underscores, in at most 30 bytes. Respond with only the JSON object required by the output schema.",
    );
    if !pinned.is_empty() {
        prompt.push_str(&format!(
            " The operator already pinned these leading candidates; choose names different from them: {pinned:?}."
        ));
    }
    prompt
}

/// JSON Schema passed to a naming `turn/start`.
fn naming_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "candidates": {
                "type": "array",
                "minItems": 3,
                "maxItems": 3,
                "items": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAX_NICKNAME_BYTES
                }
            }
        },
        "required": ["candidates"],
        "additionalProperties": false
    })
}

/// Decode and enforce the naming rules that stay outside model control.
fn validate_candidates(text: &str) -> anyhow::Result<Vec<String>> {
    let envelope: NamingEnvelope = serde_json::from_str(text)
        .map_err(|error| anyhow::anyhow!("invalid JSON naming output: {error}"))?;
    if envelope.candidates.len() != 3 {
        bail!("exactly three candidates are required");
    }
    for candidate in &envelope.candidates {
        if candidate.len() > MAX_NICKNAME_BYTES {
            bail!("candidate {candidate:?} exceeds {MAX_NICKNAME_BYTES} bytes");
        }
        let mut characters = candidate.chars();
        if !characters
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic())
        {
            bail!("candidate {candidate:?} must start with an ASCII letter");
        }
        if characters
            .any(|character| !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_')))
        {
            bail!(
                "candidate {candidate:?} may contain only ASCII letters, digits, hyphens, or underscores"
            );
        }
    }
    let mut deduplicated = envelope.candidates.clone();
    super::deduplicate_case_insensitive(&mut deduplicated);
    if deduplicated.len() != 3 {
        bail!("candidates must be distinct");
    }
    Ok(envelope.candidates)
}

/// Append later candidates that do not repeat earlier ones, capped at three.
fn merge_candidates(candidates: &mut Vec<String>, additional: &[String]) {
    for candidate in additional {
        if candidates.len() == 3 {
            break;
        }
        if !candidates
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(candidate))
        {
            candidates.push(candidate.clone());
        }
    }
}

// Random unclaimed slots keep concurrently launched unconfigured responders
// from all presenting the same fallback triple.
fn fill_candidates_from_pool(candidates: &mut Vec<String>) {
    let mut pool: Vec<&str> = DEFAULT_NICKNAME_POOL
        .iter()
        .copied()
        .filter(|name| {
            !candidates
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(name))
        })
        .collect();
    let mut entropy = u128::from_le_bytes(*Uuid::new_v4().as_bytes());
    while candidates.len() < 3 {
        let index = (entropy % pool.len() as u128) as usize;
        entropy /= pool.len() as u128;
        candidates.push(pool.swap_remove(index).to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn naming_output_is_validated_outside_model_control() {
        assert_eq!(
            validate_candidates(r#"{"candidates":["Veles","Dodola","Stribog"]}"#).expect("valid"),
            vec!["Veles", "Dodola", "Stribog"]
        );
        for rejected in [
            "not json",
            r#"{"candidates":["Veles","Dodola"]}"#,
            r#"{"candidates":["Veles","Dodola","Stribog","Extra"]}"#,
            r#"{"candidates":["Veles","Dodola","veles"]}"#,
            r#"{"candidates":["Veles","Dodola","3PO"]}"#,
            r#"{"candidates":["Veles","Dodola","Str ibog"]}"#,
            r#"{"candidates":["Veles","Dodola","Véðrfölnir"]}"#,
            r#"{"candidates":["Veles","Dodola","Stribog"],"purpose":"x"}"#,
        ] {
            assert!(validate_candidates(rejected).is_err(), "{rejected}");
        }
        let long = format!(
            r#"{{"candidates":["Veles","Dodola","{}"]}}"#,
            "a".repeat(31)
        );
        assert!(validate_candidates(&long).is_err());
    }

    #[test]
    fn merged_candidates_keep_pinned_names_first_without_repeats() {
        let mut candidates = vec!["Hecate".to_owned()];
        merge_candidates(
            &mut candidates,
            &[
                "hecate".into(),
                "Veles".into(),
                "Dodola".into(),
                "Stribog".into(),
            ],
        );
        assert_eq!(candidates, vec!["Hecate", "Veles", "Dodola"]);
    }

    #[test]
    fn pool_fallback_fills_distinct_slots_after_pinned_names() {
        let mut candidates = vec!["Ratatoskr".to_owned()];
        fill_candidates_from_pool(&mut candidates);
        assert_eq!(candidates[0], "Ratatoskr");
        assert_eq!(candidates.len(), 3);
        let distinct: HashSet<_> = candidates
            .iter()
            .map(|candidate| candidate.to_ascii_lowercase())
            .collect();
        assert_eq!(distinct.len(), 3);
        assert!(
            candidates[1..]
                .iter()
                .all(|candidate| DEFAULT_NICKNAME_POOL.contains(&candidate.as_str()))
        );
    }

    #[test]
    fn prompt_names_the_pinned_candidates_to_avoid() {
        assert!(!naming_prompt(&[]).contains("pinned"));
        assert!(naming_prompt(&["Hecate".into()]).contains("Hecate"));
    }

    #[cfg(unix)]
    mod turns {
        use super::super::super::app_server::test_support::fake_server;
        use super::*;
        use crate::responder::app_server::AppServer;

        fn test_config(workspace: std::path::PathBuf) -> RunConfig {
            RunConfig {
                mcp_url: "http://irc:8080/mcp".into(),
                state_dir: "/tmp/responder-naming-test".into(),
                workspace,
                nickname_candidates: Vec::new(),
                full_traffic_targets: Vec::new(),
                allowed_channels: vec!["#control".into()],
                bearer_token_env: None,
                codex_command: "codex".into(),
                model: None,
                effort: "low".into(),
                turn_timeout: Duration::from_secs(60),
            }
        }

        fn profile(directory: &std::path::Path) -> (StateStore, ResponderState) {
            StateStore::open(directory, "http://irc:8080/mcp", "/workspace/project")
                .expect("open naming test profile")
        }

        // The scripted turn/start only answers when the request carries the
        // naming schema, so success also proves schema pass-through.
        const NAMING_TURN_BODY: &str = r##"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"id":1,"result":{"serverInfo":{"name":"fake"}}}' ;;
    *'"method":"initialized"'*) ;;
    *'"method":"account/read"'*)
      printf '%s\n' '{"id":2,"result":{"requiresOpenaiAuth":true,"account":{"type":"chatgpt"}}}' ;;
    *'"method":"turn/start"'*'"candidates"'*)
      printf '%s\n' '{"id":3,"result":{"turn":{"id":"turn_name","status":"inProgress"}}}'
      printf '%s\n' '{"method":"item/completed","params":{"turnId":"turn_name","item":{"type":"agentMessage","id":"item_1","text":"{\"candidates\":[\"Veles\",\"Dodola\",\"Stribog\"]}"}}}'
      printf '%s\n' '{"method":"turn/completed","params":{"turn":{"id":"turn_name","status":"completed","items":[]}}}' ;;
  esac
done
"##;

        #[tokio::test]
        async fn a_fresh_identity_is_named_by_the_model_and_persisted() {
            let (directory, app_config) = fake_server(NAMING_TURN_BODY);
            let mut app = AppServer::start(app_config)
                .await
                .expect("start fake server");
            let mut config = test_config(directory.path().to_path_buf());
            let (store, mut state) = profile(&directory.path().join("profile"));
            state.thread_id = Some("thr_responder".into());

            resolve_candidates(
                &mut app,
                &mut config,
                &store,
                &mut state,
                &CancellationToken::new(),
            )
            .await
            .expect("resolve fresh identity");
            assert_eq!(
                config.nickname_candidates,
                vec!["Veles", "Dodola", "Stribog"]
            );
            assert_eq!(state.chosen_candidates, config.nickname_candidates);
            app.stop().await;
        }

        #[tokio::test]
        async fn an_established_identity_never_reruns_naming() {
            // turn/start is unscripted, so any naming attempt errors and would
            // leave pool names instead of the persisted choice.
            let (directory, app_config) =
                fake_server(super::super::super::app_server::test_support::FAKE_HANDSHAKE);
            let mut app = AppServer::start(app_config)
                .await
                .expect("start fake server");
            let mut config = test_config(directory.path().to_path_buf());
            let (store, mut state) = profile(&directory.path().join("profile"));
            state.thread_id = Some("thr_responder".into());
            state.accepted_nickname = Some("Veles".into());
            state.chosen_candidates = vec!["Veles".into(), "Dodola".into(), "Stribog".into()];

            resolve_candidates(
                &mut app,
                &mut config,
                &store,
                &mut state,
                &CancellationToken::new(),
            )
            .await
            .expect("resume without a naming turn");
            assert_eq!(
                config.nickname_candidates,
                vec!["Veles", "Dodola", "Stribog"]
            );
            app.stop().await;
        }

        #[tokio::test]
        async fn a_failed_naming_turn_falls_back_to_the_pool_without_persisting() {
            let body = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"id":1,"result":{"serverInfo":{"name":"fake"}}}' ;;
    *'"method":"initialized"'*) ;;
    *'"method":"account/read"'*)
      printf '%s\n' '{"id":2,"result":{"requiresOpenaiAuth":true,"account":{"type":"chatgpt"}}}' ;;
    *'"method":"turn/start"'*)
      printf '%s\n' '{"id":3,"error":{"code":-32000,"message":"model unavailable"}}' ;;
  esac
done
"#;
            let (directory, app_config) = fake_server(body);
            let mut app = AppServer::start(app_config)
                .await
                .expect("start fake server");
            let mut config = test_config(directory.path().to_path_buf());
            let (store, mut state) = profile(&directory.path().join("profile"));
            state.thread_id = Some("thr_responder".into());

            resolve_candidates(
                &mut app,
                &mut config,
                &store,
                &mut state,
                &CancellationToken::new(),
            )
            .await
            .expect("fall back to the pool");
            assert_eq!(config.nickname_candidates.len(), 3);
            assert!(state.chosen_candidates.is_empty());
            assert!(
                config
                    .nickname_candidates
                    .iter()
                    .all(|candidate| DEFAULT_NICKNAME_POOL.contains(&candidate.as_str()))
            );
            app.stop().await;
        }
    }
}
