//! Server state carried across the rounds of a multi round-trip request.
//!
//! MRTR replaced server-initiated requests: instead of asking a client a
//! question over the stream, a handler returns an `InputRequiredResult` and the
//! client re-sends the *original* request with its answers plus whatever
//! `requestState` string the server handed back. That string is the only thing
//! connecting the two rounds — there is no session, no request id continuity,
//! and no server-side correlation table the protocol knows about.
//!
//! Which makes it attacker-controlled input. A client can echo back anything,
//! including a value it never received, so the specification is explicit: state
//! that influences authorization, resource access, or business logic MUST be
//! integrity-protected and MUST be rejected on verification failure. Three
//! bindings are required for that to actually hold, and [`RequestStateSealer`]
//! enforces all three:
//!
//! 1. **The authenticated principal.** Without it, one caller's state opens for
//!    another and the retry inherits the first caller's authorization.
//! 2. **A short time-to-live.** Without it, a leaked state string is a
//!    permanent capability.
//! 3. **The originating operation.** Without it, state minted while answering
//!    one question can be replayed into a different tool call whose arguments
//!    the server never approved.
//!
//! All three live in HMAC *associated data*, which is authenticated but never
//! stored in the token: opening requires the server to re-derive the same
//! context from the retry it is actually holding. That fails closed — a caller
//! who forgets to bind something cannot accidentally accept anything.
//!
//! The signing key is generated once per process and never leaves it. A restart
//! invalidates outstanding state, which is the correct outcome: the in-memory
//! work the state referred to did not survive either.

use std::time::Duration;

use rmcp::{
    ErrorData as McpError,
    model::{
        ElicitRequest, ElicitRequestParams, ElicitResult, ElicitationAction, ElicitationSchema,
        InputRequest, InputResponses, RequestStateCodec, RequestStateError, SealOptions,
    },
};
use serde::{Serialize, de::DeserializeOwned};

use crate::mcp::authorization::OwnerId;

/// How long a sealed request state may be redeemed for.
///
/// Long enough for a person to read a prompt and type an answer, short enough
/// that a captured value is worthless by the time it is replayed.
pub const REQUEST_STATE_TTL: Duration = Duration::from_secs(120);

/// Domain label mixed into every binding, so a value sealed for one purpose in
/// this process can never be opened as another.
const BINDING_DOMAIN: &[u8] = b"rmcp-irc/mrtr/request-state/v1";

/// The operation a sealed request state belongs to.
///
/// Identified by the JSON-RPC method, the tool or resource name within it, and
/// the arguments that decide what the operation will do. The specification
/// suggests a digest of those arguments; because the binding is associated data
/// rather than token payload, this carries their canonical bytes instead —
/// never transmitted, strictly stronger than a digest, and it needs no
/// cryptographic hash of our own.
#[derive(Clone, Debug)]
pub struct OriginatingOperation {
    method: String,
    name: String,
    arguments: Vec<u8>,
}

impl OriginatingOperation {
    /// Describe the operation a request state is being minted for.
    ///
    /// `salient` is whichever part of the request decides its effect. Include
    /// everything the answer will be trusted for and nothing that legitimately
    /// varies between rounds, because a caller that changes a salient argument
    /// on retry is starting a different operation and must not reuse the state.
    pub fn new(method: &str, name: &str, salient: &serde_json::Value) -> Self {
        Self {
            method: method.to_owned(),
            name: name.to_owned(),
            // Canonical because `serde_json::Value` orders object keys, so the
            // same logical arguments always produce the same bytes.
            arguments: serde_json::to_vec(salient).unwrap_or_default(),
        }
    }

    /// The tool call one `tools/call` request state belongs to.
    pub fn for_tool(name: &str, arguments: &serde_json::Value) -> Self {
        Self::new("tools/call", name, arguments)
    }
}

/// Seals and opens the opaque `requestState` strings of MRTR exchanges.
#[derive(Clone)]
pub struct RequestStateSealer {
    codec: RequestStateCodec,
}

impl std::fmt::Debug for RequestStateSealer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The codec redacts its own key; naming it here keeps that guarantee
        // from being lost to a derived impl later.
        formatter
            .debug_struct("RequestStateSealer")
            .finish_non_exhaustive()
    }
}

impl RequestStateSealer {
    /// Mint a sealer with a fresh 256-bit process-local key.
    ///
    /// Two version-4 UUIDs supply the entropy: each is 122 random bits from the
    /// same generator the gateway already trusts for handles, and a key is only
    /// ever compared against itself.
    pub fn generate() -> Self {
        let mut key = [0_u8; 32];
        key[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        key[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        Self {
            codec: RequestStateCodec::new(key.to_vec()),
        }
    }

    /// Seal `state` for one owner and one operation.
    ///
    /// # Errors
    ///
    /// Fails only if `state` cannot be serialized to JSON.
    pub fn seal<T: Serialize>(
        &self,
        owner: &OwnerId,
        operation: &OriginatingOperation,
        state: &T,
    ) -> Result<String, McpError> {
        let binding = binding(owner, operation);
        self.codec
            .seal_json_with(
                state,
                &SealOptions::new()
                    .associated_data(&binding)
                    .ttl(REQUEST_STATE_TTL),
            )
            .map_err(|error| {
                McpError::internal_error(format!("could not seal request state: {error}"), None)
            })
    }

    /// Open a request state a client echoed back, requiring that it was minted
    /// for this owner and this operation and has not expired.
    ///
    /// # Errors
    ///
    /// An expired value reports itself as expired, because the caller can
    /// legitimately recover by restarting the exchange. Every other failure —
    /// a forged tag, a different owner, a different operation, a value from a
    /// previous process, or malformed text — collapses into one refusal, so the
    /// error cannot be used to probe which binding was wrong.
    pub fn open<T: DeserializeOwned>(
        &self,
        sealed: &str,
        owner: &OwnerId,
        operation: &OriginatingOperation,
    ) -> Result<T, McpError> {
        let binding = binding(owner, operation);
        self.codec
            .open_json_with(sealed, &binding)
            .map_err(|error| match error {
                RequestStateError::Expired => McpError::invalid_params(
                    "request state has expired; start the operation again",
                    None,
                ),
                _ => McpError::invalid_params(
                    "request state is not valid for this caller and operation",
                    None,
                ),
            })
    }
}

/// The associated data every sealed state is bound to.
///
/// Each component is length-prefixed so no two different bindings can serialize
/// to the same bytes: without that, a tool named `a` with arguments `bc` and a
/// tool named `ab` with arguments `c` would be interchangeable.
fn binding(owner: &OwnerId, operation: &OriginatingOperation) -> Vec<u8> {
    let owner = owner.to_string();
    let parts: [&[u8]; 4] = [
        owner.as_bytes(),
        operation.method.as_bytes(),
        operation.name.as_bytes(),
        &operation.arguments,
    ];
    let mut binding = Vec::with_capacity(
        BINDING_DOMAIN.len() + parts.iter().map(|part| part.len() + 8).sum::<usize>(),
    );
    binding.extend_from_slice(BINDING_DOMAIN);
    for part in parts {
        binding.extend_from_slice(&(part.len() as u64).to_be_bytes());
        binding.extend_from_slice(part);
    }
    binding
}

/// Build a form-mode elicitation to return inside an `InputRequiredResult`.
///
/// Form mode is the only mode this server asks for: it is what an empty
/// `elicitation` capability means, so every client that declared elicitation at
/// all can answer one. Check
/// [`RequestProfile::supports_form_elicitation`](crate::mcp::request_profile::RequestProfile::supports_form_elicitation)
/// first — sending an input request a client never declared is a protocol
/// violation, not a graceful degradation.
pub fn form_elicitation(
    message: impl Into<String>,
    requested_schema: ElicitationSchema,
) -> InputRequest {
    InputRequest::Elicitation(ElicitRequest::new(
        ElicitRequestParams::FormElicitationParams {
            meta: None,
            message: message.into(),
            requested_schema,
        },
    ))
}

/// What a client said about one form this server asked it to fill in.
///
/// The three cases are genuinely different outcomes and every flow has to
/// distinguish them: a missing answer means ask again, a refusal is terminal
/// and leaves the operation unapplied, and only an acceptance carries content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FormAnswer {
    /// No response carried this round's key.
    Missing,
    /// The caller declined or cancelled the question.
    Declined,
    /// The caller filled the form in; its content is here, possibly empty.
    Accepted(serde_json::Value),
}

/// Read one client answer out of a retry's `inputResponses`.
///
/// # Errors
///
/// A response that is not an elicitation result at all is malformed, which the
/// specification classifies as an ordinary protocol error rather than another
/// round of asking.
pub fn read_form_answer(
    responses: Option<&InputResponses>,
    key: &str,
) -> Result<FormAnswer, McpError> {
    let Some(value) = responses.and_then(|responses| responses.get(key)) else {
        return Ok(FormAnswer::Missing);
    };
    let result: ElicitResult = serde_json::from_value(value.clone()).map_err(|error| {
        McpError::invalid_params(
            format!("{key} response is not an elicitation result: {error}"),
            None,
        )
    })?;
    if result.action != ElicitationAction::Accept {
        return Ok(FormAnswer::Declined);
    }
    Ok(FormAnswer::Accepted(
        result.content.unwrap_or(serde_json::Value::Null),
    ))
}

/// One non-blank string field of a filled-in form.
///
/// Blank is absent rather than a value spelled with spaces, because a form
/// control a person tabbed through must not read as an answer.
pub fn text_field(content: &serde_json::Value, field: &str) -> Option<String> {
    content
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// One boolean field of a filled-in form.
pub fn bool_field(content: &serde_json::Value, field: &str) -> Option<bool> {
    content.get(field).and_then(serde_json::Value::as_bool)
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    /// The state one round of an exchange needs to remember.
    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct Pending {
        agent_id: String,
        round: u32,
    }

    fn pending() -> Pending {
        Pending {
            agent_id: "agent-1".into(),
            round: 2,
        }
    }

    fn operation() -> OriginatingOperation {
        OriginatingOperation::for_tool("irc.send", &serde_json::json!({ "target": "#control" }))
    }

    #[test]
    fn a_state_opens_for_the_owner_and_operation_it_was_sealed_for() {
        let sealer = RequestStateSealer::generate();
        let sealed = sealer
            .seal(&OwnerId::local(), &operation(), &pending())
            .expect("seal");
        let opened: Pending = sealer
            .open(&sealed, &OwnerId::local(), &operation())
            .expect("open");
        assert_eq!(opened, pending());
    }

    #[test]
    fn another_owner_cannot_redeem_a_state() {
        // Otherwise a retry would inherit the authorization of whoever the
        // exchange was started by.
        let sealer = RequestStateSealer::generate();
        let sealed = sealer
            .seal(&OwnerId::from_bearer("alice"), &operation(), &pending())
            .expect("seal");
        let error = sealer
            .open::<Pending>(&sealed, &OwnerId::from_bearer("bob"), &operation())
            .expect_err("a different principal must be refused");
        assert!(error.message.contains("not valid for this caller"));
        assert!(
            sealer
                .open::<Pending>(&sealed, &OwnerId::local(), &operation())
                .is_err(),
            "the shared local owner is not a wildcard either"
        );
    }

    #[test]
    fn a_state_cannot_be_replayed_into_a_different_operation() {
        let sealer = RequestStateSealer::generate();
        let sealed = sealer
            .seal(&OwnerId::local(), &operation(), &pending())
            .expect("seal");
        let other_arguments = OriginatingOperation::for_tool(
            "irc.send",
            &serde_json::json!({ "target": "#elsewhere" }),
        );
        assert!(
            sealer
                .open::<Pending>(&sealed, &OwnerId::local(), &other_arguments)
                .is_err(),
            "changing a salient argument starts a different operation"
        );
        let other_tool = OriginatingOperation::for_tool(
            "irc.part",
            &serde_json::json!({ "target": "#control" }),
        );
        assert!(
            sealer
                .open::<Pending>(&sealed, &OwnerId::local(), &other_tool)
                .is_err()
        );
        let other_method = OriginatingOperation::new(
            "prompts/get",
            "irc.send",
            &serde_json::json!({ "target": "#control" }),
        );
        assert!(
            sealer
                .open::<Pending>(&sealed, &OwnerId::local(), &other_method)
                .is_err()
        );
    }

    #[test]
    fn a_tampered_state_is_refused() {
        let sealer = RequestStateSealer::generate();
        let sealed = sealer
            .seal(&OwnerId::local(), &operation(), &pending())
            .expect("seal");
        for forged in [
            format!("{sealed}x"),
            sealed.replacen("rs1.", "rs2.", 1),
            sealed.split('.').next().expect("version").to_owned(),
            String::new(),
        ] {
            let error = sealer
                .open::<Pending>(&forged, &OwnerId::local(), &operation())
                .expect_err("a modified state must be refused");
            assert!(
                error.message.contains("not valid for this caller"),
                "unexpected message for {forged:?}: {}",
                error.message
            );
        }
    }

    #[test]
    fn a_state_from_another_process_is_refused() {
        // Keys are per-process, so a restart invalidates outstanding state
        // rather than resuming work that no longer exists.
        let sealed = RequestStateSealer::generate()
            .seal(&OwnerId::local(), &operation(), &pending())
            .expect("seal");
        assert!(
            RequestStateSealer::generate()
                .open::<Pending>(&sealed, &OwnerId::local(), &operation())
                .is_err()
        );
    }

    #[test]
    fn an_expired_state_reports_that_it_expired() {
        // The TTL is baked into the token, so this seals with the shortest
        // possible lifetime and lets it lapse rather than waiting out the
        // production value.
        let sealer = RequestStateSealer::generate();
        let binding = binding(&OwnerId::local(), &operation());
        let sealed = sealer
            .codec
            .seal_json_with(
                &pending(),
                &SealOptions::new()
                    .associated_data(&binding)
                    .ttl(Duration::from_millis(1)),
            )
            .expect("seal");
        std::thread::sleep(Duration::from_millis(20));
        let error = sealer
            .open::<Pending>(&sealed, &OwnerId::local(), &operation())
            .expect_err("an elapsed ttl must be refused");
        assert!(
            error.message.contains("expired"),
            "a caller that can recover by restarting must be told so: {}",
            error.message
        );
    }

    #[test]
    fn a_binding_cannot_be_confused_by_shifting_a_boundary() {
        let shifted = OriginatingOperation::new("tools/call", "irc.sen", &serde_json::json!("d"));
        let straight = OriginatingOperation::new("tools/call", "irc.send", &serde_json::json!(""));
        assert_ne!(
            binding(&OwnerId::local(), &shifted),
            binding(&OwnerId::local(), &straight)
        );
    }

    #[test]
    fn the_key_is_never_printed() {
        let rendered = format!("{:?}", RequestStateSealer::generate());
        assert!(!rendered.contains("key"), "{rendered}");
    }

    #[test]
    fn an_answer_is_read_only_under_the_key_it_was_asked_for() {
        assert_eq!(
            read_form_answer(None, "a").expect("none"),
            FormAnswer::Missing
        );
        let responses = InputResponses::from([(
            "a".to_owned(),
            serde_json::json!({ "action": "accept", "content": { "text": " ", "yes": true } }),
        )]);
        assert_eq!(
            read_form_answer(Some(&responses), "b").expect("other key"),
            FormAnswer::Missing
        );
        let FormAnswer::Accepted(content) =
            read_form_answer(Some(&responses), "a").expect("accepted")
        else {
            panic!("an accepted answer carries content");
        };
        assert_eq!(text_field(&content, "text"), None, "blank is not an answer");
        assert_eq!(bool_field(&content, "yes"), Some(true));
        assert_eq!(bool_field(&content, "missing"), None);

        for refusal in ["decline", "cancel"] {
            assert_eq!(
                read_form_answer(
                    Some(&InputResponses::from([(
                        "a".to_owned(),
                        serde_json::json!({ "action": refusal }),
                    )])),
                    "a",
                )
                .expect("refusal"),
                FormAnswer::Declined
            );
        }
        assert!(
            read_form_answer(
                Some(&InputResponses::from([(
                    "a".to_owned(),
                    serde_json::json!("not an elicitation result"),
                )])),
                "a",
            )
            .is_err()
        );
    }

    #[test]
    fn an_elicitation_is_built_in_form_mode() {
        let request = form_elicitation(
            "Which channel should the reply go to?",
            ElicitationSchema::builder()
                .required_string("channel")
                .build()
                .expect("schema"),
        );
        let wire = serde_json::to_value(&request).expect("serialize");
        assert_eq!(wire["method"], "elicitation/create");
        assert_eq!(wire["params"]["mode"], "form");
        assert_eq!(
            wire["params"]["requestedSchema"]["required"],
            serde_json::json!(["channel"])
        );
    }
}
