//! One discriminated envelope every complete tool result travels in.
//!
//! A tool that declares an `outputSchema` **must** return `structuredContent`
//! conforming to it (MCP 2026-07-28, server/tools). Declaring only the happy
//! path breaks that promise the first time anything goes wrong, because a
//! gateway failure, an in-band refusal, or a rejected IRC command answers with a
//! completely different object. The protocol offers no discrimination inside
//! `structuredContent` — `resultType` and `isError` are the only envelope it
//! defines — and explicitly leaves richer shapes to the server's own schema,
//! where `oneOf` is legal.
//!
//! So every tool declares the same two-branch schema over its own output:
//!
//! ```json
//! {"ok": true,  "result": { ... }, "activity": { ... }}
//! {"ok": false, "error":  {"kind": "...", "message": "...", "retriable": false}}
//! ```
//!
//! `ok` is the discriminator and always agrees with `isError`: `ok: false` is
//! exactly `isError: true`. The success branch nests the tool's existing output
//! under `result`, so nothing about a tool's own shape changed except its
//! depth, and adds the optional bounded [`ActivityHint`] — the one field this
//! envelope exists to make room for without any tool's schema becoming a lie.
//!
//! The failure branch is shared by every tool, which is why it carries room for
//! the structured extras individual refusals need: the correlated
//! [`CommandResult`] behind a rejected IRC command, the configured receive
//! roots behind a DCC acceptance that cannot choose a destination on its own,
//! the lines a partial send did deliver, and the last known state of a direct
//! session whose agent went away. The first two used to be returned as bare
//! objects that no declared schema allowed; the last two used to be dropped, and
//! each is something the caller now owns and cannot otherwise learn.
//!
//! Interim results are *not* enveloped. `InputRequiredResult` and
//! `CreateTaskResult` are different `resultType`s carrying no
//! `structuredContent` of ours; only a complete tool result is a tool result. A
//! task's terminal `result` is one, so it is enveloped like any other.

use std::{
    any::TypeId,
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use rmcp::{handler::server::tool::schema_for_output, model::JsonObject};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    dcc::session::DccSession,
    error::GatewayError,
    irc::correlation::{CommandOutcome, CommandResult},
    mcp::activity::ActivityHint,
};

/// JSON Schema dialect every generated schema declares.
const DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

/// Kind of a failure that is this server's own fault rather than the caller's
/// or the network's. Nothing about the call would be different if it were made
/// again, so it is never retriable.
pub const INTERNAL_KIND: &str = "internal_error";

/// The shared failure branch of every tool's declared output.
///
/// One shape for every way a started call can fail: a gateway error, an in-band
/// refusal, and an IRC command the server rejected. The three common fields are
/// always present; the extras appear only for the refusals that have something
/// structured to say, so a client can read `kind` and `message` without knowing
/// which tool it called.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ToolFailure {
    /// Stable machine-readable category. Gateway errors use the
    /// [`crate::error::ErrorKind`] spellings; a rejected IRC command uses its
    /// own outcome, such as `rejected` or `timed_out`.
    pub kind: String,
    /// Safe human-readable explanation, identical to the text summary.
    pub message: String,
    /// Whether retrying after external state changes is normally safe.
    pub retriable: bool,
    /// The correlated IRC exchange whose outcome made this a failure, lossless
    /// and including its raw replies. Absent when nothing was written upstream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_result: Option<CommandResult>,
    /// Configured `dcc.receive_roots` names, in declaration order, one of which
    /// a retried `irc.dcc.accept` must name in `root`. Absent unless that is
    /// the choice this failure is about.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receive_roots: Option<Vec<String>>,
    /// Destination a retry gets if it names a root and nothing else. Absent for
    /// the same reason as `receive_roots`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_destination_path: Option<PathBuf>,
    /// The physical lines one logical message did deliver before a later line
    /// failed, correlated exactly as a successful send reports them. Absent
    /// unless a partial send is what this failure is about — and then it is how
    /// a caller learns the `msgid`s it now publicly owns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_lines: Option<Vec<CommandResult>>,
    /// Last observed state of the direct session this failure is about. Absent
    /// unless the failure interrupted one, which is the only case where the
    /// server holds a snapshot the caller cannot go and read for itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<DccSession>,
}

impl ToolFailure {
    /// Report one gateway error.
    pub fn from_error(error: &GatewayError) -> Self {
        Self::new(error.kind().as_str(), error.to_string(), error.retriable())
    }

    /// Refuse a call in band, leaving whatever it would have changed untouched.
    pub fn refusal(kind: &str, message: impl Into<String>, retriable: bool) -> Self {
        Self::new(kind, message, retriable)
    }

    /// Report one correlated IRC command the server did not complete.
    ///
    /// The whole exchange comes along, because the numerics are the answer: a
    /// join refused by 475 is only actionable if the caller can still read the
    /// 475. `retriable` is the correlator's own judgement rather than a guess
    /// made here.
    pub fn from_command(
        outcome: CommandOutcome,
        message: impl Into<String>,
        result: CommandResult,
    ) -> Self {
        Self {
            command_result: Some(result.clone()),
            ..Self::new(command_outcome_kind(outcome), message, result.retriable)
        }
    }

    /// Refuse a DCC acceptance whose receive root the caller must name.
    pub fn unchosen_destination(
        message: impl Into<String>,
        receive_roots: Vec<String>,
        default_destination_path: Option<PathBuf>,
    ) -> Self {
        Self {
            receive_roots: Some(receive_roots),
            default_destination_path,
            ..Self::new(
                GatewayError::Dcc(String::new()).kind().as_str(),
                message,
                false,
            )
        }
    }

    /// Keep the lines a partial send delivered before it failed.
    ///
    /// A logical message can become several physical lines, and the ones that
    /// were written are public and correlated whatever happened afterwards.
    /// Reporting only the line that failed would leave the caller unable to
    /// name — or redact, or react to — what it had already said. An empty list
    /// is no news, so it stays absent.
    #[must_use]
    pub fn with_delivered_lines(mut self, delivered: Vec<CommandResult>) -> Self {
        self.delivered_lines = (!delivered.is_empty()).then_some(delivered);
        self
    }

    /// Keep the last known state of the direct session a failure interrupted.
    ///
    /// The snapshot a follower was holding when the agent went away is the only
    /// remaining evidence of how far the transfer got: the session went with the
    /// actor, so there is no resource left to read it from.
    #[must_use]
    pub fn with_session(mut self, session: DccSession) -> Self {
        self.session = Some(session);
        self
    }

    fn new(kind: &str, message: impl Into<String>, retriable: bool) -> Self {
        Self {
            kind: kind.to_owned(),
            message: message.into(),
            retriable,
            command_result: None,
            receive_roots: None,
            default_destination_path: None,
            delivered_lines: None,
            session: None,
        }
    }
}

/// Stable spelling of one unsuccessful command outcome.
///
/// Deliberately the outcome itself rather than a single `command_failed`: the
/// difference between a server that refused a command and one that never
/// acknowledged it is the difference between fixing the arguments and retrying
/// unchanged.
const fn command_outcome_kind(outcome: CommandOutcome) -> &'static str {
    match outcome {
        CommandOutcome::Rejected => "rejected",
        CommandOutcome::TimedOut => "timed_out",
        CommandOutcome::NotWritten => "not_written",
        CommandOutcome::Indeterminate => "indeterminate",
        // Reached only if the failure classifier and this ever disagree; naming
        // the outcome keeps that visible instead of inventing a category.
        CommandOutcome::Completed => "completed",
        CommandOutcome::SentUnconfirmed => "sent_unconfirmed",
    }
}

/// Wrap one tool's own output as the success branch.
///
/// The activity hint is attached later, by the one place that knows which agent
/// the call named and whether that caller is entitled to a hint about it.
pub fn success(value: &impl Serialize) -> Result<Value, serde_json::Error> {
    Ok(json!({ "ok": true, "result": serde_json::to_value(value)? }))
}

/// Wrap one shared failure as the error branch.
pub fn failure(value: &ToolFailure) -> Result<Value, serde_json::Error> {
    Ok(json!({ "ok": false, "error": serde_json::to_value(value)? }))
}

/// Add the activity hint to an already-built success branch.
///
/// Silently does nothing to a failure branch or to anything that is not one of
/// ours, so the caller never has to re-derive which kind of result it holds.
pub fn attach_activity(structured: &mut Value, hint: &ActivityHint) {
    let Some(object) = structured.as_object_mut() else {
        return;
    };
    if object.get("ok") != Some(&Value::Bool(true)) {
        return;
    }
    if let Ok(hint) = serde_json::to_value(hint) {
        object.insert("activity".into(), hint);
    }
}

/// The declared `outputSchema` for a tool whose own output is `T`.
///
/// Built rather than derived: the discriminator is a literal `true`/`false`,
/// which no `serde`/`schemars` representation of a Rust enum produces, and the
/// two branches must be a `oneOf` so a validator rejects an instance that is
/// neither. Both branches are closed, so a stray field is a schema violation a
/// test can see rather than something a permissive validator waves through.
///
/// Cached per output type for the same reason the SDK caches its own: the
/// Streamable HTTP transport builds a fresh handler — and therefore a fresh
/// tool router — for every POST.
pub fn envelope_schema<T: JsonSchema + std::any::Any>() -> Arc<JsonObject> {
    thread_local! {
        static CACHE: RwLock<HashMap<TypeId, Arc<JsonObject>>> = RwLock::default();
    }
    CACHE.with(|cache| {
        if let Some(schema) = cache
            .read()
            .expect("envelope schema cache lock")
            .get(&TypeId::of::<T>())
        {
            return schema.clone();
        }
        let schema = Arc::new(build_envelope_schema::<T>());
        cache
            .write()
            .expect("envelope schema cache lock")
            .insert(TypeId::of::<T>(), schema.clone());
        schema
    })
}

fn build_envelope_schema<T: JsonSchema + std::any::Any>() -> JsonObject {
    let mut definitions = serde_json::Map::new();
    let result = subschema(&schema_for_output::<T>(), &mut definitions);
    let activity = subschema(&schema_for_output::<ActivityHint>(), &mut definitions);
    let error = subschema(&schema_for_output::<ToolFailure>(), &mut definitions);

    let mut root = JsonObject::new();
    root.insert("$schema".into(), json!(DIALECT));
    if !definitions.is_empty() {
        root.insert("$defs".into(), Value::Object(definitions));
    }
    root.insert(
        "oneOf".into(),
        json!([
            {
                "type": "object",
                "description": "The operation completed. `isError` is absent or false.",
                "properties": {
                    "ok": {
                        "type": "boolean",
                        "const": true,
                        "description": "Discriminator: this result carries the tool's own output."
                    },
                    "result": result,
                    "activity": activity,
                },
                "required": ["ok", "result"],
                "additionalProperties": false,
            },
            {
                "type": "object",
                "description": "The operation failed after the call reached the tool. \
                                `isError` is true and nothing partial is reported here.",
                "properties": {
                    "ok": {
                        "type": "boolean",
                        "const": false,
                        "description": "Discriminator: this result carries the shared failure shape."
                    },
                    "error": error,
                },
                "required": ["ok", "error"],
                "additionalProperties": false,
            },
        ]),
    );
    root
}

/// Lift one generated root schema into a branch, hoisting its definitions.
///
/// `$ref`s point at `#/$defs/...`, which resolves against the document root, so
/// the definitions have to move there when the schema becomes a subschema.
/// Identical names are identical types — the generator names a definition after
/// the type it describes — so the first one wins and the rest are the same.
fn subschema(root: &JsonObject, definitions: &mut serde_json::Map<String, Value>) -> Value {
    let mut body = root.clone();
    body.remove("$schema");
    if let Some(Value::Object(own)) = body.remove("$defs") {
        for (name, definition) in own {
            definitions.entry(name).or_insert(definition);
        }
    }
    Value::Object(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::tools::DccListOutput;

    /// Both branches of the schema a tool declares, in the order they appear.
    fn branches(schema: &JsonObject) -> (&Value, &Value) {
        let branches = schema["oneOf"].as_array().expect("two branches");
        assert_eq!(branches.len(), 2, "{schema:?}");
        (&branches[0], &branches[1])
    }

    #[test]
    fn every_declared_schema_is_two_closed_branches_discriminated_by_ok() {
        let schema = envelope_schema::<DccListOutput>();
        let (success, failure) = branches(&schema);
        assert_eq!(success["properties"]["ok"]["const"], json!(true));
        assert_eq!(failure["properties"]["ok"]["const"], json!(false));
        assert_eq!(success["additionalProperties"], json!(false));
        assert_eq!(failure["additionalProperties"], json!(false));
        assert_eq!(success["required"], json!(["ok", "result"]));
        assert_eq!(failure["required"], json!(["ok", "error"]));
        // The activity hint is optional on the wire and declared regardless, so
        // a client can read the shape before it ever receives one.
        assert!(success["properties"]["activity"].is_object());
        assert_eq!(schema["$schema"], json!(DIALECT));
    }

    #[test]
    fn definitions_referenced_by_either_branch_live_at_the_document_root() {
        let schema = envelope_schema::<ActivityHint>();
        let definitions = schema["$defs"].as_object().expect("shared definitions");
        // `EventCursor` can only have come from the tool's own output and
        // `DccSession` only from the shared failure, so finding both proves the
        // hoist merged rather than replaced.
        assert!(definitions.contains_key("EventCursor"), "{definitions:?}");
        assert!(definitions.contains_key("DccSession"), "{definitions:?}");
        assert!(
            !schema["oneOf"][0]["properties"]["result"]
                .as_object()
                .expect("result branch")
                .contains_key("$defs"),
            "a branch must not keep definitions a $ref cannot reach"
        );
    }

    #[test]
    fn a_failure_carries_only_the_extras_its_own_refusal_needs() {
        let plain = ToolFailure::refusal("declined", "nothing was applied", true);
        let value = serde_json::to_value(&plain).expect("failure serializes");
        assert_eq!(value["kind"], "declined");
        assert!(value.get("receive_roots").is_none(), "{value}");
        assert!(value.get("command_result").is_none(), "{value}");
        assert!(value.get("delivered_lines").is_none(), "{value}");
        assert!(value.get("session").is_none(), "{value}");

        // An empty list of delivered lines is no news, so it stays absent
        // rather than becoming an empty array a reader has to interpret.
        let nothing_delivered =
            ToolFailure::refusal("rejected", "the first line was refused", false)
                .with_delivered_lines(Vec::new());
        let value = serde_json::to_value(&nothing_delivered).expect("failure serializes");
        assert!(value.get("delivered_lines").is_none(), "{value}");

        let roots = ToolFailure::unchosen_destination(
            "name one",
            vec!["inbox".into(), "archive".into()],
            Some(PathBuf::from("report.txt")),
        );
        let value = serde_json::to_value(&roots).expect("failure serializes");
        assert_eq!(value["receive_roots"], json!(["inbox", "archive"]));
        assert_eq!(value["default_destination_path"], "report.txt");
        assert_eq!(value["retriable"], json!(false));
    }
}
