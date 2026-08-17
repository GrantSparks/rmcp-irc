//! Settling where one accepted DCC file is written.
//!
//! `irc.dcc.accept` has to answer two questions before a byte moves: *which*
//! configured receive root the file lands in, and *where* beneath it. Both are
//! explicit tool arguments, because MCP 2026-07-28 deprecated Roots and directs
//! new servers to carry filesystem scope in tool parameters, resource URIs, or
//! server configuration — and all three of those are things a call can restate
//! from scratch, which a client-declared capability is not.
//!
//! Usually neither question needs asking. One configured root answers the first
//! by itself, and the offered filename answers the second. When several roots
//! exist and the call named none, though, the server genuinely cannot choose:
//! picking one would be inventing an authority decision on the caller's behalf.
//! That is what MRTR is for — return `input_required` with a form, and the
//! client re-sends the same call with the answer.
//!
//! This module is the decision, kept free of I/O and of result shaping so it can
//! be read and tested as what it is: a small set of rules about arguments.
//! Sealing, opening, and the eventual acceptance live in
//! [`crate::mcp::service`], which holds the gateway and the request identity
//! those need.

use std::path::PathBuf;

use rmcp::{
    ErrorData as McpError,
    model::{
        ElicitResult, ElicitationAction, ElicitationSchema, EnumSchema, InputRequest,
        InputRequests, InputResponses,
    },
};
use serde::{Deserialize, Serialize};

use crate::{
    config::DccConfig,
    dcc::{
        confine::ReceiveChoice,
        session::{DccKind, DccSession, DccSessionId},
        transfer::DestinationConflict,
    },
    mcp::{mrtr::form_elicitation, tools::DccAcceptInput},
};

/// Key the destination question is filed under within one MRTR round.
///
/// The client echoes it back as the key of its response, so it is part of the
/// wire contract between the two rounds and must not drift.
pub const DESTINATION_INPUT: &str = "dcc_destination";

/// A validated acceptance, ready to run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptPlan {
    /// Chosen receive root and root-relative destination for a SEND; absent for
    /// a CHAT, which writes nothing.
    pub destination: Option<ReceiveChoice>,
    /// Existing-destination behavior.
    pub conflict: DestinationConflict,
}

/// What one `irc.dcc.accept` call still needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Every argument is settled.
    Ready(AcceptPlan),
    /// A root has to be chosen from more than one candidate.
    Choose {
        /// Configured root names, in declaration order.
        roots: Vec<String>,
        /// Destination the caller gets by naming a root and nothing else.
        default_path: Option<PathBuf>,
    },
    /// The destination cannot be settled at all, whoever is asked.
    Refuse(String),
}

/// What a client sent back for the destination question.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Answer {
    /// No response carried this round's key.
    #[default]
    Missing,
    /// The caller refused to choose, or cancelled.
    Declined,
    /// The caller chose.
    Chosen(DestinationChoice),
}

/// The fields a client filled in on the destination form.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DestinationChoice {
    /// Selected root name.
    pub root: Option<String>,
    /// Destination relative to that root.
    pub path: Option<PathBuf>,
}

/// What one `irc.dcc.accept` exchange remembers between its rounds.
///
/// Deliberately small. Everything here is re-checked against the live offer on
/// redemption, so the state is a claim about which question was asked, never a
/// cached authorization: the sealed binding already carries the caller and the
/// originating arguments, and the acceptance itself still has to pass every
/// check an unassisted call would.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct PendingAccept {
    /// Offer the question was asked about.
    pub dcc_session_id: DccSessionId,
    /// Peer that made it.
    pub peer: String,
    /// Filename it advertised.
    pub filename: Option<String>,
    /// Root names the form offered, so an answer is validated against the same
    /// set the question was asked with rather than a wider one.
    pub roots: Vec<String>,
}

impl PendingAccept {
    /// Describe the question about to be asked.
    pub fn for_offer(offer: &DccSession, roots: Vec<String>) -> Self {
        Self {
            dcc_session_id: offer.id.clone(),
            peer: offer.peer.clone(),
            filename: offer.filename.clone(),
            roots,
        }
    }

    /// Whether this state still describes the offer in front of us.
    ///
    /// A handle alone is not enough. Checking the peer and the advertised
    /// filename too means a state can only be redeemed against the offer whose
    /// details the caller was actually shown when it chose.
    pub fn matches(&self, offer: &DccSession) -> bool {
        self.dcc_session_id == offer.id
            && self.peer == offer.peer
            && self.filename == offer.filename
    }
}

/// Read the client's answer to the destination question.
///
/// # Errors
///
/// A response that is not an elicitation result at all is malformed, which the
/// specification says is an ordinary protocol error rather than another round.
pub fn read_answer(responses: Option<&InputResponses>) -> Result<Answer, McpError> {
    let Some(value) = responses.and_then(|responses| responses.get(DESTINATION_INPUT)) else {
        return Ok(Answer::Missing);
    };
    let result: ElicitResult = serde_json::from_value(value.clone()).map_err(|error| {
        McpError::invalid_params(
            format!("{DESTINATION_INPUT} response is not an elicitation result: {error}"),
            None,
        )
    })?;
    if result.action != ElicitationAction::Accept {
        return Ok(Answer::Declined);
    }
    let content = result.content.unwrap_or(serde_json::Value::Null);
    Ok(Answer::Chosen(DestinationChoice {
        root: text(&content, "root"),
        path: text(&content, "destination_path").map(PathBuf::from),
    }))
}

/// One non-empty string field of an elicitation's content.
fn text(content: &serde_json::Value, field: &str) -> Option<String> {
    content
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// Decide what an acceptance still needs, given whatever the caller has said so
/// far.
///
/// `answered` is the client's form answer when there is one. It is treated
/// exactly like an explicit argument — validated against the same configuration
/// and refused for the same reasons — so a destination that arrives through an
/// elicitation can never be trusted further than one that arrived in the
/// original call.
pub fn decide(
    config: &DccConfig,
    offer: &DccSession,
    input: &DccAcceptInput,
    answered: Option<&DestinationChoice>,
    offered_roots: Option<&[String]>,
) -> Decision {
    if offer.kind == DccKind::Chat {
        if input.root.is_some() || input.destination_path.is_some() {
            return Decision::Refuse(
                "a DCC CHAT offer writes no file; omit root and destination_path".into(),
            );
        }
        return Decision::Ready(AcceptPlan {
            destination: None,
            conflict: input.conflict,
        });
    }

    let requested_path = answered
        .and_then(|choice| choice.path.clone())
        .or_else(|| input.destination_path.clone());
    if let Some(path) = &requested_path
        && path.is_absolute()
    {
        return Decision::Refuse(format!(
            "destination_path must be relative to the chosen receive root; {} is absolute",
            path.display()
        ));
    }

    let names = config.receive_root_names();
    let requested_root = answered
        .and_then(|choice| choice.root.clone())
        .or_else(|| input.root.clone());
    let root = match requested_root {
        Some(name) => {
            if !names.contains(&name) {
                return Decision::Refuse(format!(
                    "unknown receive root {name:?}; configured roots are {}",
                    names.join(", ")
                ));
            }
            // A form answer is only accepted for the question it answered. A
            // root that was not offered means the answer belongs to a different
            // exchange, whatever the configuration happens to allow now.
            if answered.is_some_and(|choice| choice.root.is_some())
                && offered_roots.is_some_and(|offered| !offered.contains(&name))
            {
                return Decision::Refuse(format!(
                    "receive root {name:?} was not among the roots this choice was offered"
                ));
            }
            name
        }
        None => match names.as_slice() {
            [only] => only.clone(),
            _ => {
                return Decision::Choose {
                    roots: names,
                    default_path: offer.filename.clone().map(PathBuf::from),
                };
            }
        },
    };

    let Some(path) = requested_path.or_else(|| offer.filename.clone().map(PathBuf::from)) else {
        return Decision::Refuse(
            "this offer advertised no filename, so destination_path is required".into(),
        );
    };
    Decision::Ready(AcceptPlan {
        destination: Some(ReceiveChoice { root, path }),
        conflict: input.conflict,
    })
}

/// Build the form that asks a caller where one offered file should land.
///
/// Form mode only, and only for a client that declared it: sending an input
/// request a client never said it could answer is a protocol violation, not a
/// graceful degradation.
pub fn destination_requests(
    offer: &DccSession,
    roots: &[String],
    default_path: Option<&PathBuf>,
) -> Result<InputRequests, McpError> {
    let mut root = EnumSchema::builder(roots.to_vec())
        .title("Receive root")
        .description("Configured directory this file is confined beneath.");
    // Only when the default is one the form actually lists, so the schema never
    // proposes a value it would then reject.
    if let Some(first) = roots.first() {
        root = root
            .with_default(first.clone())
            .map_err(|error| McpError::internal_error(error, None))?;
    }
    let default_path = default_path
        .and_then(|path| path.to_str())
        .map(str::to_owned);
    let schema = ElicitationSchema::builder()
        .required_enum_schema("root", root.build())
        .optional_string_with("destination_path", |field| {
            let field = field
                .title("Destination")
                .description("Path relative to the chosen root. Defaults to the offered filename.");
            default_path.map_or(field.clone(), |default| field.with_default(default))
        })
        .description("Destination for one incoming DCC file transfer.")
        .build()
        .map_err(|error| McpError::internal_error(error, None))?;

    Ok(InputRequests::from([(
        DESTINATION_INPUT.to_owned(),
        destination_elicitation(offer, schema),
    )]))
}

/// Phrase the question in terms of the offer it is about.
fn destination_elicitation(offer: &DccSession, schema: ElicitationSchema) -> InputRequest {
    let filename = offer.filename.as_deref().unwrap_or("an unnamed file");
    let size = offer
        .total_bytes
        .map_or_else(String::new, |bytes| format!(" ({bytes} bytes)"));
    form_elicitation(
        format!(
            "Choose where to receive {filename}{size} offered by {}.",
            offer.peer
        ),
        schema,
    )
}

#[cfg(test)]
mod tests {
    use crate::{
        config::DccReceiveRoot,
        dcc::session::{DccDirection, DccState},
        time::Timestamp,
    };

    use super::*;

    fn config(roots: &[(&str, &str)]) -> DccConfig {
        DccConfig {
            receive_roots: roots
                .iter()
                .map(|(name, path)| DccReceiveRoot {
                    name: (*name).to_owned(),
                    path: PathBuf::from(*path),
                })
                .collect(),
            ..DccConfig::default()
        }
    }

    fn offer(kind: DccKind, filename: Option<&str>) -> DccSession {
        let mut session = DccSession::offered(
            kind,
            DccDirection::Inbound,
            "alice",
            "2026-08-17T00:00:00.000Z"
                .parse::<Timestamp>()
                .expect("now"),
        );
        session.state = DccState::Offered;
        session.filename = filename.map(str::to_owned);
        session.total_bytes = Some(1_204);
        session
    }

    fn input(root: Option<&str>, destination_path: Option<&str>) -> DccAcceptInput {
        DccAcceptInput {
            agent_id: crate::agent::AgentId::new(),
            dcc_session_id: DccSessionId::new(),
            root: root.map(str::to_owned),
            destination_path: destination_path.map(PathBuf::from),
            conflict: DestinationConflict::Fail,
        }
    }

    #[test]
    fn a_single_configured_root_is_chosen_without_asking() {
        let decided = decide(
            &config(&[("downloads", "/srv/downloads")]),
            &offer(DccKind::Send, Some("report.txt")),
            &input(None, None),
            None,
            None,
        );
        assert_eq!(
            decided,
            Decision::Ready(AcceptPlan {
                destination: Some(ReceiveChoice {
                    root: "downloads".into(),
                    path: PathBuf::from("report.txt"),
                }),
                conflict: DestinationConflict::Fail,
            })
        );
    }

    #[test]
    fn several_roots_and_no_named_one_is_a_choice_only_the_caller_can_make() {
        let decided = decide(
            &config(&[("downloads", "/srv/downloads"), ("media", "/srv/media")]),
            &offer(DccKind::Send, Some("report.txt")),
            &input(None, None),
            None,
            None,
        );
        assert_eq!(
            decided,
            Decision::Choose {
                roots: vec!["downloads".into(), "media".into()],
                default_path: Some(PathBuf::from("report.txt")),
            }
        );
    }

    #[test]
    fn an_explicit_root_and_relative_path_settle_the_destination() {
        let decided = decide(
            &config(&[("downloads", "/srv/downloads"), ("media", "/srv/media")]),
            &offer(DccKind::Send, Some("report.txt")),
            &input(Some("media"), Some("august/report.txt")),
            None,
            None,
        );
        assert_eq!(
            decided,
            Decision::Ready(AcceptPlan {
                destination: Some(ReceiveChoice {
                    root: "media".into(),
                    path: PathBuf::from("august/report.txt"),
                }),
                conflict: DestinationConflict::Fail,
            })
        );
    }

    #[test]
    fn an_absolute_destination_is_refused_rather_than_asked_about() {
        // The root name is the whole filesystem authority a call carries, so a
        // path that supplied its own root is refused before anything else is
        // decided — including before a missing root would have been asked for.
        let absolute = if cfg!(windows) {
            r"C:\srv\media\report.txt"
        } else {
            "/srv/media/report.txt"
        };
        let decided = decide(
            &config(&[("downloads", "/srv/downloads"), ("media", "/srv/media")]),
            &offer(DccKind::Send, Some("report.txt")),
            &input(None, Some(absolute)),
            None,
            None,
        );
        let Decision::Refuse(message) = decided else {
            panic!("an absolute destination must be refused: {decided:?}");
        };
        assert!(message.contains("must be relative"), "{message}");
    }

    #[test]
    fn an_unknown_root_names_the_roots_that_do_exist() {
        let decided = decide(
            &config(&[("downloads", "/srv/downloads"), ("media", "/srv/media")]),
            &offer(DccKind::Send, Some("report.txt")),
            &input(Some("elsewhere"), None),
            None,
            None,
        );
        let Decision::Refuse(message) = decided else {
            panic!("an unknown root must be refused: {decided:?}");
        };
        assert!(message.contains("downloads, media"), "{message}");
    }

    #[test]
    fn an_answer_is_validated_exactly_like_an_explicit_argument() {
        let configured = config(&[("downloads", "/srv/downloads"), ("media", "/srv/media")]);
        let offered = offer(DccKind::Send, Some("report.txt"));
        let answered = DestinationChoice {
            root: Some("media".into()),
            path: Some(PathBuf::from("august/report.txt")),
        };
        assert_eq!(
            decide(
                &configured,
                &offered,
                &input(None, None),
                Some(&answered),
                Some(&["downloads".into(), "media".into()]),
            ),
            Decision::Ready(AcceptPlan {
                destination: Some(ReceiveChoice {
                    root: "media".into(),
                    path: PathBuf::from("august/report.txt"),
                }),
                conflict: DestinationConflict::Fail,
            })
        );

        let unoffered = DestinationChoice {
            root: Some("media".into()),
            path: None,
        };
        let decided = decide(
            &configured,
            &offered,
            &input(None, None),
            Some(&unoffered),
            Some(&["downloads".into()]),
        );
        let Decision::Refuse(message) = decided else {
            panic!("an answer outside the offered set must be refused: {decided:?}");
        };
        assert!(message.contains("not among the roots"), "{message}");
    }

    #[test]
    fn an_offer_without_a_filename_needs_a_destination_path() {
        let decided = decide(
            &config(&[("downloads", "/srv/downloads")]),
            &offer(DccKind::Send, None),
            &input(None, None),
            None,
            None,
        );
        let Decision::Refuse(message) = decided else {
            panic!("a nameless offer must be refused: {decided:?}");
        };
        assert!(
            message.contains("destination_path is required"),
            "{message}"
        );
    }

    #[test]
    fn a_chat_offer_settles_without_a_destination() {
        assert_eq!(
            decide(
                &config(&[("downloads", "/srv/downloads"), ("media", "/srv/media")]),
                &offer(DccKind::Chat, None),
                &input(None, None),
                None,
                None,
            ),
            Decision::Ready(AcceptPlan {
                destination: None,
                conflict: DestinationConflict::Fail,
            })
        );
        let decided = decide(
            &config(&[("downloads", "/srv/downloads")]),
            &offer(DccKind::Chat, None),
            &input(Some("downloads"), None),
            None,
            None,
        );
        assert!(matches!(decided, Decision::Refuse(_)));
    }

    #[test]
    fn a_missing_response_is_not_an_answer_and_a_refusal_is_not_a_choice() {
        assert_eq!(read_answer(None).expect("missing"), Answer::Missing);
        assert_eq!(
            read_answer(Some(&InputResponses::from([(
                "something_else".to_owned(),
                serde_json::json!({ "action": "accept" }),
            )])))
            .expect("other key"),
            Answer::Missing
        );
        for refusal in ["decline", "cancel"] {
            assert_eq!(
                read_answer(Some(&InputResponses::from([(
                    DESTINATION_INPUT.to_owned(),
                    serde_json::json!({ "action": refusal }),
                )])))
                .expect("refusal"),
                Answer::Declined
            );
        }
        assert_eq!(
            read_answer(Some(&InputResponses::from([(
                DESTINATION_INPUT.to_owned(),
                serde_json::json!({
                    "action": "accept",
                    "content": { "root": "media", "destination_path": " " },
                }),
            )])))
            .expect("choice"),
            Answer::Chosen(DestinationChoice {
                root: Some("media".into()),
                // Blank is absent, not a destination named by one space.
                path: None,
            })
        );
        assert!(
            read_answer(Some(&InputResponses::from([(
                DESTINATION_INPUT.to_owned(),
                serde_json::json!("not an elicitation result"),
            )])))
            .is_err()
        );
    }

    #[test]
    fn a_pending_state_only_matches_the_offer_it_was_minted_for() {
        let offered = offer(DccKind::Send, Some("report.txt"));
        let pending = PendingAccept::for_offer(&offered, vec!["downloads".into()]);
        assert!(pending.matches(&offered));

        let mut renamed = offered.clone();
        renamed.filename = Some("other.txt".into());
        assert!(!pending.matches(&renamed));

        let mut different_peer = offered.clone();
        different_peer.peer = "mallory".into();
        assert!(!pending.matches(&different_peer));

        assert!(!pending.matches(&offer(DccKind::Send, Some("report.txt"))));
    }

    #[test]
    fn the_destination_form_offers_exactly_the_configured_roots() {
        let roots = vec!["downloads".to_owned(), "media".to_owned()];
        let requests = destination_requests(
            &offer(DccKind::Send, Some("report.txt")),
            &roots,
            Some(&PathBuf::from("report.txt")),
        )
        .expect("form");
        let wire = serde_json::to_value(&requests).expect("serialize");
        let request = &wire[DESTINATION_INPUT];

        assert_eq!(request["method"], "elicitation/create");
        assert_eq!(request["params"]["mode"], "form");
        let schema = &request["params"]["requestedSchema"];
        assert_eq!(
            schema["properties"]["root"]["enum"],
            serde_json::json!(roots)
        );
        assert_eq!(schema["required"], serde_json::json!(["root"]));
        assert_eq!(
            schema["properties"]["destination_path"]["default"],
            "report.txt"
        );
        assert!(
            request["params"]["message"]
                .as_str()
                .expect("message")
                .contains("report.txt")
        );
    }

    #[test]
    fn the_default_the_form_proposes_is_one_the_decision_would_accept() {
        // Otherwise the obvious answer — take both defaults — would come back
        // and be refused, which is the worst possible time to find out.
        let configured = config(&[("downloads", "/srv/downloads"), ("media", "/srv/media")]);
        let offered = offer(DccKind::Send, Some("report.txt"));
        let Decision::Choose {
            roots,
            default_path,
        } = decide(&configured, &offered, &input(None, None), None, None)
        else {
            panic!("expected a choice");
        };
        let requests = destination_requests(&offered, &roots, default_path.as_ref()).expect("form");
        let wire = serde_json::to_value(&requests).expect("serialize");
        let schema = &wire[DESTINATION_INPUT]["params"]["requestedSchema"]["properties"];
        let proposed = DestinationChoice {
            root: schema["root"]["default"].as_str().map(str::to_owned),
            path: schema["destination_path"]["default"]
                .as_str()
                .map(PathBuf::from),
        };
        assert!(matches!(
            decide(
                &configured,
                &offered,
                &input(None, None),
                Some(&proposed),
                Some(&roots),
            ),
            Decision::Ready(_)
        ));
    }
}
