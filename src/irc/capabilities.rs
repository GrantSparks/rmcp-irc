//! Runtime IRC/IRCv3 discovery, negotiation, and the compatibility catalog.
//!
//! Negotiation is driven by the server's own `CAP` replies, including the
//! multi-line `CAP LS 302` continuation form. Exact tokens are never
//! normalized away: a draft and a ratified spelling share a feature identity
//! for behavior decisions while both remain visible in the catalog.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    commands::{
        COMMANDS, CapabilityLookup, CollectorKind, CommandPhase, MappingGrade, PrivilegeClass,
        StateEffects,
    },
    isupport::IsupportToken,
    wire::WireMessage,
};

/// Negotiation state for one exact advertised capability token.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityStatus {
    /// Advertised but not requested.
    ObservedUnnegotiated,
    /// Requested and awaiting ACK or NAK.
    Requested,
    /// Acknowledged and active.
    Negotiated,
    /// Rejected by the server.
    Rejected,
    /// Removed by CAP DEL.
    Removed,
}

/// One exact capability and its normalized internal feature identity.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct CapabilityEntry {
    /// Exact case-preserved capability name.
    pub name: String,
    /// Optional exact value following equals.
    pub value: Option<String>,
    /// Stable internal feature name shared by draft and final spellings.
    pub feature: FeatureId,
    /// Current negotiation state.
    pub status: CapabilityStatus,
    /// Compatibility grade.
    pub mapping: MappingGrade,
}

/// Where the gateway learned that a command exists.
#[derive(
    Clone, Copy, Debug, Deserialize, JsonSchema, Ord, PartialOrd, PartialEq, Eq, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CommandEvidence {
    /// The bridge's own static registry.
    StandardRegistry,
    /// Ergo's runtime HELP INDEX.
    HelpIndex,
}

/// Resource-friendly command descriptor.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct CommandDescriptor {
    /// Uppercase command name.
    pub name: String,
    /// Static registry and/or runtime HELP INDEX.
    pub reported_by: BTreeSet<CommandEvidence>,
    /// Registration phase.
    pub phase: CommandPhase,
    /// Documentary privilege class.
    pub privilege: PrivilegeClass,
    /// Required IRCv3 capabilities.
    pub required_capabilities: Vec<String>,
    /// Completion collector this command uses.
    pub response_strategy: CollectorKind,
    /// In-memory state this command can change.
    pub state_effects: StateEffects,
    /// Compatibility grade.
    pub mapping: MappingGrade,
    /// Whether HELP text is cached or retrievable.
    pub help_available: bool,
}

/// Complete in-memory compatibility resource for one agent.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct CompatibilityCatalog {
    /// Exact capability tokens keyed by exact name.
    pub capabilities: BTreeMap<String, CapabilityEntry>,
    /// Exact ISUPPORT tokens keyed by case-preserved name.
    pub isupport: BTreeMap<String, IsupportToken>,
    /// Static and runtime-discovered commands.
    pub commands: BTreeMap<String, CommandDescriptor>,
    /// Cached HELP subjects.
    pub help: BTreeMap<String, String>,
    /// Locally implemented CTCP commands, separate from IRC CAP.
    pub ctcp_commands: BTreeSet<String>,
    /// Locally implemented DCC variants, separate from IRC CAP.
    pub dcc_variants: BTreeSet<String>,
}

impl CompatibilityCatalog {
    /// Seed a catalog from the bridge's static registry.
    pub fn with_static_registry() -> Self {
        let mut catalog = Self::default();
        for spec in COMMANDS {
            catalog.commands.insert(
                spec.name.into(),
                CommandDescriptor {
                    name: spec.name.into(),
                    reported_by: BTreeSet::from([CommandEvidence::StandardRegistry]),
                    phase: spec.phase,
                    privilege: spec.privilege,
                    required_capabilities: spec
                        .required_capabilities
                        .iter()
                        .map(|item| (*item).to_owned())
                        .collect(),
                    response_strategy: spec.response.kind(),
                    state_effects: spec.state_effects,
                    mapping: spec.mapping,
                    help_available: false,
                },
            );
        }
        catalog
    }

    /// Record a command Ergo reported through HELP INDEX.
    ///
    /// Unknown discovered commands are recorded as valid passthrough inputs.
    pub fn record_discovered_command(&mut self, name: &str) {
        let key = name.to_ascii_uppercase();
        self.commands
            .entry(key.clone())
            .and_modify(|descriptor| {
                descriptor.reported_by.insert(CommandEvidence::HelpIndex);
                descriptor.help_available = true;
            })
            .or_insert_with(|| CommandDescriptor {
                name: key,
                reported_by: BTreeSet::from([CommandEvidence::HelpIndex]),
                phase: CommandPhase::Registered,
                privilege: PrivilegeClass::Normal,
                required_capabilities: Vec::new(),
                response_strategy: CollectorKind::Unconfirmed,
                state_effects: StateEffects::None,
                mapping: MappingGrade::Passthrough,
                help_available: true,
            });
    }

    /// Cache HELP text for one subject.
    pub fn record_help(&mut self, subject: &str, text: impl Into<String>) {
        let key = subject.to_ascii_uppercase();
        if let Some(descriptor) = self.commands.get_mut(&key) {
            descriptor.help_available = true;
        }
        self.help.insert(key, text.into());
    }

    /// Apply a completed HELP response to the catalog.
    pub fn record_help_response(&mut self, response: &HelpResponse) {
        match response {
            HelpResponse::Index { commands } => {
                for command in commands {
                    self.record_discovered_command(command);
                }
            }
            HelpResponse::Subject { subject, text } => self.record_help(subject, text.clone()),
        }
    }
}

/// A completed HELP reply, either the command index or one subject's text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HelpResponse {
    /// The command list Ergo returns for `HELP INDEX`.
    Index {
        /// Uppercase command names, in the order the server listed them.
        commands: Vec<String>,
    },
    /// Help text for one subject.
    Subject {
        /// Uppercase subject the help belongs to.
        subject: String,
        /// Joined help text, exactly as the server wrote it.
        text: String,
    },
}

/// Collector for the `704`/`705`/`706` HELP numeric sequence.
///
/// Ergo answers `HELP INDEX` with the same numerics as any other subject, so
/// the subject determines how the collected lines are read. Lines are kept
/// verbatim; only the index is tokenized, because only it is a list.
#[derive(Clone, Debug, Default)]
pub struct HelpCollector {
    subject: Option<String>,
    lines: Vec<String>,
}

impl HelpCollector {
    /// Empty collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one message, returning the response when the sequence ends.
    pub fn apply(&mut self, message: &WireMessage) -> Option<HelpResponse> {
        // A numeric's first parameter is the client's own nickname; the
        // subject, when the server names one, follows it.
        let subject = message
            .params
            .get(1)
            .map(|value| value.to_ascii_uppercase());
        match message.numeric()? {
            704 => {
                self.subject = subject;
                self.lines.clear();
                if let Some(text) = message.trailing.clone() {
                    self.lines.push(text);
                }
                None
            }
            705 => {
                if self.subject.is_none() {
                    self.subject = subject;
                }
                if let Some(text) = message.trailing.clone() {
                    self.lines.push(text);
                }
                None
            }
            706 => {
                let subject = self.subject.take().or(subject).unwrap_or_default();
                let lines = std::mem::take(&mut self.lines);
                Some(if subject == "INDEX" {
                    HelpResponse::Index {
                        commands: index_commands(&lines),
                    }
                } else {
                    HelpResponse::Subject {
                        subject,
                        text: lines.join("\n"),
                    }
                })
            }
            _ => None,
        }
    }
}

/// Split an index body into command names.
///
/// The index is a prose-formatted list, so anything that is not a plausible
/// command token is skipped rather than recorded as a command.
fn index_commands(lines: &[String]) -> Vec<String> {
    let mut commands = Vec::new();
    for line in lines {
        for token in line.split_whitespace() {
            let token = token.trim_matches(|character: char| !character.is_alphanumeric());
            if token.len() >= 2
                && token
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric())
            {
                let command = token.to_ascii_uppercase();
                if !commands.contains(&command) {
                    commands.push(command);
                }
            }
        }
    }
    commands
}

/// SASL mechanisms this gateway can drive.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SaslMechanism {
    /// Username and password in a single base64 payload.
    Plain,
    /// Bare authentication with an external credential, such as a TLS client
    /// certificate.
    External,
}

impl SaslMechanism {
    /// Token sent in the `AUTHENTICATE` command.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::External => "EXTERNAL",
        }
    }

    /// Whether the server advertised this mechanism in its `sasl` value.
    ///
    /// A `sasl` capability without a mechanism list is treated as supporting
    /// `PLAIN`.
    pub fn is_advertised(self, advertised: Option<&str>) -> bool {
        advertised.is_none_or(|value| {
            value.is_empty()
                || value
                    .split(',')
                    .any(|mechanism| mechanism.eq_ignore_ascii_case(self.as_str()))
        })
    }
}

/// Whether this connection authenticates, and how.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "mode", content = "mechanism")]
pub enum SaslPolicy {
    /// Connect as a guest; never request `sasl`.
    #[default]
    Guest,
    /// Request `sasl` and authenticate with this mechanism.
    Authenticate(SaslMechanism),
}

/// How far registration-time capability negotiation has progressed.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NegotiationPhase {
    /// `CAP LS` has not finished.
    #[default]
    Listing,
    /// A `CAP REQ` is outstanding.
    AwaitingAck,
    /// SASL is in progress; `CAP END` waits for it.
    Authenticating,
    /// `CAP END` has been called for; later `CAP NEW` does not repeat it.
    Complete,
}

/// Capabilities this gateway will request when a server advertises them.
///
/// Unlisted capabilities remain observed but unnegotiated. `sasl` is requested
/// separately according to [`SaslPolicy`]; `multiline` is not supported.
pub const IMPLEMENTED_CAPABILITIES: &[&str] = &[
    "batch",
    "draft/chathistory",
    "cap-notify",
    "labeled-response",
    "message-tags",
    "server-time",
    "echo-message",
    "account-tag",
    "standard-replies",
    "multi-prefix",
    "userhost-in-names",
    "extended-join",
    "away-notify",
    "account-notify",
    "chghost",
    "setname",
    "invite-notify",
];

/// Negotiation state machine for one connection.
#[derive(Clone, Debug, Default)]
pub struct CapabilityNegotiator {
    catalog_entries: BTreeMap<String, CapabilityEntry>,
    features: BTreeSet<FeatureId>,
    outstanding: BTreeSet<String>,
    phase: NegotiationPhase,
    sasl: SaslPolicy,
    ls_complete: bool,
}

impl CapabilityNegotiator {
    /// Empty negotiator before `CAP LS 302` is sent.
    pub fn new() -> Self {
        Self::default()
    }

    /// Negotiator that authenticates instead of connecting as a guest.
    pub fn with_sasl(sasl: SaslPolicy) -> Self {
        Self {
            sasl,
            ..Self::default()
        }
    }

    /// How far registration-time negotiation has progressed.
    #[cfg(test)]
    pub const fn phase(&self) -> NegotiationPhase {
        self.phase
    }

    /// Whether `CAP END` has already been called for.
    pub const fn is_complete(&self) -> bool {
        matches!(self.phase, NegotiationPhase::Complete)
    }

    /// Capability tokens whose ACK or NAK has not arrived.
    #[cfg(test)]
    pub const fn outstanding(&self) -> &BTreeSet<String> {
        &self.outstanding
    }

    /// Whether the server has finished its `CAP LS` listing.
    #[cfg(test)]
    pub const fn listing_complete(&self) -> bool {
        self.ls_complete
    }

    /// Every capability entry, keyed by exact token.
    #[cfg(test)]
    pub const fn entries(&self) -> &BTreeMap<String, CapabilityEntry> {
        &self.catalog_entries
    }

    /// Whether this exact token, or its feature equivalent, is active.
    pub fn is_active(&self, capability: &str) -> bool {
        self.features.contains(&FeatureId::of(capability))
    }

    /// Active capabilities by exact advertised token.
    #[cfg(test)]
    pub fn negotiated(&self) -> Vec<&str> {
        self.catalog_entries
            .values()
            .filter(|entry| entry.status == CapabilityStatus::Negotiated)
            .map(|entry| entry.name.as_str())
            .collect()
    }

    /// Apply one inbound message, returning what the caller must send next.
    ///
    /// This covers `CAP`, the `AUTHENTICATE` challenge, and the SASL result
    /// numerics, because all three decide when `CAP END` may be sent.
    pub fn apply(&mut self, message: &WireMessage) -> CapabilityAction {
        if message.command.eq_ignore_ascii_case("AUTHENTICATE") {
            return self.apply_authenticate(message);
        }
        if let Some(numeric) = message.numeric() {
            return self.apply_numeric(numeric);
        }
        if !message.command.eq_ignore_ascii_case("CAP") {
            return CapabilityAction::None;
        }
        // CAP <nick-or-*> <subcommand> [*] :<tokens>
        let Some(subcommand) = message.params.get(1) else {
            return CapabilityAction::None;
        };
        let more_to_come = message.params.get(2).is_some_and(|value| value == "*");
        let tokens = message.trailing.as_deref().unwrap_or_default();

        match subcommand.to_ascii_uppercase().as_str() {
            "LS" => self.apply_ls(tokens, more_to_come),
            "NEW" => self.apply_new(tokens),
            "ACK" => {
                self.set_status(tokens, CapabilityStatus::Negotiated);
                self.resolve(tokens)
            }
            "NAK" => {
                self.set_status(tokens, CapabilityStatus::Rejected);
                self.resolve(tokens)
            }
            "DEL" => {
                self.set_status(tokens, CapabilityStatus::Removed);
                CapabilityAction::None
            }
            "LIST" => CapabilityAction::None,
            _ => CapabilityAction::None,
        }
    }

    fn apply_ls(&mut self, tokens: &str, more_to_come: bool) -> CapabilityAction {
        self.record_advertised(tokens);
        if more_to_come {
            return CapabilityAction::None;
        }
        self.ls_complete = true;
        let request = self.select_requests();
        if request.is_empty() {
            self.phase = NegotiationPhase::Complete;
            return CapabilityAction::EndNegotiation;
        }
        self.phase = NegotiationPhase::AwaitingAck;
        self.outstanding = request.iter().cloned().collect();
        CapabilityAction::Request(request)
    }

    /// Clear resolved requests and decide whether negotiation can end.
    fn resolve(&mut self, tokens: &str) -> CapabilityAction {
        for (name, _) in parse_tokens(tokens) {
            self.outstanding.remove(&name);
        }
        if self.phase != NegotiationPhase::AwaitingAck || !self.outstanding.is_empty() {
            return CapabilityAction::None;
        }
        // Every request has been answered. SASL, when configured and
        // acknowledged, must finish before CAP END or the server would
        // register the connection unauthenticated.
        match self.sasl {
            SaslPolicy::Authenticate(mechanism) if self.is_active("sasl") => {
                self.phase = NegotiationPhase::Authenticating;
                CapabilityAction::Authenticate(mechanism)
            }
            _ => {
                self.phase = NegotiationPhase::Complete;
                CapabilityAction::EndNegotiation
            }
        }
    }

    /// The server's `AUTHENTICATE +` challenge asks for the payload.
    fn apply_authenticate(&mut self, message: &WireMessage) -> CapabilityAction {
        if self.phase != NegotiationPhase::Authenticating {
            return CapabilityAction::None;
        }
        let challenge = message
            .params
            .first()
            .map(String::as_str)
            .or(message.trailing.as_deref());
        if challenge == Some("+") {
            CapabilityAction::SendAuthenticationPayload
        } else {
            CapabilityAction::None
        }
    }

    /// SASL result numerics end the authentication step either way.
    ///
    /// Failure still ends capability negotiation so registration can continue
    /// as a guest.
    fn apply_numeric(&mut self, numeric: u16) -> CapabilityAction {
        if self.phase != NegotiationPhase::Authenticating {
            return CapabilityAction::None;
        }
        match numeric {
            903 => {
                self.phase = NegotiationPhase::Complete;
                CapabilityAction::EndNegotiation
            }
            902 | 904 | 905 | 906 | 907 | 908 => {
                self.phase = NegotiationPhase::Complete;
                CapabilityAction::AuthenticationFailed { numeric }
            }
            _ => CapabilityAction::None,
        }
    }

    fn apply_new(&mut self, tokens: &str) -> CapabilityAction {
        self.record_advertised(tokens);
        let request: Vec<String> = parse_tokens(tokens)
            .into_iter()
            .map(|(name, _)| name)
            .filter(|name| self.wants(name))
            .collect();
        for name in &request {
            self.mark_requested(name);
        }
        if request.is_empty() {
            CapabilityAction::None
        } else {
            CapabilityAction::Request(request)
        }
    }

    fn record_advertised(&mut self, tokens: &str) {
        for (name, value) in parse_tokens(tokens) {
            let feature = FeatureId::of(&name);
            let entry =
                self.catalog_entries
                    .entry(name.clone())
                    .or_insert_with(|| CapabilityEntry {
                        name: name.clone(),
                        value: value.clone(),
                        feature: feature.clone(),
                        status: CapabilityStatus::ObservedUnnegotiated,
                        mapping: MappingGrade::ObservedUnnegotiated,
                    });
            entry.value = value;
            if entry.status == CapabilityStatus::Removed {
                entry.status = CapabilityStatus::ObservedUnnegotiated;
                entry.mapping = MappingGrade::ObservedUnnegotiated;
            }
        }
    }

    fn select_requests(&mut self) -> Vec<String> {
        let names: Vec<String> = self
            .catalog_entries
            .values()
            .filter(|entry| {
                entry.status == CapabilityStatus::ObservedUnnegotiated && self.wants(&entry.name)
            })
            .map(|entry| entry.name.clone())
            .collect();
        for name in &names {
            self.mark_requested(name);
        }
        names
    }

    /// Whether this connection wants a capability, including configured SASL.
    fn wants(&self, capability: &str) -> bool {
        if FeatureId::of(capability) == "sasl" {
            let advertised = self
                .catalog_entries
                .get(capability)
                .and_then(|entry| entry.value.as_deref());
            return match self.sasl {
                SaslPolicy::Guest => false,
                SaslPolicy::Authenticate(mechanism) => mechanism.is_advertised(advertised),
            };
        }
        is_implemented(capability)
    }

    fn mark_requested(&mut self, name: &str) {
        if let Some(entry) = self.catalog_entries.get_mut(name) {
            entry.status = CapabilityStatus::Requested;
        }
    }

    fn set_status(&mut self, tokens: &str, status: CapabilityStatus) {
        for (name, _) in parse_tokens(tokens) {
            let feature = FeatureId::of(&name);
            let Some(entry) = self.catalog_entries.get_mut(&name) else {
                continue;
            };
            entry.status = status;
            entry.mapping = match status {
                CapabilityStatus::Negotiated => MappingGrade::Native,
                CapabilityStatus::Rejected | CapabilityStatus::Removed => MappingGrade::Unavailable,
                _ => entry.mapping,
            };
            match status {
                CapabilityStatus::Negotiated => {
                    self.features.insert(feature);
                }
                CapabilityStatus::Rejected | CapabilityStatus::Removed => {
                    self.features.remove(&feature);
                }
                _ => {}
            }
        }
    }

    /// Copy the current entries into a catalog for the protocol resource.
    pub fn publish_into(&self, catalog: &mut CompatibilityCatalog) {
        catalog.capabilities = self.catalog_entries.clone();
    }
}

impl CapabilityLookup for CapabilityNegotiator {
    fn is_negotiated(&self, capability: &str) -> bool {
        self.is_active(capability)
    }
}

/// What the caller must send after applying a message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CapabilityAction {
    /// Nothing to send.
    None,
    /// Send `CAP REQ` for these exact tokens.
    Request(Vec<String>),
    /// Send `AUTHENTICATE <mechanism>` to begin SASL.
    Authenticate(SaslMechanism),
    /// Send the base64 credential payload for the challenge.
    SendAuthenticationPayload,
    /// Authentication failed; report it and send `CAP END` to proceed as a
    /// guest rather than stalling in registration.
    AuthenticationFailed {
        /// SASL failure numeric the server sent.
        numeric: u16,
    },
    /// Send `CAP END`; negotiation is finished.
    EndNegotiation,
}

impl CapabilityAction {
    /// Whether the caller must send `CAP END` after handling this action.
    #[cfg(test)]
    pub const fn ends_negotiation(&self) -> bool {
        matches!(
            self,
            Self::EndNegotiation | Self::AuthenticationFailed { .. }
        )
    }
}

fn parse_tokens(tokens: &str) -> Vec<(String, Option<String>)> {
    tokens
        .split(' ')
        .filter(|token| !token.is_empty())
        .map(|token| {
            token.split_once('=').map_or_else(
                || (token.to_owned(), None),
                |(name, value)| (name.to_owned(), Some(value.to_owned())),
            )
        })
        .collect()
}

/// Whether this gateway would request a capability the server advertised.
fn is_implemented(capability: &str) -> bool {
    let feature = FeatureId::of(capability);
    IMPLEMENTED_CAPABILITIES
        .iter()
        .any(|known| FeatureId::of(known) == feature)
}

/// Stable identity shared by a capability's draft and final spellings.
///
/// Exact advertised tokens remain available separately in the catalog.
#[derive(
    Clone, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(transparent)]
pub struct FeatureId(String);

impl FeatureId {
    /// Normalize one capability token to its feature identity.
    pub fn of(capability: &str) -> Self {
        Self(feature_id(capability).to_owned())
    }

    /// Borrow the identity.
    #[cfg(test)]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for FeatureId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl PartialEq<str> for FeatureId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for FeatureId {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

/// Normalize evolving capability spellings without discarding exact tokens.
pub fn feature_id(capability: &str) -> &str {
    match capability {
        "draft/chathistory" | "chathistory" => "chathistory",
        "draft/multiline" | "multiline" => "multiline",
        "draft/read-marker" | "read-marker" => "read-marker",
        "draft/message-redaction" | "message-redaction" => "message-redaction",
        "draft/account-registration" | "account-registration" => "account-registration",
        "draft/event-playback" | "event-playback" => "event-playback",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn parse(line: &str) -> WireMessage {
        WireMessage::parse(Bytes::copy_from_slice(line.as_bytes())).expect("parse")
    }

    #[test]
    fn a_feature_identity_equates_draft_and_stable_spellings() {
        assert_eq!(
            FeatureId::of("draft/chathistory"),
            FeatureId::of("chathistory")
        );
        assert_ne!(FeatureId::of("batch"), FeatureId::of("chathistory"));
        assert_eq!(FeatureId::of("vendor/example").as_str(), "vendor/example");
        assert!(FeatureId::of("draft/multiline") == "multiline");
    }

    #[test]
    fn draft_and_stable_names_share_a_feature_but_remain_exact_keys() {
        assert_eq!(feature_id("draft/chathistory"), "chathistory");
        assert_eq!(feature_id("chathistory"), "chathistory");
        assert_eq!(feature_id("vendor/example"), "vendor/example");
    }

    #[test]
    fn a_multiline_ls_continuation_requests_only_after_the_final_line() {
        let mut negotiator = CapabilityNegotiator::new();
        let action = negotiator.apply(&parse(
            ":server CAP * LS * :server-time message-tags vendor/unknown",
        ));
        assert_eq!(action, CapabilityAction::None);
        assert!(!negotiator.listing_complete());

        let action = negotiator.apply(&parse(":server CAP * LS :batch sasl=PLAIN"));
        assert!(negotiator.listing_complete());
        let CapabilityAction::Request(request) = action else {
            panic!("expected a request after the final LS line");
        };
        assert!(request.contains(&"server-time".to_owned()));
        assert!(request.contains(&"batch".to_owned()));
        assert!(!request.contains(&"vendor/unknown".to_owned()));
        assert_eq!(
            negotiator.entries()["vendor/unknown"].status,
            CapabilityStatus::ObservedUnnegotiated
        );
        assert_eq!(negotiator.entries()["sasl"].value.as_deref(), Some("PLAIN"));
    }

    #[test]
    fn ack_and_nak_move_only_the_named_capabilities() {
        let mut negotiator = CapabilityNegotiator::new();
        negotiator.apply(&parse(":server CAP * LS :server-time echo-message"));
        negotiator.apply(&parse(":server CAP * ACK :server-time"));
        negotiator.apply(&parse(":server CAP * NAK :echo-message"));

        assert!(negotiator.is_active("server-time"));
        assert!(!negotiator.is_active("echo-message"));
        assert_eq!(negotiator.negotiated(), ["server-time"]);
        assert_eq!(
            negotiator.entries()["echo-message"].mapping,
            MappingGrade::Unavailable
        );
    }

    #[test]
    fn cap_new_requests_and_cap_del_deactivates() {
        let mut negotiator = CapabilityNegotiator::new();
        negotiator.apply(&parse(":server CAP * LS :cap-notify"));
        negotiator.apply(&parse(":server CAP * ACK :cap-notify"));

        let action = negotiator.apply(&parse(":server CAP * NEW :away-notify"));
        assert_eq!(
            action,
            CapabilityAction::Request(vec!["away-notify".into()])
        );
        negotiator.apply(&parse(":server CAP * ACK :away-notify"));
        assert!(negotiator.is_active("away-notify"));

        negotiator.apply(&parse(":server CAP * DEL :away-notify"));
        assert!(!negotiator.is_active("away-notify"));
        assert_eq!(
            negotiator.entries()["away-notify"].status,
            CapabilityStatus::Removed
        );
    }

    #[test]
    fn negotiation_ends_only_after_every_request_is_answered() {
        let mut negotiator = CapabilityNegotiator::new();
        negotiator.apply(&parse(":server CAP * LS :server-time echo-message batch"));
        assert_eq!(negotiator.phase(), NegotiationPhase::AwaitingAck);
        assert_eq!(negotiator.outstanding().len(), 3);

        assert_eq!(
            negotiator.apply(&parse(":server CAP * ACK :server-time batch")),
            CapabilityAction::None
        );
        assert!(!negotiator.is_complete());

        let action = negotiator.apply(&parse(":server CAP * NAK :echo-message"));
        assert_eq!(action, CapabilityAction::EndNegotiation);
        assert!(action.ends_negotiation());
        assert!(negotiator.is_complete());
        assert!(negotiator.outstanding().is_empty());
    }

    #[test]
    fn a_later_cap_new_does_not_repeat_cap_end() {
        let mut negotiator = CapabilityNegotiator::new();
        negotiator.apply(&parse(":server CAP * LS :cap-notify"));
        assert_eq!(
            negotiator.apply(&parse(":server CAP * ACK :cap-notify")),
            CapabilityAction::EndNegotiation
        );

        negotiator.apply(&parse(":server CAP * NEW :away-notify"));
        assert_eq!(
            negotiator.apply(&parse(":server CAP * ACK :away-notify")),
            CapabilityAction::None
        );
        assert!(negotiator.is_active("away-notify"));
    }

    #[test]
    fn a_guest_connection_never_requests_sasl() {
        let mut negotiator = CapabilityNegotiator::new();
        let action = negotiator.apply(&parse(":server CAP * LS :sasl=PLAIN batch"));
        assert_eq!(action, CapabilityAction::Request(vec!["batch".into()]));
    }

    #[test]
    fn configured_sasl_completes_before_cap_end() {
        let mut negotiator =
            CapabilityNegotiator::with_sasl(SaslPolicy::Authenticate(SaslMechanism::Plain));
        let action = negotiator.apply(&parse(":server CAP * LS :sasl=PLAIN,EXTERNAL batch"));
        let CapabilityAction::Request(request) = action else {
            panic!("expected a request");
        };
        assert!(request.contains(&"sasl".to_owned()));

        assert_eq!(
            negotiator.apply(&parse(":server CAP * ACK :sasl batch")),
            CapabilityAction::Authenticate(SaslMechanism::Plain)
        );
        assert_eq!(negotiator.phase(), NegotiationPhase::Authenticating);
        assert_eq!(
            negotiator.apply(&parse(":server AUTHENTICATE +")),
            CapabilityAction::SendAuthenticationPayload
        );
        // CAP END must not be sent until SASL resolves.
        assert!(!negotiator.is_complete());
        assert_eq!(
            negotiator.apply(&parse(
                ":server 903 Kuebiko :SASL authentication successful"
            )),
            CapabilityAction::EndNegotiation
        );
        assert!(negotiator.is_complete());
    }

    #[test]
    fn a_failed_authentication_still_reaches_cap_end() {
        let mut negotiator =
            CapabilityNegotiator::with_sasl(SaslPolicy::Authenticate(SaslMechanism::Plain));
        negotiator.apply(&parse(":server CAP * LS :sasl"));
        negotiator.apply(&parse(":server CAP * ACK :sasl"));

        let action = negotiator.apply(&parse(":server 904 Kuebiko :SASL authentication failed"));
        assert_eq!(
            action,
            CapabilityAction::AuthenticationFailed { numeric: 904 }
        );
        assert!(action.ends_negotiation());
        assert!(negotiator.is_complete());
    }

    #[test]
    fn sasl_is_skipped_when_the_server_lacks_the_configured_mechanism() {
        let mut negotiator =
            CapabilityNegotiator::with_sasl(SaslPolicy::Authenticate(SaslMechanism::External));
        let action = negotiator.apply(&parse(":server CAP * LS :sasl=PLAIN batch"));
        assert_eq!(action, CapabilityAction::Request(vec!["batch".into()]));
        assert_eq!(
            negotiator.apply(&parse(":server CAP * ACK :batch")),
            CapabilityAction::EndNegotiation
        );
    }

    #[test]
    fn chathistory_is_requested_but_multiline_is_not() {
        let mut negotiator = CapabilityNegotiator::new();
        let action = negotiator.apply(&parse(
            ":server CAP * LS :draft/chathistory draft/multiline batch",
        ));
        let CapabilityAction::Request(request) = action else {
            panic!("expected a request");
        };
        assert!(request.contains(&"draft/chathistory".to_owned()));
        assert!(!request.contains(&"draft/multiline".to_owned()));
    }

    #[test]
    fn a_help_index_populates_the_catalog_with_discovered_commands() {
        let mut collector = HelpCollector::new();
        assert!(
            collector
                .apply(&parse(":server 704 Kuebiko INDEX :Help topics:"))
                .is_none()
        );
        assert!(
            collector
                .apply(&parse(":server 705 Kuebiko INDEX :NICK JOIN PART NPCHAT"))
                .is_none()
        );
        let response = collector
            .apply(&parse(":server 706 Kuebiko INDEX :End of /HELP"))
            .expect("index");

        let HelpResponse::Index { commands } = &response else {
            panic!("expected an index");
        };
        assert!(commands.contains(&"NPCHAT".to_owned()));
        assert!(commands.contains(&"JOIN".to_owned()));

        let mut catalog = CompatibilityCatalog::with_static_registry();
        catalog.record_help_response(&response);
        assert_eq!(
            catalog.commands["NPCHAT"].mapping,
            MappingGrade::Passthrough
        );
        assert_eq!(
            catalog.commands["JOIN"].reported_by,
            BTreeSet::from([
                CommandEvidence::StandardRegistry,
                CommandEvidence::HelpIndex
            ])
        );
        assert_eq!(
            catalog.commands["JOIN"].response_strategy,
            CollectorKind::Echo
        );
    }

    #[test]
    fn help_for_one_subject_is_cached_verbatim() {
        let mut collector = HelpCollector::new();
        collector.apply(&parse(":server 704 Kuebiko WHOIS :WHOIS <nick>"));
        collector.apply(&parse(":server 705 Kuebiko WHOIS :Returns information."));
        let response = collector
            .apply(&parse(":server 706 Kuebiko WHOIS :End of /HELP"))
            .expect("subject");

        assert_eq!(
            response,
            HelpResponse::Subject {
                subject: "WHOIS".into(),
                text: "WHOIS <nick>\nReturns information.".into(),
            }
        );

        let mut catalog = CompatibilityCatalog::with_static_registry();
        catalog.record_help_response(&response);
        assert!(catalog.commands["WHOIS"].help_available);
        assert!(catalog.help["WHOIS"].contains("Returns information."));
    }

    #[test]
    fn a_server_with_nothing_implemented_ends_negotiation() {
        let mut negotiator = CapabilityNegotiator::new();
        assert_eq!(
            negotiator.apply(&parse(":server CAP * LS :vendor/one vendor/two")),
            CapabilityAction::EndNegotiation
        );
    }

    #[test]
    fn a_draft_spelling_activates_its_stable_feature() {
        let mut negotiator = CapabilityNegotiator::new();
        negotiator.apply(&parse(":server CAP * LS :draft/chathistory"));
        negotiator.apply(&parse(":server CAP * ACK :draft/chathistory"));
        assert!(negotiator.is_active("chathistory"));
        assert!(negotiator.is_active("draft/chathistory"));
    }

    #[test]
    fn the_catalog_publishes_state_effects_and_discovered_commands() {
        let mut catalog = CompatibilityCatalog::with_static_registry();
        assert_eq!(
            catalog.commands["JOIN"].state_effects,
            StateEffects::Channel
        );
        assert_eq!(catalog.commands["JOIN"].phase, CommandPhase::Registered);
        assert_eq!(
            catalog.commands["JOIN"].response_strategy,
            CollectorKind::Echo
        );

        catalog.record_discovered_command("ergo-only");
        assert_eq!(
            catalog.commands["ERGO-ONLY"].mapping,
            MappingGrade::Passthrough
        );
        catalog.record_help("whois", "WHOIS <nick>");
        assert!(catalog.commands["WHOIS"].help_available);
        assert_eq!(catalog.help["WHOIS"], "WHOIS <nick>");
    }
}
