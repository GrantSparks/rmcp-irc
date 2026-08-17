//! Static command metadata augmented by runtime Ergo discovery.
//!
//! The registry is descriptive, not an allowlist. Collector selection must use
//! [`CommandSpec::strategy`] because negotiated capabilities determine which
//! completion signals are observable.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Registration phase in which a command is valid.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandPhase {
    /// Before guest registration.
    Registration,
    /// After RPL_WELCOME.
    Registered,
    /// Connection lifecycle operation.
    Lifecycle,
    /// Valid in every phase, including during registration.
    Any,
}

/// Documentary IRC privilege class; never a local authorization decision.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivilegeClass {
    /// Available to ordinary users when accepted by Ergo.
    Normal,
    /// Traditionally requires channel privileges.
    ChannelOperator,
    /// Traditionally requires IRC operator privileges.
    IrcOperator,
    /// Server or service extension.
    ServerSpecific,
}

/// Compatibility grade exposed through the protocol resource.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MappingGrade {
    /// Typed semantic projection is implemented.
    Native,
    /// Lossless wire representation only.
    Passthrough,
    /// A documented fallback loses some semantics.
    Degraded,
    /// Not advertised or deliberately unsupported.
    Unavailable,
    /// Advertised but intentionally not negotiated.
    ObservedUnnegotiated,
}

/// In-memory state potentially changed by a command.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StateEffects {
    /// No reducer mutation is expected.
    None,
    /// Own or peer identity state.
    Identity,
    /// Channel membership or state.
    Channel,
    /// Connection lifecycle.
    Connection,
    /// DCC state negotiated through CTCP.
    Dcc,
}

impl StateEffects {
    /// Stable protocol-resource spelling.
    #[cfg(test)]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Identity => "identity",
            Self::Channel => "channel",
            Self::Connection => "connection",
            Self::Dcc => "dcc",
        }
    }
}

/// Completion collector selected for an outbound command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseStrategy {
    /// Labeled ACK is normally sufficient.
    Ack,
    /// Exactly one semantic reply or error is expected.
    SingleReply,
    /// Collect numerics until one of these terminal numerics.
    NumericSequence {
        /// Terminal numeric replies.
        terminators: &'static [u16],
    },
    /// Collect a complete batch with this type when known.
    Batch {
        /// Expected batch type.
        expected_type: &'static str,
    },
    /// Complete on a matching state-changing echo.
    Echo {
        /// Echo commands that can confirm completion.
        commands: &'static [&'static str],
    },
    /// Registration, quit, or reconnect completion.
    ConnectionLifecycle,
    /// No reliable completion signal exists.
    Unconfirmed,
}

/// Stable identity of a collector, independent of its parameters.
///
/// The strategy itself carries terminators and echo commands, which are
/// implementation detail; published contracts name only the kind.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectorKind {
    /// Labeled ACK is normally sufficient.
    Ack,
    /// Exactly one semantic reply or error is expected.
    SingleReply,
    /// Numerics are collected until a terminal numeric.
    NumericSequence,
    /// A complete batch is collected.
    Batch,
    /// A matching server echo confirms completion.
    Echo,
    /// Registration, quit, or reconnect completion.
    ConnectionLifecycle,
    /// No reliable completion signal exists.
    Unconfirmed,
}

impl ResponseStrategy {
    /// Collector identity without its parameters.
    pub const fn kind(self) -> CollectorKind {
        match self {
            Self::Ack => CollectorKind::Ack,
            Self::SingleReply => CollectorKind::SingleReply,
            Self::NumericSequence { .. } => CollectorKind::NumericSequence,
            Self::Batch { .. } => CollectorKind::Batch,
            Self::Echo { .. } => CollectorKind::Echo,
            Self::ConnectionLifecycle => CollectorKind::ConnectionLifecycle,
            Self::Unconfirmed => CollectorKind::Unconfirmed,
        }
    }
}

/// Static knowledge for one IRC command.
#[derive(Clone, Copy, Debug)]
pub struct CommandSpec {
    /// Uppercase command name.
    pub name: &'static str,
    /// Registration phase.
    pub phase: CommandPhase,
    /// Capabilities required for exact semantics.
    pub required_capabilities: &'static [&'static str],
    /// Completion collector assuming its capabilities were negotiated.
    pub response: ResponseStrategy,
    /// Collector to use when those capabilities were not negotiated.
    pub degraded_response: ResponseStrategy,
    /// Reducer impact.
    pub state_effects: StateEffects,
    /// Documentary server-side privilege class.
    pub privilege: PrivilegeClass,
    /// Initial compatibility grade.
    pub mapping: MappingGrade,
}

impl CommandSpec {
    /// Collector for this command given the negotiated capability set.
    ///
    /// A command whose completion depends on a capability degrades to its
    /// documented fallback rather than waiting for a signal that cannot
    /// arrive; the caller reports that degradation in the tool result.
    pub fn strategy(&self, negotiated: &dyn CapabilityLookup) -> ResponseStrategy {
        if self
            .required_capabilities
            .iter()
            .all(|capability| negotiated.is_negotiated(capability))
        {
            self.response
        } else {
            self.degraded_response
        }
    }

    /// Capabilities this command needs that were not negotiated.
    #[cfg(test)]
    pub fn missing_capabilities(&self, negotiated: &dyn CapabilityLookup) -> Vec<&'static str> {
        self.required_capabilities
            .iter()
            .filter(|capability| !negotiated.is_negotiated(capability))
            .copied()
            .collect()
    }
}

/// Negotiated-capability lookup used by collector selection.
pub trait CapabilityLookup {
    /// Whether this exact capability, or its feature equivalent, is active.
    fn is_negotiated(&self, capability: &str) -> bool;
}

impl CapabilityLookup for [&str] {
    fn is_negotiated(&self, capability: &str) -> bool {
        self.contains(&capability)
    }
}

impl<const N: usize> CapabilityLookup for [&str; N] {
    fn is_negotiated(&self, capability: &str) -> bool {
        self.contains(&capability)
    }
}

const WHOIS_END: &[u16] = &[318];
const WHOWAS_END: &[u16] = &[369];
const WHO_END: &[u16] = &[315];
const NAMES_END: &[u16] = &[366];
const LIST_END: &[u16] = &[323];
const MOTD_END: &[u16] = &[376, 422];
const LINKS_END: &[u16] = &[365];
const STATS_END: &[u16] = &[219];
const HELP_END: &[u16] = &[706, 705];
const INFO_END: &[u16] = &[374];
const MODE_END: &[u16] = &[324, 221, 368, 349, 347, 344, 346];
const LUSERS_END: &[u16] = &[255];
const ADMIN_END: &[u16] = &[259];
const MONITOR_END: &[u16] = &[733, 734];

const ECHO_MESSAGE: &[&str] = &["echo-message"];
const CHATHISTORY: &[&str] = &["draft/chathistory"];
const MONITOR_CAPS: &[&str] = &[];

/// Initial static registry. Runtime HELP INDEX augments but never replaces it.
pub static COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        name: "CAP",
        phase: CommandPhase::Any,
        required_capabilities: &[],
        response: ResponseStrategy::SingleReply,
        degraded_response: ResponseStrategy::SingleReply,
        state_effects: StateEffects::Connection,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "AUTHENTICATE",
        phase: CommandPhase::Registration,
        required_capabilities: &["sasl"],
        response: ResponseStrategy::SingleReply,
        degraded_response: ResponseStrategy::SingleReply,
        state_effects: StateEffects::Identity,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "PASS",
        phase: CommandPhase::Registration,
        required_capabilities: &[],
        response: ResponseStrategy::Unconfirmed,
        degraded_response: ResponseStrategy::Unconfirmed,
        state_effects: StateEffects::Connection,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "NICK",
        phase: CommandPhase::Any,
        required_capabilities: &[],
        response: ResponseStrategy::Echo {
            commands: &["NICK"],
        },
        degraded_response: ResponseStrategy::Echo {
            commands: &["NICK"],
        },
        state_effects: StateEffects::Identity,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "USER",
        phase: CommandPhase::Registration,
        required_capabilities: &[],
        response: ResponseStrategy::ConnectionLifecycle,
        degraded_response: ResponseStrategy::ConnectionLifecycle,
        state_effects: StateEffects::Identity,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "PING",
        phase: CommandPhase::Any,
        required_capabilities: &[],
        response: ResponseStrategy::SingleReply,
        degraded_response: ResponseStrategy::SingleReply,
        state_effects: StateEffects::Connection,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "PONG",
        phase: CommandPhase::Any,
        required_capabilities: &[],
        response: ResponseStrategy::Unconfirmed,
        degraded_response: ResponseStrategy::Unconfirmed,
        state_effects: StateEffects::None,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "JOIN",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::Echo {
            commands: &["JOIN"],
        },
        degraded_response: ResponseStrategy::Echo {
            commands: &["JOIN"],
        },
        state_effects: StateEffects::Channel,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "PART",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::Echo {
            commands: &["PART"],
        },
        degraded_response: ResponseStrategy::Echo {
            commands: &["PART"],
        },
        state_effects: StateEffects::Channel,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "TOPIC",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::NumericSequence {
            terminators: &[331, 332, 333],
        },
        degraded_response: ResponseStrategy::NumericSequence {
            terminators: &[331, 332, 333],
        },
        state_effects: StateEffects::Channel,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "KICK",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::Echo {
            commands: &["KICK"],
        },
        degraded_response: ResponseStrategy::Echo {
            commands: &["KICK"],
        },
        state_effects: StateEffects::Channel,
        privilege: PrivilegeClass::ChannelOperator,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "INVITE",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::SingleReply,
        degraded_response: ResponseStrategy::SingleReply,
        state_effects: StateEffects::Channel,
        privilege: PrivilegeClass::ChannelOperator,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "AWAY",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::NumericSequence {
            terminators: &[305, 306],
        },
        degraded_response: ResponseStrategy::NumericSequence {
            terminators: &[305, 306],
        },
        state_effects: StateEffects::Identity,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "PRIVMSG",
        phase: CommandPhase::Registered,
        required_capabilities: ECHO_MESSAGE,
        response: ResponseStrategy::Echo {
            commands: &["PRIVMSG"],
        },
        // Without echo-message, no server signal confirms delivery.
        degraded_response: ResponseStrategy::Unconfirmed,
        state_effects: StateEffects::None,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "NOTICE",
        phase: CommandPhase::Registered,
        required_capabilities: ECHO_MESSAGE,
        response: ResponseStrategy::Echo {
            commands: &["NOTICE"],
        },
        degraded_response: ResponseStrategy::Unconfirmed,
        state_effects: StateEffects::None,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "TAGMSG",
        phase: CommandPhase::Registered,
        required_capabilities: &["message-tags"],
        response: ResponseStrategy::Echo {
            commands: &["TAGMSG"],
        },
        degraded_response: ResponseStrategy::Unconfirmed,
        state_effects: StateEffects::None,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "WHOIS",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::NumericSequence {
            terminators: WHOIS_END,
        },
        degraded_response: ResponseStrategy::NumericSequence {
            terminators: WHOIS_END,
        },
        state_effects: StateEffects::None,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "WHOWAS",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::NumericSequence {
            terminators: WHOWAS_END,
        },
        degraded_response: ResponseStrategy::NumericSequence {
            terminators: WHOWAS_END,
        },
        state_effects: StateEffects::None,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "WHO",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::NumericSequence {
            terminators: WHO_END,
        },
        degraded_response: ResponseStrategy::NumericSequence {
            terminators: WHO_END,
        },
        state_effects: StateEffects::None,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "NAMES",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::NumericSequence {
            terminators: NAMES_END,
        },
        degraded_response: ResponseStrategy::NumericSequence {
            terminators: NAMES_END,
        },
        state_effects: StateEffects::Channel,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "LIST",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::NumericSequence {
            terminators: LIST_END,
        },
        degraded_response: ResponseStrategy::NumericSequence {
            terminators: LIST_END,
        },
        state_effects: StateEffects::None,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "MOTD",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::NumericSequence {
            terminators: MOTD_END,
        },
        degraded_response: ResponseStrategy::NumericSequence {
            terminators: MOTD_END,
        },
        state_effects: StateEffects::Connection,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "LUSERS",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::NumericSequence {
            terminators: LUSERS_END,
        },
        degraded_response: ResponseStrategy::NumericSequence {
            terminators: LUSERS_END,
        },
        state_effects: StateEffects::None,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "ADMIN",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::NumericSequence {
            terminators: ADMIN_END,
        },
        degraded_response: ResponseStrategy::NumericSequence {
            terminators: ADMIN_END,
        },
        state_effects: StateEffects::None,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "LINKS",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::NumericSequence {
            terminators: LINKS_END,
        },
        degraded_response: ResponseStrategy::NumericSequence {
            terminators: LINKS_END,
        },
        state_effects: StateEffects::None,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "STATS",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::NumericSequence {
            terminators: STATS_END,
        },
        degraded_response: ResponseStrategy::NumericSequence {
            terminators: STATS_END,
        },
        state_effects: StateEffects::None,
        privilege: PrivilegeClass::IrcOperator,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "HELP",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::NumericSequence {
            terminators: HELP_END,
        },
        degraded_response: ResponseStrategy::NumericSequence {
            terminators: HELP_END,
        },
        state_effects: StateEffects::None,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "INFO",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::NumericSequence {
            terminators: INFO_END,
        },
        degraded_response: ResponseStrategy::NumericSequence {
            terminators: INFO_END,
        },
        state_effects: StateEffects::None,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "MODE",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        // Covers user/channel replies and list terminators.
        response: ResponseStrategy::NumericSequence {
            terminators: MODE_END,
        },
        degraded_response: ResponseStrategy::NumericSequence {
            terminators: MODE_END,
        },
        state_effects: StateEffects::Channel,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "MONITOR",
        phase: CommandPhase::Registered,
        required_capabilities: MONITOR_CAPS,
        response: ResponseStrategy::NumericSequence {
            terminators: MONITOR_END,
        },
        degraded_response: ResponseStrategy::Unconfirmed,
        state_effects: StateEffects::Identity,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Passthrough,
    },
    CommandSpec {
        name: "CHATHISTORY",
        phase: CommandPhase::Registered,
        required_capabilities: CHATHISTORY,
        response: ResponseStrategy::Batch {
            expected_type: "chathistory",
        },
        degraded_response: ResponseStrategy::Unconfirmed,
        state_effects: StateEffects::None,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
    CommandSpec {
        name: "SETNAME",
        phase: CommandPhase::Registered,
        required_capabilities: &["setname"],
        response: ResponseStrategy::Echo {
            commands: &["SETNAME"],
        },
        degraded_response: ResponseStrategy::Unconfirmed,
        state_effects: StateEffects::Identity,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Passthrough,
    },
    CommandSpec {
        name: "OPER",
        phase: CommandPhase::Registered,
        required_capabilities: &[],
        response: ResponseStrategy::SingleReply,
        degraded_response: ResponseStrategy::SingleReply,
        state_effects: StateEffects::Identity,
        privilege: PrivilegeClass::IrcOperator,
        mapping: MappingGrade::Passthrough,
    },
    CommandSpec {
        name: "QUIT",
        phase: CommandPhase::Lifecycle,
        required_capabilities: &[],
        response: ResponseStrategy::ConnectionLifecycle,
        degraded_response: ResponseStrategy::ConnectionLifecycle,
        state_effects: StateEffects::Connection,
        privilege: PrivilegeClass::Normal,
        mapping: MappingGrade::Native,
    },
];

/// Find static knowledge without imposing a command allowlist.
pub fn spec_for(command: &str) -> Option<&'static CommandSpec> {
    COMMANDS
        .iter()
        .find(|spec| spec.name.eq_ignore_ascii_case(command))
}

/// Collector for a command that may not be in the static registry.
///
/// An unknown command has no documented completion signal, so it collects
/// under `labeled-response` when that is negotiated and otherwise reports a
/// successful write as unconfirmed.
pub fn strategy_for(command: &str, negotiated: &dyn CapabilityLookup) -> ResponseStrategy {
    spec_for(command).map_or_else(
        || {
            if negotiated.is_negotiated("labeled-response") {
                ResponseStrategy::Ack
            } else {
                ResponseStrategy::Unconfirmed
            }
        },
        |spec| spec.strategy(negotiated),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_is_case_insensitive_and_unknown_commands_remain_unknown() {
        assert_eq!(spec_for("whois").map(|spec| spec.name), Some("WHOIS"));
        assert!(spec_for("FUTURE").is_none());
    }

    #[test]
    fn messages_degrade_to_unconfirmed_without_echo_message() {
        let spec = spec_for("PRIVMSG").expect("PRIVMSG");
        assert_eq!(
            spec.strategy(&["echo-message"]),
            ResponseStrategy::Echo {
                commands: &["PRIVMSG"]
            }
        );
        assert_eq!(spec.strategy(&[]), ResponseStrategy::Unconfirmed);
        assert_eq!(spec.missing_capabilities(&[]), ["echo-message"]);
    }

    #[test]
    fn mode_queries_have_a_reachable_terminator() {
        let ResponseStrategy::NumericSequence { terminators } =
            spec_for("MODE").expect("MODE").response
        else {
            panic!("MODE must collect numerics");
        };
        for numeric in [324_u16, 221, 368, 349, 347] {
            assert!(terminators.contains(&numeric), "missing {numeric}");
        }
    }

    #[test]
    fn registration_and_keepalive_commands_are_known() {
        for command in ["PING", "PONG", "PASS", "AUTHENTICATE", "CAP", "NICK"] {
            assert!(spec_for(command).is_some(), "missing {command}");
        }
        assert_eq!(spec_for("PING").expect("PING").phase, CommandPhase::Any);
    }

    #[test]
    fn unknown_commands_collect_only_when_labels_are_available() {
        assert_eq!(
            strategy_for("FUTURE", &["labeled-response"]),
            ResponseStrategy::Ack
        );
        assert_eq!(strategy_for("FUTURE", &[]), ResponseStrategy::Unconfirmed);
    }

    #[test]
    fn every_entry_reports_its_state_effects_for_the_resource() {
        assert_eq!(
            spec_for("JOIN").expect("JOIN").state_effects.as_str(),
            "channel"
        );
        assert_eq!(
            spec_for("PRIVMSG").expect("PRIVMSG").state_effects,
            StateEffects::None
        );
    }
}
