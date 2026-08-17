//! Correlation of concurrent MCP requests with their IRC replies.
//!
//! Callers register before writing, attribute and ingest inbound messages, and
//! advance deadlines through [`Correlator::tick`].

use std::collections::{BTreeMap, HashMap, VecDeque};

#[cfg(test)]
use std::collections::HashSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    agent::{AgentId, journal::EventCursor},
    irc::wire::{MAX_LABEL_BYTES, WireMessage},
};

use super::{
    batch::{ActiveBatch, BatchTracker},
    capabilities::FeatureId,
    commands::ResponseStrategy,
    isupport::CaseMapping,
    registration::Nickname,
    semantic::SemanticProjection,
};

/// Something the caller must be told about how a command was handled.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CommandWarning {
    /// A capability the exact semantics needed was not negotiated.
    MissingCapability {
        /// Capability token that was absent.
        capability: String,
        /// What the gateway did instead.
        fallback: Fallback,
    },
    /// The server sent a standard reply about this command.
    StandardReply {
        /// FAIL, WARN, or NOTE.
        severity: StandardReplySeverity,
        /// Command the reply refers to.
        command: String,
        /// Machine-readable code.
        code: String,
        /// Human-readable text.
        text: String,
    },
}

/// Machine-readable behavior applied when a capability is unavailable.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Fallback {
    /// A successful write is reported without server confirmation.
    ReportedUnconfirmed,
    /// A synthetic sent event stands in for a missing server echo.
    SyntheticSentEvent,
    /// Collectors are serialized by response class instead of labeled.
    SerializedByClass,
    /// Receipt time is used, with explicit local provenance.
    LocalReceiptTime,
    /// A gateway identifier is used instead of a server-assigned one.
    GatewayIdentifier,
    /// The operation is reported as unavailable rather than emulated.
    ReportedUnavailable,
}

/// Severity of an IRCv3 standard reply.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StandardReplySeverity {
    /// The command failed.
    Fail,
    /// The command succeeded with a caveat.
    Warn,
    /// Informational only.
    Note,
}

impl StandardReplySeverity {
    /// Classify a command name as a standard reply, if it is one.
    fn parse(command: &str) -> Option<Self> {
        match command.to_ascii_uppercase().as_str() {
            "FAIL" => Some(Self::Fail),
            "WARN" => Some(Self::Warn),
            "NOTE" => Some(Self::Note),
            _ => None,
        }
    }
}

/// Gateway-generated identifier for one outbound operation.
#[derive(
    Clone, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(transparent)]
pub struct CommandId(String);

impl CommandId {
    /// Create a fresh command identifier.
    pub fn new() -> Self {
        Self(format!("cmd_{}", Uuid::new_v4()))
    }

    /// Borrow the identifier text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Correlation label derived from this identifier.
    ///
    /// Labels are bounded by the IRCv3 limit the encoder enforces, so a long
    /// identifier is truncated rather than producing an unencodable line.
    pub fn label(&self) -> String {
        let mut label = self.0.clone();
        label.truncate(MAX_LABEL_BYTES);
        label
    }
}

impl Default for CommandId {
    fn default() -> Self {
        Self::new()
    }
}

/// Completion classification returned by every command tool.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandOutcome {
    /// Definitive success.
    Completed,
    /// Written successfully without an available acknowledgment.
    SentUnconfirmed,
    /// Ergo definitively rejected the command.
    Rejected,
    /// The selected collector reached its deadline.
    TimedOut,
    /// The command never reached the socket.
    NotWritten,
    /// Written, but completion became unknowable.
    Indeterminate,
}

/// Common structured result envelope.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct CommandResult {
    /// Gateway command identifier.
    pub command_id: CommandId,
    /// Owning guest agent.
    pub agent_id: AgentId,
    /// Case-preserved outbound command.
    pub command: String,
    /// Completion classification.
    pub outcome: CommandOutcome,
    /// Whether bytes reached the socket writer.
    pub written: bool,
    /// Whether a definitive server acknowledgment was observed.
    pub acknowledged: bool,
    /// Whether an identical retry is safe by default.
    pub retriable: bool,
    /// Bridge-generated IRC label when negotiated.
    pub label: Option<String>,
    /// Complete collected wire replies.
    pub replies: Vec<WireMessage>,
    /// Typed projection of collected replies, or `None` when none were collected.
    pub semantic_result: Option<Vec<SemanticProjection>>,
    /// Visible degraded behavior or standard replies.
    pub warnings: Vec<CommandWarning>,
    /// First related event in the agent journal.
    pub first_event_cursor: Option<EventCursor>,
}

/// Actor-owned collector metadata for one in-flight command.
#[derive(Clone, Debug)]
pub struct PendingCommand {
    /// Gateway command identifier.
    pub command_id: CommandId,
    /// Owning guest agent.
    pub agent_id: AgentId,
    /// Case-preserved outbound command.
    pub command: String,
    /// IRC label when supported.
    pub label: Option<String>,
    /// Selected collector strategy.
    pub response: ResponseStrategy,
    /// Whether the socket writer accepted the line.
    pub written: bool,
    /// Milliseconds after which the collector gives up.
    pub deadline_ms: u64,
    /// Degradations recorded before or during collection.
    pub warnings: Vec<CommandWarning>,
    /// Replies accumulated so far.
    pub replies: Vec<WireMessage>,
}

impl PendingCommand {
    /// Response class used to serialize collectors without labels.
    fn class(&self) -> ResponseClass {
        ResponseClass::of(&self.command, self.response)
    }
}

/// Coarse grouping of collectors whose reply streams could be confused.
///
/// Without labels, even nominally different numeric families can receive the
/// same generic error numeric. Reply-bearing collectors therefore share one
/// conservative class; only operations that collect nothing remain freely
/// concurrent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResponseClass {
    /// Any operation awaiting a server reply, batch, or echo.
    Correlated,
    /// Nothing to collect.
    None,
}

impl ResponseClass {
    fn of(_command: &str, strategy: ResponseStrategy) -> Self {
        match strategy {
            ResponseStrategy::Ack | ResponseStrategy::Unconfirmed => Self::None,
            ResponseStrategy::SingleReply
            | ResponseStrategy::NumericSequence { .. }
            | ResponseStrategy::Batch { .. }
            | ResponseStrategy::Echo { .. }
            | ResponseStrategy::ConnectionLifecycle => Self::Correlated,
        }
    }
}

/// One collector reaching a terminal state.
#[derive(Clone, Debug)]
pub struct Completion {
    /// Which command completed.
    pub command_id: CommandId,
    /// Owning guest agent.
    pub agent_id: AgentId,
    /// Case-preserved outbound command.
    pub command: String,
    /// How it completed.
    pub outcome: CommandOutcome,
    /// Whether a definitive acknowledgment was observed.
    pub acknowledged: bool,
    /// Label used, when one was attached.
    pub label: Option<String>,
    /// Every reply attributed to this command, in arrival order.
    pub replies: Vec<WireMessage>,
    /// Degradation notes and standard-reply text.
    pub warnings: Vec<CommandWarning>,
}

impl Completion {
    /// Whether an identical retry is safe by default.
    ///
    /// Only a command that never reached the socket is retriable without an
    /// explicit caller decision; anything else may already have taken effect.
    pub const fn retriable(&self) -> bool {
        matches!(self.outcome, CommandOutcome::NotWritten)
    }
}

/// Why a command could not be registered.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum CorrelationError {
    /// The pending-command bound was reached.
    #[error("pending command limit reached: {0}")]
    PendingLimit(usize),
    /// The identifier is already registered.
    #[error("command is already pending: {0}")]
    Duplicate(String),
}

/// Bounds applied to the correlation engine.
#[derive(Clone, Copy, Debug)]
pub struct CorrelatorLimits {
    /// Largest number of simultaneously pending commands.
    pub max_pending: usize,
    /// Largest number of simultaneously active batches.
    pub max_active_batches: usize,
    /// Largest number of replies retained per command.
    pub max_replies_per_command: usize,
}

impl Default for CorrelatorLimits {
    fn default() -> Self {
        Self {
            max_pending: 256,
            max_active_batches: 32,
            max_replies_per_command: 4_096,
        }
    }
}

/// Pure collector engine for one connection.
#[derive(Debug)]
pub struct Correlator {
    limits: CorrelatorLimits,
    pending: BTreeMap<CommandId, PendingCommand>,
    order: VecDeque<CommandId>,
    by_label: HashMap<String, CommandId>,
    by_batch: HashMap<String, CommandId>,
    batches: BatchTracker,
    nickname: Option<Nickname>,
    case_mapping: CaseMapping,
}

/// Reply ownership resolved before an inbound line is journaled.
///
/// Collection and event metadata use this shared attribution result.
#[derive(Clone, Debug)]
pub(crate) struct MessageAttribution {
    command_id: Option<CommandId>,
    command: Option<String>,
    label: Option<String>,
    batch_lineage: Vec<ActiveBatch>,
    closing_batch_kind: Option<String>,
}

impl MessageAttribution {
    /// Command that owns the inbound line, when it is a correlated reply.
    pub(crate) const fn command_id(&self) -> Option<&CommandId> {
        self.command_id.as_ref()
    }

    /// Label of the owning command, inherited by lines inside its batch.
    pub(crate) fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    /// Command name whose collector owns this line.
    pub(crate) fn command(&self) -> Option<&str> {
        self.command.as_deref()
    }

    /// Whether the line belongs to a batch of `kind`, directly or by nesting.
    pub(crate) fn has_batch_kind(&self, kind: &str) -> bool {
        self.batch_lineage
            .iter()
            .any(|batch| batch.kind.eq_ignore_ascii_case(kind))
    }

    /// Direct batch kind for an open/close line or direct `batch` tag.
    pub(crate) fn direct_batch_kind(&self) -> Option<&str> {
        self.batch_lineage.first().map(|batch| batch.kind.as_str())
    }

    /// Batch identifiers from the direct batch through its active ancestors.
    pub(crate) fn batch_ids(&self) -> impl Iterator<Item = &str> {
        self.batch_lineage.iter().map(|batch| batch.id.as_str())
    }
}

impl Correlator {
    /// Create an engine with explicit bounds.
    pub fn new(limits: CorrelatorLimits) -> Self {
        Self {
            limits,
            pending: BTreeMap::new(),
            order: VecDeque::new(),
            by_label: HashMap::new(),
            by_batch: HashMap::new(),
            batches: BatchTracker::new(limits.max_active_batches),
            nickname: None,
            case_mapping: CaseMapping::default(),
        }
    }

    /// Track the nickname whose echoes confirm our own state changes.
    pub fn set_nickname(&mut self, nickname: Nickname) {
        self.nickname = Some(nickname);
    }

    /// Use the server's advertised case mapping when comparing nicknames.
    pub fn set_case_mapping(&mut self, mapping: CaseMapping) {
        self.case_mapping = mapping;
    }

    /// Number of collectors currently open.
    #[cfg(test)]
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Clear connection-local routing after every pending command has been
    /// completed or detached on socket loss.
    pub fn reset_connection(&mut self) {
        debug_assert!(self.pending.is_empty());
        self.order.clear();
        self.by_label.clear();
        self.by_batch.clear();
        self.batches.clear();
    }

    /// Whether a command may be written now without ambiguity.
    ///
    /// With `labeled-response` every reply carries its label, so commands are
    /// never ambiguous. Without it, two open collectors of the same response
    /// class would compete for the same replies, so the caller must wait.
    pub fn admits(&self, command: &str, strategy: ResponseStrategy, labeled: bool) -> bool {
        if labeled {
            return self.pending.len() < self.limits.max_pending;
        }
        let class = ResponseClass::of(command, strategy);
        if class == ResponseClass::None {
            return self.pending.len() < self.limits.max_pending;
        }
        self.pending.len() < self.limits.max_pending
            && !self
                .pending
                .values()
                .any(|entry| entry.label.is_none() && entry.class() == class)
    }

    /// Register a collector before the command is written.
    pub fn register(&mut self, pending: PendingCommand) -> Result<(), CorrelationError> {
        if self.pending.len() >= self.limits.max_pending {
            return Err(CorrelationError::PendingLimit(self.limits.max_pending));
        }
        if self.pending.contains_key(&pending.command_id) {
            return Err(CorrelationError::Duplicate(
                pending.command_id.as_str().to_owned(),
            ));
        }
        if let Some(label) = &pending.label {
            self.by_label
                .insert(label.clone(), pending.command_id.clone());
        }
        self.order.push_back(pending.command_id.clone());
        self.pending.insert(pending.command_id.clone(), pending);
        Ok(())
    }

    /// Record that the writer accepted or refused the encoded line.
    ///
    /// A command that never reached the socket completes immediately as
    /// `not_written`, and one whose collector cannot observe anything
    /// completes as `sent_unconfirmed`.
    pub fn record_write(&mut self, command_id: &CommandId, written: bool) -> Option<Completion> {
        let entry = self.pending.get_mut(command_id)?;
        entry.written = written;
        if !written {
            return self.finish(command_id, CommandOutcome::NotWritten, false);
        }
        if entry.response == ResponseStrategy::Unconfirmed {
            return self.finish(command_id, CommandOutcome::SentUnconfirmed, false);
        }
        None
    }

    /// Stop waiting without reversing an already-written command.
    pub fn cancel(&mut self, command_id: &CommandId) -> Option<Completion> {
        let written = self.pending.get(command_id)?.written;
        let outcome = if written {
            CommandOutcome::Indeterminate
        } else {
            CommandOutcome::NotWritten
        };
        self.finish(command_id, outcome, false)
    }

    /// Complete every collector whose deadline has passed.
    pub fn tick(&mut self, now_ms: u64) -> Vec<Completion> {
        let expired: Vec<CommandId> = self
            .pending
            .values()
            .filter(|entry| now_ms >= entry.deadline_ms)
            .map(|entry| entry.command_id.clone())
            .collect();
        expired
            .iter()
            .filter_map(|command_id| self.finish(command_id, CommandOutcome::TimedOut, false))
            .collect()
    }

    /// Attribute one inbound message and report any completions it caused.
    ///
    /// A collected reply is never removed from wire-level event delivery; the
    /// caller journals every message regardless of what this returns.
    #[cfg(test)]
    pub fn ingest(&mut self, message: &WireMessage) -> Vec<Completion> {
        let attribution = self.attribute(message);
        self.ingest_attributed(message, attribution)
    }

    /// Resolve a reply owner before the actor journals the line.
    pub(crate) fn attribute(&mut self, message: &WireMessage) -> MessageAttribution {
        let closing_batch = message
            .command
            .eq_ignore_ascii_case("BATCH")
            .then(|| message.params.first())
            .flatten()
            .filter(|reference| reference.starts_with('-'))
            .and_then(|reference| reference.get(1..))
            .map(|id| self.batches.lineage(id));
        let closing_batch_kind = self.track_batch(message);
        let command_id = self.route(message);
        let entry = command_id
            .as_ref()
            .and_then(|command_id| self.pending.get(command_id));
        let command = entry.map(|entry| entry.command.clone());
        let label = entry.and_then(|entry| entry.label.clone());
        let batch_lineage = closing_batch.unwrap_or_else(|| self.batch_lineage(message));
        MessageAttribution {
            command_id,
            command,
            label,
            batch_lineage,
            closing_batch_kind,
        }
    }

    /// Batch directly referenced by a BATCH line or batch tag, plus parents.
    fn batch_lineage(&self, message: &WireMessage) -> Vec<ActiveBatch> {
        let id = if message.command.eq_ignore_ascii_case("BATCH") {
            message
                .params
                .first()
                .and_then(|reference| reference.get(1..))
        } else {
            message.tag_value("batch")
        };
        id.map_or_else(Vec::new, |id| self.batches.lineage(id))
    }

    /// Collect a line using ownership already resolved for event journaling.
    pub(crate) fn ingest_attributed(
        &mut self,
        message: &WireMessage,
        attribution: MessageAttribution,
    ) -> Vec<Completion> {
        let Some(command_id) = attribution.command_id else {
            return Vec::new();
        };
        let Some(entry) = self.pending.get_mut(&command_id) else {
            return Vec::new();
        };
        if entry.replies.len() < self.limits.max_replies_per_command {
            entry.replies.push(message.clone());
        }

        let strategy = entry.response;
        let outcome = classify(
            message,
            strategy,
            &entry.command,
            attribution.closing_batch_kind.as_deref(),
        );
        if let Some(warning) = standard_reply(message) {
            entry.warnings.push(warning);
        }
        match outcome {
            Some(outcome) => self
                .finish(&command_id, outcome, outcome == CommandOutcome::Completed)
                .into_iter()
                .collect(),
            None => Vec::new(),
        }
    }

    /// Maintain the active batch table and bind batches to their commands.
    fn track_batch(&mut self, message: &WireMessage) -> Option<String> {
        if !message.command.eq_ignore_ascii_case("BATCH") {
            return None;
        }
        let reference = message.params.first()?;
        let id = reference.get(1..)?;
        if reference.starts_with('+') {
            let kind = message
                .params
                .get(1)
                .cloned()
                .or_else(|| message.trailing.clone())
                .unwrap_or_default();
            let parent = message.tag_value("batch").map(str::to_owned);
            let owner = message
                .tag_value("label")
                .and_then(|label| self.by_label.get(label))
                .cloned()
                .or_else(|| {
                    parent
                        .as_ref()
                        .and_then(|parent| self.by_batch.get(parent))
                        .cloned()
                })
                .or_else(|| {
                    self.order.iter().find_map(|command_id| {
                        let entry = self.pending.get(command_id)?;
                        matches!(
                            entry.response,
                            ResponseStrategy::Batch { expected_type }
                                if entry.label.is_none()
                                    && FeatureId::of(expected_type) == FeatureId::of(&kind)
                        )
                        .then(|| command_id.clone())
                    })
                });
            if self.batches.start(id, kind, parent).is_ok()
                && let Some(command_id) = owner
            {
                self.by_batch.insert(id.to_owned(), command_id);
            }
        } else if reference.starts_with('-') {
            let kind = self.batches.get(id).map(|batch| batch.kind.clone());
            return self.batches.end(id).ok().and(kind);
        }
        None
    }

    /// Decide which pending command, if any, owns this message.
    fn route(&self, message: &WireMessage) -> Option<CommandId> {
        if let Some(command_id) = message
            .tag_value("label")
            .and_then(|label| self.by_label.get(label))
        {
            return Some(command_id.clone());
        }
        if let Some(command_id) = message
            .tag_value("batch")
            .and_then(|batch| self.by_batch.get(batch))
        {
            return Some(command_id.clone());
        }
        // A batch's own open and close lines carry the identifier as a
        // parameter rather than a tag, so the close still reaches the command
        // that opened the batch.
        if message.command.eq_ignore_ascii_case("BATCH")
            && let Some(command_id) = message
                .params
                .first()
                .and_then(|reference| reference.get(1..))
                .and_then(|id| self.by_batch.get(id))
        {
            return Some(command_id.clone());
        }
        // Without a label, the oldest unlabeled collector that can explain
        // this message owns it. Same-class collectors were serialized at
        // admission, so at most one can match.
        self.order.iter().find_map(|command_id| {
            let entry = self.pending.get(command_id)?;
            (entry.label.is_none() && self.explains(entry, message)).then(|| command_id.clone())
        })
    }

    fn explains(&self, entry: &PendingCommand, message: &WireMessage) -> bool {
        match entry.response {
            ResponseStrategy::NumericSequence { .. } | ResponseStrategy::SingleReply => {
                message.numeric().is_some()
            }
            ResponseStrategy::Echo { commands } => {
                commands
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(&message.command))
                    && self.is_own_echo(message)
                    || message.numeric().is_some_and(is_error_numeric)
            }
            ResponseStrategy::ConnectionLifecycle => {
                message.numeric().is_some() || message.command.eq_ignore_ascii_case("ERROR")
            }
            ResponseStrategy::Batch { .. } => message.tag_value("batch").is_some(),
            ResponseStrategy::Ack | ResponseStrategy::Unconfirmed => false,
        }
    }

    /// Whether a message is this connection's own echo rather than a peer's.
    fn is_own_echo(&self, message: &WireMessage) -> bool {
        message.prefix.as_ref().is_none_or(|prefix| {
            self.nickname
                .as_ref()
                .is_none_or(|nickname| self.case_mapping.same(&prefix.name, nickname.as_str()))
        })
    }

    fn finish(
        &mut self,
        command_id: &CommandId,
        outcome: CommandOutcome,
        acknowledged: bool,
    ) -> Option<Completion> {
        let entry = self.pending.remove(command_id)?;
        if let Some(label) = &entry.label {
            self.by_label.remove(label);
        }
        self.by_batch.retain(|_, owner| owner != command_id);
        self.order.retain(|open| open != command_id);
        Some(Completion {
            command_id: entry.command_id,
            agent_id: entry.agent_id,
            command: entry.command,
            outcome,
            acknowledged,
            label: entry.label,
            replies: entry.replies,
            warnings: entry.warnings,
        })
    }
}

/// Whether this numeric is an error reply rather than an informational one.
pub const fn is_error_numeric(numeric: u16) -> bool {
    numeric >= 400 && numeric <= 599
}

/// Decide whether this message ends the collector.
fn classify(
    message: &WireMessage,
    strategy: ResponseStrategy,
    command: &str,
    closing_batch_kind: Option<&str>,
) -> Option<CommandOutcome> {
    if message.command.eq_ignore_ascii_case("FAIL") {
        return Some(CommandOutcome::Rejected);
    }
    if message.command.eq_ignore_ascii_case("ERROR") {
        return Some(CommandOutcome::Indeterminate);
    }
    // A declared terminator wins over the generic error rule. Some terminators
    // are themselves error numerics -- 422 ends MOTD on a server that has none
    // -- and treating those as rejections would report a successful query as a
    // failure.
    let declares_terminator = match strategy {
        ResponseStrategy::NumericSequence { terminators } => message
            .numeric()
            .is_some_and(|numeric| terminators.contains(&numeric)),
        _ => false,
    };
    if !declares_terminator
        && let Some(numeric) = message.numeric()
        && is_error_numeric(numeric)
    {
        return Some(CommandOutcome::Rejected);
    }

    match strategy {
        ResponseStrategy::NumericSequence { terminators } => message
            .numeric()
            .filter(|numeric| terminators.contains(numeric))
            .map(|_| CommandOutcome::Completed),
        ResponseStrategy::SingleReply | ResponseStrategy::ConnectionLifecycle => {
            Some(CommandOutcome::Completed)
        }
        ResponseStrategy::Echo { commands } => commands
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&message.command))
            .then_some(CommandOutcome::Completed),
        ResponseStrategy::Batch { expected_type } => {
            (message.command.eq_ignore_ascii_case("BATCH")
                && message
                    .params
                    .first()
                    .is_some_and(|reference| reference.starts_with('-'))
                && closing_batch_kind
                    .is_some_and(|kind| FeatureId::of(kind) == FeatureId::of(expected_type)))
            .then_some(CommandOutcome::Completed)
        }
        ResponseStrategy::Ack => message
            .command
            .eq_ignore_ascii_case("ACK")
            .then_some(CommandOutcome::Completed),
        ResponseStrategy::Unconfirmed => {
            let _ = command;
            None
        }
    }
}

/// A standard reply worth attaching to a result, in typed form.
fn standard_reply(message: &WireMessage) -> Option<CommandWarning> {
    let severity = StandardReplySeverity::parse(&message.command)?;
    Some(CommandWarning::StandardReply {
        severity,
        command: message.params.first().cloned().unwrap_or_default(),
        code: message.params.get(1).cloned().unwrap_or_default(),
        text: message.trailing.clone().unwrap_or_default(),
    })
}

/// Labels observed on inbound traffic that this gateway did not issue.
///
/// A server echoing an unknown label indicates a correlation defect, so the
/// caller can report it rather than silently attributing the reply.
#[cfg(test)]
pub fn unknown_labels<'a>(
    messages: impl IntoIterator<Item = &'a WireMessage>,
    issued: &HashSet<String>,
) -> Vec<String> {
    messages
        .into_iter()
        .filter_map(|message| message.tag_value("label"))
        .filter(|label| !issued.contains(*label))
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn parse(line: &str) -> WireMessage {
        WireMessage::parse(Bytes::copy_from_slice(line.as_bytes())).expect("parse")
    }

    fn correlator() -> Correlator {
        let mut correlator = Correlator::new(CorrelatorLimits::default());
        correlator.set_nickname(Nickname::new("Kuebiko").expect("nickname"));
        correlator
    }

    fn pending(command: &str, strategy: ResponseStrategy, label: Option<&str>) -> PendingCommand {
        PendingCommand {
            command_id: CommandId::new(),
            agent_id: AgentId::new(),
            command: command.to_owned(),
            label: label.map(str::to_owned),
            response: strategy,
            written: false,
            deadline_ms: 10_000,
            warnings: Vec::new(),
            replies: Vec::new(),
        }
    }

    fn strategy(command: &str) -> ResponseStrategy {
        crate::irc::commands::spec_for(command)
            .expect("known command")
            .response
    }

    #[test]
    fn a_numeric_sequence_collects_until_its_terminator() {
        let mut correlator = correlator();
        let entry = pending("WHOIS", strategy("WHOIS"), None);
        let command_id = entry.command_id.clone();
        correlator.register(entry).expect("register");
        assert!(correlator.record_write(&command_id, true).is_none());

        assert!(
            correlator
                .ingest(&parse(":server 311 me alice ~a h * :Alice"))
                .is_empty()
        );
        assert!(
            correlator
                .ingest(&parse(":server 312 me alice server :Info"))
                .is_empty()
        );
        let completions = correlator.ingest(&parse(":server 318 me alice :End of /WHOIS"));

        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].outcome, CommandOutcome::Completed);
        assert!(completions[0].acknowledged);
        assert_eq!(completions[0].replies.len(), 3);
        assert_eq!(correlator.pending_count(), 0);
    }

    #[test]
    fn an_error_numeric_rejects_without_waiting_for_a_terminator() {
        let mut correlator = correlator();
        let entry = pending("WHOIS", strategy("WHOIS"), None);
        let command_id = entry.command_id.clone();
        correlator.register(entry).expect("register");
        correlator.record_write(&command_id, true);

        let completions = correlator.ingest(&parse(":server 401 me ghost :No such nick"));
        assert_eq!(completions[0].outcome, CommandOutcome::Rejected);
        assert!(!completions[0].retriable());
    }

    #[test]
    fn a_terminator_that_is_also_an_error_numeric_still_completes() {
        let mut correlator = correlator();
        let entry = pending("MOTD", strategy("MOTD"), None);
        let command_id = entry.command_id.clone();
        correlator.register(entry).expect("register");
        correlator.record_write(&command_id, true);

        // 422 is ERR_NOMOTD and the declared terminator for MOTD: a server
        // with no MOTD answered the query, it did not reject it.
        let completions = correlator.ingest(&parse(":server 422 me :MOTD File is missing"));
        assert_eq!(completions[0].outcome, CommandOutcome::Completed);
        assert!(completions[0].acknowledged);
    }

    #[test]
    fn an_undeclared_error_numeric_still_rejects() {
        let mut correlator = correlator();
        let entry = pending("MOTD", strategy("MOTD"), None);
        let command_id = entry.command_id.clone();
        correlator.register(entry).expect("register");
        correlator.record_write(&command_id, true);

        let completions = correlator.ingest(&parse(":server 451 me :You have not registered"));
        assert_eq!(completions[0].outcome, CommandOutcome::Rejected);
    }

    #[test]
    fn labels_route_concurrent_commands_of_the_same_class() {
        let mut correlator = correlator();
        let first = pending("WHOIS", strategy("WHOIS"), Some("cmd_a"));
        let second = pending("WHOIS", strategy("WHOIS"), Some("cmd_b"));
        let (first_id, second_id) = (first.command_id.clone(), second.command_id.clone());
        correlator.register(first).expect("first");
        correlator.register(second).expect("second");
        correlator.record_write(&first_id, true);
        correlator.record_write(&second_id, true);

        let completions = correlator.ingest(&parse("@label=cmd_b :server 318 me bob :End"));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].command_id, second_id);
        assert_eq!(correlator.pending_count(), 1);
    }

    #[test]
    fn unlabeled_commands_of_one_class_are_not_admitted_concurrently() {
        let mut correlator = correlator();
        assert!(correlator.admits("WHOIS", strategy("WHOIS"), false));
        correlator
            .register(pending("WHOIS", strategy("WHOIS"), None))
            .expect("register");

        assert!(!correlator.admits("WHOIS", strategy("WHOIS"), false));
        // A different numeric family can still receive the same generic error
        // numeric, so it is conservatively serialized too.
        assert!(!correlator.admits("LIST", strategy("LIST"), false));
        assert!(correlator.admits("NOTICE", ResponseStrategy::Unconfirmed, false));
        assert!(correlator.admits("WHOIS", strategy("WHOIS"), true));
    }

    #[test]
    fn a_batch_completes_the_command_that_opened_it() {
        let mut correlator = correlator();
        let entry = pending(
            "CHATHISTORY",
            ResponseStrategy::Batch {
                expected_type: "chathistory",
            },
            Some("cmd_h"),
        );
        let command_id = entry.command_id.clone();
        correlator.register(entry).expect("register");
        correlator.record_write(&command_id, true);

        assert!(
            correlator
                .ingest(&parse("@label=cmd_h :server BATCH +hist chathistory #room"))
                .is_empty()
        );
        assert!(
            correlator
                .ingest(&parse("@batch=hist :alice PRIVMSG #room :old line"))
                .is_empty()
        );
        let completions = correlator.ingest(&parse(":server BATCH -hist"));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].outcome, CommandOutcome::Completed);
        assert_eq!(completions[0].replies.len(), 3);
    }

    #[test]
    fn an_echo_completes_only_for_our_own_nickname() {
        let mut correlator = correlator();
        let entry = pending("JOIN", strategy("JOIN"), None);
        let command_id = entry.command_id.clone();
        correlator.register(entry).expect("register");
        correlator.record_write(&command_id, true);

        // Another user joining the same channel must not complete our command.
        assert!(
            correlator
                .ingest(&parse(":someone!u@h JOIN #room"))
                .is_empty()
        );
        let completions = correlator.ingest(&parse(":Kuebiko!u@h JOIN #room"));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].outcome, CommandOutcome::Completed);
    }

    #[test]
    fn a_batch_collector_ignores_a_different_batch_type() {
        let mut correlator = correlator();
        let entry = pending(
            "CHATHISTORY",
            ResponseStrategy::Batch {
                expected_type: "chathistory",
            },
            Some("cmd_h"),
        );
        let command_id = entry.command_id.clone();
        correlator.register(entry).expect("register");
        correlator.record_write(&command_id, true);

        correlator.ingest(&parse("@label=cmd_h :server BATCH +other vendor/example"));
        assert!(correlator.ingest(&parse(":server BATCH -other")).is_empty());
        correlator.ingest(&parse("@label=cmd_h :server BATCH +hist chathistory #room"));
        assert_eq!(correlator.ingest(&parse(":server BATCH -hist")).len(), 1);
    }

    #[test]
    fn an_unlabeled_batch_is_bound_by_its_expected_type() {
        let mut correlator = correlator();
        let entry = pending(
            "CHATHISTORY",
            ResponseStrategy::Batch {
                expected_type: "chathistory",
            },
            None,
        );
        let command_id = entry.command_id.clone();
        correlator.register(entry).expect("register");
        correlator.record_write(&command_id, true);

        let opening = parse(":server BATCH +hist draft/chathistory #room");
        let opening_attribution = correlator.attribute(&opening);
        assert_eq!(opening_attribution.command_id(), Some(&command_id));
        assert!(
            correlator
                .ingest_attributed(&opening, opening_attribution)
                .is_empty()
        );
        let message = parse("@batch=hist :alice PRIVMSG #room :old");
        let message_attribution = correlator.attribute(&message);
        assert_eq!(message_attribution.command_id(), Some(&command_id));
        assert!(
            correlator
                .ingest_attributed(&message, message_attribution)
                .is_empty()
        );
        assert_eq!(correlator.ingest(&parse(":server BATCH -hist")).len(), 1);
    }

    #[test]
    fn a_nested_batch_inherits_the_outer_command_owner() {
        let mut correlator = correlator();
        let entry = pending(
            "CHATHISTORY",
            ResponseStrategy::Batch {
                expected_type: "chathistory",
            },
            Some("cmd_nested"),
        );
        let command_id = entry.command_id.clone();
        correlator.register(entry).expect("register");
        correlator.record_write(&command_id, true);

        assert!(
            correlator
                .ingest(&parse(
                    "@label=cmd_nested :server BATCH +outer chathistory #room"
                ))
                .is_empty()
        );
        assert!(
            correlator
                .ingest(&parse("@batch=outer :server BATCH +inner vendor/example"))
                .is_empty()
        );
        assert!(
            correlator
                .ingest(&parse("@batch=inner :alice PRIVMSG #room :nested"))
                .is_empty()
        );
        assert!(
            correlator
                .ingest(&parse("@batch=outer :server BATCH -inner"))
                .is_empty()
        );
        let completions = correlator.ingest(&parse(":server BATCH -outer"));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].replies.len(), 5);
    }

    #[test]
    fn a_write_failure_is_not_written_and_retriable() {
        let mut correlator = correlator();
        let entry = pending("JOIN", strategy("JOIN"), None);
        let command_id = entry.command_id.clone();
        correlator.register(entry).expect("register");

        let completion = correlator
            .record_write(&command_id, false)
            .expect("completion");
        assert_eq!(completion.outcome, CommandOutcome::NotWritten);
        assert!(completion.retriable());
        assert_eq!(correlator.pending_count(), 0);
    }

    #[test]
    fn an_unconfirmed_command_completes_as_soon_as_it_is_written() {
        let mut correlator = correlator();
        let entry = pending("PRIVMSG", ResponseStrategy::Unconfirmed, None);
        let command_id = entry.command_id.clone();
        correlator.register(entry).expect("register");

        let completion = correlator
            .record_write(&command_id, true)
            .expect("completion");
        assert_eq!(completion.outcome, CommandOutcome::SentUnconfirmed);
        assert!(!completion.acknowledged);
        assert!(!completion.retriable());
    }

    #[test]
    fn deadlines_expire_only_the_commands_that_reached_them() {
        let mut correlator = correlator();
        let mut early = pending("WHOIS", strategy("WHOIS"), Some("cmd_early"));
        early.deadline_ms = 1_000;
        let mut late = pending("LIST", strategy("LIST"), Some("cmd_late"));
        late.deadline_ms = 9_000;
        correlator.register(early).expect("early");
        correlator.register(late).expect("late");

        let expired = correlator.tick(1_500);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].outcome, CommandOutcome::TimedOut);
        assert_eq!(correlator.pending_count(), 1);
    }

    #[test]
    fn cancellation_detaches_without_claiming_the_command_failed() {
        let mut correlator = correlator();
        let entry = pending("JOIN", strategy("JOIN"), None);
        let command_id = entry.command_id.clone();
        correlator.register(entry).expect("register");
        correlator.record_write(&command_id, true);

        let completion = correlator.cancel(&command_id).expect("cancelled");
        assert_eq!(completion.outcome, CommandOutcome::Indeterminate);
        assert!(!completion.retriable());
    }

    #[test]
    fn a_standard_reply_failure_is_rejected_and_keeps_its_text() {
        let mut correlator = correlator();
        let entry = pending("JOIN", strategy("JOIN"), Some("cmd_j"));
        let command_id = entry.command_id.clone();
        correlator.register(entry).expect("register");
        correlator.record_write(&command_id, true);

        let completions = correlator.ingest(&parse(
            "@label=cmd_j :server FAIL JOIN CHANNEL_LIMIT :Too many",
        ));
        assert_eq!(completions[0].outcome, CommandOutcome::Rejected);
        assert_eq!(
            completions[0].warnings,
            [CommandWarning::StandardReply {
                severity: StandardReplySeverity::Fail,
                command: "JOIN".into(),
                code: "CHANNEL_LIMIT".into(),
                text: "Too many".into(),
            }]
        );
    }

    #[test]
    fn the_pending_limit_is_enforced() {
        let mut correlator = Correlator::new(CorrelatorLimits {
            max_pending: 1,
            ..CorrelatorLimits::default()
        });
        correlator
            .register(pending("WHOIS", strategy("WHOIS"), Some("cmd_1")))
            .expect("first");
        assert_eq!(
            correlator.register(pending("LIST", strategy("LIST"), Some("cmd_2"))),
            Err(CorrelationError::PendingLimit(1))
        );
    }

    #[test]
    fn unexpected_labels_are_reported_rather_than_attributed() {
        let issued = HashSet::from(["cmd_a".to_owned()]);
        let messages = vec![
            parse("@label=cmd_a :server 318 me alice :End"),
            parse("@label=cmd_zzz :server 318 me bob :End"),
        ];
        assert_eq!(unknown_labels(&messages, &issued), ["cmd_zzz"]);
    }

    #[test]
    fn a_label_is_bounded_by_the_encoder_limit() {
        assert!(CommandId::new().label().len() <= MAX_LABEL_BYTES);
    }
}
