//! Explicit watch handles over an agent's event stream.
//!
//! A watch is a named, server-side subscription: the caller says once which
//! targets and classes it cares about, and gets back a resource URI. Reading
//! that URI returns everything matching since the previous read, together with
//! the position it advanced to, so `subscriptions/listen` plus `resources/read`
//! is a complete delivery loop with no tool call in it.
//!
//! Two properties make it usable as the primary context plane rather than as a
//! polling fallback:
//!
//! * notifications are evaluated against the watch's own filter, so a watch on
//!   one channel is not woken by traffic on another;
//! * the position lives with the watch, so a read needs no cursor argument and
//!   a lagging or reconnecting client is told explicitly that it lost records
//!   instead of silently restarting.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::RwLock,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    agent::{
        AgentId,
        journal::{CursorStatus, EventClass, EventCursor, EventDirection, IrcEvent},
    },
    irc::isupport::CaseMapping,
    mcp::conversation::CompactEvent,
};

/// Opaque handle for one registered watch.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct WatchId(String);

impl WatchId {
    /// Mint a fresh handle.
    pub fn new() -> Self {
        Self(format!("watch-{}", Uuid::new_v4()))
    }
}

impl Default for WatchId {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for WatchId {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // Validate the shape rather than trusting the path segment: a handle is
        // a lookup key, and anything that is not one should fail as a bad URI
        // rather than as a missing watch.
        let suffix = value
            .strip_prefix("watch-")
            .ok_or("watch handle must begin with watch-")?;
        Uuid::parse_str(suffix).map_err(|_| "watch handle must carry a UUID")?;
        Ok(Self(value.to_owned()))
    }
}

impl std::fmt::Display for WatchId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// What one watch selects from an agent's stream.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct WatchFilter {
    /// Case-preserved channels and nicknames to include. Empty means every
    /// target.
    #[serde(default)]
    pub targets: BTreeSet<String>,
    /// Event classes to include. Empty means every class.
    #[serde(default)]
    pub classes: BTreeSet<EventClass>,
    /// Keep only events addressed to the owning agent.
    #[serde(default)]
    pub mentions_only: bool,
    /// Keep only events the gateway received, excluding the agent's own echoed
    /// messages.
    #[serde(default)]
    pub inbound_only: bool,
}

impl WatchFilter {
    /// Whether one event belongs to this watch.
    ///
    /// Target comparison folds case with the server's advertised mapping, so a
    /// watch created for `#Control` still matches traffic the server reports as
    /// `#control`.
    pub fn matches(&self, event: &IrcEvent, case_mapping: CaseMapping) -> bool {
        if self.mentions_only && !event.mentions_me {
            return false;
        }
        if self.inbound_only && event.direction != EventDirection::Inbound {
            return false;
        }
        if !self.classes.is_empty() && !self.classes.contains(&event.class) {
            return false;
        }
        if self.targets.is_empty() {
            return true;
        }
        let Some(target) = event.target.as_ref() else {
            return false;
        };
        let folded = case_mapping.fold(target);
        self.targets
            .iter()
            .any(|wanted| case_mapping.fold(wanted) == folded)
    }
}

/// A registered watch and the position it has delivered through.
#[derive(Clone, Debug)]
struct Watch {
    agent_id: AgentId,
    filter: WatchFilter,
    cursor: Option<EventCursor>,
}

/// Published description of one watch.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct WatchDescriptor {
    /// Opaque handle.
    pub watch_id: WatchId,
    /// Agent whose stream is watched.
    pub agent_id: AgentId,
    /// Selection applied to that stream.
    pub filter: WatchFilter,
    /// Stable resource URI for this watch.
    pub uri: String,
}

/// Payload returned by reading a watch resource.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct WatchResource {
    /// Which watch this is.
    pub watch: WatchDescriptor,
    /// Relationship of the watch's stored position to the retained window.
    /// Anything other than `current` means records were lost and the reader
    /// should treat its prior view as incomplete.
    pub status: CursorStatus,
    /// Matching events since the previous read, oldest first.
    pub events: Vec<CompactEvent>,
    /// Position this read advanced the watch to. Recorded server-side as well,
    /// so the next read continues from here without an argument.
    pub next_cursor: EventCursor,
    /// Whether more retained events remain past `next_cursor`.
    pub has_more: bool,
}

/// Build the stable resource URI for a watch handle.
pub fn watch_uri(watch_id: &WatchId) -> String {
    format!("irc://watches/{watch_id}")
}

/// Input accepted by `irc.watch.create`.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WatchCreateInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Case-preserved channels and nicknames to include. Omit for every
    /// target.
    #[serde(default)]
    pub targets: BTreeSet<String>,
    /// Event classes to include. Omit for every class.
    #[serde(default)]
    pub classes: BTreeSet<EventClass>,
    /// Keep only events addressed to this agent: private messages, and channel
    /// messages naming its current nickname.
    #[serde(default)]
    pub mentions_only: bool,
    /// Keep only events received from IRC, excluding this agent's own echoed
    /// messages.
    #[serde(default)]
    pub inbound_only: bool,
    /// Position to begin from. Omit to start at the oldest retained event, or
    /// supply the cursor a previous session ended on to resume without a gap.
    #[serde(default)]
    pub cursor: Option<EventCursor>,
}

/// Result of `irc.watch.create`.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct WatchCreateOutput {
    /// The registered watch.
    pub watch: WatchDescriptor,
    /// How to consume it without polling.
    pub instructions: &'static str,
}

/// Input accepted by `irc.watch.close`.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WatchCloseInput {
    /// Handle returned by `irc.watch.create`.
    pub watch_id: WatchId,
}

/// Result of `irc.watch.close`.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct WatchCloseOutput {
    /// Handle that was released.
    pub watch_id: WatchId,
}

impl WatchCreateInput {
    /// The selection this input describes.
    pub fn filter(&self) -> WatchFilter {
        WatchFilter {
            targets: self.targets.clone(),
            classes: self.classes.clone(),
            mentions_only: self.mentions_only,
            inbound_only: self.inbound_only,
        }
    }
}

/// Process-wide registry of watch handles.
///
/// Shared between the gateway, which creates and reads watches, and each agent
/// actor, which tests newly journaled events against every watch on itself so a
/// notification means "there is something here for you" rather than "something
/// happened somewhere".
#[derive(Debug, Default)]
pub struct WatchRegistry {
    watches: RwLock<BTreeMap<WatchId, Watch>>,
}

impl WatchRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a watch, returning its published description.
    pub fn create(
        &self,
        agent_id: AgentId,
        filter: WatchFilter,
        cursor: Option<EventCursor>,
    ) -> WatchDescriptor {
        let watch_id = WatchId::new();
        let watch = Watch {
            agent_id: agent_id.clone(),
            filter: filter.clone(),
            cursor,
        };
        self.watches
            .write()
            .expect("watch registry lock")
            .insert(watch_id.clone(), watch);
        WatchDescriptor {
            uri: watch_uri(&watch_id),
            watch_id,
            agent_id,
            filter,
        }
    }

    /// Remove a watch, reporting whether it existed.
    pub fn close(&self, watch_id: &WatchId) -> bool {
        self.watches
            .write()
            .expect("watch registry lock")
            .remove(watch_id)
            .is_some()
    }

    /// Drop every watch belonging to a disconnected agent.
    pub fn close_agent(&self, agent_id: &AgentId) {
        self.watches
            .write()
            .expect("watch registry lock")
            .retain(|_, watch| watch.agent_id != *agent_id);
    }

    /// Describe one registered watch.
    pub fn describe(&self, watch_id: &WatchId) -> Option<WatchDescriptor> {
        let watches = self.watches.read().expect("watch registry lock");
        let watch = watches.get(watch_id)?;
        Some(WatchDescriptor {
            watch_id: watch_id.clone(),
            agent_id: watch.agent_id.clone(),
            filter: watch.filter.clone(),
            uri: watch_uri(watch_id),
        })
    }

    /// Every registered watch, in handle order.
    pub fn list(&self) -> Vec<WatchDescriptor> {
        self.watches
            .read()
            .expect("watch registry lock")
            .iter()
            .map(|(watch_id, watch)| WatchDescriptor {
                watch_id: watch_id.clone(),
                agent_id: watch.agent_id.clone(),
                filter: watch.filter.clone(),
                uri: watch_uri(watch_id),
            })
            .collect()
    }

    /// The agent and stored position for a watch, if it is still registered.
    pub fn position(
        &self,
        watch_id: &WatchId,
    ) -> Option<(AgentId, WatchFilter, Option<EventCursor>)> {
        let watches = self.watches.read().expect("watch registry lock");
        let watch = watches.get(watch_id)?;
        Some((
            watch.agent_id.clone(),
            watch.filter.clone(),
            watch.cursor.clone(),
        ))
    }

    /// Record the position a read advanced to.
    ///
    /// Ignores a watch that was closed while the read was in flight rather than
    /// resurrecting it.
    pub fn advance(&self, watch_id: &WatchId, cursor: EventCursor) {
        if let Some(watch) = self
            .watches
            .write()
            .expect("watch registry lock")
            .get_mut(watch_id)
        {
            watch.cursor = Some(cursor);
        }
    }

    /// URIs of every watch on one agent that selects this event.
    ///
    /// Called on the actor's own task for each journaled record, so it takes
    /// the read lock briefly and allocates only for watches that actually
    /// matched.
    pub fn matching_uris(
        &self,
        agent_id: &AgentId,
        event: &IrcEvent,
        case_mapping: CaseMapping,
    ) -> Vec<String> {
        self.watches
            .read()
            .expect("watch registry lock")
            .iter()
            .filter(|(_, watch)| watch.agent_id == *agent_id)
            .filter(|(_, watch)| watch.filter.matches(event, case_mapping))
            .map(|(watch_id, _)| watch_uri(watch_id))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        agent::journal::{EventCorrelation, EventOrigin, EventVerbosity},
        time::Timestamp,
    };

    fn event(target: Option<&str>, class: EventClass, mentions_me: bool) -> IrcEvent {
        IrcEvent {
            cursor: EventCursor {
                stream_id: "stream".into(),
                sequence: 1,
            },
            agent_id: AgentId::new(),
            direction: EventDirection::Inbound,
            class,
            origin: EventOrigin::Live,
            verbosity: EventVerbosity::Semantic,
            target: target.map(str::to_owned),
            server_time: None,
            received_at: Timestamp::now(),
            correlation: EventCorrelation::default(),
            semantic: None,
            wire: None,
            mentions_me,
        }
    }

    #[test]
    fn an_empty_filter_matches_everything() {
        let filter = WatchFilter::default();
        assert!(filter.matches(
            &event(Some("#control"), EventClass::MessageChannel, false),
            CaseMapping::default()
        ));
    }

    #[test]
    fn targets_fold_case_using_the_servers_mapping() {
        let filter = WatchFilter {
            targets: BTreeSet::from(["#Control".to_string()]),
            ..WatchFilter::default()
        };
        assert!(filter.matches(
            &event(Some("#control"), EventClass::MessageChannel, false),
            CaseMapping::default()
        ));
        assert!(!filter.matches(
            &event(Some("#other"), EventClass::MessageChannel, false),
            CaseMapping::default()
        ));
    }

    #[test]
    fn a_targeted_watch_excludes_events_with_no_target() {
        let filter = WatchFilter {
            targets: BTreeSet::from(["#control".to_string()]),
            ..WatchFilter::default()
        };
        assert!(!filter.matches(
            &event(None, EventClass::ConnectionLifecycle, false),
            CaseMapping::default()
        ));
    }

    #[test]
    fn mentions_only_keeps_addressed_events_alone() {
        let filter = WatchFilter {
            mentions_only: true,
            ..WatchFilter::default()
        };
        assert!(filter.matches(
            &event(Some("#control"), EventClass::MessageChannel, true),
            CaseMapping::default()
        ));
        assert!(!filter.matches(
            &event(Some("#control"), EventClass::MessageChannel, false),
            CaseMapping::default()
        ));
    }

    #[test]
    fn only_watches_on_the_same_agent_are_woken() {
        let registry = WatchRegistry::new();
        let mine = AgentId::new();
        let theirs = AgentId::new();
        let descriptor = registry.create(mine.clone(), WatchFilter::default(), None);
        registry.create(theirs, WatchFilter::default(), None);

        let uris = registry.matching_uris(
            &mine,
            &event(Some("#control"), EventClass::MessageChannel, false),
            CaseMapping::default(),
        );
        assert_eq!(uris, vec![descriptor.uri]);
    }

    #[test]
    fn a_closed_watch_stops_matching_and_cannot_be_advanced() {
        let registry = WatchRegistry::new();
        let agent = AgentId::new();
        let descriptor = registry.create(agent.clone(), WatchFilter::default(), None);
        assert!(registry.close(&descriptor.watch_id));
        assert!(!registry.close(&descriptor.watch_id));
        registry.advance(
            &descriptor.watch_id,
            EventCursor {
                stream_id: "stream".into(),
                sequence: 9,
            },
        );
        assert!(registry.describe(&descriptor.watch_id).is_none());
    }

    #[test]
    fn watch_handles_round_trip_through_their_uri_form() {
        let watch_id = WatchId::new();
        let uri = watch_uri(&watch_id);
        let parsed = uri
            .strip_prefix("irc://watches/")
            .map(WatchId::from_str)
            .expect("watch URI prefix")
            .expect("valid watch handle");
        assert_eq!(parsed, watch_id);
        assert!(WatchId::from_str("not-a-watch").is_err());
    }
}
