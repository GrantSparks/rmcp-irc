//! Explicit watch handles over an agent's event stream.
//!
//! A watch is a named, server-side *selection*: the caller says once which
//! targets and classes it cares about, and gets back `irc://watches/{id}`, a
//! resource it can subscribe to. Notifications are evaluated against that
//! selection, so a watch on one channel is never woken by traffic on another.
//!
//! A watch holds no position. The descriptor resource is immutable and reading
//! it consumes nothing, because a spec-compliant host re-reads resources at
//! moments the model did not choose — refreshing a panel, resynchronizing after
//! dropped notifications, two components on one URI — and a read that consumed
//! events would lose them silently. Every position is the caller's own and is
//! supplied on each read, through either consumption path:
//!
//! * `irc.events.read` with `watch_id` and the caller's cursor, which applies
//!   this selection to the lossless journal and can long poll;
//! * `irc://watches/{id}/events/after/{stream_id}/{sequence}`, which carries the
//!   position in the path and answers with a compact selected window. Ordinary
//!   watches select conversational records; attention watches can also select
//!   sparse actionable lifecycle summaries.
//!
//! Both are idempotent: the same position always returns the same window, and
//! the cursor they hand back advances only over events they actually returned.
//!
//! The descriptor URI is also the only subscribable form. The positioned window
//! is a different URI for every position, is never published as changed, and is
//! excluded from the accepted subscription filter for that reason: subscribe to
//! `irc://watches/{id}`, and read the position you hold when it wakes you.
//!
//! A handle expires when nobody is using it, where "in use" covers both a
//! caller reading it and the watch delivering a match — and an expiry announces
//! itself on the resource-update channel rather than letting a subscriber wait
//! on a handle that has quietly stopped existing. See [`WatchRegistry`].

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::RwLock,
    time::Duration,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{
    agent::{
        AgentId,
        journal::{
            CursorQuery, CursorStatus, EventClass, EventCursor, EventDirection, EventFilter,
            EventPage, IrcEvent, JournalStats,
        },
    },
    error::{GatewayError, Result},
    irc::isupport::CaseMapping,
    mcp::{attention::AttentionEvent, authorization::OwnerId, conversation::source_account},
    time::Timestamp,
};

/// Prefix under which every watch resource lives.
///
/// Watch handles are addressed under their own authority rather than under an
/// agent, because a watch outlives any single read and is the thing a client
/// subscribes to.
pub const WATCH_URI_PREFIX: &str = "irc://watches/";

/// Resource template for one positioned window over a watch's selection.
///
/// The position is in the path rather than in server state, which is what makes
/// the read idempotent: a host may fetch the same URI any number of times and
/// always receives the same window.
pub const WATCH_EVENTS_TEMPLATE: &str =
    "irc://watches/{watch_id}/events/after/{stream_id}/{sequence}";

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

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
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
    /// Compound model-attention selection. Ordinary watches omit this; an
    /// `irc.attention.open` watch uses it to combine direct/addressed traffic,
    /// account-identified human messages, and complete traffic in selected
    /// task channels with sparse actionable lifecycle signals, without
    /// maintaining several overlapping watches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention: Option<AttentionSelection>,
}

/// The compound portion of a model-attention watch.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct AttentionSelection {
    /// Channels or nicknames whose inbound conversational traffic is all
    /// relevant, even when it neither mentions the agent nor carries an
    /// authenticated account.
    #[serde(default)]
    pub full_traffic_targets: BTreeSet<String>,
}

/// Low-volume operational classes that need model attention even when they
/// are not conversation. This single list is shared by selection, projection
/// invariants, and tests so a newly selected class cannot be silently dropped.
pub const SPARSE_ATTENTION_CLASSES: &[EventClass] = &[
    EventClass::ConnectionLifecycle,
    EventClass::ServerMotd,
    EventClass::ProtocolCompatibility,
    EventClass::ChannelState,
    EventClass::JournalPressure,
    EventClass::DccChatOffered,
    EventClass::DccTransferOffered,
    EventClass::DccConnected,
    EventClass::DccChatClosed,
    EventClass::DccTransferCompleted,
    EventClass::DccRejected,
    EventClass::DccCancelled,
    EventClass::DccFailed,
];

impl AttentionSelection {
    /// Whether one event requires model attention.
    pub fn matches(&self, event: &IrcEvent, case_mapping: CaseMapping) -> bool {
        // Nothing the agent itself wrote needs a model turn, whatever class it
        // lands in. Its own request is already in the caller's context from the
        // tool call that produced it, and the server's echo of that request
        // says the same thing a second time. This has to come before the sparse
        // rule below: a self-issued TOPIC or MODE is a `channel.state` event, so
        // checking it only on the conversational path would leave an agent
        // waking itself up every time it curated a channel.
        if event.authored_by_me {
            return false;
        }
        // Sparse lifecycle signals are relevant even when gateway-internal. A
        // scheduled fallback that cannot be woken by the listen stream still
        // needs to observe them on its next check.
        if SPARSE_ATTENTION_CLASSES.contains(&event.class) {
            return true;
        }
        if !matches!(
            event.class,
            EventClass::MessageChannel
                | EventClass::MessagePrivate
                | EventClass::MessageAction
                | EventClass::MessageNotice
                | EventClass::DccChatMessage
        ) {
            return false;
        }
        if event.direction != EventDirection::Inbound {
            return false;
        }
        if event.class == EventClass::DccChatMessage
            || event.class == EventClass::MessagePrivate
            || event.mentions_me
            || source_account(event).is_some()
        {
            return true;
        }
        let Some(target) = event.target.as_ref() else {
            return false;
        };
        let folded = case_mapping.fold(target);
        self.full_traffic_targets
            .iter()
            .any(|wanted| case_mapping.fold(wanted) == folded)
    }
}

impl WatchFilter {
    /// Whether one event belongs to this watch.
    ///
    /// Target comparison folds case with the server's advertised mapping, so a
    /// watch created for `#Control` still matches traffic the server reports as
    /// `#control`.
    pub fn matches(&self, event: &IrcEvent, case_mapping: CaseMapping) -> bool {
        if self.inbound_only && event.direction != EventDirection::Inbound {
            return false;
        }
        if !self.classes.is_empty() && !self.classes.contains(&event.class) {
            return false;
        }
        if let Some(attention) = &self.attention {
            return attention.matches(event, case_mapping);
        }
        if self.mentions_only && !event.mentions_me {
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

    /// The journal selection this watch applies to a cursor read.
    ///
    /// Folding the targets once here, rather than per event after the read, is
    /// what lets the journal apply the watch's own selection *during* the read.
    /// That is the whole point: a position handed back by a read that already
    /// knew the selection advances only over events the watch delivered, so
    /// records the watch declined can neither be consumed silently nor block
    /// progress behind a page of traffic it does not want.
    pub fn cursor_query(&self, case_mapping: CaseMapping) -> CursorQuery {
        CursorQuery {
            filter: EventFilter {
                direction: self.inbound_only.then_some(EventDirection::Inbound),
                mentions_me: (self.attention.is_none() && self.mentions_only).then_some(true),
                ..EventFilter::default()
            },
            folded_targets: if self.attention.is_some() {
                BTreeSet::new()
            } else {
                self.targets
                    .iter()
                    .map(|target| case_mapping.fold(target))
                    .collect()
            },
            case_mapping,
            classes: self.classes.clone(),
            attention: self.attention.clone(),
            conversational_only: false,
        }
    }
}

/// A registered watch: one immutable selection over one agent's stream.
///
/// There is deliberately no position here. Delivery state behind a
/// host-readable resource is shared mutable state: two authorized components
/// reading the same URI would consume each other's window, and a retry after a
/// lost response would consume a second one. Callers keep their own cursors
/// instead.
#[derive(Clone, Debug)]
struct Watch {
    owner: OwnerId,
    agent_id: AgentId,
    filter: WatchFilter,
    expires_at: Timestamp,
}

impl Watch {
    fn descriptor(&self, watch_id: &WatchId) -> WatchDescriptor {
        WatchDescriptor {
            watch_id: watch_id.clone(),
            agent_id: self.agent_id.clone(),
            filter: self.filter.clone(),
            uri: watch_uri(watch_id),
            expires_at: self.expires_at,
        }
    }
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
    /// When an unused handle stops working. Reading either consumption path or
    /// the descriptor itself pushes this out again, and so does delivering a
    /// match to this watch's subscribers, so a watch expires only after it has
    /// gone the configured window with nothing to say and nobody asking.
    pub expires_at: Timestamp,
}

/// The two ways to read what a watch selects. Both are positioned by the
/// caller, so neither disturbs the other.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct WatchConsumption {
    /// Tool that applies this watch's selection to the lossless journal. Pass
    /// `watch_id`, the `next_cursor` from your previous read, and optionally
    /// `wait_ms` to long poll.
    pub events_read_tool: &'static str,
    /// Resource template for the compact window, positioned in the path.
    pub events_after_template: &'static str,
    /// That template expanded at the oldest retained position: everything this
    /// watch selects that is still in the window.
    pub from_oldest_retained: String,
    /// That template expanded at the newest position: only what happens next.
    pub from_latest: String,
    /// One concrete loop, spelled out.
    pub instructions: &'static str,
}

/// Payload returned by reading `irc://watches/{watch_id}`.
///
/// A filter, a health report, and a notification target — never a delivery
/// queue. Reading it is pure: any number of host-initiated re-reads return the
/// same thing and lose nothing, which is why `consume` names positioned paths
/// instead of returning events here.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct WatchResource {
    /// Which watch this is.
    pub watch: WatchDescriptor,
    /// Retained bounds of the stream this watch selects from, so a caller can
    /// tell whether the position it holds is still inside the window before it
    /// reads.
    pub journal: JournalStats,
    /// Where to read what this watch selected.
    pub consume: WatchConsumption,
}

impl WatchResource {
    /// Describe one watch against the current bounds of its stream.
    pub fn describe(watch: WatchDescriptor, journal: JournalStats) -> Self {
        // One before the oldest retained sequence, so the window starting there
        // includes the oldest record itself.
        let oldest = EventCursor {
            stream_id: journal.stream_id.clone(),
            sequence: journal
                .oldest_available
                .as_ref()
                .map_or(journal.latest.sequence, |cursor| {
                    cursor.sequence.saturating_sub(1)
                }),
        };
        let consume = WatchConsumption {
            events_read_tool: "irc.events.read",
            events_after_template: WATCH_EVENTS_TEMPLATE,
            from_oldest_retained: watch_events_uri(&watch.watch_id, &oldest),
            from_latest: watch_events_uri(&watch.watch_id, &journal.latest),
            instructions: WATCH_INSTRUCTIONS,
        };
        Self {
            watch,
            journal,
            consume,
        }
    }
}

/// One positioned window over the events a watch selects.
///
/// Idempotent by construction: the position comes from the URI, nothing on the
/// server moves, and `next_cursor` advances only over the events in `events`.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct WatchEventsResource {
    /// Which watch produced this window.
    pub watch: WatchDescriptor,
    /// Position this window was requested from, taken from the URI.
    pub requested_cursor: EventCursor,
    /// Relationship of that position to the retained window. Anything other
    /// than `current` means records were lost between it and what follows, and
    /// the reader should treat its prior view as incomplete.
    pub status: CursorStatus,
    /// Oldest position still retained, if the stream retains anything.
    pub oldest_available: Option<EventCursor>,
    /// Newest position assigned in this stream.
    pub latest: EventCursor,
    /// Selected events, compact and oldest first. Ordinary watches exclude
    /// records with no conversational form. Attention watches additionally
    /// project the sparse lifecycle classes in their compound selection.
    pub events: Vec<AttentionEvent>,
    /// Position to read from next. It advances only over `events`, so re-reading
    /// `requested_cursor` returns this same window and a lost response costs
    /// nothing.
    pub next_cursor: EventCursor,
    /// Whether at least one further selected event is retained past
    /// `next_cursor`. True means read `next_uri` immediately rather than waiting
    /// for another notification; false means this watch is drained, even when
    /// the stream holds later records it did not select.
    pub has_more: bool,
    /// This resource's own URI expanded at `next_cursor`.
    pub next_uri: String,
}

impl WatchEventsResource {
    /// Project one journal page into the watch's compact window.
    pub fn from_page(
        watch: WatchDescriptor,
        requested_cursor: EventCursor,
        page: EventPage,
    ) -> Self {
        Self {
            next_uri: watch_events_uri(&watch.watch_id, &page.next_cursor),
            watch,
            requested_cursor,
            status: page.status,
            oldest_available: page.oldest_available,
            latest: page.latest,
            // Every event the selection returned projects: ordinary watch
            // windows preselect conversational records, while attention
            // watches select only records with an AttentionEvent projection.
            events: page
                .events
                .iter()
                .filter_map(AttentionEvent::project)
                .collect(),
            next_cursor: page.next_cursor,
            has_more: page.has_more,
        }
    }
}

/// The single delivery loop a caller should implement, published from the
/// descriptor resource and from `irc.watch.create` so both say the same thing.
pub const WATCH_INSTRUCTIONS: &str = "Merge this watch's own irc://watches/{watch_id} into \
     resourceSubscriptions on the client's one subscriptions/listen stream; that descriptor is \
     the only subscribable form, and a positioned events/after URI is read rather than subscribed \
     to. On each notification, read irc.events.read with this watch_id and the \
     cursor you last persisted, or fetch \
     irc://watches/{watch_id}/events/after/{stream_id}/{sequence} for the compact window. Persist \
     the next_cursor each read returns; nothing on the server moves, so re-reading a cursor is \
     always safe. Keep reading while has_more is true. Without subscriptions, call irc.events.read \
     with this watch_id and a positive wait_ms. Notifications wake the host, not a model; ongoing \
     model responsiveness should use irc.attention.open and its one-minute scheduler fallback. \
     Release the handle with irc.watch.close.";

/// Build the stable resource URI for a watch handle.
pub fn watch_uri(watch_id: &WatchId) -> String {
    format!("{WATCH_URI_PREFIX}{watch_id}")
}

/// Build the positioned window URI for a watch and an explicit cursor.
pub fn watch_events_uri(watch_id: &WatchId, cursor: &EventCursor) -> String {
    format!(
        "{WATCH_URI_PREFIX}{watch_id}/events/after/{}/{}",
        cursor.stream_id, cursor.sequence
    )
}

/// Which of a watch's resources a URI addresses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchTarget {
    /// The immutable descriptor, journal health, and consumption pointers.
    Descriptor,
    /// One compact window of selected events after an explicit position.
    EventsAfter(EventCursor),
}

/// Parsed `irc://watches/...` resource URI.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchUri {
    /// Watch the URI addresses.
    pub watch_id: WatchId,
    /// Which of that watch's resources it addresses.
    pub target: WatchTarget,
}

impl FromStr for WatchUri {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let path = value
            .strip_prefix(WATCH_URI_PREFIX)
            .ok_or("watch resource URI must begin with irc://watches/")?;
        // Match the whole remainder so a trailing segment is rejected rather
        // than silently ignored.
        let segments: Vec<&str> = path.split('/').collect();
        let (handle, rest) = segments
            .split_first()
            .ok_or("watch resource URI must name a handle")?;
        let watch_id = WatchId::from_str(handle)?;
        let target = match rest {
            [] => WatchTarget::Descriptor,
            ["events", "after", stream_id, sequence] if !stream_id.is_empty() => {
                WatchTarget::EventsAfter(EventCursor {
                    stream_id: (*stream_id).to_owned(),
                    sequence: sequence
                        .parse()
                        .map_err(|_| "watch event position must end in a decimal sequence")?,
                })
            }
            _ => return Err("unknown watch resource path"),
        };
        Ok(Self { watch_id, target })
    }
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
}

/// Result of `irc.watch.create`.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct WatchCreateOutput {
    /// The registered watch.
    pub watch: WatchDescriptor,
    /// Journal position at the moment this watch was created. Persist it as the
    /// starting cursor to receive only what happens from now on; a watch stores
    /// no position of its own, so this is the caller's "now".
    pub latest_cursor: EventCursor,
    /// `latest_cursor` already expanded into the positioned window URI, so the
    /// first read needs no cursor arithmetic.
    pub next_uri: String,
    /// The delivery loop to implement.
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
            attention: None,
        }
    }
}

/// Bounds the registry enforces on watch handles.
#[derive(Clone, Copy, Debug)]
pub struct WatchLimits {
    /// Watches one caller may hold at once, across all of its agents.
    pub max_per_owner: usize,
    /// How long a watch survives without being used. Refreshed whenever a
    /// caller resolves the handle and whenever the watch delivers a match, so
    /// a watch that is being read or being woken never expires under its
    /// caller.
    pub time_to_live: Duration,
}

/// Process-wide registry of watch handles.
///
/// Shared between the gateway, which creates and resolves watches, and each
/// agent actor, which tests newly journaled events against every watch on
/// itself so a notification means "there is something here for you" rather than
/// "something happened somewhere".
///
/// Handles are bearer-shaped: they name state a caller may read, so they are
/// bounded per owner and expire when nobody is using them. Expiry is applied
/// opportunistically — every read treats a lapsed handle as absent and every
/// write reclaims it — rather than from a timer, which keeps the registry a
/// plain lock with no task of its own.
///
/// # What keeps a watch alive
///
/// "In use" means one of two things, and both push the expiry out:
///
/// * a caller resolved the handle through [`Self::touch`], which every
///   consumption path and every descriptor read does;
/// * the watch delivered a match through [`Self::matching_uris`], because a
///   notification went out naming this handle and the caller is expected to
///   come back and read it.
///
/// Counting a delivery matters for the watch that is quiet by design. A
/// mentions-only watch a host subscribed to can go a whole TTL without a match,
/// and if a match then arrived at a lapsed handle it would be filtered out and
/// dropped in silence — the subscriber holding a live subscription and hearing
/// nothing ever again. Traffic that is merely *counted* is deliberately not a
/// use: see [`Self::filters_for`].
///
/// Retirement is announced. When a lapsed handle is reclaimed, its descriptor
/// URI is published on the same resource-update channel that carries ordinary
/// notifications, so a subscribed host re-reads the descriptor, is told the
/// handle is unknown, and can create a replacement instead of waiting on a
/// resource that will never speak again.
#[derive(Debug)]
pub struct WatchRegistry {
    limits: WatchLimits,
    watches: RwLock<BTreeMap<WatchId, Watch>>,
    /// Where the retirement of a lapsed handle is announced. This is the
    /// gateway's resource-update channel, so the expiry of a watch reaches a
    /// subscriber by the same route as an event that matched it.
    expiry_updates: broadcast::Sender<String>,
}

impl WatchRegistry {
    /// Create an empty registry with the bounds it will enforce, publishing
    /// expiries onto `expiry_updates`.
    pub fn new(limits: WatchLimits, expiry_updates: broadcast::Sender<String>) -> Self {
        Self {
            limits,
            watches: RwLock::new(BTreeMap::new()),
            expiry_updates,
        }
    }

    /// Register a watch for one caller, returning its published description.
    ///
    /// Lapsed handles are reclaimed first, so a caller that walked away from a
    /// watch cannot permanently occupy its own allowance.
    pub fn create(
        &self,
        owner: OwnerId,
        agent_id: AgentId,
        filter: WatchFilter,
    ) -> Result<WatchDescriptor> {
        let now = Timestamp::now();
        let mut watches = self.watches.write().expect("watch registry lock");
        self.retire_expired(&mut watches, now);
        let held = watches
            .values()
            .filter(|watch| watch.owner == owner)
            .count();
        if held >= self.limits.max_per_owner {
            return Err(GatewayError::ResourceLimit(format!(
                "watch limit reached: this caller already holds {held} of {} watches; close one \
                 with irc.watch.close",
                self.limits.max_per_owner
            )));
        }
        let watch_id = WatchId::new();
        let watch = Watch {
            owner,
            agent_id,
            filter,
            expires_at: self.expiry_after(now),
        };
        let descriptor = watch.descriptor(&watch_id);
        watches.insert(watch_id, watch);
        Ok(descriptor)
    }

    /// Remove a watch, reporting whether it existed.
    pub fn close(&self, watch_id: &WatchId) -> bool {
        let now = Timestamp::now();
        let mut watches = self.watches.write().expect("watch registry lock");
        self.retire_expired(&mut watches, now);
        watches.remove(watch_id).is_some()
    }

    /// Drop every watch belonging to a disconnected agent.
    pub fn close_agent(&self, agent_id: &AgentId) {
        self.watches
            .write()
            .expect("watch registry lock")
            .retain(|_, watch| watch.agent_id != *agent_id);
    }

    /// Describe one live watch without touching its expiry.
    pub fn describe(&self, watch_id: &WatchId) -> Option<WatchDescriptor> {
        let now = Timestamp::now();
        let watches = self.watches.read().expect("watch registry lock");
        let watch = watches
            .get(watch_id)
            .filter(|watch| watch.expires_at > now)?;
        Some(watch.descriptor(watch_id))
    }

    /// Describe one live watch and push its expiry out again.
    ///
    /// Used by the consumption paths, so a caller asking for this watch's
    /// events is the clearest form of "in use". Traffic that merely matched is
    /// the other form, applied by [`Self::matching_uris`] at the point a
    /// notification is actually published.
    pub fn touch(&self, watch_id: &WatchId) -> Option<WatchDescriptor> {
        let now = Timestamp::now();
        let mut watches = self.watches.write().expect("watch registry lock");
        self.retire_expired(&mut watches, now);
        let watch = watches.get_mut(watch_id)?;
        watch.expires_at = self.expiry_after(now);
        Some(watch.descriptor(watch_id))
    }

    /// Every live watch, in handle order.
    pub fn list(&self) -> Vec<WatchDescriptor> {
        let now = Timestamp::now();
        self.watches
            .read()
            .expect("watch registry lock")
            .iter()
            .filter(|(_, watch)| watch.expires_at > now)
            .map(|(watch_id, watch)| watch.descriptor(watch_id))
            .collect()
    }

    /// The selections of every live watch on one agent, in handle order.
    ///
    /// Read-only in every sense: it neither refreshes a watch's time to live nor
    /// tells the registry that anybody consumed anything. Activity hints are
    /// computed from these on ordinary tool results, and a hint is neither a
    /// read of the handle nor a delivery to it — it is a tally piggybacked on
    /// some other call, so a watch whose only trace in the system is having been
    /// counted here is exactly the abandoned handle expiry exists to reclaim.
    pub fn filters_for(&self, agent_id: &AgentId) -> Vec<WatchFilter> {
        let now = Timestamp::now();
        self.watches
            .read()
            .expect("watch registry lock")
            .values()
            .filter(|watch| watch.agent_id == *agent_id && watch.expires_at > now)
            .map(|watch| watch.filter.clone())
            .collect()
    }

    /// URIs of every live watch on one agent that selects this event, pushing
    /// the expiry of each one out.
    ///
    /// Delivering a match *is* a use of the handle. A watch that is quiet by
    /// design — mentions only, one rarely used channel — can otherwise sit
    /// through its whole time to live and lapse a moment before the message it
    /// exists to catch arrives, and the caller would learn nothing: the
    /// notification it was subscribed to would simply never come. Refreshing
    /// here means a watch stays alive for as long as traffic keeps reaching it
    /// inside one TTL window, and lapses only once both its caller and its
    /// stream have gone quiet.
    ///
    /// Called on the actor's own task for each journaled record, so it holds the
    /// lock briefly and allocates only for watches that actually matched.
    pub fn matching_uris(
        &self,
        agent_id: &AgentId,
        event: &IrcEvent,
        case_mapping: CaseMapping,
    ) -> Vec<String> {
        let now = Timestamp::now();
        let refreshed = self.expiry_after(now);
        let mut watches = self.watches.write().expect("watch registry lock");
        self.retire_expired(&mut watches, now);
        watches
            .iter_mut()
            .filter(|(_, watch)| watch.agent_id == *agent_id)
            .filter(|(_, watch)| watch.filter.matches(event, case_mapping))
            .map(|(watch_id, watch)| {
                watch.expires_at = refreshed;
                watch_uri(watch_id)
            })
            .collect()
    }

    /// Drop every lapsed handle, announcing each retirement once.
    ///
    /// The announcement is the point. Expiry is discovered opportunistically
    /// rather than on a timer, so this runs at every write, and a subscriber
    /// that hears its watch's URI, re-reads it, and is told the handle is
    /// unknown has learned the one thing silence could never tell it.
    fn retire_expired(&self, watches: &mut BTreeMap<WatchId, Watch>, now: Timestamp) {
        watches.retain(|watch_id, watch| {
            if watch.expires_at > now {
                return true;
            }
            let _ = self.expiry_updates.send(watch_uri(watch_id));
            false
        });
    }

    fn expiry_after(&self, now: Timestamp) -> Timestamp {
        let millis = i64::try_from(self.limits.time_to_live.as_millis()).unwrap_or(i64::MAX);
        now.saturating_add(chrono::Duration::milliseconds(millis))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        agent::journal::{EventCorrelation, EventOrigin, EventPayload, EventVerbosity},
        irc::{
            semantic::{SemanticEvent, SemanticProjection, Source},
            target::ChannelName,
        },
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
            authored_by_me: false,
        }
    }

    fn message(
        target: &str,
        account: Option<&str>,
        mentions_me: bool,
        direction: EventDirection,
    ) -> IrcEvent {
        authored_message(target, account, mentions_me, direction, false)
    }

    fn authored_message(
        target: &str,
        account: Option<&str>,
        mentions_me: bool,
        direction: EventDirection,
        authored_by_me: bool,
    ) -> IrcEvent {
        let semantic = SemanticEvent::MessageChannel {
            source: Source {
                name: "speaker".into(),
                user: None,
                host: None,
                account: account.map(str::to_owned),
            },
            channel: ChannelName::new(target.to_owned()).expect("channel"),
            text: "hello".into(),
        };
        IrcEvent {
            direction,
            semantic: Some(EventPayload::Irc(SemanticProjection::from(semantic))),
            authored_by_me,
            ..event(Some(target), EventClass::MessageChannel, mentions_me)
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
    fn attention_is_one_compound_non_overlapping_selection() {
        let mapping = CaseMapping::default();
        let filter = WatchFilter {
            inbound_only: true,
            attention: Some(AttentionSelection {
                full_traffic_targets: BTreeSet::from(["#Project".into()]),
            }),
            ..WatchFilter::default()
        };
        let project = message("#project", None, false, EventDirection::Inbound);
        let human = message("#control", Some("grant"), false, EventDirection::Inbound);
        let mention = message("#control", None, true, EventDirection::Inbound);
        let background = message("#control", None, false, EventDirection::Inbound);
        let own_echo = message("#project", None, false, EventDirection::Outbound);
        // What `echo-message` actually returns: the agent's own line, inbound,
        // in a target whose whole conversation it asked to follow.
        let echoed_back = authored_message("#project", None, false, EventDirection::Inbound, true);

        for selected in [&project, &human, &mention] {
            assert!(filter.matches(selected, mapping));
            assert!(filter.cursor_query(mapping).selects(selected));
        }
        for ignored in [&background, &own_echo, &echoed_back] {
            assert!(!filter.matches(ignored, mapping));
            assert!(!filter.cursor_query(mapping).selects(ignored));
        }
    }

    #[test]
    fn an_agents_own_channel_curation_never_wakes_it() {
        let mapping = CaseMapping::default();
        let filter = WatchFilter {
            attention: Some(AttentionSelection::default()),
            ..WatchFilter::default()
        };
        // A self-issued TOPIC is a sparse `channel.state` event, so it reaches
        // attention by a different route than a message does: once as the
        // outbound request, and again as the server's echo of it. Neither is
        // news to the agent that just asked for it.
        let request = channel_state(EventDirection::Outbound, "");
        let echo = channel_state(EventDirection::Inbound, "Vohumanah");
        for own in [&request, &echo] {
            assert!(!filter.matches(own, mapping));
            assert!(!filter.cursor_query(mapping).selects(own));
        }

        // Somebody else curating the same channel is still worth a turn.
        let by_another = IrcEvent {
            authored_by_me: false,
            ..channel_state(EventDirection::Inbound, "grant")
        };
        assert!(filter.matches(&by_another, mapping));
        assert!(filter.cursor_query(mapping).selects(&by_another));
    }

    /// One topic change the agent itself made, as the journal records it.
    fn channel_state(direction: EventDirection, source: &str) -> IrcEvent {
        let semantic = SemanticEvent::ChannelState {
            source: Source {
                name: source.to_owned(),
                user: None,
                host: None,
                account: None,
            },
            channel: Some(ChannelName::new("#project".to_owned()).expect("channel")),
            topic: Some("curated by its own occupant".into()),
            modes: None,
        };
        IrcEvent {
            direction,
            semantic: Some(EventPayload::Irc(SemanticProjection::from(semantic))),
            authored_by_me: true,
            ..event(Some("#project"), EventClass::ChannelState, false)
        }
    }

    /// A registry with the expiry channel nothing in these tests listens on.
    fn registry() -> WatchRegistry {
        registry_with(WatchLimits {
            max_per_owner: 4,
            time_to_live: Duration::from_secs(60),
        })
        .0
    }

    /// A registry and the receiver its retirements are announced on.
    fn registry_with(limits: WatchLimits) -> (WatchRegistry, broadcast::Receiver<String>) {
        let (updates, receiver) = broadcast::channel(16);
        (WatchRegistry::new(limits, updates), receiver)
    }

    /// Every URI announced so far, in order.
    fn announced(receiver: &mut broadcast::Receiver<String>) -> Vec<String> {
        let mut seen = Vec::new();
        while let Ok(uri) = receiver.try_recv() {
            seen.push(uri);
        }
        seen
    }

    #[test]
    fn the_journal_query_a_watch_builds_selects_exactly_what_the_watch_matches() {
        let mapping = CaseMapping::default();
        let filters = [
            WatchFilter::default(),
            WatchFilter {
                targets: BTreeSet::from(["#Control".to_string(), "grant".to_string()]),
                ..WatchFilter::default()
            },
            WatchFilter {
                classes: BTreeSet::from([EventClass::MessagePrivate]),
                mentions_only: true,
                ..WatchFilter::default()
            },
            WatchFilter {
                inbound_only: true,
                ..WatchFilter::default()
            },
        ];
        let events = [
            event(Some("#control"), EventClass::MessageChannel, false),
            event(Some("#control"), EventClass::MessageChannel, true),
            event(Some("GRANT"), EventClass::MessagePrivate, true),
            event(Some("#other"), EventClass::ProtocolReply, false),
            event(None, EventClass::ConnectionLifecycle, false),
            IrcEvent {
                direction: EventDirection::Outbound,
                ..event(Some("#control"), EventClass::MessageChannel, false)
            },
        ];
        for filter in &filters {
            let query = filter.cursor_query(mapping);
            for event in &events {
                assert_eq!(
                    filter.matches(event, mapping),
                    query.selects(event),
                    "the notification filter and the read selection disagreed about \
                     {event:?} under {filter:?}"
                );
            }
        }
    }

    #[test]
    fn only_watches_on_the_same_agent_are_woken() {
        let registry = registry();
        let owner = OwnerId::local();
        let mine = AgentId::new();
        let theirs = AgentId::new();
        let descriptor = registry
            .create(owner.clone(), mine.clone(), WatchFilter::default())
            .expect("create watch");
        registry
            .create(owner, theirs, WatchFilter::default())
            .expect("create watch");

        let uris = registry.matching_uris(
            &mine,
            &event(Some("#control"), EventClass::MessageChannel, false),
            CaseMapping::default(),
        );
        assert_eq!(uris, vec![descriptor.uri]);
    }

    #[test]
    fn a_closed_watch_stops_matching_and_stops_being_described() {
        let registry = registry();
        let agent = AgentId::new();
        let descriptor = registry
            .create(OwnerId::local(), agent.clone(), WatchFilter::default())
            .expect("create watch");
        assert!(registry.close(&descriptor.watch_id));
        assert!(!registry.close(&descriptor.watch_id));
        assert!(registry.describe(&descriptor.watch_id).is_none());
        assert!(registry.touch(&descriptor.watch_id).is_none());
        assert!(
            registry
                .matching_uris(
                    &agent,
                    &event(Some("#control"), EventClass::MessageChannel, false),
                    CaseMapping::default(),
                )
                .is_empty()
        );
    }

    #[test]
    fn a_caller_cannot_hold_more_watches_than_its_own_allowance() {
        let (registry, _updates) = registry_with(WatchLimits {
            max_per_owner: 2,
            time_to_live: Duration::from_secs(60),
        });
        let mine = OwnerId::from_bearer("mine");
        let theirs = OwnerId::from_bearer("theirs");
        let agent = AgentId::new();
        for _ in 0..2 {
            registry
                .create(mine.clone(), agent.clone(), WatchFilter::default())
                .expect("within the allowance");
        }
        let refused = registry
            .create(mine.clone(), agent.clone(), WatchFilter::default())
            .expect_err("past the allowance");
        assert!(matches!(refused, GatewayError::ResourceLimit(_)));
        assert!(
            refused.to_string().contains("irc.watch.close"),
            "the refusal must say how to make room: {refused}"
        );
        // The bound is per caller, so another caller is unaffected.
        registry
            .create(theirs, agent, WatchFilter::default())
            .expect("another caller has its own allowance");
    }

    #[test]
    fn an_unused_watch_expires_and_reading_it_frees_its_slot() {
        let (registry, _updates) = registry_with(WatchLimits {
            max_per_owner: 1,
            // Already lapsed by the time anything looks at it, which is the
            // same condition an idle handle reaches later.
            time_to_live: Duration::ZERO,
        });
        let owner = OwnerId::local();
        let agent = AgentId::new();
        let descriptor = registry
            .create(owner.clone(), agent.clone(), WatchFilter::default())
            .expect("create watch");
        assert!(registry.describe(&descriptor.watch_id).is_none());
        assert!(registry.touch(&descriptor.watch_id).is_none());
        assert!(registry.list().is_empty());
        assert!(
            registry
                .matching_uris(
                    &agent,
                    &event(Some("#control"), EventClass::MessageChannel, false),
                    CaseMapping::default(),
                )
                .is_empty()
        );
        // The lapsed handle no longer occupies the allowance.
        registry
            .create(owner, agent, WatchFilter::default())
            .expect("an expired watch does not hold a slot");
    }

    #[test]
    fn using_a_watch_pushes_its_expiry_out() {
        let registry = registry();
        let descriptor = registry
            .create(OwnerId::local(), AgentId::new(), WatchFilter::default())
            .expect("create watch");
        let refreshed = registry
            .touch(&descriptor.watch_id)
            .expect("a live watch resolves");
        assert!(
            refreshed.expires_at >= descriptor.expires_at,
            "using a watch must not shorten its life"
        );
    }

    #[test]
    fn delivering_a_match_pushes_a_quiet_watchs_expiry_out() {
        // The failure this prevents: a mentions-only watch a host subscribed to
        // stays quiet for a whole time to live, lapses, and then silently
        // declines the very message it was created to catch.
        let registry = registry();
        let agent = AgentId::new();
        let descriptor = registry
            .create(OwnerId::local(), agent.clone(), WatchFilter::default())
            .expect("create watch");
        // Timestamps carry milliseconds, so the refresh needs one to land in.
        std::thread::sleep(Duration::from_millis(2));

        let woken = registry.matching_uris(
            &agent,
            &event(Some("#control"), EventClass::MessageChannel, false),
            CaseMapping::default(),
        );

        assert_eq!(woken, vec![descriptor.uri.clone()]);
        let refreshed = registry
            .describe(&descriptor.watch_id)
            .expect("a live watch resolves");
        assert!(
            refreshed.expires_at > descriptor.expires_at,
            "delivering a match is a use of the handle: {} did not move past {}",
            refreshed.expires_at,
            descriptor.expires_at
        );
    }

    #[test]
    fn traffic_a_watch_declines_does_not_extend_its_life() {
        // The other half of the definition: only traffic that actually reaches
        // a subscriber counts, so a busy channel cannot keep a watch nobody
        // reads and nothing matches alive forever.
        let registry = registry();
        let agent = AgentId::new();
        let descriptor = registry
            .create(
                OwnerId::local(),
                agent.clone(),
                WatchFilter {
                    mentions_only: true,
                    ..WatchFilter::default()
                },
            )
            .expect("create watch");
        std::thread::sleep(Duration::from_millis(2));

        let woken = registry.matching_uris(
            &agent,
            &event(Some("#control"), EventClass::MessageChannel, false),
            CaseMapping::default(),
        );

        assert!(woken.is_empty());
        assert_eq!(
            registry
                .describe(&descriptor.watch_id)
                .expect("a live watch resolves")
                .expires_at,
            descriptor.expires_at,
            "an event this watch declined is not a use of the handle"
        );
    }

    #[test]
    fn retiring_a_lapsed_watch_announces_it_once() {
        let (registry, mut updates) = registry_with(WatchLimits {
            max_per_owner: 4,
            time_to_live: Duration::ZERO,
        });
        let agent = AgentId::new();
        let descriptor = registry
            .create(OwnerId::local(), agent.clone(), WatchFilter::default())
            .expect("create watch");
        assert!(
            announced(&mut updates).is_empty(),
            "creating a watch announces nothing about expiry"
        );

        // Any write reclaims it, and the reclaim is what a subscriber hears.
        assert!(registry.touch(&descriptor.watch_id).is_none());

        assert_eq!(announced(&mut updates), vec![descriptor.uri.clone()]);
        assert!(registry.touch(&descriptor.watch_id).is_none());
        assert!(
            announced(&mut updates).is_empty(),
            "a handle is retired, and announced, exactly once"
        );
    }

    #[test]
    fn watch_uris_round_trip_through_both_of_their_forms() {
        let watch_id = WatchId::new();
        let descriptor = watch_uri(&watch_id);
        let parsed = WatchUri::from_str(&descriptor).expect("descriptor URI");
        assert_eq!(parsed.watch_id, watch_id);
        assert_eq!(parsed.target, WatchTarget::Descriptor);

        let cursor = EventCursor {
            stream_id: "a5c2f1e0-0000-4000-8000-000000000000".into(),
            sequence: 42,
        };
        let positioned = watch_events_uri(&watch_id, &cursor);
        assert!(positioned.ends_with("/events/after/a5c2f1e0-0000-4000-8000-000000000000/42"));
        let parsed = WatchUri::from_str(&positioned).expect("positioned URI");
        assert_eq!(parsed.watch_id, watch_id);
        assert_eq!(parsed.target, WatchTarget::EventsAfter(cursor));

        assert!(WatchId::from_str("not-a-watch").is_err());
        for rejected in [
            format!("irc://watches/{watch_id}/events/after/stream"),
            format!("irc://watches/{watch_id}/events/after/stream/latest"),
            format!("irc://watches/{watch_id}/events/after/stream/4/extra"),
            format!("irc://watches/{watch_id}/events"),
            format!("irc://agents/{watch_id}"),
        ] {
            assert!(
                WatchUri::from_str(&rejected).is_err(),
                "{rejected} should not parse as a watch resource"
            );
        }
    }
}
