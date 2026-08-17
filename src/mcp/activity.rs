//! Bounded unread-activity hints piggybacked on successful tool results.
//!
//! Notifications reach the *host*. The only channel guaranteed to reach the
//! model's context window is the result of an operation the model itself
//! started, and for a host that neither subscribes nor schedules a turn on a
//! notification, the model's real event loop is its own tool-call cadence. A
//! hint makes that cadence self-refreshing: `irc.send` to one channel can say,
//! in the same result, that three messages arrived in another.
//!
//! Three properties make that safe rather than merely convenient.
//!
//! **It is a mirror, never a read.** Computing a hint moves nothing. No watch,
//! no delivery cursor, and no journal position changes, so a hint can be
//! attached to ordinary successful results without a caller ever losing an event to one. The
//! counts are relative to an anchor the caller owns and only the caller moves,
//! by passing `set_activity_anchor` to `irc.events.read` or
//! `irc.attention.check`. Nothing else — no ordinary tool result, resource read,
//! or notification — advances it. Two identical calls over an unchanged stream
//! therefore produce byte-identical hints.
//!
//! **It is bounded by construction.** At most [`TARGET_CAP`] targets appear,
//! chosen by count and then by name so the choice is deterministic, with
//! `truncated` set when any were dropped. Inlined records are capped at
//! [`INLINE_MENTIONS_CAP`], are only ever mentions or DMs, and have their text
//! clipped to [`MENTION_TEXT_BYTES`]. The worst case is roughly 700 characters
//! with counts alone and roughly 1 900 with the maximum inline preference —
//! a few hundred tokens either way, on a result that would otherwise carry no
//! news at all.
//!
//! **It is suppressed only on purpose.** `[mcp] activity_hints` turns the
//! feature off for the process; the `activity` block on `irc.connect` turns it
//! off for one agent. Nothing suppresses it implicitly — least of all the
//! existence of a subscription, which proves the host can be woken and proves
//! nothing about whether it will start a model turn.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    agent::journal::{ActivityTally, EventCursor},
    mcp::conversation::CompactEvent,
};

/// Targets one hint's `unread` map may name.
///
/// Eight is more channels than a guest is usefully following at once, and the
/// map is the part of a hint that grows with the world rather than with
/// configuration, so it is the part that needs a ceiling.
pub const TARGET_CAP: usize = 8;

/// Records `activity.inline_mentions` may ask for.
///
/// Small on purpose: an inlined record is a message the caller did not ask to
/// read, arriving in a result about something else. Three is enough to notice a
/// conversation; more is a transcript, and there is a tool for that.
pub const INLINE_MENTIONS_CAP: usize = 3;

/// Bytes of an inlined record's text retained before it is clipped.
pub const MENTION_TEXT_BYTES: usize = 200;

/// Marker appended to an inlined record whose text was clipped.
const CLIPPED: &str = "…";

/// Per-agent preference for the hints attached to its tool results.
///
/// Stored on the agent handle at `irc.connect` and never changed afterwards: it
/// describes how this caller wants its own results shaped, and a preference
/// that any later call could flip would make results depend on request order.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ActivityPreference {
    /// Attach activity hints to this agent's successful tool results. Default
    /// true; `[mcp] activity_hints = false` turns them off for every agent
    /// regardless.
    pub enabled: bool,
    /// Inline up to this many of the newest records addressed to this agent —
    /// private messages, and channel messages naming its nickname — alongside
    /// the counts. Default 0; the maximum is 3.
    pub inline_mentions: usize,
}

impl Default for ActivityPreference {
    fn default() -> Self {
        Self {
            enabled: true,
            inline_mentions: 0,
        }
    }
}

impl ActivityPreference {
    /// The preference field that exceeds its published bound, if any.
    ///
    /// Refused rather than clamped: a caller that asked for ten records and
    /// silently received three would have no way to tell that from a quiet
    /// stream.
    pub const fn out_of_bounds(&self) -> Option<&'static str> {
        if self.inline_mentions > INLINE_MENTIONS_CAP {
            return Some("activity.inline_mentions");
        }
        None
    }
}

/// Compact unread hint attached to one successful tool result.
///
/// Everything here is derived; nothing here is delivery. `anchor` is where the
/// caller last said it had read to, `latest` is where the stream is now, and
/// the counts are what a watch would have selected in between.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ActivityHint {
    /// Records after `anchor` per case-folded target, such as
    /// `{"#dev": 3, "hermes": 1}`. Only targets this agent's watches select
    /// appear, and only records the agent did not send itself. At most eight
    /// entries: the busiest targets win, ties broken by name.
    pub unread: BTreeMap<String, u64>,
    /// Every record after `anchor` the watches selected, whether or not it
    /// could be keyed into `unread`.
    ///
    /// It counts targets the cap dropped, and it also counts records that have
    /// no target at all: a peer quitting or going away is conversational and is
    /// selected, but IRC gives it no channel or nickname to file it under. So
    /// `total` can exceed the sum of `unread` for two different reasons, and
    /// `{"total": 1, "unread": {}, "truncated": false}` is a legitimate hint
    /// meaning "one thing you are watching happened, and it was not addressed
    /// to any one conversation" — not a lost count.
    pub total: u64,
    /// Whether `unread` dropped targets at the cap. `total` still counts them.
    /// It is not the only reason `total` can exceed `unread`; see `total`.
    pub truncated: bool,
    /// Live watches these counts were drawn from. Zero means this agent holds
    /// no watches, which is why `unread` is empty — create one with
    /// `irc.watch.create` to make the counts meaningful.
    pub watches: usize,
    /// Position the counts are relative to. Owned by the caller: it starts at
    /// registration and moves only when an `irc.events.read` or
    /// `irc.attention.check` passes `set_activity_anchor`. Nothing else on the
    /// server touches it.
    pub anchor: EventCursor,
    /// Newest position assigned in this agent's stream, so a caller can tell an
    /// unchanged stream from a stream it has already read.
    pub latest: EventCursor,
    /// Newest records addressed to this agent, oldest first, when
    /// `activity.inline_mentions` asked for any. Text longer than 200 bytes is
    /// clipped; read the full record through `irc.events.read`.
    pub mentions: Vec<CompactEvent>,
}

impl ActivityHint {
    /// Bound one journal tally into the published hint.
    pub fn from_tally(anchor: EventCursor, watches: usize, tally: ActivityTally) -> Self {
        let (unread, truncated) = cap_targets(tally.per_target);
        Self {
            unread,
            total: tally.total,
            truncated,
            watches,
            anchor,
            latest: tally.latest,
            mentions: tally
                .mentions
                .iter()
                .filter_map(CompactEvent::project)
                .map(clip)
                .collect(),
        }
    }
}

/// Keep the busiest targets, in a deterministic order, and say so when any were
/// dropped.
///
/// Sorting by count and then by name means the same journal always produces the
/// same map: a hint that reordered itself between two identical calls would
/// make the purity this feature promises unobservable.
fn cap_targets(per_target: BTreeMap<String, u64>) -> (BTreeMap<String, u64>, bool) {
    if per_target.len() <= TARGET_CAP {
        return (per_target, false);
    }
    let mut ranked: Vec<(String, u64)> = per_target.into_iter().collect();
    ranked.sort_by(|(left_name, left), (right_name, right)| {
        right.cmp(left).then_with(|| left_name.cmp(right_name))
    });
    ranked.truncate(TARGET_CAP);
    (ranked.into_iter().collect(), true)
}

/// Clip one inlined record's text to the published byte bound.
fn clip(mut event: CompactEvent) -> CompactEvent {
    event.text = event.text.map(|text| clip_text(&text));
    event.summary = event.summary.map(|summary| clip_text(&summary));
    event
}

fn clip_text(text: &str) -> String {
    if text.len() <= MENTION_TEXT_BYTES {
        return text.to_owned();
    }
    // Back off to a character boundary so the clip never produces invalid
    // UTF-8 for a caller that counts bytes rather than characters.
    let mut end = MENTION_TEXT_BYTES;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{CLIPPED}", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::journal::{
        EventClass, EventCorrelation, EventDirection, EventOrigin, EventPayload, EventVerbosity,
        IrcEvent,
    };
    use crate::irc::semantic::{SemanticEvent, Source};
    use crate::time::Timestamp;

    fn cursor(sequence: u64) -> EventCursor {
        EventCursor {
            stream_id: "stream".into(),
            sequence,
        }
    }

    fn mention(text: &str) -> IrcEvent {
        IrcEvent {
            cursor: cursor(9),
            agent_id: crate::agent::AgentId::new(),
            direction: EventDirection::Inbound,
            class: EventClass::MessagePrivate,
            origin: EventOrigin::Live,
            verbosity: EventVerbosity::Semantic,
            target: Some("Ariadne".into()),
            server_time: None,
            received_at: Timestamp::now(),
            correlation: EventCorrelation::default(),
            semantic: Some(EventPayload::Irc(
                SemanticEvent::MessagePrivate {
                    source: Source {
                        name: "hermes".into(),
                        user: None,
                        host: None,
                        account: None,
                    },
                    target: "Ariadne".into(),
                    text: text.to_owned(),
                }
                .into(),
            )),
            wire: None,
            mentions_me: true,
        }
    }

    #[test]
    fn the_counts_map_keeps_the_busiest_targets_and_admits_dropping_the_rest() {
        let per_target: BTreeMap<String, u64> = (0..TARGET_CAP + 4)
            .map(|index| (format!("#channel{index:02}"), index as u64))
            .collect();
        let total: u64 = per_target.values().sum();
        let hint = ActivityHint::from_tally(
            cursor(1),
            2,
            ActivityTally {
                per_target,
                total,
                latest: cursor(40),
                mentions: Vec::new(),
            },
        );
        assert_eq!(hint.unread.len(), TARGET_CAP);
        assert!(hint.truncated);
        assert!(
            hint.unread.contains_key("#channel11"),
            "the busiest target survives the cap: {:?}",
            hint.unread
        );
        assert!(
            !hint.unread.contains_key("#channel00"),
            "the quietest target is the one dropped: {:?}",
            hint.unread
        );
        assert_eq!(
            hint.total, total,
            "the total never loses what the map drops"
        );
    }

    #[test]
    fn a_small_counts_map_is_returned_whole_and_unmarked() {
        let per_target = BTreeMap::from([("#dev".to_owned(), 3), ("hermes".to_owned(), 1)]);
        let hint = ActivityHint::from_tally(
            cursor(1),
            1,
            ActivityTally {
                per_target: per_target.clone(),
                total: 4,
                latest: cursor(5),
                mentions: Vec::new(),
            },
        );
        assert_eq!(hint.unread, per_target);
        assert!(!hint.truncated);
    }

    #[test]
    fn an_inlined_record_is_clipped_on_a_character_boundary() {
        let long = "ω".repeat(MENTION_TEXT_BYTES);
        let hint = ActivityHint::from_tally(
            cursor(1),
            1,
            ActivityTally {
                per_target: BTreeMap::new(),
                total: 1,
                latest: cursor(9),
                mentions: vec![mention(&long)],
            },
        );
        let text = hint.mentions[0].text.as_deref().expect("inlined text");
        assert!(text.ends_with(CLIPPED), "{text}");
        assert!(text.len() <= MENTION_TEXT_BYTES + CLIPPED.len());
        assert!(text.trim_end_matches(CLIPPED).chars().all(|c| c == 'ω'));
    }

    #[test]
    fn a_preference_beyond_the_published_bound_is_named_rather_than_clamped() {
        let asked = ActivityPreference {
            enabled: true,
            inline_mentions: INLINE_MENTIONS_CAP + 1,
        };
        assert_eq!(asked.out_of_bounds(), Some("activity.inline_mentions"));
        assert_eq!(ActivityPreference::default().out_of_bounds(), None);
        assert!(ActivityPreference::default().enabled);
        assert_eq!(ActivityPreference::default().inline_mentions, 0);
    }
}
