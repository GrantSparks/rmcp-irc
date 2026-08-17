//! Progress notifications for work that stays inside one request.
//!
//! A tool that blocks for ten seconds is indistinguishable from a tool that has
//! hung. `notifications/progress` is the core-protocol answer: a client that
//! wants to see inside a call supplies a `progressToken` in the request's
//! `_meta`, and the server may then narrate that request's own stream.
//!
//! Three rules make this correct rather than merely chatty, and
//! [`ProgressReporter`] is where all three are enforced:
//!
//! 1. **Only when asked.** A notification may reference nothing but a token an
//!    active request supplied, so a request without one gets silence — and no
//!    reporter exists for that request at all, which keeps the cost of the
//!    silent case at one `Option` check rather than a channel and a task.
//! 2. **Strictly increasing.** `progress` MUST rise on every notification for a
//!    token. Milestones come from a live connection, which can report them in an
//!    order the protocol does not guarantee, so ordering is not assumed: a
//!    milestone that would not advance the count is dropped instead of sent
//!    backwards.
//! 3. **Nothing after the result.** Progress belongs to an in-flight request. A
//!    reporter is scoped to the call that created it and stops when that call
//!    returns, which also matches how the transport behaves — on Streamable HTTP
//!    progress may only travel on the originating request's response stream, and
//!    that stream ends with the result.
//!
//! The token is read through [`RequestProfile`], never from the typed
//! `params.meta`: inbound `_meta` is hoisted onto the request context during
//! deserialization, so the typed field is always `None` server-side and a server
//! reading it would silently never report anything.

use rmcp::{
    Peer, RoleServer,
    model::{ProgressNotificationParam, ProgressToken},
    service::RequestContext,
};

use crate::mcp::request_profile::RequestProfile;

/// Sends one request's progress notifications, in order, at most once each.
pub struct ProgressReporter {
    peer: Peer<RoleServer>,
    token: ProgressToken,
    total: f64,
    steps: MonotonicSteps,
}

impl ProgressReporter {
    /// Build a reporter for a request that asked for progress.
    ///
    /// Returns `None` when the caller supplied no token, which is the signal to
    /// do no progress work at all rather than to compute notifications and
    /// discard them.
    pub fn new(context: &RequestContext<RoleServer>, total: u32) -> Option<Self> {
        let profile = RequestProfile::from_context(context);
        Some(Self {
            peer: context.peer.clone(),
            token: profile.progress_token()?.clone(),
            total: f64::from(total),
            steps: MonotonicSteps::default(),
        })
    }

    /// Report reaching step `progress` of the total, described by `message`.
    ///
    /// A step that does not advance past the highest already reported is
    /// dropped: repeating or regressing a count would violate the notification
    /// contract, and the honest reading of an out-of-order milestone is that the
    /// client has already been told at least this much.
    pub async fn report(&mut self, progress: u32, message: impl Into<String>) {
        let progress = f64::from(progress);
        if !self.steps.advance(progress) {
            return;
        }
        // A client that stopped reading its own stream is not a reason to fail
        // the work it asked for.
        let _ = self
            .peer
            .notify_progress(
                ProgressNotificationParam::new(self.token.clone(), progress)
                    .with_total(self.total)
                    .with_message(message),
            )
            .await;
    }
}

/// Report one step to a reporter the request may not have asked for.
///
/// Progress is opt-in, so every site that reports is holding an `Option`.
/// Threading the option rather than the reporter keeps that check in one place
/// instead of repeating it around each milestone.
pub async fn report(
    reporter: &mut Option<ProgressReporter>,
    progress: u32,
    message: impl Into<String>,
) {
    if let Some(reporter) = reporter {
        reporter.report(progress, message).await;
    }
}

/// The ordering rule every progress token obeys.
///
/// Separate from [`ProgressReporter`] because it is the whole correctness
/// argument and the rest of the reporter is a peer this crate cannot fabricate:
/// keeping the rule in a type of its own makes it exhaustively testable.
#[derive(Debug, Default)]
struct MonotonicSteps {
    reported: f64,
}

impl MonotonicSteps {
    /// Whether `progress` may be sent, recording it when it may.
    fn advance(&mut self, progress: f64) -> bool {
        if progress <= self.reported {
            return false;
        }
        self.reported = progress;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_reported_for_a_token_only_ever_increases() {
        let mut steps = MonotonicSteps::default();
        let sent: Vec<u32> = [1, 2, 2, 5, 3, 6, 1, 7]
            .into_iter()
            .filter(|step| steps.advance(f64::from(*step)))
            .collect();
        assert_eq!(
            sent,
            vec![1, 2, 5, 6, 7],
            "an out-of-order or repeated milestone is dropped, not sent backwards"
        );
    }

    #[test]
    fn the_first_step_is_never_dropped_for_being_zero() {
        // Milestones are one-based precisely so the first report is
        // distinguishable from "nothing has happened yet".
        let mut steps = MonotonicSteps::default();
        assert!(steps.advance(1.0));
        assert!(!steps.advance(0.0));
        assert!(!steps.advance(1.0));
    }

    #[test]
    fn a_skipped_milestone_does_not_block_the_ones_after_it() {
        // Not every connect reaches every stage — a guest connection never
        // authenticates — so the sequence is increasing, not contiguous.
        let mut steps = MonotonicSteps::default();
        assert!(steps.advance(1.0));
        assert!(steps.advance(3.0));
        assert!(steps.advance(7.0));
    }
}
