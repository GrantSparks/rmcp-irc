//! Actor-owned bounded DCC session table.

use std::collections::BTreeMap;

use super::session::{DccSession, DccSessionId, DccState, DccTransitionError};
use crate::time::Timestamp;

/// In-memory sessions owned by one guest agent.
#[derive(Debug)]
pub struct DccManager {
    max_active: usize,
    max_terminal: usize,
    sessions: BTreeMap<DccSessionId, DccSession>,
}

impl DccManager {
    /// Create an empty manager bounded for active and retained terminal sessions.
    pub fn new(max_active: usize) -> Self {
        assert!(max_active > 0, "DCC session bound must be non-zero");
        Self {
            max_active,
            max_terminal: max_active,
            sessions: BTreeMap::new(),
        }
    }

    /// Number of sessions that still own or may open resources.
    pub fn active_len(&self) -> usize {
        self.sessions
            .values()
            .filter(|session| !session.state.is_terminal())
            .count()
    }

    /// Check capacity before reserving a listener to avoid orphaned socket tasks.
    pub fn ensure_capacity(&self) -> Result<(), DccManagerError> {
        if self.active_len() >= self.max_active {
            Err(DccManagerError::Limit(self.max_active))
        } else {
            Ok(())
        }
    }

    /// Retain a new offer.
    pub fn insert(&mut self, session: DccSession) -> Result<(), DccManagerError> {
        if self.sessions.contains_key(&session.id) {
            return Err(DccManagerError::Duplicate(session.id));
        }
        if !session.state.is_terminal() {
            self.ensure_capacity()?;
        }
        self.sessions.insert(session.id.clone(), session);
        self.prune_terminal();
        Ok(())
    }

    /// Resolve a session snapshot.
    pub fn get(&self, id: &DccSessionId) -> Result<&DccSession, DccManagerError> {
        self.sessions
            .get(id)
            .ok_or_else(|| DccManagerError::Unknown(id.clone()))
    }

    /// Resolve a mutable session for actor-owned metadata updates.
    pub fn get_mut(&mut self, id: &DccSessionId) -> Result<&mut DccSession, DccManagerError> {
        self.sessions
            .get_mut(id)
            .ok_or_else(|| DccManagerError::Unknown(id.clone()))
    }

    /// Apply a validated material lifecycle transition.
    pub fn transition(
        &mut self,
        id: &DccSessionId,
        state: DccState,
        now: Timestamp,
    ) -> Result<DccSession, DccManagerError> {
        self.get_mut(id)?.transition(state, now)?;
        let snapshot = self.get(id)?.clone();
        self.prune_terminal();
        Ok(snapshot)
    }

    /// Update monotonically increasing transfer progress.
    pub fn update_progress(
        &mut self,
        id: &DccSessionId,
        transferred: u64,
        now: Timestamp,
    ) -> Result<DccSession, DccManagerError> {
        let session = self.get_mut(id)?;
        if session.state != DccState::Transferring {
            return Err(DccManagerError::ProgressState(session.state));
        }
        if transferred < session.transferred_bytes
            || session.total_bytes.is_some_and(|total| transferred > total)
        {
            return Err(DccManagerError::InvalidProgress {
                previous: session.transferred_bytes,
                requested: transferred,
                total: session.total_bytes,
            });
        }
        session.transferred_bytes = transferred;
        session.updated_at = now;
        Ok(session.clone())
    }

    /// Mark a non-terminal session failed with a safe explanation.
    pub fn fail(
        &mut self,
        id: &DccSessionId,
        error: impl Into<String>,
        now: Timestamp,
    ) -> Result<DccSession, DccManagerError> {
        let session = self.get_mut(id)?;
        if !session.state.is_terminal() {
            session.transition(DccState::Failed, now)?;
        }
        session.error = Some(error.into());
        let snapshot = session.clone();
        self.prune_terminal();
        Ok(snapshot)
    }

    /// Return snapshots in deterministic handle order.
    pub fn list(&self) -> Vec<DccSession> {
        self.sessions.values().cloned().collect()
    }

    fn prune_terminal(&mut self) {
        let excess = self
            .sessions
            .values()
            .filter(|session| session.state.is_terminal())
            .count()
            .saturating_sub(self.max_terminal);
        if excess == 0 {
            return;
        }
        let mut terminal: Vec<_> = self
            .sessions
            .values()
            .filter(|session| session.state.is_terminal())
            .map(|session| (session.updated_at, session.created_at, session.id.clone()))
            .collect();
        terminal.sort();
        for (_, _, id) in terminal.into_iter().take(excess) {
            self.sessions.remove(&id);
        }
    }
}

/// Invalid DCC table operation.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DccManagerError {
    /// Configured active-session bound was reached.
    #[error("active DCC session limit reached: {0}")]
    Limit(usize),
    /// The same handle was inserted twice.
    #[error("DCC session already exists: {0}")]
    Duplicate(DccSessionId),
    /// The handle is absent or expired.
    #[error("unknown DCC session: {0}")]
    Unknown(DccSessionId),
    /// Lifecycle transition is invalid.
    #[error(transparent)]
    Transition(#[from] DccTransitionError),
    /// Progress was reported for a non-transfer session state.
    #[error("cannot report transfer progress while DCC session is {0:?}")]
    ProgressState(DccState),
    /// Progress moved backwards or beyond the advertised total.
    #[error("invalid DCC progress {requested}; previous {previous}, total {total:?}")]
    InvalidProgress {
        /// Previous byte count.
        previous: u64,
        /// Requested byte count.
        requested: u64,
        /// Advertised total.
        total: Option<u64>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dcc::session::{DccDirection, DccKind};

    fn at(second: u32) -> Timestamp {
        format!("2026-08-17T00:00:{second:02}.000Z")
            .parse()
            .expect("test instant")
    }

    fn offer(peer: &str, created: Timestamp) -> DccSession {
        DccSession::offered(DccKind::Send, DccDirection::Inbound, peer, created)
    }

    #[test]
    fn terminal_sessions_do_not_consume_active_capacity() {
        let mut manager = DccManager::new(1);
        let first = offer("one", at(1));
        let first_id = first.id.clone();
        manager.insert(first).expect("first");
        assert!(manager.insert(offer("two", at(2))).is_err());
        manager
            .transition(&first_id, DccState::Rejected, at(3))
            .expect("reject");
        manager.insert(offer("two", at(4))).expect("second");
        assert_eq!(manager.active_len(), 1);
    }

    #[test]
    fn progress_is_monotonic_and_bounded() {
        let mut manager = DccManager::new(1);
        let mut session = offer("one", at(1));
        session.total_bytes = Some(10);
        let id = session.id.clone();
        manager.insert(session).expect("insert");
        manager
            .transition(&id, DccState::Connecting, at(2))
            .expect("connect");
        manager
            .transition(&id, DccState::Transferring, at(3))
            .expect("transfer");
        manager.update_progress(&id, 5, at(4)).expect("progress");
        assert!(manager.update_progress(&id, 4, at(5)).is_err());
        assert!(manager.update_progress(&id, 11, at(5)).is_err());
    }
}
