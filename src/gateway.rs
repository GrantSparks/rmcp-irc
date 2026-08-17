//! Routing from opaque handles to supervised IRC agents.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use tokio::sync::{RwLock, Semaphore, broadcast};

use crate::{
    agent::{
        AgentId,
        actor::{
            AgentHandle, AgentSnapshot, CompletionMode, DccAcceptRequest, DisconnectReceipt,
            ExecuteRequest, RegistrationRequest,
        },
        journal::{CursorStatus, EventCursor, EventFilter, EventPage, EventVerbosity, RecentQuery},
    },
    config::Config,
    dcc::{
        session::{DccKind, DccSession, DccSessionId, DccState},
        transfer::DestinationConflict,
    },
    error::{GatewayError, Result},
    irc::{
        correlation::CommandResult,
        isupport::CaseMapping,
        registration::{NickConflictPolicy, Nickname},
        target::ChannelName,
        wire::OutboundMessage,
    },
    mcp::{
        conversation::CompactEvent,
        resources::{ConversationResource, WireResource},
        watch::{WatchDescriptor, WatchFilter, WatchId, WatchRegistry, WatchResource},
    },
};

/// Validated caller choices for one provisional guest actor.
#[derive(Clone, Debug)]
pub struct ConnectRequest {
    /// Preferred nickname.
    pub nickname: Nickname,
    /// Ordered explicit alternatives.
    pub nickname_fallbacks: Vec<Nickname>,
    /// Suffix or fail collision handling.
    pub nick_conflict_policy: NickConflictPolicy,
    /// Optional USER username override.
    pub username: Option<String>,
    /// Optional USER real-name override.
    pub real_name: Option<String>,
    /// Additional initial channels.
    pub channels: BTreeSet<ChannelName>,
}

/// Which conversational window a resource read wants.
#[derive(Clone, Copy, Debug)]
pub enum ConversationWindow<'a> {
    /// Everything addressed to the agent, across every channel and peer.
    Inbox,
    /// One channel or peer.
    Target(&'a str),
}

/// Published agent and its initial registration details.
#[derive(Clone, Debug)]
pub struct ConnectedAgent {
    /// Opaque routing handle.
    pub agent_id: AgentId,
    /// Final accepted nickname.
    pub nickname: Nickname,
    /// Whether the preferred candidate changed.
    pub nickname_adjusted: bool,
    /// Complete initial MOTD.
    pub motd: crate::agent::state::MotdState,
}

/// Shared in-memory gateway for all MCP transports.
#[derive(Debug)]
pub struct Gateway {
    config: Arc<Config>,
    agents: RwLock<BTreeMap<AgentId, AgentHandle>>,
    capacity: Arc<Semaphore>,
    resource_updates: broadcast::Sender<String>,
    watches: Arc<WatchRegistry>,
}

impl Gateway {
    /// Create an empty gateway.
    pub fn new(config: Config) -> Self {
        let capacity = Arc::new(Semaphore::new(config.limits.max_agents));
        let update_capacity = config.limits.command_queue.max(16);
        let (resource_updates, _) = broadcast::channel(update_capacity);
        Self {
            config: Arc::new(config),
            agents: RwLock::new(BTreeMap::new()),
            capacity,
            resource_updates,
            watches: Arc::new(WatchRegistry::new()),
        }
    }

    /// Shared registry of watch handles.
    pub fn watches(&self) -> &Arc<WatchRegistry> {
        &self.watches
    }

    /// Shared immutable process configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Subscribe to coalescing stable-resource URI changes.
    pub fn subscribe_resource_updates(&self) -> broadcast::Receiver<String> {
        self.resource_updates.subscribe()
    }

    /// Number of published guest actors.
    #[cfg(test)]
    pub async fn agent_count(&self) -> usize {
        self.agents.read().await.len()
    }

    /// Register a guest, publishing its handle only after success.
    pub async fn connect(&self, request: ConnectRequest) -> Result<ConnectedAgent> {
        validate_identity_field(request.username.as_deref(), "username", false)?;
        validate_identity_field(request.real_name.as_deref(), "real name", true)?;
        let permit = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| GatewayError::ResourceLimit("maximum agent count reached".into()))?;
        let agent_id = AgentId::new();
        let username = request
            .username
            .unwrap_or_else(|| self.config.onboarding.username(agent_id.as_str()));
        let real_name = request.real_name.unwrap_or_else(|| {
            self.config
                .onboarding
                .real_name(agent_id.as_str(), request.nickname.as_str())
        });
        let channels = self
            .config
            .onboarding
            .initial_channels
            .iter()
            .filter_map(|channel| ChannelName::new(channel.clone()).ok())
            .chain(request.channels)
            .collect();
        let registration = RegistrationRequest {
            nickname: request.nickname,
            nickname_fallbacks: request.nickname_fallbacks,
            nick_conflict_policy: request.nick_conflict_policy,
            username,
            real_name,
            channels,
        };
        let (handle, actor, ready) = AgentHandle::spawn(
            agent_id.clone(),
            self.config.clone(),
            registration,
            self.resource_updates.clone(),
            self.watches.clone(),
            permit,
        );
        tokio::spawn(actor.run());
        let receipt = ready
            .await
            .map_err(|_| GatewayError::ActorStopped(agent_id.clone()))??;
        self.agents.write().await.insert(agent_id.clone(), handle);
        let _ = self.resource_updates.send("irc://agents".into());
        Ok(ConnectedAgent {
            agent_id,
            nickname: receipt.nickname,
            nickname_adjusted: receipt.nickname_adjusted,
            motd: receipt.motd,
        })
    }

    /// Return a consistent actor-owned status/resource snapshot.
    pub async fn snapshot(&self, agent_id: &AgentId) -> Result<AgentSnapshot> {
        self.resolve(agent_id).await?.snapshot().await
    }

    /// Execute one structured upstream operation.
    pub async fn execute(
        &self,
        agent_id: &AgentId,
        message: OutboundMessage,
        completion_mode: CompletionMode,
        timeout: Duration,
    ) -> Result<CommandResult> {
        self.resolve(agent_id)
            .await?
            .execute(ExecuteRequest {
                message,
                completion_mode,
                timeout,
            })
            .await
    }

    /// Read events after a caller-owned cursor.
    pub async fn read_events(
        &self,
        agent_id: &AgentId,
        cursor: Option<EventCursor>,
        limit: usize,
        wait: Duration,
        filter: EventFilter,
    ) -> Result<EventPage> {
        self.resolve(agent_id)
            .await?
            .read_events(cursor, limit, wait, filter)
            .await
    }

    /// Serve one conversational or diagnostic resource that is a window over
    /// the journal rather than a field of the actor snapshot.
    ///
    /// These read from the newest end of the stream. A mention or a quiet
    /// channel is exactly the case where the oldest hundred retained records
    /// contain nothing, so a snapshot-shaped preview would show an empty
    /// transcript for a conversation that is actively happening.
    pub async fn read_conversation(
        &self,
        agent_id: &AgentId,
        window: ConversationWindow<'_>,
    ) -> Result<ConversationResource> {
        let handle = self.resolve(agent_id).await?;
        let snapshot = handle.snapshot().await?;
        let limit = self.config.limits.max_event_page_size;
        let (target, query) = match window {
            ConversationWindow::Inbox => (None, RecentQuery::mentions()),
            // Targets are compared with the server's own mapping, so a
            // transcript URI spelled `#Control` finds `#control`.
            ConversationWindow::Target(target) => (
                Some(target.to_owned()),
                RecentQuery::for_target(target, case_mapping_of(&snapshot)),
            ),
        };
        let events = handle.read_recent(limit, query).await?;
        let through_cursor = events.last().map(|event| event.cursor.clone());
        Ok(ConversationResource {
            target,
            events: events.iter().filter_map(CompactEvent::project).collect(),
            through_cursor,
            journal: snapshot.journal,
        })
    }

    /// Serve the lossless wire-diagnostics window.
    pub async fn read_wire(&self, agent_id: &AgentId) -> Result<WireResource> {
        let handle = self.resolve(agent_id).await?;
        let snapshot = handle.snapshot().await?;
        let events = handle
            .read_recent(
                self.config.limits.max_event_page_size,
                RecentQuery::default(),
            )
            .await?;
        Ok(WireResource {
            events: events
                .into_iter()
                .filter(|event| event.wire.is_some() || event.verbosity == EventVerbosity::Wire)
                .collect(),
            journal: snapshot.journal,
            line_budget: snapshot.line_budget,
        })
    }

    /// Register a watch over one agent's stream.
    ///
    /// Resolving the agent first means an unusable handle fails here rather
    /// than on the caller's first read of a watch that could never produce
    /// anything.
    pub async fn create_watch(
        &self,
        agent_id: &AgentId,
        filter: WatchFilter,
        cursor: Option<EventCursor>,
    ) -> Result<WatchDescriptor> {
        self.resolve(agent_id).await?;
        let descriptor = self.watches.create(agent_id.clone(), filter, cursor);
        let _ = self.resource_updates.send("irc://agents".into());
        Ok(descriptor)
    }

    /// Release a watch handle.
    pub fn close_watch(&self, watch_id: &WatchId) -> Result<()> {
        if !self.watches.close(watch_id) {
            return Err(GatewayError::WatchNotFound(watch_id.clone()));
        }
        let _ = self.resource_updates.send("irc://agents".into());
        Ok(())
    }

    /// Read everything a watch has not yet delivered, advancing its position.
    ///
    /// The journal read itself is unfiltered so the stored position advances
    /// over records the watch does not want; otherwise a quiet watch on a busy
    /// stream would rescan the same events forever. Selection is applied
    /// afterwards, which is also what lets the compact projection drop protocol
    /// records without those records blocking the cursor.
    pub async fn read_watch(&self, watch_id: &WatchId) -> Result<WatchResource> {
        let (agent_id, filter, cursor) = self
            .watches
            .position(watch_id)
            .ok_or_else(|| GatewayError::WatchNotFound(watch_id.clone()))?;
        let handle = self.resolve(&agent_id).await?;
        let snapshot = handle.snapshot().await?;
        let case_mapping = case_mapping_of(&snapshot);
        let limit = self.config.limits.max_event_page_size;

        // Keep reading while the page is entirely uninteresting, so a
        // notification never resolves to an empty read while matching records
        // sit just past the first page. Bounded by the retained window, since
        // every pass consumes at least one record.
        let mut cursor = cursor;
        // The opening read decides the status: a gap or a stream reset relative
        // to the position the watch actually held is what the caller needs to
        // hear about, and later passes are all continuations of this same read.
        let mut status = None;
        let mut events = Vec::new();
        let has_more = loop {
            let page = handle
                .read_events(
                    cursor.clone(),
                    limit,
                    Duration::ZERO,
                    EventFilter::default(),
                )
                .await?;
            status.get_or_insert(page.status);
            let advanced = page.next_cursor.clone();
            events.extend(
                page.events
                    .iter()
                    .filter(|event| filter.matches(event, case_mapping))
                    .filter_map(CompactEvent::project),
            );
            let more = advanced.sequence < page.latest.sequence;
            // A pass that could not move is exhausted even if the journal
            // reports later records, so this always terminates.
            let stalled = cursor.as_ref() == Some(&advanced);
            cursor = Some(advanced);
            if !events.is_empty() || stalled || !more {
                break more;
            }
        };

        let status = status.unwrap_or(CursorStatus::Current);
        let next_cursor = cursor.expect("a watch read always advances to a cursor");
        self.watches.advance(watch_id, next_cursor.clone());
        let watch = self
            .watches
            .describe(watch_id)
            .ok_or_else(|| GatewayError::WatchNotFound(watch_id.clone()))?;
        Ok(WatchResource {
            watch,
            status,
            events,
            next_cursor,
            has_more,
        })
    }

    /// Invalidate an agent handle and request clean shutdown.
    pub async fn disconnect(
        &self,
        agent_id: &AgentId,
        reason: Option<String>,
    ) -> Result<DisconnectReceipt> {
        let actor = self
            .agents
            .write()
            .await
            .remove(agent_id)
            .ok_or_else(|| GatewayError::AgentNotFound(agent_id.clone()))?;
        let result = actor.disconnect(reason).await;
        // Watches outlive nothing: their stream is gone, so leaving them
        // registered would only produce handles that can never be read.
        self.watches.close_agent(agent_id);
        let _ = self.resource_updates.send("irc://agents".into());
        result
    }

    /// Remove every published handle and ask each actor to send a clean QUIT.
    pub async fn shutdown_all(&self, reason: Option<String>) -> usize {
        let actors = {
            let mut agents = self.agents.write().await;
            std::mem::take(&mut *agents)
        };
        let count = actors.len();
        for agent_id in actors.keys() {
            self.watches.close_agent(agent_id);
        }
        let _ = self.resource_updates.send("irc://agents".into());
        let mut shutdowns = futures_util::stream::FuturesUnordered::new();
        for (agent_id, actor) in actors {
            let reason = reason.clone();
            shutdowns.push(async move { (agent_id, actor.disconnect(reason).await) });
        }
        while let Some((agent_id, result)) = futures_util::StreamExt::next(&mut shutdowns).await {
            if let Err(error) = result {
                tracing::warn!(%agent_id, %error, "could not cleanly stop IRC agent");
            }
        }
        count
    }

    /// Open an outbound direct-chat offer.
    pub async fn dcc_chat_open(
        &self,
        agent_id: &AgentId,
        peer: String,
        reverse: bool,
    ) -> Result<DccSession> {
        self.resolve(agent_id)
            .await?
            .dcc_chat_open(peer, reverse)
            .await
    }

    /// Queue one direct-chat line.
    pub async fn dcc_chat_send(
        &self,
        agent_id: &AgentId,
        session_id: DccSessionId,
        text: String,
    ) -> Result<()> {
        self.resolve(agent_id)
            .await?
            .dcc_chat_send(session_id, text)
            .await
    }

    /// Offer one local file to a direct peer.
    pub async fn dcc_send(
        &self,
        agent_id: &AgentId,
        peer: String,
        source_path: std::path::PathBuf,
        filename: Option<String>,
        reverse: bool,
    ) -> Result<DccSession> {
        self.resolve(agent_id)
            .await?
            .dcc_send(peer, source_path, filename, reverse)
            .await
    }

    /// Accept one incoming direct offer.
    pub async fn dcc_accept(
        &self,
        agent_id: &AgentId,
        session_id: DccSessionId,
        destination_path: Option<std::path::PathBuf>,
        conflict: DestinationConflict,
    ) -> Result<DccSession> {
        self.resolve(agent_id)
            .await?
            .dcc_accept(DccAcceptRequest {
                session_id,
                destination_path,
                conflict,
            })
            .await
    }

    /// Reject one incoming offered direct session.
    pub async fn dcc_reject(
        &self,
        agent_id: &AgentId,
        session_id: DccSessionId,
    ) -> Result<DccSession> {
        self.resolve(agent_id).await?.dcc_reject(session_id).await
    }

    /// Cancel one active or offered direct session.
    pub async fn dcc_cancel(
        &self,
        agent_id: &AgentId,
        session_id: DccSessionId,
    ) -> Result<DccSession> {
        self.resolve(agent_id).await?.dcc_cancel(session_id).await
    }

    /// Filter retained direct sessions in deterministic handle order.
    pub async fn dcc_list(
        &self,
        agent_id: &AgentId,
        state: Option<DccState>,
        kind: Option<DccKind>,
        peer: Option<&str>,
    ) -> Result<Vec<DccSession>> {
        let snapshot = self.snapshot(agent_id).await?;
        Ok(snapshot
            .dcc_sessions
            .into_iter()
            .filter(|session| state.is_none_or(|value| session.state == value))
            .filter(|session| kind.is_none_or(|value| session.kind == value))
            .filter(|session| peer.is_none_or(|value| session.peer.eq_ignore_ascii_case(value)))
            .collect())
    }

    /// Return published handles in deterministic order.
    pub async fn agent_ids(&self) -> Vec<AgentId> {
        self.agents.read().await.keys().cloned().collect()
    }

    async fn resolve(&self, agent_id: &AgentId) -> Result<AgentHandle> {
        self.agents
            .read()
            .await
            .get(agent_id)
            .cloned()
            .ok_or_else(|| GatewayError::AgentNotFound(agent_id.clone()))
    }
}

/// The server's advertised nickname/channel comparison rule, or the RFC
/// default when it has not advertised one.
fn case_mapping_of(snapshot: &AgentSnapshot) -> CaseMapping {
    snapshot
        .protocol
        .isupport
        .get("CASEMAPPING")
        .and_then(|token| token.value.as_deref())
        .map(CaseMapping::parse)
        .unwrap_or_default()
}

fn validate_identity_field(value: Option<&str>, name: &str, allow_spaces: bool) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_empty()
        || value
            .bytes()
            .any(|byte| matches!(byte, b'\0' | b'\r' | b'\n') || (!allow_spaces && byte == b' '))
    {
        return Err(GatewayError::InvalidMessage(format!(
            "IRC {name} is empty or contains a forbidden wire character"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_fields_reject_wire_delimiters() {
        assert!(validate_identity_field(Some("bad\nname"), "username", false).is_err());
        assert!(validate_identity_field(Some("two words"), "username", false).is_err());
        assert!(validate_identity_field(Some("two words"), "real name", true).is_ok());
    }
}
