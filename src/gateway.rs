//! Routing from opaque handles to supervised IRC agents.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use tokio::sync::{RwLock, Semaphore, broadcast, mpsc};

use crate::{
    agent::{
        AgentId,
        actor::{
            AgentHandle, AgentSnapshot, CompletionMode, ConnectMilestone, DccAcceptRequest,
            DisconnectReceipt, ExecuteRequest, RegistrationRequest,
        },
        journal::{CursorQuery, EventCursor, EventFilter, EventPage, EventVerbosity, RecentQuery},
    },
    config::Config,
    dcc::{
        confine::ReceiveChoice,
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
        activity::{ActivityHint, ActivityPreference},
        authorization::{OwnerId, not_authorized},
        confirm_action::RedeemedConfirmations,
        conversation::CompactEvent,
        mrtr::RequestStateSealer,
        resources::{ConversationResource, WireResource},
        tasks::TaskLedger,
        watch::{
            WatchDescriptor, WatchEventsResource, WatchFilter, WatchId, WatchLimits, WatchRegistry,
            WatchResource,
        },
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
    /// How this agent's tool results should carry activity hints.
    pub activity: ActivityPreference,
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

/// A newly registered watch and the position its stream had at that moment.
#[derive(Clone, Debug)]
pub struct WatchCreation {
    /// The registered watch.
    pub watch: WatchDescriptor,
    /// Latest journal position when the watch was created, so a caller can
    /// start from "now" instead of from the oldest retained record.
    pub latest_cursor: EventCursor,
}

/// One published agent, the caller identity that created it, and the two pieces
/// of activity-hint state that belong to that caller rather than to the stream.
#[derive(Clone, Debug)]
struct OwnedAgent {
    owner: OwnerId,
    handle: AgentHandle,
    /// How this caller asked for its hints, fixed at registration.
    activity: ActivityPreference,
    /// The position hint counts are measured from. Born as the stream's
    /// position at registration and moved only by an explicit
    /// `set_activity_anchor` on an event or attention read. It is not a delivery
    /// cursor and nothing reads through it.
    activity_anchor: EventCursor,
}

/// Shared in-memory gateway for all MCP transports.
#[derive(Debug)]
pub struct Gateway {
    config: Arc<Config>,
    agents: RwLock<BTreeMap<AgentId, OwnedAgent>>,
    capacity: Arc<Semaphore>,
    resource_updates: broadcast::Sender<String>,
    watches: Arc<WatchRegistry>,
    request_states: RequestStateSealer,
    redeemed_confirmations: RedeemedConfirmations,
    tasks: TaskLedger,
}

impl Gateway {
    /// Create an empty gateway.
    pub fn new(config: Config) -> Self {
        let capacity = Arc::new(Semaphore::new(config.limits.max_agents));
        let update_capacity = config.limits.command_queue.max(16);
        let (resource_updates, _) = broadcast::channel(update_capacity);
        let watch_limits = WatchLimits {
            max_per_owner: config.limits.max_watches_per_owner,
            time_to_live: Duration::from_millis(config.limits.watch_ttl_ms),
        };
        // The registry publishes an expiring handle onto the same channel every
        // other resource change travels on, so a subscriber learns that its
        // watch lapsed instead of waiting on one that no longer exists.
        let watches = Arc::new(WatchRegistry::new(watch_limits, resource_updates.clone()));
        Self {
            config: Arc::new(config),
            agents: RwLock::new(BTreeMap::new()),
            capacity,
            resource_updates,
            watches,
            request_states: RequestStateSealer::generate(),
            redeemed_confirmations: RedeemedConfirmations::default(),
            tasks: TaskLedger::new(),
        }
    }

    /// Shorten the task retention window.
    ///
    /// Expiry is only observable in a test that does not wait out the
    /// production TTL, and the ledger is constructed here rather than injected
    /// because nothing outside a test has any business choosing it.
    #[cfg(test)]
    pub fn with_task_ttl(mut self, ttl: Duration) -> Self {
        self.tasks = TaskLedger::with_ttl(ttl);
        self
    }

    /// Shared registry of watch handles.
    pub fn watches(&self) -> &Arc<WatchRegistry> {
        &self.watches
    }

    /// The one task ledger this process serves.
    ///
    /// It lives here for the same reason the request-state key does: Streamable
    /// HTTP builds a fresh handler for every POST, so a task created while
    /// answering one request must be found by the `tasks/get` that arrives as
    /// the next one.
    pub fn tasks(&self) -> &TaskLedger {
        &self.tasks
    }

    /// Sealer for multi round-trip request state.
    ///
    /// It lives here because the gateway is the one object every per-request
    /// service instance shares: Streamable HTTP builds a fresh handler for each
    /// POST, so a key held by the handler would change between the round that
    /// sealed a state and the round that must open it.
    pub fn request_states(&self) -> &RequestStateSealer {
        &self.request_states
    }

    /// Confirmations already acted on, so one approval applies one action.
    ///
    /// Beside the sealer for the same reason: the round that mints a
    /// confirmation and the round that spends it are separate POSTs served by
    /// separate handlers, so a ledger held by the handler would forget every
    /// approval the instant it was given.
    pub fn redeemed_confirmations(&self) -> &RedeemedConfirmations {
        &self.redeemed_confirmations
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

    /// Register a guest owned by the local caller.
    #[cfg(test)]
    pub async fn connect(&self, request: ConnectRequest) -> Result<ConnectedAgent> {
        self.connect_as(OwnerId::local(), request, None).await
    }

    /// Register a guest, publishing its handle only after success and
    /// recording the caller that may use it.
    ///
    /// `milestones` receives the stages of this one connect attempt while it is
    /// still in flight. It exists because registration is a multi-round-trip
    /// wait that this method resolves in a single `await`: the caller cannot see
    /// inside it otherwise, and the actor that does has no MCP peer to tell.
    /// Passing `None` — which every caller that was not asked for progress
    /// does — leaves the actor with nothing to publish to.
    pub async fn connect_as(
        &self,
        owner: OwnerId,
        request: ConnectRequest,
        milestones: Option<mpsc::Sender<ConnectMilestone>>,
    ) -> Result<ConnectedAgent> {
        validate_identity_field(request.username.as_deref(), "username", false)?;
        validate_identity_field(request.real_name.as_deref(), "real name", true)?;
        let activity = request.activity;
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
            milestones,
        );
        tokio::spawn(actor.run());
        let receipt = ready
            .await
            .map_err(|_| GatewayError::ActorStopped(agent_id.clone()))??;
        // The anchor is born here, at the position registration left behind, so
        // the first hint this caller sees counts what arrived after it
        // connected rather than the MOTD it has already been shown.
        let activity_anchor = handle.snapshot().await?.journal.latest;
        self.agents.write().await.insert(
            agent_id.clone(),
            OwnedAgent {
                owner,
                handle,
                activity,
                activity_anchor,
            },
        );
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
            .read_events(cursor, limit, wait, CursorQuery::filtered(filter))
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

    /// Measure one agent's unread activity against its caller's anchor.
    ///
    /// Pure, and deliberately so. It reads the watch registry without touching
    /// any handle's time to live, tallies the journal without moving any
    /// position, and returns `None` — never an error — for every reason a hint
    /// might not exist: the deployment turned hints off, this caller turned them
    /// off for this agent, the handle is gone, or the actor stopped. A hint is a
    /// courtesy attached to somebody else's result, so nothing about failing to
    /// produce one may change that result.
    pub async fn activity_hint(&self, agent_id: &AgentId) -> Option<ActivityHint> {
        if !self.config.mcp.activity_hints {
            return None;
        }
        let (handle, preference, anchor) = {
            let agents = self.agents.read().await;
            let agent = agents.get(agent_id)?;
            (
                agent.handle.clone(),
                agent.activity,
                agent.activity_anchor.clone(),
            )
        };
        if !preference.enabled {
            return None;
        }
        let filters = self.watches.filters_for(agent_id);
        let watches = filters.len();
        let tally = handle
            .tally_activity(Some(anchor.clone()), filters, preference.inline_mentions)
            .await
            .ok()?;
        Some(ActivityHint::from_tally(anchor, watches, tally))
    }

    /// Record where this caller says it has read to.
    ///
    /// The only writer of the anchor in the whole gateway. Both model-facing
    /// read tools call it only when their explicit flag asks them to. Everything
    /// else that looks like progress — an ordinary tool result, a resource read,
    /// a notification, a watch window — leaves it exactly where the caller put
    /// it, which is what makes two identical calls produce two identical hints.
    pub async fn set_activity_anchor(&self, agent_id: &AgentId, cursor: EventCursor) {
        if let Some(agent) = self.agents.write().await.get_mut(agent_id) {
            agent.activity_anchor = cursor;
        }
    }

    /// Register a watch over one agent's stream.
    ///
    /// Resolving the agent first means an unusable handle fails here rather
    /// than on the caller's first read of a watch that could never produce
    /// anything. The watch is charged to whoever owns that agent, so the
    /// per-caller bound holds however many agents the caller has.
    ///
    /// The stream's current latest position comes back with the descriptor
    /// because a watch keeps no position of its own: without it a caller's only
    /// starting point would be the oldest retained record, and the common
    /// intention — "tell me about things from now on" — would need a throwaway
    /// read to establish.
    pub async fn create_watch(
        &self,
        agent_id: &AgentId,
        filter: WatchFilter,
    ) -> Result<WatchCreation> {
        let handle = self.resolve(agent_id).await?;
        let owner = self
            .agents
            .read()
            .await
            .get(agent_id)
            .map(|agent| agent.owner.clone())
            .ok_or_else(|| GatewayError::AgentNotFound(agent_id.clone()))?;
        let latest_cursor = handle.snapshot().await?.journal.latest;
        let watch = self.watches.create(owner, agent_id.clone(), filter)?;
        let _ = self.resource_updates.send("irc://agents".into());
        Ok(WatchCreation {
            watch,
            latest_cursor,
        })
    }

    /// Release a watch handle.
    pub fn close_watch(&self, watch_id: &WatchId) -> Result<()> {
        if !self.watches.close(watch_id) {
            return Err(GatewayError::WatchNotFound(watch_id.clone()));
        }
        let _ = self.resource_updates.send("irc://agents".into());
        Ok(())
    }

    /// Describe a watch: its selection, the health of the stream it selects
    /// from, and where to read what it selected.
    ///
    /// Pure with respect to delivery. A watch holds no position, so a host may
    /// read this URI whenever it likes — refreshing a panel, resynchronizing
    /// after dropped notifications, two components on the same URI — and no
    /// caller loses an event for it. The only effect is that the handle's time
    /// to live is refreshed, which is what "in use" has to mean for a handle
    /// nobody is obliged to close.
    pub async fn read_watch(&self, watch_id: &WatchId) -> Result<WatchResource> {
        let watch = self.touch_watch(watch_id)?;
        let journal = self
            .resolve(&watch.agent_id)
            .await?
            .snapshot()
            .await?
            .journal;
        Ok(WatchResource::describe(watch, journal))
    }

    /// Read the events one watch selects, from an explicit caller-owned cursor.
    ///
    /// The watch supplies the selection and nothing else. Two consumers of one
    /// watch therefore never disturb each other, and a caller that lost a
    /// response recovers it by presenting the same cursor again. Long polling
    /// works exactly as it does for an unfiltered read, because the parked read
    /// keeps this selection and re-runs it when the journal grows.
    pub async fn read_watch_events(
        &self,
        agent_id: &AgentId,
        watch_id: &WatchId,
        cursor: Option<EventCursor>,
        limit: usize,
        wait: Duration,
    ) -> Result<EventPage> {
        let watch = self.touch_watch(watch_id)?;
        if watch.agent_id != *agent_id {
            return Err(GatewayError::InvalidMessage(format!(
                "watch {watch_id} selects from agent {} rather than {agent_id}",
                watch.agent_id
            )));
        }
        let handle = self.resolve(&watch.agent_id).await?;
        let case_mapping = case_mapping_of(&handle.snapshot().await?);
        handle
            .read_events(cursor, limit, wait, watch.filter.cursor_query(case_mapping))
            .await
    }

    /// Read one compound attention watch and advance only the returned journal
    /// page; the MCP projection decides whether a fully drained page may adopt
    /// the stream high-water mark as its next checkpoint.
    ///
    /// Unlike the general watch path, every selected record must have a compact
    /// attention projection: conversation or a sparse actionable lifecycle
    /// summary. This keeps a scheduled quiet check small and ensures its
    /// checkpoint never advances over a selected event the model result omitted.
    pub async fn read_attention_events(
        &self,
        agent_id: &AgentId,
        watch_id: &WatchId,
        cursor: EventCursor,
        limit: usize,
        wait: Duration,
    ) -> Result<EventPage> {
        let watch = self.touch_watch(watch_id)?;
        if watch.agent_id != *agent_id {
            return Err(GatewayError::InvalidMessage(format!(
                "watch {watch_id} selects from agent {} rather than {agent_id}",
                watch.agent_id
            )));
        }
        if watch.filter.attention.is_none() {
            return Err(GatewayError::InvalidMessage(format!(
                "watch {watch_id} is a general watch; create model attention with \
                 irc.attention.open"
            )));
        }
        let handle = self.resolve(&watch.agent_id).await?;
        let case_mapping = case_mapping_of(&handle.snapshot().await?);
        let query = watch.filter.cursor_query(case_mapping);
        handle.read_events(Some(cursor), limit, wait, query).await
    }

    /// Read one positioned, compact window of what a watch selects.
    ///
    /// The position arrives in the resource URI, so the read is idempotent: the
    /// same URI always answers with the same window. Ordinary watches exclude
    /// records with no conversational form during selection. Attention watches
    /// admit only conversation plus lifecycle classes with compact attention
    /// projections. In both cases, `next_cursor` advances over events the caller
    /// actually received.
    pub async fn read_watch_window(
        &self,
        watch_id: &WatchId,
        cursor: EventCursor,
    ) -> Result<WatchEventsResource> {
        let watch = self.touch_watch(watch_id)?;
        let handle = self.resolve(&watch.agent_id).await?;
        let snapshot = handle.snapshot().await?;
        let mut query = watch.filter.cursor_query(case_mapping_of(&snapshot));
        query.conversational_only = watch.filter.attention.is_none();
        let page = handle
            .read_events(
                Some(cursor.clone()),
                self.config.limits.max_event_page_size,
                Duration::ZERO,
                query,
            )
            .await?;
        Ok(WatchEventsResource::from_page(watch, cursor, page))
    }

    /// Resolve a watch handle for consumption, refreshing its time to live.
    fn touch_watch(&self, watch_id: &WatchId) -> Result<WatchDescriptor> {
        self.watches
            .touch(watch_id)
            .ok_or_else(|| GatewayError::WatchNotFound(watch_id.clone()))
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
            .ok_or_else(|| GatewayError::AgentNotFound(agent_id.clone()))?
            .handle;
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
        let actors: BTreeMap<AgentId, AgentHandle> = actors
            .into_iter()
            .map(|(agent_id, agent)| (agent_id, agent.handle))
            .collect();
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
    ///
    /// `destination` names a configured receive root and a path relative to it,
    /// and is required for SEND. Confinement is settled inside the owning
    /// actor, where the configuration lives, so no caller of this can hand a
    /// resolved path down.
    pub async fn dcc_accept(
        &self,
        agent_id: &AgentId,
        session_id: DccSessionId,
        destination: Option<ReceiveChoice>,
        conflict: DestinationConflict,
    ) -> Result<DccSession> {
        self.resolve(agent_id)
            .await?
            .dcc_accept(DccAcceptRequest {
                session_id,
                destination,
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

    /// Read one retained direct session.
    pub async fn dcc_session(
        &self,
        agent_id: &AgentId,
        session_id: &DccSessionId,
    ) -> Result<DccSession> {
        self.snapshot(agent_id)
            .await?
            .dcc_sessions
            .into_iter()
            .find(|session| session.id == *session_id)
            .ok_or_else(|| GatewayError::Dcc(format!("unknown direct session: {session_id}")))
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

    /// Return the handles one caller owns, in deterministic order.
    pub async fn agent_ids_for(&self, owner: &OwnerId) -> Vec<AgentId> {
        self.agents
            .read()
            .await
            .iter()
            .filter(|(_, agent)| agent.owner == *owner)
            .map(|(agent_id, _)| agent_id.clone())
            .collect()
    }

    /// Confirm one caller may use an agent handle.
    ///
    /// A handle owned by somebody else fails exactly as a handle that does not
    /// exist, so this can never be used to discover other callers' agents.
    pub async fn authorize_agent(
        &self,
        owner: &OwnerId,
        agent_id: &AgentId,
    ) -> std::result::Result<(), rmcp::ErrorData> {
        match self.agents.read().await.get(agent_id) {
            Some(agent) if agent.owner == *owner => Ok(()),
            _ => Err(not_authorized(agent_id.as_str())),
        }
    }

    /// Confirm one caller may use a watch handle, through the agent it watches.
    pub async fn authorize_watch(
        &self,
        owner: &OwnerId,
        watch_id: &WatchId,
    ) -> std::result::Result<(), rmcp::ErrorData> {
        let Some(descriptor) = self.watches.describe(watch_id) else {
            return Err(not_authorized(&watch_id.to_string()));
        };
        self.authorize_agent(owner, &descriptor.agent_id)
            .await
            .map_err(|_| not_authorized(&watch_id.to_string()))
    }

    async fn resolve(&self, agent_id: &AgentId) -> Result<AgentHandle> {
        self.agents
            .read()
            .await
            .get(agent_id)
            .map(|agent| agent.handle.clone())
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
