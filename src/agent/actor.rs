//! Supervised exclusive-writer boundary for one upstream IRC identity.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use rand::rng;
use schemars::JsonSchema;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpStream,
    sync::{OwnedSemaphorePermit, broadcast, mpsc, oneshot, watch},
    time::{Instant, MissedTickBehavior},
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use crate::{
    config::{Config, IrcTransport},
    dcc::{
        chat::{DccChatEvent, DccChatHandle, spawn_chat},
        confine::{ReceiveChoice, ReceiveDestination},
        manager::DccManager,
        negotiation::{CtcpMessage, DccOffer, encode_address, parse_address, validate_filename},
        runtime::{accept_offer, bind_listener, connect as connect_direct, offer_accept_timeout},
        session::{DccDirection, DccKind, DccSession, DccSessionId, DccState},
        transfer::{
            DestinationConflict, ReceiveOptions, ReceivedFile, TransferOptions, TransferProgress,
            receive_file, send_file,
        },
    },
    error::{GatewayError, Result},
    irc::{
        capabilities::{
            CapabilityAction, CapabilityNegotiator, CompatibilityCatalog, HelpCollector,
            SaslMechanism, SaslPolicy,
        },
        codec::{CodecError, InboundFrame, IrcCodec, OutboundFrame},
        commands::{ResponseStrategy, strategy_for_message},
        correlation::{
            CommandId, CommandResult, Completion, Correlator, CorrelatorLimits, MessageAttribution,
            PendingCommand,
        },
        history::HistoryReference,
        isupport::IsupportRegistry,
        registration::{Nickname, NicknamePlan, NicknameRejection},
        semantic::project,
        target::ChannelName,
        wire::{LineBudget, OutboundMessage, WireMessage},
    },
    mcp::{resources::ResourceUris, watch::WatchRegistry},
    time::Timestamp,
};

use super::{
    AgentId,
    connection::{BoxedIrcStream, connect},
    journal::{
        ConnectionEvent, CorrelationRole, CursorQuery, DccChatMessage, DccFailure, EventClass,
        EventCorrelation, EventCursor, EventDirection, EventOrigin, EventPage, EventPayload,
        EventVerbosity, IrcEvent, JournalStats, MalformedLine, NewEvent, RecentQuery,
        addresses_nickname,
    },
    reconnect::ReconnectBackoff,
    state::{
        AgentState, ChannelState, ConnectionState, MotdSource, MotdState, MotdStatus,
        ReconnectState,
    },
};

type IrcFramed = Framed<BoxedIrcStream, IrcCodec>;

/// Validated values required to register and restore one guest identity.
#[derive(Clone, Debug)]
pub struct RegistrationRequest {
    /// Preferred nickname.
    pub nickname: Nickname,
    /// Ordered caller-supplied alternatives.
    pub nickname_fallbacks: Vec<Nickname>,
    /// Collision behavior after explicit candidates.
    pub nick_conflict_policy: crate::irc::registration::NickConflictPolicy,
    /// USER username.
    pub username: String,
    /// USER real-name field.
    pub real_name: String,
    /// Initial and remembered channels.
    pub channels: BTreeSet<ChannelName>,
}

/// Successful registration details returned before the handle is published.
#[derive(Clone, Debug)]
pub struct RegistrationReceipt {
    /// Final accepted nickname.
    pub nickname: Nickname,
    /// Whether the preferred candidate changed.
    pub nickname_adjusted: bool,
    /// Complete initial MOTD state.
    pub motd: MotdState,
}

/// One observable stage of an initial connect.
///
/// Registering an IRC guest is a sequence of round trips — transport, capability
/// negotiation, authentication, welcome, MOTD, rejoins — that the caller
/// experiences as a single opaque wait of up to `onboarding.connect_timeout_ms`.
/// These are the points at which something verifiable has changed, published so
/// the tool handling the request can narrate them without knowing anything about
/// the IRC protocol.
///
/// Two properties are deliberate. The steps are *ordinals*, not a count of work
/// completed, because the sequence is increasing rather than contiguous: a guest
/// connection never authenticates, and a server may have no MOTD to send.
/// "Registered" and "autojoin synchronized" are also separate stages — the
/// server accepting a nickname and the guest being present in its channels are
/// different facts, and a caller that acts on the first as though it were the
/// second will address a channel it has not joined.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectMilestone {
    /// Opening the transport to the configured server.
    Connecting,
    /// The transport is up, encrypted or not as configured.
    TransportReady {
        /// Whether the transport is TLS rather than plain TCP.
        encrypted: bool,
    },
    /// Capability negotiation settled; nothing further will be requested.
    CapabilitiesNegotiated,
    /// The server accepted the configured SASL credential.
    Authenticated,
    /// The server issued its welcome for an accepted nickname.
    Registered,
    /// The MOTD arrived in full, or the server reported having none.
    MotdComplete,
    /// Configured and remembered channels have been rejoined.
    AutojoinSynchronized,
}

impl ConnectMilestone {
    /// How many stages one connect can report.
    ///
    /// This is the `total` a progress notification carries, so it counts the
    /// stages that exist rather than the ones a particular connect reaches.
    pub const TOTAL: u32 = 7;

    /// Position of this stage in the sequence, counted from one.
    ///
    /// One-based so the first report is distinguishable from "nothing has
    /// happened yet", which zero would not be.
    pub const fn step(self) -> u32 {
        match self {
            Self::Connecting => 1,
            Self::TransportReady { .. } => 2,
            Self::CapabilitiesNegotiated => 3,
            Self::Authenticated => 4,
            Self::Registered => 5,
            Self::MotdComplete => 6,
            Self::AutojoinSynchronized => 7,
        }
    }

    /// What reaching this stage means, for a human reading progress.
    pub fn describe(self) -> &'static str {
        match self {
            Self::Connecting => "Opening a connection to the IRC server.",
            Self::TransportReady { encrypted: true } => "TLS established.",
            Self::TransportReady { encrypted: false } => "Connected over plain TCP.",
            Self::CapabilitiesNegotiated => "Capability negotiation complete.",
            Self::Authenticated => "SASL authentication accepted.",
            Self::Registered => "Registered with the server.",
            Self::MotdComplete => "Message of the day received.",
            Self::AutojoinSynchronized => "Autojoin channels synchronized.",
        }
    }
}

/// Completion strategy selected by the public execute tool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionMode {
    /// Use static and negotiated protocol knowledge.
    Auto,
    /// Require labeled collection for an otherwise unknown command.
    Collect,
    /// Complete immediately after a successful write.
    FireAndForget,
}

/// One structured outbound actor operation.
#[derive(Clone, Debug)]
pub struct ExecuteRequest {
    /// Message without a raw CRLF-delimited line.
    pub message: OutboundMessage,
    /// Collector selection.
    pub completion_mode: CompletionMode,
    /// Caller-selected deadline, bounded by configuration.
    pub timeout: Duration,
}

/// Complete immutable actor view used by MCP resources and status.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct AgentSnapshot {
    /// Advisory reduced state.
    pub state: AgentState,
    /// Capability, ISUPPORT, command, CTCP, and DCC compatibility.
    pub protocol: CompatibilityCatalog,
    /// Active line budget after ISUPPORT and local ceilings.
    pub line_budget: ActiveLineBudget,
    /// Journal bounds.
    pub journal: JournalStats,
    /// Small recent event window.
    pub recent_events: Vec<IrcEvent>,
    /// Direct sessions retained by this actor.
    pub dcc_sessions: Vec<DccSession>,
}

/// JSON-friendly active IRC line limits.
#[derive(Clone, Copy, Debug, JsonSchema, Serialize)]
pub struct ActiveLineBudget {
    /// IRC message body including CRLF.
    pub max_body_bytes: usize,
    /// IRCv3 tag section including its separator.
    pub max_tag_bytes: usize,
}

impl From<LineBudget> for ActiveLineBudget {
    fn from(value: LineBudget) -> Self {
        Self {
            max_body_bytes: value.max_body_bytes,
            max_tag_bytes: value.max_tag_bytes,
        }
    }
}

/// Completed clean shutdown details.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct DisconnectReceipt {
    /// Whether QUIT reached the upstream writer.
    pub quit_sent: bool,
    /// Direct sessions closed while stopping.
    pub dcc_sessions_closed: usize,
}

/// Caller choices when accepting an incoming DCC offer.
#[derive(Clone, Debug)]
pub struct DccAcceptRequest {
    /// Offered session.
    pub session_id: DccSessionId,
    /// Required receive root and root-relative destination for incoming SEND;
    /// absent for CHAT, which writes nothing.
    pub destination: Option<ReceiveChoice>,
    /// Existing destination behavior.
    pub conflict: DestinationConflict,
}

/// Commands accepted by an agent actor over its bounded queue.
#[derive(Debug)]
enum AgentCommand {
    Execute {
        request: ExecuteRequest,
        completion: oneshot::Sender<Result<CommandResult>>,
    },
    ReadEvents {
        cursor: Option<EventCursor>,
        limit: usize,
        wait: Duration,
        query: CursorQuery,
        completion: oneshot::Sender<Result<EventPage>>,
    },
    ReadRecent {
        limit: usize,
        query: RecentQuery,
        completion: oneshot::Sender<Vec<IrcEvent>>,
    },
    Snapshot {
        completion: oneshot::Sender<AgentSnapshot>,
    },
    DccChatOpen {
        peer: String,
        reverse: bool,
        completion: oneshot::Sender<Result<DccSession>>,
    },
    DccChatSend {
        session_id: DccSessionId,
        text: String,
        completion: oneshot::Sender<Result<()>>,
    },
    DccSend {
        peer: String,
        source_path: PathBuf,
        filename: Option<String>,
        reverse: bool,
        completion: oneshot::Sender<Result<DccSession>>,
    },
    DccAccept {
        request: DccAcceptRequest,
        completion: oneshot::Sender<Result<DccSession>>,
    },
    DccReject {
        session_id: DccSessionId,
        completion: oneshot::Sender<Result<DccSession>>,
    },
    DccCancel {
        session_id: DccSessionId,
        completion: oneshot::Sender<Result<DccSession>>,
    },
    Disconnect {
        reason: Option<String>,
        completion: oneshot::Sender<Result<DisconnectReceipt>>,
    },
}

/// Cloneable routing handle held by the gateway.
#[derive(Clone, Debug)]
pub struct AgentHandle {
    id: AgentId,
    commands: mpsc::Sender<AgentCommand>,
}

impl AgentHandle {
    /// Create the actor and its provisional registration result channel.
    pub fn spawn(
        id: AgentId,
        config: Arc<Config>,
        registration: RegistrationRequest,
        resource_updates: broadcast::Sender<String>,
        watches: Arc<WatchRegistry>,
        capacity_permit: OwnedSemaphorePermit,
        milestones: Option<mpsc::Sender<ConnectMilestone>>,
    ) -> (
        Self,
        AgentActor,
        oneshot::Receiver<Result<RegistrationReceipt>>,
    ) {
        let (commands, command_rx) = mpsc::channel(config.limits.command_queue);
        let initial_state = AgentState::new(id.clone(), Timestamp::now());
        let (state_tx, _) = watch::channel(initial_state);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (dcc_events, dcc_event_rx) = mpsc::channel(config.limits.command_queue);
        let (dcc_chat_events, dcc_chat_event_rx) = mpsc::channel(config.limits.command_queue);
        let actor = AgentActor {
            id: id.clone(),
            config: config.clone(),
            registration,
            commands: command_rx,
            state: state_tx,
            ready: Some(ready_tx),
            milestones,
            journal: super::journal::EventJournal::new(
                config.limits.event_count,
                config.limits.event_bytes,
            ),
            isupport: IsupportRegistry::new(),
            capabilities: CapabilityNegotiator::new(),
            protocol: protocol_catalog(),
            help_collector: HelpCollector::new(),
            correlator: Correlator::new(CorrelatorLimits {
                max_pending: config.limits.pending_commands,
                max_active_batches: config.limits.active_batches,
                max_replies_per_command: config.limits.replies_per_command,
            }),
            pending_commands: HashMap::new(),
            deferred_commands: VecDeque::new(),
            command_labels: HashMap::new(),
            command_first_events: HashMap::new(),
            pending_event_reads: Vec::new(),
            dcc: DccManager::new(config.dcc.max_sessions),
            dcc_events,
            dcc_event_rx,
            dcc_chat_events,
            dcc_chat_event_rx,
            dcc_chat_handles: HashMap::new(),
            dcc_tasks: HashMap::new(),
            dcc_cancellations: HashMap::new(),
            dcc_resume_offsets: HashMap::new(),
            recovery_history_batches: BTreeSet::new(),
            recovery_history_targets: BTreeSet::new(),
            history_markers: BTreeMap::new(),
            seen_message_ids: HashSet::new(),
            seen_message_order: VecDeque::new(),
            motd_query: None,
            resource_updates,
            watches,
            started_at: Instant::now(),
            active_budget: LineBudget::TRADITIONAL,
            _capacity_permit: capacity_permit,
        };
        (Self { id, commands }, actor, ready_rx)
    }

    /// Execute and correlate one structured IRC command.
    pub async fn execute(&self, request: ExecuteRequest) -> Result<CommandResult> {
        let (completion, result) = oneshot::channel();
        self.send(AgentCommand::Execute {
            request,
            completion,
        })
        .await?;
        result
            .await
            .map_err(|_| GatewayError::ActorStopped(self.id.clone()))?
    }

    /// Read the actor journal, optionally waiting for a change.
    pub async fn read_events(
        &self,
        cursor: Option<EventCursor>,
        limit: usize,
        wait: Duration,
        query: CursorQuery,
    ) -> Result<EventPage> {
        let (completion, result) = oneshot::channel();
        self.send(AgentCommand::ReadEvents {
            cursor,
            limit,
            wait,
            query,
            completion,
        })
        .await?;
        result
            .await
            .map_err(|_| GatewayError::ActorStopped(self.id.clone()))?
    }

    /// Read the newest matching retained events, oldest-first.
    ///
    /// Distinct from [`Self::read_events`], which answers "what is after this
    /// cursor". A conversational resource has no caller cursor and wants the
    /// end of the stream, so scanning from the newest record is what keeps a
    /// rare class such as a mention visible in a busy journal.
    pub async fn read_recent(&self, limit: usize, query: RecentQuery) -> Result<Vec<IrcEvent>> {
        let (completion, result) = oneshot::channel();
        self.send(AgentCommand::ReadRecent {
            limit,
            query,
            completion,
        })
        .await?;
        result
            .await
            .map_err(|_| GatewayError::ActorStopped(self.id.clone()))
    }

    /// Read a consistent actor-owned resource snapshot.
    pub async fn snapshot(&self) -> Result<AgentSnapshot> {
        let (completion, result) = oneshot::channel();
        self.send(AgentCommand::Snapshot { completion }).await?;
        result
            .await
            .map_err(|_| GatewayError::ActorStopped(self.id.clone()))
    }

    /// Open an outbound DCC CHAT offer.
    pub async fn dcc_chat_open(&self, peer: String, reverse: bool) -> Result<DccSession> {
        let (completion, result) = oneshot::channel();
        self.send(AgentCommand::DccChatOpen {
            peer,
            reverse,
            completion,
        })
        .await?;
        result
            .await
            .map_err(|_| GatewayError::ActorStopped(self.id.clone()))?
    }

    /// Queue one line to an active DCC CHAT socket.
    pub async fn dcc_chat_send(&self, session_id: DccSessionId, text: String) -> Result<()> {
        let (completion, result) = oneshot::channel();
        self.send(AgentCommand::DccChatSend {
            session_id,
            text,
            completion,
        })
        .await?;
        result
            .await
            .map_err(|_| GatewayError::ActorStopped(self.id.clone()))?
    }

    /// Offer one local file through DCC SEND.
    pub async fn dcc_send(
        &self,
        peer: String,
        source_path: PathBuf,
        filename: Option<String>,
        reverse: bool,
    ) -> Result<DccSession> {
        let (completion, result) = oneshot::channel();
        self.send(AgentCommand::DccSend {
            peer,
            source_path,
            filename,
            reverse,
            completion,
        })
        .await?;
        result
            .await
            .map_err(|_| GatewayError::ActorStopped(self.id.clone()))?
    }

    /// Accept an incoming DCC offer.
    pub async fn dcc_accept(&self, request: DccAcceptRequest) -> Result<DccSession> {
        let (completion, result) = oneshot::channel();
        self.send(AgentCommand::DccAccept {
            request,
            completion,
        })
        .await?;
        result
            .await
            .map_err(|_| GatewayError::ActorStopped(self.id.clone()))?
    }

    /// Reject an offered DCC session.
    pub async fn dcc_reject(&self, session_id: DccSessionId) -> Result<DccSession> {
        let (completion, result) = oneshot::channel();
        self.send(AgentCommand::DccReject {
            session_id,
            completion,
        })
        .await?;
        result
            .await
            .map_err(|_| GatewayError::ActorStopped(self.id.clone()))?
    }

    /// Cancel one active or offered DCC session.
    pub async fn dcc_cancel(&self, session_id: DccSessionId) -> Result<DccSession> {
        let (completion, result) = oneshot::channel();
        self.send(AgentCommand::DccCancel {
            session_id,
            completion,
        })
        .await?;
        result
            .await
            .map_err(|_| GatewayError::ActorStopped(self.id.clone()))?
    }

    /// Request clean actor shutdown.
    pub async fn disconnect(&self, reason: Option<String>) -> Result<DisconnectReceipt> {
        let (completion, result) = oneshot::channel();
        self.send(AgentCommand::Disconnect { reason, completion })
            .await?;
        result
            .await
            .map_err(|_| GatewayError::ActorStopped(self.id.clone()))?
    }

    async fn send(&self, command: AgentCommand) -> Result<()> {
        self.commands
            .send(command)
            .await
            .map_err(|_| GatewayError::ActorStopped(self.id.clone()))
    }
}

struct PendingEventRead {
    cursor: Option<EventCursor>,
    limit: usize,
    query: CursorQuery,
    deadline: Instant,
    completion: oneshot::Sender<Result<EventPage>>,
}

#[derive(Clone, Debug)]
struct HistoryMarker {
    target: String,
    reference: HistoryReference,
}

fn is_history_batch_kind(kind: &str) -> bool {
    kind.eq_ignore_ascii_case("chathistory")
        || kind.eq_ignore_ascii_case("draft/chathistory")
        || kind.eq_ignore_ascii_case("history")
}

/// Render a connection failure for `AgentState.last_error`.
///
/// A failure during reconnect never returns to a request, so this string and
/// the `connection.lifecycle` record carrying it are the only places a caller
/// can see why the relay is degraded. The `Display` form of a registration
/// failure drops the nicknames it tried, which is exactly the detail needed to
/// tell "the server refused our credentials" apart from "every candidate
/// nickname was taken", so spell them out here.
fn connection_failure_detail(error: &GatewayError) -> String {
    match error {
        GatewayError::Registration {
            attempted_nicknames,
            ..
        } if !attempted_nicknames.is_empty() => format!(
            "{error} (attempted nicknames: {})",
            attempted_nicknames.join(", ")
        ),
        other => other.to_string(),
    }
}

#[derive(Debug)]
enum DccRuntimeEvent {
    ChatConnected {
        session_id: DccSessionId,
        stream: TcpStream,
    },
    TransferProgress(TransferProgress),
    TransferConnected {
        session_id: DccSessionId,
    },
    TransferCompleted {
        session_id: DccSessionId,
        received: Option<ReceivedFile>,
        transferred_bytes: u64,
    },
    Failed {
        session_id: DccSessionId,
        error: String,
    },
}

/// Runtime task that owns one socket, reducer, journal, correlator, and DCC manager.
pub struct AgentActor {
    id: AgentId,
    config: Arc<Config>,
    registration: RegistrationRequest,
    commands: mpsc::Receiver<AgentCommand>,
    state: watch::Sender<AgentState>,
    ready: Option<oneshot::Sender<Result<RegistrationReceipt>>>,
    /// Where to publish initial-connect stages, when the caller asked to see
    /// them. Dropped once the initial connect settles: a later reconnect has no
    /// request waiting on it, so it has nobody to narrate to.
    milestones: Option<mpsc::Sender<ConnectMilestone>>,
    journal: super::journal::EventJournal,
    isupport: IsupportRegistry,
    capabilities: CapabilityNegotiator,
    protocol: CompatibilityCatalog,
    help_collector: HelpCollector,
    correlator: Correlator,
    pending_commands: HashMap<CommandId, oneshot::Sender<Result<CommandResult>>>,
    deferred_commands: VecDeque<(ExecuteRequest, oneshot::Sender<Result<CommandResult>>)>,
    command_labels: HashMap<String, CommandId>,
    command_first_events: HashMap<CommandId, EventCursor>,
    pending_event_reads: Vec<PendingEventRead>,
    dcc: DccManager,
    dcc_events: mpsc::Sender<DccRuntimeEvent>,
    dcc_event_rx: mpsc::Receiver<DccRuntimeEvent>,
    dcc_chat_events: mpsc::Sender<DccChatEvent>,
    dcc_chat_event_rx: mpsc::Receiver<DccChatEvent>,
    dcc_chat_handles: HashMap<DccSessionId, DccChatHandle>,
    dcc_tasks: HashMap<DccSessionId, tokio::task::JoinHandle<()>>,
    dcc_cancellations: HashMap<DccSessionId, CancellationToken>,
    dcc_resume_offsets: HashMap<DccSessionId, watch::Sender<u64>>,
    recovery_history_batches: BTreeSet<String>,
    recovery_history_targets: BTreeSet<String>,
    history_markers: BTreeMap<String, HistoryMarker>,
    seen_message_ids: HashSet<String>,
    seen_message_order: VecDeque<String>,
    motd_query: Option<MotdState>,
    resource_updates: broadcast::Sender<String>,
    watches: Arc<WatchRegistry>,
    started_at: Instant,
    active_budget: LineBudget,
    _capacity_permit: OwnedSemaphorePermit,
}

impl std::fmt::Debug for AgentActor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentActor")
            .field("id", &self.id)
            .field("connection_state", &self.state.borrow().connection_state)
            .field("pending_commands", &self.pending_commands.len())
            .field("pending_event_reads", &self.pending_event_reads.len())
            .finish_non_exhaustive()
    }
}

impl AgentActor {
    /// Connect, supervise network failures, and stop when the command channel closes.
    pub async fn run(mut self) {
        let initial = tokio::time::timeout(
            Duration::from_millis(self.config.onboarding.connect_timeout_ms),
            self.establish(MotdSource::Initial),
        )
        .await;
        let (mut connection, receipt) = match initial {
            Ok(Ok(registered)) => registered,
            Ok(Err(error)) => {
                self.fail_initial(error);
                return;
            }
            Err(_) => {
                self.fail_initial(GatewayError::TimedOut(
                    "initial IRC registration and MOTD".into(),
                ));
                return;
            }
        };
        self.registration.nickname = receipt.nickname.clone();
        self.milestones = None;
        if let Some(ready) = self.ready.take() {
            let _ = ready.send(Ok(receipt));
        }

        let mut reconnect = ReconnectBackoff::new(self.config.reconnect.clone());
        loop {
            match self.serve_connection(&mut connection).await {
                ConnectionExit::Shutdown => break,
                ConnectionExit::Lost(error) => {
                    self.fail_pending_commands(error.to_string());
                    self.set_connection_state(
                        ConnectionState::Reconnecting,
                        Some(connection_failure_detail(&error)),
                    );
                }
            }

            let delay = reconnect.next_delay(&mut rng());
            self.update_reconnect_state(reconnect.attempt(), delay);
            if self.wait_reconnect(delay).await == ReconnectDecision::Shutdown {
                break;
            }
            match tokio::time::timeout(
                Duration::from_millis(self.config.onboarding.connect_timeout_ms),
                self.establish(MotdSource::Reconnect),
            )
            .await
            {
                Ok(Ok((new_connection, receipt))) => {
                    connection = new_connection;
                    self.registration.nickname = receipt.nickname;
                    reconnect.reset();
                    self.update_reconnect_state(0, Duration::ZERO);
                }
                Ok(Err(error)) => {
                    self.set_connection_state(
                        ConnectionState::Reconnecting,
                        Some(connection_failure_detail(&error)),
                    );
                }
                Err(_) => {
                    self.set_connection_state(
                        ConnectionState::Reconnecting,
                        Some(format!(
                            "reconnect registration timed out after {}ms",
                            self.config.onboarding.connect_timeout_ms
                        )),
                    );
                }
            }
        }
        self.finish_shutdown();
        tracing::debug!(agent_id = %self.id, "agent actor stopped");
    }

    fn fail_initial(&mut self, error: GatewayError) {
        self.set_connection_state(
            ConnectionState::TerminalError,
            Some(connection_failure_detail(&error)),
        );
        self.milestones = None;
        if let Some(ready) = self.ready.take() {
            let _ = ready.send(Err(error));
        }
        self.finish_shutdown();
    }

    /// Publish one connect stage to whoever is awaiting this registration.
    ///
    /// Best effort by construction. The channel is bounded and sized for the
    /// whole sequence, so nothing is dropped in practice, but progress is
    /// advisory: a caller that stopped reading must lose notifications rather
    /// than slow the connection down or fail it.
    fn reach(&self, milestone: ConnectMilestone) {
        if let Some(milestones) = &self.milestones {
            let _ = milestones.try_send(milestone);
        }
    }

    async fn establish(&mut self, source: MotdSource) -> Result<(IrcFramed, RegistrationReceipt)> {
        self.set_connection_state(ConnectionState::Connecting, None);
        self.reach(ConnectMilestone::Connecting);
        let stream = connect(&self.config.irc).await?;
        self.reach(ConnectMilestone::TransportReady {
            encrypted: matches!(self.config.irc.transport, IrcTransport::Tls),
        });
        self.reset_connection_protocol();
        let mut framed = Framed::new(
            stream,
            IrcCodec::new(LineBudget::TRADITIONAL, self.config.limits.max_line_bytes),
        );
        self.set_connection_state(ConnectionState::Registering, None);
        let credentials = self.config.resolve_credentials()?;
        let sasl_policy = if credentials.sasl.is_some() {
            SaslPolicy::Authenticate(SaslMechanism::Plain)
        } else {
            SaslPolicy::Guest
        };
        self.capabilities = CapabilityNegotiator::with_sasl(sasl_policy);

        if let Some(password) = credentials.server_password.as_ref() {
            self.write_uncorrelated(
                &mut framed,
                OutboundMessage::new("PASS", Vec::new()).with_trailing(password.expose_secret()),
                false,
            )
            .await?;
        }
        self.write_uncorrelated(
            &mut framed,
            OutboundMessage::new("CAP", vec!["LS".into(), "302".into()]),
            true,
        )
        .await?;

        let mut nickname_plan = NicknamePlan::new(
            &self.registration.nickname,
            &self.registration.nickname_fallbacks,
            self.registration.nick_conflict_policy,
            self.isupport.advertised_nick_len(),
            self.config.onboarding.nickname_attempts,
        );
        let first =
            nickname_plan
                .next_candidate()
                .cloned()
                .ok_or_else(|| GatewayError::Registration {
                    message: "no valid nickname candidates remain".into(),
                    attempted_nicknames: Vec::new(),
                })?;
        let mut attempted = vec![first.to_string()];
        self.write_uncorrelated(
            &mut framed,
            OutboundMessage::new("NICK", vec![first.to_string()]),
            true,
        )
        .await?;
        self.write_uncorrelated(
            &mut framed,
            OutboundMessage::new(
                "USER",
                vec![self.registration.username.clone(), "0".into(), "*".into()],
            )
            .with_trailing(self.registration.real_name.clone()),
            true,
        )
        .await?;

        let mut registered_nickname = None;
        let mut motd = MotdState {
            source: Some(source),
            ..MotdState::default()
        };
        // Keep the server's own explanation next to the numeric: a reconnect
        // failure is only ever seen through `last_error`, and "904" alone tells
        // an operator nothing about which credential the server refused.
        let mut sasl_failure: Option<(u16, Option<String>)> = None;
        while let Some(frame) = framed.next().await {
            let frame =
                frame.map_err(|source| GatewayError::io("read IRC registration", source))?;
            let InboundFrame::Message(message) = frame else {
                self.record_malformed(frame);
                continue;
            };
            let message = *message;
            if message.command.eq_ignore_ascii_case("PING") {
                self.send_pong(&mut framed, &message).await?;
            }

            let action = self.capabilities.apply(&message);
            self.observe_protocol(&message, &mut framed);
            self.record_inbound(
                message.clone(),
                EventOrigin::Live,
                true,
                EventCorrelation::default(),
            );
            // Both actions mean the capability round has settled: the server has
            // answered every request, so the only steps left are authenticating
            // or ending. Reading it here rather than from `is_complete()` keeps
            // the stage ahead of authentication, which the negotiator folds into
            // the same completion flag.
            if matches!(
                action,
                CapabilityAction::Authenticate(_) | CapabilityAction::EndNegotiation
            ) {
                self.reach(ConnectMilestone::CapabilitiesNegotiated);
            }
            if message.numeric() == Some(903) && credentials.sasl.is_some() {
                self.reach(ConnectMilestone::Authenticated);
            }
            match action {
                CapabilityAction::None => {}
                CapabilityAction::Request(capabilities) => {
                    self.write_uncorrelated(
                        &mut framed,
                        OutboundMessage::new("CAP", vec!["REQ".into()])
                            .with_trailing(capabilities.join(" ")),
                        true,
                    )
                    .await?;
                }
                CapabilityAction::Authenticate(mechanism) => {
                    self.write_uncorrelated(
                        &mut framed,
                        OutboundMessage::new("AUTHENTICATE", vec![mechanism.as_str().into()]),
                        true,
                    )
                    .await?;
                }
                CapabilityAction::SendAuthenticationPayload => {
                    self.send_sasl_payload(&mut framed, credentials.sasl.as_ref())
                        .await?;
                }
                CapabilityAction::AuthenticationFailed { numeric } => {
                    sasl_failure = Some((numeric, message.trailing.clone()));
                    self.write_uncorrelated(
                        &mut framed,
                        OutboundMessage::new("CAP", vec!["END".into()]),
                        true,
                    )
                    .await?;
                }
                CapabilityAction::EndNegotiation => {
                    self.write_uncorrelated(
                        &mut framed,
                        OutboundMessage::new("CAP", vec!["END".into()]),
                        true,
                    )
                    .await?;
                }
            }

            if let Some(rejection) = message.numeric().and_then(NicknameRejection::from_numeric) {
                if rejection.is_retriable()
                    && let Some(candidate) = nickname_plan.next_candidate().cloned()
                {
                    attempted.push(candidate.to_string());
                    self.write_uncorrelated(
                        &mut framed,
                        OutboundMessage::new("NICK", vec![candidate.to_string()]),
                        true,
                    )
                    .await?;
                    continue;
                }
                return Err(GatewayError::Registration {
                    message: message
                        .trailing
                        .clone()
                        .unwrap_or_else(|| format!("nickname rejected with {rejection:?}")),
                    attempted_nicknames: attempted,
                });
            }

            match message.numeric() {
                Some(1) => {
                    let accepted = message
                        .params
                        .first()
                        .cloned()
                        .unwrap_or_else(|| attempted.last().cloned().unwrap_or_default());
                    registered_nickname = Some(Nickname::new(accepted).map_err(|error| {
                        GatewayError::Registration {
                            message: format!("server accepted an invalid nickname: {error}"),
                            attempted_nicknames: attempted.clone(),
                        }
                    })?);
                    self.reach(ConnectMilestone::Registered);
                }
                Some(375 | 372 | 376 | 422) => {
                    if message.numeric() == Some(372) {
                        motd.lines
                            .push(message.trailing.clone().unwrap_or_default());
                    }
                    motd.wire_replies.push(message.clone());
                    if message.numeric() == Some(376) {
                        motd.status = MotdStatus::Received;
                        motd.text = motd.lines.join("\n");
                        motd.received_at = Some(Timestamp::now());
                        self.reach(ConnectMilestone::MotdComplete);
                    } else if message.numeric() == Some(422) {
                        motd.status = MotdStatus::NotAvailable;
                        motd.text = message.trailing.clone().unwrap_or_default();
                        motd.received_at = Some(Timestamp::now());
                        self.reach(ConnectMilestone::MotdComplete);
                    }
                }
                Some(numeric @ 400..=599)
                    if !matches!(numeric, 422 | 431 | 432 | 433 | 436 | 437)
                        && !matches!(numeric, 902 | 904 | 905 | 906 | 907 | 908) =>
                {
                    return Err(GatewayError::Registration {
                        message: message
                            .trailing
                            .clone()
                            .unwrap_or_else(|| format!("server returned numeric {numeric}")),
                        attempted_nicknames: attempted,
                    });
                }
                _ => {}
            }

            let motd_complete = motd.status != MotdStatus::NotReceived;
            if let Some(nickname) = registered_nickname.clone()
                && motd_complete
                && self.capabilities.is_complete()
            {
                if let Some((numeric, detail)) = sasl_failure {
                    let message = match detail.as_deref() {
                        Some(detail) if !detail.is_empty() => format!(
                            "configured SASL authentication failed with {numeric}: {detail}"
                        ),
                        _ => format!("configured SASL authentication failed with {numeric}"),
                    };
                    return Err(GatewayError::Registration {
                        message,
                        attempted_nicknames: attempted,
                    });
                }
                if credentials.sasl.is_some() && !self.capabilities.is_active("sasl") {
                    return Err(GatewayError::Registration {
                        message: "configured SASL PLAIN is unavailable on the server".into(),
                        attempted_nicknames: attempted,
                    });
                }
                self.finish_registration(&nickname, &motd);
                self.write_uncorrelated(
                    &mut framed,
                    OutboundMessage::new("HELP", vec!["INDEX".into()]),
                    true,
                )
                .await?;
                self.restore_channels(&mut framed).await?;
                // Distinct from registration on purpose: the server accepting a
                // nickname does not put the guest in any channel, and a caller
                // that treats the two as one will address a channel it has not
                // joined yet.
                self.reach(ConnectMilestone::AutojoinSynchronized);
                if source == MotdSource::Reconnect {
                    self.recover_history(&mut framed).await?;
                }
                return Ok((
                    framed,
                    RegistrationReceipt {
                        nickname,
                        nickname_adjusted: nickname_plan.adjusted(),
                        motd,
                    },
                ));
            }
        }
        Err(GatewayError::Registration {
            message: "server closed the connection during registration".into(),
            attempted_nicknames: attempted,
        })
    }

    async fn serve_connection(&mut self, framed: &mut IrcFramed) -> ConnectionExit {
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                frame = framed.next() => match frame {
                    Some(Ok(InboundFrame::Message(message))) => {
                        if let Err(error) = self.handle_inbound(framed, *message).await {
                            return ConnectionExit::Lost(error);
                        }
                    }
                    Some(Ok(malformed)) => self.record_malformed(malformed),
                    Some(Err(source)) => {
                        return ConnectionExit::Lost(GatewayError::io("read IRC connection", source));
                    }
                    None => {
                        return ConnectionExit::Lost(GatewayError::NotConnected(self.id.clone()));
                    }
                },
                Some(command) = self.commands.recv() => {
                    if self.handle_online_command(command, framed).await {
                        return ConnectionExit::Shutdown;
                    }
                }
                Some(event) = self.dcc_event_rx.recv() => self.handle_dcc_runtime_event(event),
                Some(event) = self.dcc_chat_event_rx.recv() => self.handle_dcc_chat_event(event),
                _ = tick.tick() => {
                    self.tick();
                    self.drain_deferred(framed).await;
                },
                else => return ConnectionExit::Shutdown,
            }
        }
    }

    async fn handle_inbound(&mut self, framed: &mut IrcFramed, message: WireMessage) -> Result<()> {
        if message.command.eq_ignore_ascii_case("PING") {
            self.send_pong(framed, &message).await?;
        }
        let action = self.capabilities.apply(&message);
        match action {
            CapabilityAction::Request(capabilities) => {
                self.write_uncorrelated(
                    framed,
                    OutboundMessage::new("CAP", vec!["REQ".into()])
                        .with_trailing(capabilities.join(" ")),
                    true,
                )
                .await?;
            }
            CapabilityAction::EndNegotiation | CapabilityAction::AuthenticationFailed { .. } => {
                self.write_uncorrelated(
                    framed,
                    OutboundMessage::new("CAP", vec!["END".into()]),
                    true,
                )
                .await?;
            }
            CapabilityAction::Authenticate(_)
            | CapabilityAction::SendAuthenticationPayload
            | CapabilityAction::None => {}
        }
        self.observe_protocol(&message, framed);
        self.observe_queried_motd(&message);
        let attribution = self.correlator.attribute(&message);
        let correlation = EventCorrelation {
            command_id: attribution
                .command_id()
                .map(|command_id| command_id.as_str().to_owned()),
            label: attribution
                .label()
                .or_else(|| message.tag_value("label"))
                .map(str::to_owned),
            role: attribution.command_id().map(|_| CorrelationRole::Response),
        };
        let (origin, deduplicate_message) = self.history_origin(&message, &attribution);
        let cursor = self.record_inbound(message.clone(), origin, deduplicate_message, correlation);
        if let (Some(label), Some(cursor)) = (message.tag_value("label"), cursor.as_ref())
            && let Some(command_id) = self.command_labels.get(label)
        {
            self.command_first_events
                .entry(command_id.clone())
                .or_insert_with(|| cursor.clone());
        }
        self.respond_to_ctcp(framed, &message).await?;
        self.observe_dcc_control(framed, &message).await;
        for completion in self.correlator.ingest_attributed(&message, attribution) {
            self.finish_command(completion, cursor.clone());
        }
        self.drain_deferred(framed).await;
        self.flush_event_reads(false);
        Ok(())
    }

    async fn handle_online_command(
        &mut self,
        command: AgentCommand,
        framed: &mut IrcFramed,
    ) -> bool {
        match command {
            AgentCommand::Execute {
                request,
                completion,
            } => self.execute(request, completion, framed).await,
            AgentCommand::ReadEvents {
                cursor,
                limit,
                wait,
                query,
                completion,
            } => self.read_events(cursor, limit, wait, query, completion),
            AgentCommand::ReadRecent {
                limit,
                query,
                completion,
            } => {
                let _ = completion.send(self.journal.read_latest(limit, &query));
            }
            AgentCommand::Snapshot { completion } => {
                let _ = completion.send(self.snapshot());
            }
            AgentCommand::DccChatOpen {
                peer,
                reverse,
                completion,
            } => {
                let result = self.open_dcc_chat(framed, peer, reverse).await;
                let _ = completion.send(result);
            }
            AgentCommand::DccChatSend {
                session_id,
                text,
                completion,
            } => {
                let result = self.send_dcc_chat(&session_id, text).await;
                let _ = completion.send(result);
            }
            AgentCommand::DccSend {
                peer,
                source_path,
                filename,
                reverse,
                completion,
            } => {
                let result = self
                    .open_dcc_send(framed, peer, source_path, filename, reverse)
                    .await;
                let _ = completion.send(result);
            }
            AgentCommand::DccAccept {
                request,
                completion,
            } => {
                let result = self.accept_dcc(framed, request).await;
                let _ = completion.send(result);
            }
            AgentCommand::DccReject {
                session_id,
                completion,
            } => {
                let result = self.reject_dcc(&session_id);
                let _ = completion.send(result);
            }
            AgentCommand::DccCancel {
                session_id,
                completion,
            } => {
                let result = self.cancel_dcc(&session_id);
                let _ = completion.send(result);
            }
            AgentCommand::Disconnect { reason, completion } => {
                let quit = OutboundMessage::new("QUIT", Vec::new());
                let quit = reason.map_or(quit.clone(), |reason| quit.with_trailing(reason));
                let quit_sent = self.write_uncorrelated(framed, quit, true).await.is_ok();
                let _ = completion.send(Ok(DisconnectReceipt {
                    quit_sent,
                    dcc_sessions_closed: self.dcc.active_len(),
                }));
                return true;
            }
        }
        false
    }

    async fn execute(
        &mut self,
        request: ExecuteRequest,
        completion: oneshot::Sender<Result<CommandResult>>,
        framed: &mut IrcFramed,
    ) {
        let timeout_limit = Duration::from_millis(self.config.limits.max_command_timeout_ms);
        if request.timeout.is_zero() || request.timeout > timeout_limit {
            let _ = completion.send(Err(GatewayError::InvalidMessage(format!(
                "timeout must be between 1 and {} milliseconds",
                timeout_limit.as_millis()
            ))));
            return;
        }
        let labeled = self.capabilities.is_active("labeled-response");
        let strategy = match request.completion_mode {
            CompletionMode::Auto => strategy_for_message(&request.message, &self.capabilities),
            CompletionMode::Collect if labeled => ResponseStrategy::Ack,
            CompletionMode::Collect => {
                let _ = completion.send(Err(GatewayError::InvalidMessage(
                    "collect mode requires labeled-response".into(),
                )));
                return;
            }
            CompletionMode::FireAndForget => ResponseStrategy::Unconfirmed,
        };
        if !self
            .correlator
            .admits(&request.message.command, strategy, labeled)
        {
            if self.deferred_commands.len() >= self.config.limits.pending_commands {
                let _ = completion.send(Err(GatewayError::ResourceLimit(
                    "deferred command collector limit reached".into(),
                )));
            } else {
                self.deferred_commands.push_back((request, completion));
            }
            return;
        }

        let command_id = CommandId::new();
        let label = labeled.then(|| command_id.label());
        let now_ms = self.elapsed_ms();
        let pending = PendingCommand {
            command_id: command_id.clone(),
            agent_id: self.id.clone(),
            command: request.message.command.clone(),
            label: label.clone(),
            response: strategy,
            written: false,
            deadline_ms: now_ms.saturating_add(request.timeout.as_millis() as u64),
            warnings: Vec::new(),
            replies: Vec::new(),
        };
        if let Err(error) = self.correlator.register(pending) {
            let _ = completion.send(Err(GatewayError::ResourceLimit(error.to_string())));
            return;
        }
        self.pending_commands.insert(command_id.clone(), completion);
        if let Some(label) = &label {
            self.command_labels
                .insert(label.clone(), command_id.clone());
        }

        let encoded = crate::irc::wire::encode_with_label(
            &request.message,
            label.as_deref(),
            framed.codec().budget(),
        );
        let write = match encoded {
            Ok(_) => framed
                .send(OutboundFrame {
                    message: request.message.clone(),
                    label: label.clone(),
                })
                .await
                .map_err(map_codec_error),
            Err(error) => Err(GatewayError::InvalidMessage(error.to_string())),
        };
        if let Err(error) = write {
            if let Some(done) = self.correlator.record_write(&command_id, false) {
                self.finish_command(done, None);
            } else if let Some(sender) = self.pending_commands.remove(&command_id) {
                let _ = sender.send(Err(error));
            }
            return;
        }
        self.record_outbound(&request.message, label.as_deref(), Some(&command_id));
        if let Some(done) = self.correlator.record_write(&command_id, true) {
            self.finish_command(done, None);
        }
    }

    fn finish_command(&mut self, completion: Completion, cursor: Option<EventCursor>) {
        let retriable = completion.retriable();
        let first_event_cursor = self
            .command_first_events
            .remove(&completion.command_id)
            .or(cursor);
        if let Some(label) = &completion.label {
            self.command_labels.remove(label);
        }
        let semantic_result = (!completion.replies.is_empty()).then(|| {
            completion
                .replies
                .iter()
                .map(|message| project(message, &self.isupport))
                .collect()
        });
        let result = CommandResult {
            command_id: completion.command_id.clone(),
            agent_id: completion.agent_id,
            command: completion.command,
            outcome: completion.outcome,
            written: completion.outcome != crate::irc::correlation::CommandOutcome::NotWritten,
            acknowledged: completion.acknowledged,
            retriable,
            label: completion.label,
            replies: completion.replies,
            semantic_result,
            warnings: completion.warnings,
            first_event_cursor,
        };
        if let Some(sender) = self.pending_commands.remove(&completion.command_id) {
            let _ = sender.send(Ok(result));
        }
    }

    async fn drain_deferred(&mut self, framed: &mut IrcFramed) {
        let attempts = self.deferred_commands.len();
        for _ in 0..attempts {
            let Some((request, completion)) = self.deferred_commands.pop_front() else {
                break;
            };
            self.execute(request, completion, framed).await;
        }
    }

    fn read_events(
        &mut self,
        cursor: Option<EventCursor>,
        limit: usize,
        wait: Duration,
        query: CursorQuery,
        completion: oneshot::Sender<Result<EventPage>>,
    ) {
        if limit == 0 || limit > self.config.limits.max_event_page_size {
            let _ = completion.send(Err(GatewayError::InvalidMessage(format!(
                "event limit must be between 1 and {}",
                self.config.limits.max_event_page_size
            ))));
            return;
        }
        let max_wait = Duration::from_millis(self.config.limits.max_event_wait_ms);
        if wait > max_wait {
            let _ = completion.send(Err(GatewayError::InvalidMessage(format!(
                "event wait must not exceed {} milliseconds",
                max_wait.as_millis()
            ))));
            return;
        }
        let page = self.journal.read(cursor.as_ref(), limit, &query);
        if !page.events.is_empty()
            || page.status != super::journal::CursorStatus::Current
            || wait.is_zero()
        {
            let _ = completion.send(Ok(page));
        } else if self.pending_event_reads.len() >= self.config.limits.pending_commands {
            let _ = completion.send(Err(GatewayError::ResourceLimit(
                "pending event-read limit reached".into(),
            )));
        } else {
            self.pending_event_reads.push(PendingEventRead {
                cursor,
                limit,
                query,
                deadline: Instant::now() + wait,
                completion,
            });
        }
    }

    fn flush_event_reads(&mut self, include_expired: bool) {
        let now = Instant::now();
        let mut pending = Vec::with_capacity(self.pending_event_reads.len());
        for read in self.pending_event_reads.drain(..) {
            let page = self
                .journal
                .read(read.cursor.as_ref(), read.limit, &read.query);
            if !page.events.is_empty()
                || page.status != super::journal::CursorStatus::Current
                || (include_expired && now >= read.deadline)
            {
                let _ = read.completion.send(Ok(page));
            } else if !read.completion.is_closed() {
                pending.push(read);
            }
        }
        self.pending_event_reads = pending;
    }

    fn tick(&mut self) {
        let cancelled: Vec<_> = self
            .pending_commands
            .iter()
            .filter(|(_, completion)| completion.is_closed())
            .map(|(command_id, _)| command_id.clone())
            .collect();
        for command_id in cancelled {
            if let Some(completion) = self.correlator.cancel(&command_id) {
                self.finish_command(completion, None);
            }
        }
        self.deferred_commands
            .retain(|(_, completion)| !completion.is_closed());
        for completion in self.correlator.tick(self.elapsed_ms()) {
            self.finish_command(completion, None);
        }
        let cutoff = Timestamp::from_datetime(
            Timestamp::now().as_datetime()
                - chrono::Duration::milliseconds(
                    i64::try_from(self.config.dcc.offer_ttl_ms).unwrap_or(i64::MAX),
                ),
        );
        let expired: Vec<_> = self
            .dcc
            .list()
            .into_iter()
            .filter(|session| {
                session.direction == DccDirection::Inbound
                    && session.state == DccState::Offered
                    && session.updated_at <= cutoff
            })
            .map(|session| session.id)
            .collect();
        for session_id in expired {
            if let Ok(session) =
                self.dcc
                    .transition(&session_id, DccState::Rejected, Timestamp::now())
            {
                self.notify_dcc_change(EventClass::DccRejected, &session);
            }
        }
        self.report_journal_pressure();
        self.flush_event_reads(true);
    }

    /// Journal a retention-pressure record when eviction has begun discarding
    /// events, and wake the resources that carry the accounting.
    ///
    /// This lives on the housekeeping tick rather than in the journal: a report
    /// is itself an event, so emitting it from the eviction loop would re-enter
    /// `push` while the journal is still over budget. Draining it here also
    /// means the report rate is bounded by the tick and by the journal's own
    /// rate limit, never by how fast events arrive.
    fn report_journal_pressure(&mut self) {
        let now = Timestamp::now();
        let Some(pressure) = self.journal.take_pressure_report(now) else {
            return;
        };
        tracing::warn!(
            agent_id = %self.id,
            evicted_events = pressure.evicted_events,
            evicted_bytes = pressure.evicted_bytes,
            evicted_since_previous_report = pressure.evicted_since_previous_report,
            retained_events = pressure.retained_events,
            max_events = pressure.max_events,
            "event journal is discarding records to stay inside its bounds"
        );
        let event = NewEvent {
            agent_id: self.id.clone(),
            direction: EventDirection::Internal,
            class: EventClass::JournalPressure,
            origin: EventOrigin::Gateway,
            verbosity: EventVerbosity::Semantic,
            target: None,
            server_time: None,
            received_at: now,
            correlation: EventCorrelation::default(),
            semantic: Some(EventPayload::Pressure(pressure)),
            wire: None,
            mentions_me: false,
        };
        if self.journal.push(event).is_ok() {
            self.notify_journal_append();
            self.flush_event_reads(false);
        }
        // The same counters appear in `status`, so a client watching connection
        // health learns it is losing events without reading the stream at all.
        self.notify_resources(&["status"]);
    }

    async fn wait_reconnect(&mut self, delay: Duration) -> ReconnectDecision {
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                () = &mut sleep => return ReconnectDecision::Reconnect,
                Some(command) = self.commands.recv() => match command {
                    AgentCommand::Execute { completion, .. } => {
                        let _ = completion.send(Err(GatewayError::NotConnected(self.id.clone())));
                    }
                    AgentCommand::ReadEvents { cursor, limit, wait, query, completion } => {
                        self.read_events(cursor, limit, wait, query, completion);
                    }
                    AgentCommand::ReadRecent { limit, query, completion } => {
                        let _ = completion.send(self.journal.read_latest(limit, &query));
                    }
                    AgentCommand::Snapshot { completion } => {
                        let _ = completion.send(self.snapshot());
                    }
                    AgentCommand::DccChatOpen { completion, .. }
                    | AgentCommand::DccSend { completion, .. } => {
                        let _ = completion.send(Err(GatewayError::NotConnected(self.id.clone())));
                    }
                    AgentCommand::DccChatSend { session_id, text, completion } => {
                        let result = self.send_dcc_chat(&session_id, text).await;
                        let _ = completion.send(result);
                    }
                    AgentCommand::DccAccept { completion, .. } => {
                        let _ = completion.send(Err(GatewayError::NotConnected(self.id.clone())));
                    }
                    AgentCommand::DccReject { session_id, completion } => {
                        let result = self.reject_dcc(&session_id);
                        let _ = completion.send(result);
                    }
                    AgentCommand::DccCancel { session_id, completion } => {
                        let result = self.cancel_dcc(&session_id);
                        let _ = completion.send(result);
                    }
                    AgentCommand::Disconnect { completion, .. } => {
                        let _ = completion.send(Ok(DisconnectReceipt {
                            quit_sent: false,
                            dcc_sessions_closed: self.dcc.active_len(),
                        }));
                        return ReconnectDecision::Shutdown;
                    }
                },
                Some(event) = self.dcc_event_rx.recv() => self.handle_dcc_runtime_event(event),
                Some(event) = self.dcc_chat_event_rx.recv() => self.handle_dcc_chat_event(event),
                else => return ReconnectDecision::Shutdown,
            }
        }
    }

    fn snapshot(&self) -> AgentSnapshot {
        // The newest window, not the oldest: once the journal retains more than
        // this many records a cursor-less `read` would hand back the start of
        // the buffer, which is the least useful end of it.
        let recent = self.journal.read_latest(
            self.config.limits.max_event_page_size.min(100),
            &RecentQuery::default(),
        );
        AgentSnapshot {
            state: self.state.borrow().clone(),
            protocol: self.protocol.clone(),
            line_budget: self.active_budget.into(),
            journal: self.journal.stats(),
            recent_events: recent,
            dcc_sessions: self.dcc.list(),
        }
    }

    fn reset_connection_protocol(&mut self) {
        self.isupport.reset();
        self.protocol = protocol_catalog();
        self.help_collector = HelpCollector::new();
        self.correlator.reset_connection();
        self.recovery_history_batches.clear();
        self.recovery_history_targets.clear();
        self.motd_query = None;
        self.active_budget = LineBudget::TRADITIONAL;
    }

    fn observe_protocol(&mut self, message: &WireMessage, framed: &mut IrcFramed) {
        if message.numeric() == Some(5) {
            self.isupport
                .apply_tokens(message.params.iter().skip(1).map(String::as_str));
            self.active_budget = self.isupport.line_budget();
            framed.codec_mut().set_budget(self.active_budget);
            self.correlator
                .set_case_mapping(self.isupport.case_mapping());
        }
        self.capabilities.publish_into(&mut self.protocol);
        self.protocol.isupport = self.isupport.tokens().clone();
        if let Some(response) = self.help_collector.apply(message) {
            self.protocol.record_help_response(&response);
            self.notify_resources(&["protocol"]);
        }
    }

    fn observe_queried_motd(&mut self, message: &WireMessage) {
        let Some(numeric) = message.numeric() else {
            return;
        };
        match numeric {
            375 => {
                self.motd_query = Some(MotdState {
                    source: Some(MotdSource::Query),
                    wire_replies: vec![message.clone()],
                    ..MotdState::default()
                });
            }
            372 => {
                let motd = self.motd_query.get_or_insert_with(|| MotdState {
                    source: Some(MotdSource::Query),
                    ..MotdState::default()
                });
                motd.lines
                    .push(message.trailing.clone().unwrap_or_default());
                motd.wire_replies.push(message.clone());
            }
            376 => {
                let mut motd = self.motd_query.take().unwrap_or_else(|| MotdState {
                    source: Some(MotdSource::Query),
                    ..MotdState::default()
                });
                motd.wire_replies.push(message.clone());
                motd.status = MotdStatus::Received;
                motd.text = motd.lines.join("\n");
                motd.received_at = Some(Timestamp::now());
                self.publish_queried_motd(motd);
            }
            422 => {
                let motd = MotdState {
                    status: MotdStatus::NotAvailable,
                    text: message.trailing.clone().unwrap_or_default(),
                    wire_replies: vec![message.clone()],
                    source: Some(MotdSource::Query),
                    received_at: Some(Timestamp::now()),
                    ..MotdState::default()
                };
                self.motd_query = None;
                self.publish_queried_motd(motd);
            }
            _ => {}
        }
    }

    fn publish_queried_motd(&mut self, motd: MotdState) {
        let now = Timestamp::now();
        let mut state = self.state.borrow().clone();
        state.motd = motd.clone();
        state.snapshot_at = now;
        self.state.send_replace(state);
        let _ = self.journal.push(NewEvent {
            agent_id: self.id.clone(),
            direction: EventDirection::Internal,
            class: EventClass::ServerMotd,
            origin: EventOrigin::Gateway,
            verbosity: EventVerbosity::Semantic,
            target: None,
            server_time: None,
            received_at: now,
            correlation: EventCorrelation::default(),
            semantic: Some(EventPayload::Motd(motd.clone())),
            wire: None,
            mentions_me: false,
        });
        self.notify_resources(&["motd", "state", "events"]);
    }

    /// Track server message IDs for reconnect deduplication and recovery.
    /// Explicit history reads are not deduplicated.
    fn remember_message(
        &mut self,
        message: &WireMessage,
        origin: EventOrigin,
        deduplicate: bool,
    ) -> bool {
        let is_message = matches!(
            message.command.to_ascii_uppercase().as_str(),
            "PRIVMSG" | "NOTICE" | "TAGMSG"
        );
        if !is_message {
            return false;
        }
        let message_id = message.tag_value("msgid").map(str::to_owned);
        let already_seen = message_id
            .as_ref()
            .is_some_and(|id| self.seen_message_ids.contains(id));
        let duplicate = deduplicate && already_seen;

        if let Some(target) = message
            .params
            .first()
            .filter(|target| self.isupport.is_channel(target))
        {
            let reference = HistoryReference::best(
                message_id.as_deref(),
                message.tag_value("time").and_then(|time| time.parse().ok()),
            );
            if let Some(reference) = reference
                && origin == EventOrigin::Live
            {
                self.history_markers.insert(
                    self.isupport.case_mapping().fold(target),
                    HistoryMarker {
                        target: target.clone(),
                        reference,
                    },
                );
            }
        }

        if let Some(message_id) = message_id
            && !already_seen
        {
            self.seen_message_ids.insert(message_id.clone());
            self.seen_message_order.push_back(message_id);
            while self.seen_message_order.len() > self.config.limits.event_count {
                if let Some(evicted) = self.seen_message_order.pop_front() {
                    self.seen_message_ids.remove(&evicted);
                }
            }
        }
        duplicate
    }

    fn remember_membership(
        &mut self,
        projection: &crate::irc::semantic::SemanticProjection,
        state: &AgentState,
    ) {
        let crate::irc::semantic::SemanticEvent::Membership {
            source,
            channel: Some(channel),
            subject,
            change,
            ..
        } = &projection.event
        else {
            return;
        };
        let own_nickname = state.identity.nickname.as_deref();
        let affected = subject.as_deref().unwrap_or(&source.name);
        if !own_nickname
            .is_some_and(|nickname| self.isupport.case_mapping().same(nickname, affected))
        {
            return;
        }
        match change {
            crate::irc::semantic::MembershipChange::Joined => {
                self.registration.channels.insert(channel.clone());
            }
            crate::irc::semantic::MembershipChange::Parted
            | crate::irc::semantic::MembershipChange::Kicked => {
                let mapping = self.isupport.case_mapping();
                self.registration
                    .channels
                    .retain(|remembered| !mapping.same(remembered.as_str(), channel.as_str()));
            }
            crate::irc::semantic::MembershipChange::Invited
            | crate::irc::semantic::MembershipChange::Quit => {}
        }
    }

    fn record_inbound(
        &mut self,
        message: WireMessage,
        origin: EventOrigin,
        deduplicate_message: bool,
        correlation: EventCorrelation,
    ) -> Option<EventCursor> {
        if self.remember_message(&message, origin, deduplicate_message) {
            return None;
        }
        let projection = project(&message, &self.isupport);
        let class = EventClass::from(projection.class);
        let target = message.params.first().cloned();
        let mentions_me = self.addresses_me(&projection);
        let event = NewEvent {
            agent_id: self.id.clone(),
            direction: EventDirection::Inbound,
            class,
            origin,
            verbosity: EventVerbosity::Semantic,
            target,
            server_time: message.tag_value("time").and_then(|time| time.parse().ok()),
            received_at: Timestamp::now(),
            correlation,
            semantic: Some(EventPayload::Irc(projection.clone())),
            wire: Some(message.clone()),
            mentions_me,
        };
        match self.journal.push(event) {
            Ok(cursor) => {
                let previous = self.state.borrow().clone();
                let mut state = previous.clone();
                state.reduce(
                    &projection,
                    cursor.clone(),
                    self.isupport.case_mapping(),
                    Timestamp::now(),
                );
                state.reduce_wire(&message, &self.isupport);
                self.remember_membership(&projection, &state);
                let channels_changed = previous.channels != state.channels;
                let identity_changed = previous.identity != state.identity;
                self.state.send_replace(state.clone());
                self.notify_journal_append();
                // Only claim the aggregate resources changed when they did.
                // Every inbound line used to invalidate `state`, which made a
                // subscription to it a fair approximation of "the connection is
                // receiving traffic" rather than a signal about state at all.
                if channels_changed || identity_changed {
                    self.notify_resources(&["state"]);
                }
                if identity_changed {
                    self.notify_resources(&["status"]);
                }
                if channels_changed {
                    self.notify_changed_channels(&previous.channels, &state.channels);
                }
                Some(cursor)
            }
            Err(error) => {
                tracing::warn!(agent_id = %self.id, %error, "event journal rejected inbound record");
                None
            }
        }
    }

    fn history_origin(
        &mut self,
        message: &WireMessage,
        attribution: &MessageAttribution,
    ) -> (EventOrigin, bool) {
        let explicit_history = attribution.command().is_some_and(|command| {
            command.eq_ignore_ascii_case("CHATHISTORY") || command.eq_ignore_ascii_case("HISTORY")
        });
        let batch_history =
            attribution.has_batch_kind("chathistory") || attribution.has_batch_kind("history");
        let mut recovery_history = attribution
            .batch_ids()
            .any(|batch| self.recovery_history_batches.contains(batch));
        if message.command.eq_ignore_ascii_case("BATCH")
            && let Some(reference) = message.params.first()
            && let Some(id) = reference.get(1..)
        {
            if reference.starts_with('+')
                && attribution
                    .direct_batch_kind()
                    .is_some_and(is_history_batch_kind)
            {
                let recovery_target = message
                    .params
                    .get(2)
                    .map(|target| self.isupport.case_mapping().fold(target));
                if recovery_target
                    .is_some_and(|target| self.recovery_history_targets.remove(&target))
                {
                    self.recovery_history_batches.insert(id.to_owned());
                    recovery_history = true;
                }
            }
            if reference.starts_with('-') {
                self.recovery_history_batches.remove(id);
            }
        }
        if explicit_history || batch_history {
            (EventOrigin::History, recovery_history)
        } else {
            (EventOrigin::Live, true)
        }
    }

    fn record_outbound(
        &mut self,
        message: &OutboundMessage,
        label: Option<&str>,
        command_id: Option<&CommandId>,
    ) {
        let Ok(encoded) = crate::irc::wire::encode_with_label(message, label, self.active_budget)
        else {
            return;
        };
        let without_crlf = encoded.slice(..encoded.len().saturating_sub(2));
        let Ok(wire) = WireMessage::parse(without_crlf) else {
            return;
        };
        let projection = project(&wire, &self.isupport);
        let is_message = matches!(
            wire.command.to_ascii_uppercase().as_str(),
            "PRIVMSG" | "NOTICE" | "TAGMSG"
        );
        let awaits_echo = is_message && self.capabilities.is_active("echo-message");
        let event = NewEvent {
            agent_id: self.id.clone(),
            direction: EventDirection::Outbound,
            class: EventClass::from(projection.class),
            origin: if is_message && !awaits_echo {
                EventOrigin::Synthetic
            } else {
                EventOrigin::Live
            },
            verbosity: if awaits_echo {
                EventVerbosity::Wire
            } else {
                EventVerbosity::Semantic
            },
            target: wire.params.first().cloned(),
            server_time: None,
            received_at: Timestamp::now(),
            correlation: EventCorrelation {
                command_id: command_id.map(|id| id.as_str().to_owned()),
                label: label.map(str::to_owned),
                role: Some(CorrelationRole::Request),
            },
            semantic: (!awaits_echo).then_some(EventPayload::Irc(projection)),
            wire: Some(wire),
            mentions_me: false,
        };
        if self.journal.push(event).is_ok() {
            self.notify_journal_append();
            self.flush_event_reads(false);
        }
    }

    fn record_malformed(&mut self, frame: InboundFrame) {
        let InboundFrame::Malformed(malformed) = frame else {
            return;
        };
        let semantic = EventPayload::MalformedLine(MalformedLine {
            reason: malformed.reason,
            observed_bytes_base64: STANDARD.encode(malformed.observed_bytes),
        });
        let event = NewEvent {
            agent_id: self.id.clone(),
            direction: EventDirection::Inbound,
            class: EventClass::ProtocolUnknown,
            origin: EventOrigin::Live,
            verbosity: EventVerbosity::Wire,
            target: None,
            server_time: None,
            received_at: Timestamp::now(),
            correlation: EventCorrelation::default(),
            semantic: Some(semantic),
            wire: None,
            mentions_me: false,
        };
        if self.journal.push(event).is_ok() {
            self.notify_journal_append();
        }
    }

    fn set_connection_state(&mut self, connection: ConnectionState, error: Option<String>) {
        let now = Timestamp::now();
        let mut state = self.state.borrow().clone();
        state.set_connection_state(connection, now);
        state.last_error = error;
        if connection != ConnectionState::Ready {
            state.registered = false;
        }
        // Degradation is the transition an operator has to be able to find in a
        // trace without a client attached, so it is logged at a level that
        // survives a default filter and carries the detail callers see in
        // `last_error`.
        if matches!(
            connection,
            ConnectionState::Reconnecting | ConnectionState::TerminalError
        ) {
            tracing::warn!(
                agent_id = %self.id,
                connection_state = ?connection,
                detail = state.last_error.as_deref().unwrap_or("none"),
                "IRC connection degraded"
            );
        } else {
            tracing::debug!(
                agent_id = %self.id,
                connection_state = ?connection,
                "IRC connection state changed"
            );
        }
        self.state.send_replace(state);
        let event = NewEvent {
            agent_id: self.id.clone(),
            direction: EventDirection::Internal,
            class: EventClass::ConnectionLifecycle,
            origin: EventOrigin::Gateway,
            verbosity: EventVerbosity::Semantic,
            target: None,
            server_time: None,
            received_at: now,
            correlation: EventCorrelation::default(),
            semantic: Some(EventPayload::Connection(ConnectionEvent {
                state: connection,
            })),
            wire: None,
            mentions_me: false,
        };
        let _ = self.journal.push(event);
        self.notify_journal_append();
        self.notify_resources(&["status", "state"]);
    }

    fn finish_registration(&mut self, nickname: &Nickname, motd: &MotdState) {
        self.correlator.set_nickname(nickname.clone());
        self.capabilities.publish_into(&mut self.protocol);
        let now = Timestamp::now();
        let mut state = self.state.borrow().clone();
        state.connection_state = ConnectionState::Ready;
        state.registered = true;
        state.connected_since = Some(now);
        state.identity.nickname = Some(nickname.to_string());
        state.identity.username = Some(self.registration.username.clone());
        state.identity.real_name = Some(self.registration.real_name.clone());
        state.motd = motd.clone();
        state.snapshot_at = now;
        state.last_error = None;
        // A ready connection has no attempt pending. The supervisor resets the
        // backoff a moment later, but clearing it here is what makes "ready
        // implies no scheduled attempt" true for every snapshot taken in
        // between, rather than only for the ones that arrive late enough.
        state.reconnect = ReconnectState::default();
        self.state.send_replace(state);
        let event = NewEvent {
            agent_id: self.id.clone(),
            direction: EventDirection::Internal,
            class: EventClass::ServerMotd,
            origin: EventOrigin::Gateway,
            verbosity: EventVerbosity::Semantic,
            target: None,
            server_time: None,
            received_at: now,
            correlation: EventCorrelation::default(),
            semantic: Some(EventPayload::Motd(motd.clone())),
            wire: None,
            mentions_me: false,
        };
        let _ = self.journal.push(event);
        self.notify_resources(&["status", "state", "motd", "protocol", "events"]);
    }

    async fn restore_channels(&mut self, framed: &mut IrcFramed) -> Result<()> {
        let channels: Vec<String> = self
            .registration
            .channels
            .iter()
            .map(ChannelName::to_string)
            .collect();
        for channel in channels {
            self.write_uncorrelated(
                framed,
                OutboundMessage::new("JOIN", vec![channel.clone()]),
                true,
            )
            .await?;
            for message in [
                OutboundMessage::new("NAMES", vec![channel.clone()]),
                OutboundMessage::new("TOPIC", vec![channel.clone()]),
                OutboundMessage::new("MODE", vec![channel]),
            ] {
                self.write_uncorrelated(framed, message, true).await?;
            }
        }
        Ok(())
    }

    async fn recover_history(&mut self, framed: &mut IrcFramed) -> Result<()> {
        if !self.capabilities.is_active("chathistory") {
            return Ok(());
        }
        let server_limit = self
            .isupport
            .token("CHATHISTORY")
            .and_then(|token| token.value.as_deref())
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|limit| *limit > 0)
            .unwrap_or(100);
        let limit = server_limit
            .min(self.config.limits.max_event_page_size)
            .max(1);
        let channels: Vec<String> = self
            .registration
            .channels
            .iter()
            .map(ChannelName::to_string)
            .collect();
        for channel in channels {
            let key = self.isupport.case_mapping().fold(&channel);
            let params = self.history_markers.get(&key).map_or_else(
                || {
                    vec![
                        "LATEST".into(),
                        channel.clone(),
                        "*".into(),
                        limit.to_string(),
                    ]
                },
                |marker| {
                    vec![
                        "AFTER".into(),
                        marker.target.clone(),
                        marker.reference.to_wire(),
                        limit.to_string(),
                    ]
                },
            );
            self.recovery_history_targets.insert(key.clone());
            if let Err(error) = self
                .write_uncorrelated(framed, OutboundMessage::new("CHATHISTORY", params), true)
                .await
            {
                self.recovery_history_targets.remove(&key);
                return Err(error);
            }
        }
        Ok(())
    }

    async fn write_uncorrelated(
        &mut self,
        framed: &mut IrcFramed,
        message: OutboundMessage,
        observable: bool,
    ) -> Result<()> {
        tokio::time::timeout(
            Duration::from_millis(self.config.limits.max_command_timeout_ms),
            framed.send(message.clone()),
        )
        .await
        .map_err(|_| GatewayError::Indeterminate("the IRC write deadline elapsed".into()))?
        .map_err(map_codec_error)?;
        if observable {
            self.record_outbound(&message, None, None);
        }
        Ok(())
    }

    async fn send_pong(&mut self, framed: &mut IrcFramed, ping: &WireMessage) -> Result<()> {
        let mut pong = OutboundMessage::new("PONG", ping.params.clone());
        pong.trailing.clone_from(&ping.trailing);
        self.write_uncorrelated(framed, pong, true).await
    }

    async fn send_sasl_payload(
        &mut self,
        framed: &mut IrcFramed,
        sasl: Option<&crate::config::ResolvedSasl>,
    ) -> Result<()> {
        let sasl = sasl.ok_or_else(|| {
            GatewayError::Configuration("SASL was negotiated without configured credentials".into())
        })?;
        let payload = STANDARD.encode(format!(
            "\0{}\0{}",
            sasl.username,
            sasl.password.expose_secret()
        ));
        for chunk in payload.as_bytes().chunks(400) {
            let chunk = std::str::from_utf8(chunk)
                .expect("base64 is ASCII")
                .to_owned();
            self.write_uncorrelated(
                framed,
                OutboundMessage::new("AUTHENTICATE", vec![chunk]),
                false,
            )
            .await?;
        }
        if payload.len().is_multiple_of(400) {
            self.write_uncorrelated(
                framed,
                OutboundMessage::new("AUTHENTICATE", vec!["+".into()]),
                false,
            )
            .await?;
        }
        Ok(())
    }

    /// Publish the scheduled reconnect attempt, or clear it after recovery.
    ///
    /// `next_attempt_at` is the only field that tells a caller *when* the relay
    /// will try again; `delay_ms` alone leaves it guessing how much of the wait
    /// has already elapsed since it read the resource. A zero delay means the
    /// backoff sequence was reset, so both fields clear together.
    fn update_reconnect_state(&mut self, attempt: u32, delay: Duration) {
        let now = Timestamp::now();
        let mut state = self.state.borrow().clone();
        state.reconnect.attempt = attempt;
        state.reconnect.delay_ms = (!delay.is_zero()).then_some(delay.as_millis() as u64);
        state.reconnect.next_attempt_at = (!delay.is_zero()).then(|| {
            Timestamp::from_datetime(
                now.as_datetime()
                    + chrono::Duration::milliseconds(
                        i64::try_from(delay.as_millis()).unwrap_or(i64::MAX),
                    ),
            )
        });
        state.snapshot_at = now;
        let scheduled = state.reconnect.next_attempt_at.map(Timestamp::to_rfc3339);
        tracing::info!(
            agent_id = %self.id,
            attempt,
            delay_ms = delay.as_millis() as u64,
            next_attempt_at = scheduled.as_deref().unwrap_or("none"),
            "reconnect schedule updated"
        );
        self.state.send_replace(state);
        self.notify_resources(&["status"]);
    }

    async fn open_dcc_chat(
        &mut self,
        framed: &mut IrcFramed,
        peer: String,
        reverse: bool,
    ) -> Result<DccSession> {
        validate_peer(&peer)?;
        self.dcc.ensure_capacity().map_err(dcc_manager_error)?;
        let now = Timestamp::now();
        let mut session = DccSession::offered(DccKind::Chat, DccDirection::Outbound, &peer, now);
        session.reverse = reverse;
        let mut ordinary_listener = None;
        let offer = if reverse {
            let token = uuid::Uuid::new_v4().simple().to_string();
            session.token = Some(token.clone());
            DccOffer::Chat {
                address: "0".into(),
                port: 0,
                token: Some(token),
            }
        } else {
            let listener = bind_listener(&self.config.dcc, &self.config.irc).await?;
            session.endpoint = Some(listener.advertised_endpoint);
            ordinary_listener = Some(listener.listener);
            DccOffer::Chat {
                address: encode_address(listener.advertised_endpoint.ip()),
                port: listener.advertised_endpoint.port(),
                token: None,
            }
        };
        self.dcc
            .insert(session.clone())
            .map_err(dcc_manager_error)?;
        if let Err(error) = self.send_dcc_offer(framed, &peer, offer).await {
            let _ = self
                .dcc
                .fail(&session.id, error.to_string(), Timestamp::now());
            self.cancel_dcc_runtime(&session.id);
            return Err(error);
        }
        // Start the deadline only after the IRC offer is written so the peer
        // receives the complete configured acceptance window.
        if let Some(listener) = ordinary_listener {
            self.spawn_chat_connection(
                session.id.clone(),
                accept_offer(listener, offer_accept_timeout(&self.config.dcc)),
            );
        }
        self.notify_dcc_change(EventClass::DccChatOffered, &session);
        Ok(session)
    }

    async fn send_dcc_chat(&mut self, session_id: &DccSessionId, text: String) -> Result<()> {
        let session = self.dcc.get(session_id).map_err(dcc_manager_error)?;
        if session.kind != DccKind::Chat || session.state != DccState::Active {
            return Err(GatewayError::Dcc(format!(
                "DCC CHAT session {session_id} is not active"
            )));
        }
        let handle = self.dcc_chat_handles.get(session_id).ok_or_else(|| {
            GatewayError::Dcc(format!("DCC CHAT runtime is missing for {session_id}"))
        })?;
        handle
            .send(text)
            .await
            .map_err(|error| GatewayError::Dcc(error.to_string()))
    }

    async fn open_dcc_send(
        &mut self,
        framed: &mut IrcFramed,
        peer: String,
        source_path: PathBuf,
        advertised_filename: Option<String>,
        reverse: bool,
    ) -> Result<DccSession> {
        validate_peer(&peer)?;
        self.dcc.ensure_capacity().map_err(dcc_manager_error)?;
        let metadata = dcc_source_metadata(&source_path).await?;
        let filename = advertised_filename.unwrap_or_else(|| {
            source_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("download")
                .to_owned()
        });
        validate_filename(&filename).map_err(|error| GatewayError::Dcc(error.to_string()))?;
        let now = Timestamp::now();
        let mut session = DccSession::offered(DccKind::Send, DccDirection::Outbound, &peer, now);
        session.reverse = reverse;
        session.filename = Some(filename.clone());
        session.local_path = Some(source_path.clone());
        session.total_bytes = Some(metadata.len());
        let mut ordinary_listener = None;
        let offer = if reverse {
            let token = uuid::Uuid::new_v4().simple().to_string();
            session.token = Some(token.clone());
            DccOffer::Send {
                filename,
                address: "0".into(),
                port: 0,
                size: Some(metadata.len()),
                token: Some(token),
            }
        } else {
            let listener = bind_listener(&self.config.dcc, &self.config.irc).await?;
            session.endpoint = Some(listener.advertised_endpoint);
            ordinary_listener = Some(listener.listener);
            DccOffer::Send {
                filename,
                address: encode_address(listener.advertised_endpoint.ip()),
                port: listener.advertised_endpoint.port(),
                size: Some(metadata.len()),
                token: None,
            }
        };
        self.dcc
            .insert(session.clone())
            .map_err(dcc_manager_error)?;
        if let Err(error) = self.send_dcc_offer(framed, &peer, offer).await {
            let _ = self
                .dcc
                .fail(&session.id, error.to_string(), Timestamp::now());
            self.cancel_dcc_runtime(&session.id);
            return Err(error);
        }
        // Start the deadline only after the IRC offer is written so the peer
        // receives the complete configured acceptance window.
        if let Some(listener) = ordinary_listener {
            let (resume_tx, resume_rx) = watch::channel(0);
            self.dcc_resume_offsets
                .insert(session.id.clone(), resume_tx);
            self.spawn_send_transfer(
                session.id.clone(),
                accept_offer(listener, offer_accept_timeout(&self.config.dcc)),
                source_path,
                resume_rx,
            );
        }
        self.notify_dcc_change(EventClass::DccTransferOffered, &session);
        Ok(session)
    }

    async fn accept_dcc(
        &mut self,
        framed: &mut IrcFramed,
        request: DccAcceptRequest,
    ) -> Result<DccSession> {
        let session = self
            .dcc
            .get(&request.session_id)
            .map_err(dcc_manager_error)?
            .clone();
        if session.direction != DccDirection::Inbound || session.state != DccState::Offered {
            return Err(GatewayError::Dcc(format!(
                "DCC session {} is not an incoming offer",
                request.session_id
            )));
        }
        let destination = match session.kind {
            DccKind::Chat => None,
            DccKind::Send => Some(
                self.resolve_dcc_destination(request.destination.as_ref().ok_or_else(|| {
                    GatewayError::Dcc(
                        "a receive root and destination are required for DCC SEND".into(),
                    )
                })?)
                .await?,
            ),
        };
        let stream = if session.reverse {
            let listener = bind_listener(&self.config.dcc, &self.config.irc).await?;
            let endpoint = listener.advertised_endpoint;
            let offer = match session.kind {
                DccKind::Chat => DccOffer::Chat {
                    address: encode_address(endpoint.ip()),
                    port: endpoint.port(),
                    token: session.token.clone(),
                },
                DccKind::Send => DccOffer::Send {
                    filename: session
                        .filename
                        .clone()
                        .unwrap_or_else(|| "download".into()),
                    address: encode_address(endpoint.ip()),
                    port: endpoint.port(),
                    size: session.total_bytes,
                    token: session.token.clone(),
                },
            };
            self.send_dcc_offer(framed, &session.peer, offer).await?;
            self.dcc
                .get_mut(&request.session_id)
                .map_err(dcc_manager_error)?
                .endpoint = Some(endpoint);
            DirectSetup::Accept(listener.listener)
        } else {
            let endpoint = session.endpoint.ok_or_else(|| {
                GatewayError::Dcc("incoming DCC offer has no usable endpoint".into())
            })?;
            DirectSetup::Connect(endpoint)
        };
        if let Some(destination) = &destination {
            let session = self
                .dcc
                .get_mut(&request.session_id)
                .map_err(dcc_manager_error)?;
            session.local_path = Some(destination.path());
            session.receive_root = Some(destination.root().to_owned());
            session.receive_path = Some(destination.relative_path());
        }
        self.dcc
            .transition(&request.session_id, DccState::Connecting, Timestamp::now())
            .map_err(dcc_manager_error)?;

        match session.kind {
            DccKind::Chat => match stream {
                DirectSetup::Accept(listener) => self.spawn_chat_connection(
                    request.session_id.clone(),
                    accept_offer(listener, offer_accept_timeout(&self.config.dcc)),
                ),
                DirectSetup::Connect(endpoint) => self.spawn_chat_connection(
                    request.session_id.clone(),
                    connect_direct(endpoint, self.dcc_connect_timeout()),
                ),
            },
            DccKind::Send => {
                let destination = destination.expect("DCC SEND destination validated above");
                let expected = session.total_bytes;
                match stream {
                    DirectSetup::Accept(listener) => self.spawn_receive_transfer(
                        request.session_id.clone(),
                        accept_offer(listener, offer_accept_timeout(&self.config.dcc)),
                        destination,
                        request.conflict,
                        expected,
                    ),
                    DirectSetup::Connect(endpoint) => self.spawn_receive_transfer(
                        request.session_id.clone(),
                        connect_direct(endpoint, self.dcc_connect_timeout()),
                        destination,
                        request.conflict,
                        expected,
                    ),
                }
            }
        }
        self.notify_resources(&["dcc"]);
        self.dcc
            .get(&request.session_id)
            .cloned()
            .map_err(dcc_manager_error)
    }

    fn reject_dcc(&mut self, session_id: &DccSessionId) -> Result<DccSession> {
        let session = self.dcc.get(session_id).map_err(dcc_manager_error)?;
        if session.state.is_terminal() {
            return Ok(session.clone());
        }
        if session.state != DccState::Offered {
            return Err(GatewayError::Dcc(format!(
                "only an offered DCC session can be rejected: {session_id}"
            )));
        }
        self.cancel_dcc_runtime(session_id);
        let session = self
            .dcc
            .transition(session_id, DccState::Rejected, Timestamp::now())
            .map_err(dcc_manager_error)?;
        self.notify_dcc_change(EventClass::DccRejected, &session);
        Ok(session)
    }

    fn cancel_dcc(&mut self, session_id: &DccSessionId) -> Result<DccSession> {
        let session = self.dcc.get(session_id).map_err(dcc_manager_error)?;
        if session.state.is_terminal() {
            return Ok(session.clone());
        }
        self.cancel_dcc_runtime(session_id);
        let session = self
            .dcc
            .transition(session_id, DccState::Cancelled, Timestamp::now())
            .map_err(dcc_manager_error)?;
        self.notify_dcc_change(EventClass::DccCancelled, &session);
        Ok(session)
    }

    /// Turn a caller's root name and relative path into a directory this
    /// process holds open plus the file to create inside it.
    ///
    /// The root name is looked up in configuration here, at the boundary, and
    /// never carried further: everything downstream works from the resolved
    /// capability, so no later code can be handed a path that only claims to be
    /// beneath a root.
    async fn resolve_dcc_destination(&self, choice: &ReceiveChoice) -> Result<ReceiveDestination> {
        let root = self.config.dcc.receive_root(&choice.root).ok_or_else(|| {
            GatewayError::Dcc(format!(
                "unknown DCC receive root {:?}; configured roots are {}",
                choice.root,
                self.config.dcc.receive_root_names().join(", ")
            ))
        })?;
        ReceiveDestination::resolve(&root, &choice.path)
            .await
            .map_err(|error| GatewayError::Dcc(error.to_string()))
    }

    fn incoming_offer_limit_reached(&self, peer: &str) -> bool {
        self.dcc
            .list()
            .into_iter()
            .filter(|session| {
                session.direction == DccDirection::Inbound
                    && session.state == DccState::Offered
                    && self.isupport.case_mapping().same(&session.peer, peer)
            })
            .count()
            >= self.config.dcc.max_offers_per_peer
    }

    fn record_dcc_offer_limit(&mut self, peer: &str) {
        self.record_dcc_event(
            EventClass::DccFailed,
            Some(peer.to_owned()),
            EventPayload::DccFailure(DccFailure {
                peer: None,
                error: format!(
                    "incoming DCC offer limit reached for peer ({})",
                    self.config.dcc.max_offers_per_peer
                ),
            }),
        );
    }

    async fn respond_to_ctcp(
        &mut self,
        framed: &mut IrcFramed,
        message: &WireMessage,
    ) -> Result<()> {
        if !message.command.eq_ignore_ascii_case("PRIVMSG") {
            return Ok(());
        }
        let Some(target) = message.params.first() else {
            return Ok(());
        };
        let Some(own_nickname) = self.state.borrow().identity.nickname.clone() else {
            return Ok(());
        };
        if !self.isupport.case_mapping().same(target, &own_nickname) {
            return Ok(());
        }
        let Some(peer) = message.prefix.as_ref().map(|prefix| prefix.name.clone()) else {
            return Ok(());
        };
        if self.isupport.case_mapping().same(&peer, &own_nickname) {
            return Ok(());
        }
        let Some(query) = message.trailing.as_deref().and_then(CtcpMessage::parse) else {
            return Ok(());
        };
        let reply_body = match query.command.as_str() {
            "CLIENTINFO" => Some(Some("ACTION CLIENTINFO DCC PING TIME VERSION".into())),
            "PING" => Some(query.body),
            "TIME" => Some(Some(Timestamp::now().to_rfc3339())),
            "VERSION" => Some(Some(format!("rmcp-irc/{}", env!("CARGO_PKG_VERSION")))),
            // ACTION is content and DCC has its own negotiation path. Unknown
            // CTCP remains observable without inventing a reply.
            "ACTION" | "DCC" => None,
            _ => None,
        };
        let Some(body) = reply_body else {
            return Ok(());
        };
        let reply = CtcpMessage {
            command: query.command,
            body,
        }
        .encode()
        .map_err(|error| GatewayError::InvalidMessage(error.to_string()))?;
        self.write_uncorrelated(
            framed,
            OutboundMessage::new("NOTICE", vec![peer]).with_trailing(reply),
            true,
        )
        .await
    }

    async fn observe_dcc_control(&mut self, framed: &mut IrcFramed, message: &WireMessage) {
        if !message.command.eq_ignore_ascii_case("PRIVMSG") {
            return;
        }
        let Some(target) = message.params.first() else {
            return;
        };
        let Some(own_nickname) = self.state.borrow().identity.nickname.clone() else {
            return;
        };
        if !self.isupport.case_mapping().same(target, &own_nickname) {
            // DCC negotiation is only meaningful as a direct message. In
            // particular, a channel message must not consume offer capacity.
            return;
        }
        let Some(ctcp) = message.trailing.as_deref().and_then(CtcpMessage::parse) else {
            return;
        };
        if ctcp.command != "DCC" {
            return;
        }
        let Some(peer) = message.prefix.as_ref().map(|prefix| prefix.name.clone()) else {
            return;
        };
        if self
            .state
            .borrow()
            .identity
            .nickname
            .as_deref()
            .is_some_and(|nickname| self.isupport.case_mapping().same(&peer, nickname))
        {
            // `echo-message` reflects our outbound CTCP. It confirms the IRC
            // write but must never be reinterpreted as a peer's inbound offer.
            return;
        }
        let parsed = ctcp
            .body
            .as_deref()
            .ok_or_else(|| GatewayError::Dcc("DCC CTCP has no body".into()))
            .and_then(|body| {
                DccOffer::parse(body).map_err(|error| GatewayError::Dcc(error.to_string()))
            });
        let offer = match parsed {
            Ok(offer) => offer,
            Err(error) => {
                self.record_dcc_event(
                    EventClass::DccFailed,
                    None,
                    EventPayload::DccFailure(DccFailure {
                        peer: Some(peer.clone()),
                        error: error.to_string(),
                    }),
                );
                return;
            }
        };
        match offer {
            DccOffer::Chat {
                address,
                port,
                token,
            } => {
                if port != 0
                    && let Some(id) = self.reverse_session(&peer, DccKind::Chat, token.as_deref())
                {
                    match parse_endpoint(&address, port, self.config.dcc.allow_private_addresses) {
                        Ok(endpoint) => {
                            if let Ok(session) = self.dcc.get_mut(&id) {
                                session.endpoint = Some(endpoint);
                            }
                            self.spawn_chat_connection(
                                id,
                                connect_direct(endpoint, self.dcc_connect_timeout()),
                            );
                        }
                        Err(error) => self.fail_dcc(&id, error.to_string()),
                    }
                    return;
                }
                let mut session = DccSession::offered(
                    DccKind::Chat,
                    DccDirection::Inbound,
                    peer,
                    Timestamp::now(),
                );
                session.reverse = port == 0;
                session.token = token;
                if port != 0 {
                    match parse_endpoint(&address, port, self.config.dcc.allow_private_addresses) {
                        Ok(endpoint) => session.endpoint = Some(endpoint),
                        Err(error) => {
                            self.record_dcc_event(
                                EventClass::DccFailed,
                                None,
                                EventPayload::DccFailure(DccFailure {
                                    peer: None,
                                    error: error.to_string(),
                                }),
                            );
                            return;
                        }
                    }
                }
                if self.incoming_offer_limit_reached(&session.peer) {
                    self.record_dcc_offer_limit(&session.peer);
                    return;
                }
                if let Err(error) = self.dcc.insert(session.clone()) {
                    self.record_dcc_event(
                        EventClass::DccFailed,
                        Some(session.peer.clone()),
                        EventPayload::DccFailure(DccFailure {
                            peer: None,
                            error: dcc_manager_error(error).to_string(),
                        }),
                    );
                    return;
                }
                self.notify_dcc_change(EventClass::DccChatOffered, &session);
                if self.config.dcc.automatic_accept_chat {
                    let request = DccAcceptRequest {
                        session_id: session.id.clone(),
                        destination: None,
                        conflict: DestinationConflict::Fail,
                    };
                    if let Err(error) = self.accept_dcc(framed, request).await {
                        self.fail_dcc(&session.id, error.to_string());
                    }
                }
            }
            DccOffer::Send {
                filename,
                address,
                port,
                size,
                token,
            } => {
                if port != 0
                    && let Some(id) = self.reverse_session(&peer, DccKind::Send, token.as_deref())
                {
                    match parse_endpoint(&address, port, self.config.dcc.allow_private_addresses) {
                        Ok(endpoint) => {
                            let source = self.dcc.get(&id).ok().and_then(|s| s.local_path.clone());
                            if let Some(source) = source {
                                if let Ok(session) = self.dcc.get_mut(&id) {
                                    session.endpoint = Some(endpoint);
                                }
                                let (resume_tx, resume_rx) = watch::channel(0);
                                self.dcc_resume_offsets.insert(id.clone(), resume_tx);
                                self.spawn_send_transfer(
                                    id,
                                    connect_direct(endpoint, self.dcc_connect_timeout()),
                                    source,
                                    resume_rx,
                                );
                            }
                        }
                        Err(error) => self.fail_dcc(&id, error.to_string()),
                    }
                    return;
                }
                let mut session = DccSession::offered(
                    DccKind::Send,
                    DccDirection::Inbound,
                    peer,
                    Timestamp::now(),
                );
                session.reverse = port == 0;
                session.token = token;
                session.filename = Some(filename);
                session.total_bytes = size;
                if size.is_some_and(|size| size > self.config.dcc.max_transfer_bytes) {
                    self.record_dcc_event(
                        EventClass::DccFailed,
                        Some(session.peer.clone()),
                        EventPayload::DccFailure(DccFailure {
                            peer: None,
                            error: format!(
                                "DCC SEND exceeds the configured {}-byte limit",
                                self.config.dcc.max_transfer_bytes
                            ),
                        }),
                    );
                    return;
                }
                if port != 0 {
                    match parse_endpoint(&address, port, self.config.dcc.allow_private_addresses) {
                        Ok(endpoint) => session.endpoint = Some(endpoint),
                        Err(error) => {
                            self.record_dcc_event(
                                EventClass::DccFailed,
                                None,
                                EventPayload::DccFailure(DccFailure {
                                    peer: None,
                                    error: error.to_string(),
                                }),
                            );
                            return;
                        }
                    }
                }
                if self.incoming_offer_limit_reached(&session.peer) {
                    self.record_dcc_offer_limit(&session.peer);
                    return;
                }
                if let Err(error) = self.dcc.insert(session.clone()) {
                    self.record_dcc_event(
                        EventClass::DccFailed,
                        Some(session.peer.clone()),
                        EventPayload::DccFailure(DccFailure {
                            peer: None,
                            error: dcc_manager_error(error).to_string(),
                        }),
                    );
                    return;
                }
                self.notify_dcc_change(EventClass::DccTransferOffered, &session);
                if self.config.dcc.automatic_accept_send {
                    // Nobody is present to choose, so this takes the default
                    // root and the offered filename — which negotiation has
                    // already reduced to a single ordinary component.
                    let destination = session.filename.as_ref().map(|filename| ReceiveChoice {
                        root: self.config.dcc.default_receive_root().name,
                        path: PathBuf::from(filename),
                    });
                    let request = DccAcceptRequest {
                        session_id: session.id.clone(),
                        destination,
                        conflict: DestinationConflict::Fail,
                    };
                    if let Err(error) = self.accept_dcc(framed, request).await {
                        self.fail_dcc(&session.id, error.to_string());
                    }
                }
            }
            DccOffer::Resume {
                filename,
                port,
                position,
                token,
            } => {
                if let Some(id) = self.resume_session(&filename, port, token.as_deref(), position) {
                    if let Some(offset) = self.dcc_resume_offsets.get(&id) {
                        offset.send_replace(position);
                    }
                    let accept = DccOffer::Accept {
                        filename,
                        port,
                        position,
                        token,
                    };
                    if let Some(peer) = self.dcc.get(&id).ok().map(|session| session.peer.clone())
                        && let Err(error) = self.send_dcc_offer(framed, &peer, accept).await
                    {
                        self.fail_dcc(&id, error.to_string());
                    }
                }
            }
            DccOffer::Accept { .. } => {
                // The outbound sender resumes after answering RESUME; ACCEPT
                // remains observable on the IRC wire.
            }
        }
    }

    fn reverse_session(
        &self,
        peer: &str,
        kind: DccKind,
        token: Option<&str>,
    ) -> Option<DccSessionId> {
        self.dcc.list().into_iter().find_map(|session| {
            (session.direction == DccDirection::Outbound
                && session.reverse
                && session.kind == kind
                && session.peer.eq_ignore_ascii_case(peer)
                && session.token.as_deref() == token
                && !session.state.is_terminal())
            .then_some(session.id)
        })
    }

    fn resume_session(
        &self,
        filename: &str,
        port: u16,
        token: Option<&str>,
        position: u64,
    ) -> Option<DccSessionId> {
        self.dcc.list().into_iter().find_map(|session| {
            (session.direction == DccDirection::Outbound
                && session.kind == DccKind::Send
                && session.filename.as_deref() == Some(filename)
                && session
                    .endpoint
                    .is_some_and(|endpoint| endpoint.port() == port)
                && session.token.as_deref() == token
                && session.total_bytes.is_some_and(|total| position <= total)
                && !session.state.is_terminal())
            .then_some(session.id)
        })
    }

    async fn send_dcc_offer(
        &mut self,
        framed: &mut IrcFramed,
        peer: &str,
        offer: DccOffer,
    ) -> Result<()> {
        let text = offer
            .encode_ctcp()
            .map_err(|error| GatewayError::Dcc(error.to_string()))?;
        self.write_uncorrelated(
            framed,
            OutboundMessage::new("PRIVMSG", vec![peer.to_owned()]).with_trailing(text),
            true,
        )
        .await
    }

    fn spawn_chat_connection<F>(&mut self, session_id: DccSessionId, connection: F)
    where
        F: Future<Output = Result<TcpStream>> + Send + 'static,
    {
        let cancellation = CancellationToken::new();
        let child = cancellation.clone();
        let events = self.dcc_events.clone();
        let event_id = session_id.clone();
        let task = tokio::spawn(async move {
            let result = tokio::select! {
                () = child.cancelled() => return,
                result = connection => result,
            };
            let event = match result {
                Ok(stream) => DccRuntimeEvent::ChatConnected {
                    session_id: event_id.clone(),
                    stream,
                },
                Err(error) => DccRuntimeEvent::Failed {
                    session_id: event_id.clone(),
                    error: error.to_string(),
                },
            };
            let _ = events.send(event).await;
        });
        self.install_dcc_task(session_id, cancellation, task);
    }

    fn spawn_send_transfer<F>(
        &mut self,
        session_id: DccSessionId,
        connection: F,
        source: PathBuf,
        resume: watch::Receiver<u64>,
    ) where
        F: Future<Output = Result<TcpStream>> + Send + 'static,
    {
        let cancellation = CancellationToken::new();
        let child = cancellation.clone();
        let events = self.dcc_events.clone();
        let event_id = session_id.clone();
        let buffer = self.config.dcc.transfer_buffer_bytes;
        let idle_timeout = Duration::from_millis(self.config.dcc.idle_timeout_ms);
        let queue = self.config.limits.command_queue;
        let task = tokio::spawn(async move {
            let stream = match tokio::select! {
                () = child.cancelled() => return,
                result = connection => result,
            } {
                Ok(stream) => stream,
                Err(error) => {
                    let _ = events
                        .send(DccRuntimeEvent::Failed {
                            session_id: event_id,
                            error: error.to_string(),
                        })
                        .await;
                    return;
                }
            };
            let _ = events
                .send(DccRuntimeEvent::TransferConnected {
                    session_id: event_id.clone(),
                })
                .await;
            let (progress_tx, mut progress_rx) = mpsc::channel(queue);
            let transfer = send_file(
                stream,
                event_id.clone(),
                &source,
                TransferOptions {
                    resume_offset: *resume.borrow(),
                    buffer_bytes: buffer,
                    idle_timeout,
                    progress: Some(progress_tx),
                },
            );
            tokio::pin!(transfer);
            loop {
                tokio::select! {
                    () = child.cancelled() => return,
                    progress = progress_rx.recv() => {
                        if let Some(progress) = progress {
                            let _ = events.try_send(DccRuntimeEvent::TransferProgress(progress));
                        }
                    }
                    result = &mut transfer => {
                        let event = match result {
                            Ok(transferred_bytes) => DccRuntimeEvent::TransferCompleted {
                                session_id: event_id.clone(),
                                received: None,
                                transferred_bytes,
                            },
                            Err(error) => DccRuntimeEvent::Failed {
                                session_id: event_id.clone(),
                                error: error.to_string(),
                            },
                        };
                        let _ = events.send(event).await;
                        return;
                    }
                }
            }
        });
        self.install_dcc_task(session_id, cancellation, task);
    }

    fn spawn_receive_transfer<F>(
        &mut self,
        session_id: DccSessionId,
        connection: F,
        destination: ReceiveDestination,
        conflict: DestinationConflict,
        expected_bytes: Option<u64>,
    ) where
        F: Future<Output = Result<TcpStream>> + Send + 'static,
    {
        let cancellation = CancellationToken::new();
        let child = cancellation.clone();
        let events = self.dcc_events.clone();
        let event_id = session_id.clone();
        let buffer = self.config.dcc.transfer_buffer_bytes;
        let idle_timeout = Duration::from_millis(self.config.dcc.idle_timeout_ms);
        let max_bytes = self.config.dcc.max_transfer_bytes;
        let queue = self.config.limits.command_queue;
        let task = tokio::spawn(async move {
            let stream = match tokio::select! {
                () = child.cancelled() => return,
                result = connection => result,
            } {
                Ok(stream) => stream,
                Err(error) => {
                    let _ = events
                        .send(DccRuntimeEvent::Failed {
                            session_id: event_id,
                            error: error.to_string(),
                        })
                        .await;
                    return;
                }
            };
            let _ = events
                .send(DccRuntimeEvent::TransferConnected {
                    session_id: event_id.clone(),
                })
                .await;
            let (progress_tx, mut progress_rx) = mpsc::channel(queue);
            let transfer = receive_file(
                stream,
                event_id.clone(),
                destination,
                ReceiveOptions {
                    conflict,
                    expected_size: expected_bytes,
                    max_bytes,
                    transfer: TransferOptions {
                        resume_offset: 0,
                        buffer_bytes: buffer,
                        idle_timeout,
                        progress: Some(progress_tx),
                    },
                },
            );
            tokio::pin!(transfer);
            loop {
                tokio::select! {
                    () = child.cancelled() => return,
                    progress = progress_rx.recv() => {
                        if let Some(progress) = progress {
                            let _ = events.try_send(DccRuntimeEvent::TransferProgress(progress));
                        }
                    }
                    result = &mut transfer => {
                        let event = match result {
                            Ok(received) => DccRuntimeEvent::TransferCompleted {
                                session_id: event_id.clone(),
                                transferred_bytes: received.bytes,
                                received: Some(received),
                            },
                            Err(error) => DccRuntimeEvent::Failed {
                                session_id: event_id.clone(),
                                error: error.to_string(),
                            },
                        };
                        let _ = events.send(event).await;
                        return;
                    }
                }
            }
        });
        self.install_dcc_task(session_id, cancellation, task);
    }

    fn install_dcc_task(
        &mut self,
        session_id: DccSessionId,
        cancellation: CancellationToken,
        task: tokio::task::JoinHandle<()>,
    ) {
        self.cancel_dcc_runtime(&session_id);
        self.dcc_cancellations
            .insert(session_id.clone(), cancellation);
        self.dcc_tasks.insert(session_id, task);
    }

    fn cancel_dcc_runtime(&mut self, session_id: &DccSessionId) {
        if let Some(cancellation) = self.dcc_cancellations.remove(session_id) {
            cancellation.cancel();
        }
        if let Some(task) = self.dcc_tasks.remove(session_id) {
            task.abort();
        }
        if let Some(handle) = self.dcc_chat_handles.remove(session_id) {
            handle.cancel();
        }
        self.dcc_resume_offsets.remove(session_id);
    }

    fn handle_dcc_runtime_event(&mut self, event: DccRuntimeEvent) {
        match event {
            DccRuntimeEvent::ChatConnected { session_id, stream } => {
                self.dcc_tasks.remove(&session_id);
                self.dcc_cancellations.remove(&session_id);
                if self
                    .transition_connected(&session_id, DccState::Active)
                    .is_err()
                {
                    return;
                }
                match spawn_chat(
                    stream,
                    session_id.clone(),
                    self.config.dcc.chat_queue,
                    self.config.dcc.chat_line_bytes,
                    Duration::from_millis(self.config.dcc.idle_timeout_ms),
                    self.dcc_chat_events.clone(),
                ) {
                    Ok((handle, task)) => {
                        self.dcc_chat_handles.insert(session_id.clone(), handle);
                        self.dcc_tasks.insert(session_id.clone(), task);
                        if let Ok(session) = self.dcc.get(&session_id).cloned() {
                            self.notify_dcc_change(EventClass::DccConnected, &session);
                        }
                    }
                    Err(error) => self.fail_dcc(&session_id, error.to_string()),
                }
            }
            DccRuntimeEvent::TransferConnected { session_id } => {
                let transition = self.transition_connected(&session_id, DccState::Transferring);
                if let Ok(session) = transition {
                    self.notify_dcc_change(EventClass::DccConnected, &session);
                }
            }
            DccRuntimeEvent::TransferProgress(progress) => {
                if let Ok(session) = self.dcc.update_progress(
                    &progress.session_id,
                    progress.transferred_bytes,
                    Timestamp::now(),
                ) {
                    self.record_dcc_event(
                        EventClass::DccTransferProgress,
                        Some(session.peer.clone()),
                        EventPayload::DccProgress(progress.clone()),
                    );
                    self.notify_resources(&["dcc"]);
                }
            }
            DccRuntimeEvent::TransferCompleted {
                session_id,
                received,
                transferred_bytes,
            } => {
                if let Some(received) = received
                    && let Ok(session) = self.dcc.get_mut(&session_id)
                {
                    // A conflict rename settles the committed name only here,
                    // so the reported destination has to be restated rather
                    // than left as the one acceptance planned.
                    session.local_path = Some(received.path);
                    session.receive_root = Some(received.root);
                    session.receive_path = Some(received.relative_path);
                }
                let _ = self
                    .dcc
                    .update_progress(&session_id, transferred_bytes, Timestamp::now());
                if let Ok(session) =
                    self.dcc
                        .transition(&session_id, DccState::Completed, Timestamp::now())
                {
                    self.notify_dcc_change(EventClass::DccTransferCompleted, &session);
                }
                self.dcc_tasks.remove(&session_id);
                self.dcc_cancellations.remove(&session_id);
                self.dcc_resume_offsets.remove(&session_id);
            }
            DccRuntimeEvent::Failed { session_id, error } => self.fail_dcc(&session_id, error),
        }
    }

    fn handle_dcc_chat_event(&mut self, event: DccChatEvent) {
        match event {
            DccChatEvent::Inbound(line) => {
                let peer = self
                    .dcc
                    .get(&line.session_id)
                    .ok()
                    .map(|session| session.peer.clone());
                self.record_dcc_event(
                    EventClass::DccChatMessage,
                    peer,
                    EventPayload::DccChatMessage(DccChatMessage {
                        session_id: line.session_id.clone(),
                        direction: EventDirection::Inbound,
                        text: line.text.clone(),
                    }),
                );
            }
            DccChatEvent::Outbound(line) => {
                let peer = self
                    .dcc
                    .get(&line.session_id)
                    .ok()
                    .map(|session| session.peer.clone());
                self.record_dcc_event(
                    EventClass::DccChatMessage,
                    peer,
                    EventPayload::DccChatMessage(DccChatMessage {
                        session_id: line.session_id.clone(),
                        direction: EventDirection::Outbound,
                        text: line.text.clone(),
                    }),
                );
            }
            DccChatEvent::Closed(session_id) => {
                if let Ok(session) =
                    self.dcc
                        .transition(&session_id, DccState::Completed, Timestamp::now())
                {
                    self.notify_dcc_change(EventClass::DccChatClosed, &session);
                }
                self.cancel_dcc_runtime(&session_id);
            }
            DccChatEvent::Failed { session_id, error } => self.fail_dcc(&session_id, error),
        }
    }

    fn transition_connected(
        &mut self,
        session_id: &DccSessionId,
        active_state: DccState,
    ) -> std::result::Result<DccSession, crate::dcc::manager::DccManagerError> {
        if self.dcc.get(session_id)?.state == DccState::Offered {
            self.dcc
                .transition(session_id, DccState::Connecting, Timestamp::now())?;
        }
        self.dcc
            .transition(session_id, active_state, Timestamp::now())
    }

    fn fail_dcc(&mut self, session_id: &DccSessionId, error: String) {
        self.cancel_dcc_runtime(session_id);
        if let Ok(session) = self.dcc.fail(session_id, &error, Timestamp::now()) {
            self.notify_dcc_change(EventClass::DccFailed, &session);
        }
    }

    fn notify_dcc_change(&mut self, class: EventClass, session: &DccSession) {
        self.record_dcc_event(
            class,
            Some(session.peer.clone()),
            EventPayload::DccSession(session.clone()),
        );
        self.notify_resources(&["dcc"]);
    }

    fn record_dcc_event(
        &mut self,
        class: EventClass,
        target: Option<String>,
        semantic: EventPayload,
    ) {
        let event = NewEvent {
            agent_id: self.id.clone(),
            direction: EventDirection::Internal,
            class,
            origin: EventOrigin::Gateway,
            verbosity: EventVerbosity::Semantic,
            target,
            server_time: None,
            received_at: Timestamp::now(),
            correlation: EventCorrelation::default(),
            semantic: Some(semantic),
            wire: None,
            mentions_me: false,
        };
        if self.journal.push(event).is_ok() {
            self.notify_journal_append();
            self.flush_event_reads(false);
        }
    }

    /// Whether an inbound projection is addressed to this agent's current
    /// nickname. Returns false before registration, when no nickname is known.
    fn addresses_me(&self, projection: &crate::irc::semantic::SemanticProjection) -> bool {
        let state = self.state.borrow();
        let Some(nickname) = state.identity.nickname.as_deref() else {
            return false;
        };
        addresses_nickname(&projection.event, nickname, self.isupport.case_mapping())
    }

    fn dcc_connect_timeout(&self) -> Duration {
        Duration::from_millis(self.config.dcc.connect_timeout_ms)
    }

    fn fail_pending_commands(&mut self, reason: String) {
        let ids: Vec<_> = self.pending_commands.keys().cloned().collect();
        for id in ids {
            if let Some(completion) = self.correlator.cancel(&id) {
                self.finish_command(completion, None);
            }
        }
        tracing::debug!(agent_id = %self.id, %reason, "failed pending commands on connection loss");
    }

    fn finish_shutdown(&mut self) {
        self.fail_pending_commands("actor shutdown".into());
        for (_, completion) in self.deferred_commands.drain(..) {
            let _ = completion.send(Err(GatewayError::ActorStopped(self.id.clone())));
        }
        let active: Vec<_> = self
            .dcc
            .list()
            .into_iter()
            .filter(|session| !session.state.is_terminal())
            .map(|session| session.id)
            .collect();
        for session_id in active {
            let _ = self.cancel_dcc(&session_id);
        }
        for read in self.pending_event_reads.drain(..) {
            let _ = read
                .completion
                .send(Err(GatewayError::ActorStopped(self.id.clone())));
        }
        self.set_connection_state(ConnectionState::Disconnected, None);
    }

    fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    fn notify_resources(&self, suffixes: &[&str]) {
        let base = format!("irc://agents/{}", self.id);
        for suffix in suffixes {
            let _ = self.resource_updates.send(format!("{base}/{suffix}"));
        }
    }

    /// Invalidate the event resources, and every watch that selects the record
    /// just appended.
    ///
    /// Testing the watch filters here rather than at the subscription is what
    /// keeps a watch quiet: a client watching one channel is woken by traffic
    /// in that channel, not by every line the connection happens to see.
    fn notify_journal_append(&self) {
        self.notify_resources(&["events"]);
        let Some(event) = self.journal.latest_event() else {
            return;
        };
        if event.mentions_me {
            self.notify_resources(&["inbox"]);
        }
        if event.verbosity == EventVerbosity::Wire || event.semantic.is_none() {
            self.notify_resources(&["wire"]);
        }
        if let Some(target) = event.target.as_deref() {
            let _ = self.resource_updates.send(ResourceUris::transcript(
                &self.id,
                &self.isupport.case_mapping().fold(target),
            ));
        }
        for uri in self
            .watches
            .matching_uris(&self.id, event, self.isupport.case_mapping())
        {
            let _ = self.resource_updates.send(uri);
        }
    }

    /// Invalidate only the channel resources whose contents actually changed.
    ///
    /// The expanded channel resources were previously invisible to
    /// subscribers: state changes notified the aggregate `state` resource and
    /// nothing else, so a client subscribed to one channel never heard about
    /// it. Diffing here also means a busy channel does not invalidate the
    /// quiet one beside it.
    fn notify_changed_channels(
        &self,
        before: &BTreeMap<String, ChannelState>,
        after: &BTreeMap<String, ChannelState>,
    ) {
        for (key, channel) in after {
            let previous = before.get(key);
            if previous == Some(channel) {
                continue;
            }
            let _ = self
                .resource_updates
                .send(ResourceUris::channel(&self.id, &channel.name));
            if previous.map(|previous| &previous.members) != Some(&channel.members) {
                let _ = self
                    .resource_updates
                    .send(ResourceUris::channel_members(&self.id, &channel.name));
            }
            if previous.map(|previous| &previous.topic) != Some(&channel.topic) {
                let _ = self
                    .resource_updates
                    .send(ResourceUris::channel_topic(&self.id, &channel.name));
            }
        }
        // A channel we left disappears from the map; subscribers to it still
        // need to hear that it is gone.
        for (key, channel) in before {
            if !after.contains_key(key) {
                let _ = self
                    .resource_updates
                    .send(ResourceUris::channel(&self.id, &channel.name));
            }
        }
    }
}

fn protocol_catalog() -> CompatibilityCatalog {
    let mut catalog = CompatibilityCatalog::with_static_registry();
    catalog.ctcp_commands = BTreeSet::from([
        "ACTION".into(),
        "CLIENTINFO".into(),
        "DCC".into(),
        "PING".into(),
        "TIME".into(),
        "VERSION".into(),
    ]);
    catalog.dcc_variants = BTreeSet::from([
        "CHAT".into(),
        "SEND".into(),
        "RESUME".into(),
        "ACCEPT".into(),
        "REVERSE".into(),
    ]);
    catalog
}

enum ConnectionExit {
    Shutdown,
    Lost(GatewayError),
}

enum DirectSetup {
    Accept(tokio::net::TcpListener),
    Connect(SocketAddr),
}

async fn dcc_source_metadata(source_path: &std::path::Path) -> Result<std::fs::Metadata> {
    let metadata = tokio::fs::metadata(source_path).await.map_err(|source| {
        GatewayError::io("inspect DCC source_path on the gateway host", source)
    })?;
    if !metadata.is_file() {
        return Err(GatewayError::Dcc(format!(
            "DCC source_path on the gateway host is not a regular file: {}",
            source_path.display()
        )));
    }
    Ok(metadata)
}

fn validate_peer(peer: &str) -> Result<()> {
    Nickname::new(peer)
        .map(|_| ())
        .map_err(|error| GatewayError::InvalidMessage(format!("invalid DCC peer: {error}")))
}

fn parse_endpoint(address: &str, port: u16, allow_private_addresses: bool) -> Result<SocketAddr> {
    if port == 0 {
        return Err(GatewayError::Dcc(
            "ordinary DCC endpoint uses port zero".into(),
        ));
    }
    let address = parse_address(address).map_err(|error| GatewayError::Dcc(error.to_string()))?;
    if address.is_unspecified()
        || address.is_multicast()
        || matches!(address, std::net::IpAddr::V4(address) if address.is_broadcast())
        || (!allow_private_addresses && is_private_or_local(address))
    {
        return Err(GatewayError::Dcc(format!(
            "DCC peer advertised unusable address {address}"
        )));
    }
    Ok(SocketAddr::new(address, port))
}

fn is_private_or_local(address: std::net::IpAddr) -> bool {
    match address {
        std::net::IpAddr::V4(address) => {
            let octets = address.octets();
            address.is_loopback()
                || address.is_private()
                || address.is_link_local()
                || (octets[0] == 100 && (64..=127).contains(&octets[1]))
        }
        std::net::IpAddr::V6(address) => {
            address.is_loopback() || address.is_unique_local() || address.is_unicast_link_local()
        }
    }
}

fn dcc_manager_error(error: crate::dcc::manager::DccManagerError) -> GatewayError {
    match error {
        crate::dcc::manager::DccManagerError::Limit(limit) => {
            GatewayError::ResourceLimit(format!("active DCC session limit reached: {limit}"))
        }
        other => GatewayError::Dcc(other.to_string()),
    }
}

fn map_codec_error(error: CodecError) -> GatewayError {
    match error {
        CodecError::Io(source) => GatewayError::io("write IRC message", source),
        CodecError::Encode(source) => GatewayError::InvalidMessage(source.to_string()),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReconnectDecision {
    Reconnect,
    Shutdown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_catalog_keeps_ctcp_separate_from_capabilities() {
        let catalog = protocol_catalog();
        assert!(catalog.ctcp_commands.contains("DCC"));
        assert!(catalog.capabilities.is_empty());
    }

    #[test]
    fn line_budget_projection_is_typed() {
        let projected = ActiveLineBudget::from(LineBudget::TRADITIONAL);
        assert_eq!(projected.max_body_bytes, 512);
        assert_eq!(projected.max_tag_bytes, 4096);
    }

    #[test]
    fn only_known_batch_types_are_history() {
        assert!(is_history_batch_kind("chathistory"));
        assert!(is_history_batch_kind("draft/chathistory"));
        assert!(is_history_batch_kind("HISTORY"));
        assert!(!is_history_batch_kind("draft/no-history"));
    }

    #[tokio::test]
    async fn dcc_send_source_errors_name_the_gateway_host() {
        let directory = tempfile::tempdir().expect("tempdir");
        let missing = directory.path().join("missing.bin");
        let error = dcc_source_metadata(&missing)
            .await
            .expect_err("missing source must fail");

        assert!(
            error
                .to_string()
                .contains("source_path on the gateway host")
        );
    }

    #[test]
    fn private_dcc_endpoints_require_an_explicit_opt_in() {
        assert!(parse_endpoint("127.0.0.1", 5000, false).is_err());
        assert!(parse_endpoint("10.0.0.2", 5000, false).is_err());
        assert!(parse_endpoint("10.0.0.2", 5000, true).is_ok());
        assert!(parse_endpoint("203.0.113.2", 5000, false).is_ok());
    }
}
