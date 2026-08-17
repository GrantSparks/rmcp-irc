//! One `rmcp` handler type shared by stdio and Streamable HTTP.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::{
        router::{prompt::PromptRouter, tool::ToolRouter},
        tool::{InputResponses, RequestState, ToolCallContext},
        wrapper::Parameters,
    },
    model::{
        Annotations, CallToolRequestParams, CallToolResponse, CallToolResult, CancelTaskParams,
        CompleteRequestParams, CompleteResult, CompletionInfo, ContentBlock, CreateTaskResult,
        GetTaskParams, GetTaskResult, Implementation, InputRequiredResult,
        ListResourceTemplatesResult, ListResourcesResult, PaginatedRequestParams, PromptMessage,
        ProtocolVersion, ReadResourceRequestParams, ReadResourceResponse, ReadResourceResult,
        Reference, Resource, ResourceContents, ResourceTemplate, Role, ServerCapabilities,
        ServerInfo, SubscriptionFilter, TASKS_EXTENSION_ID, UpdateTaskParams,
    },
    prompt, prompt_handler, prompt_router,
    service::{RequestContext, RoleServer, SubscriptionContext},
    task_manager::{TaskContext, TaskExit},
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    MCP_INSTRUCTIONS,
    agent::AgentId,
    agent::{
        actor::{CompletionMode, ConnectMilestone},
        journal::{EventClass, EventCursor, EventFilter, EventOrigin},
    },
    dcc::session::{DccDirection, DccSession, DccSessionId, DccState},
    error::{ErrorKind, GatewayError},
    gateway::{ConnectRequest, ConversationWindow, Gateway},
    irc::{
        capabilities::CapabilityStatus,
        correlation::{CommandOutcome, CommandResult},
        registration::{NickConflictPolicy, Nickname},
        wire::{OutboundMessage, Tag},
    },
    mcp::{
        attention::{
            ATTENTION_ONBOARDING, AttentionCheckInput, AttentionCheckOutput, AttentionCheckState,
            AttentionOpenInput, AttentionOpenOutput, AttentionSchedule, AttentionSubscription,
        },
        authorization::{CallerPolicy, OwnerId},
        confirm_action, connect_nickname, dcc_accept,
        envelope::{self, ToolFailure, envelope_schema},
        join_key,
        mrtr::OriginatingOperation,
        progress::ProgressReporter,
        request_profile::RequestProfile,
        resources::{
            AgentResourceUri, ResourceDescriptor, ResourceKind, ResourcePayload, ResourceUris,
            describe as describe_resource, descriptors_for_agent, encode_channel_segment,
        },
        tasks::TASK_POLL_INTERVAL,
        tools::*,
        watch::{
            WATCH_EVENTS_TEMPLATE, WATCH_INSTRUCTIONS, WATCH_URI_PREFIX, WatchCloseInput,
            WatchCloseOutput, WatchCreateInput, WatchCreateOutput, WatchDescriptor, WatchId,
            WatchTarget, WatchUri, watch_events_uri,
        },
    },
};

/// Resource templates this server exposes. Declared once so the template
/// listing and its argument completion cannot drift apart.
const CHANNEL_STATE_TEMPLATE: &str = "irc://agents/{agent_id}/channels/{encoded_channel}";

/// Member list of one channel, addressable without its topic and modes.
const CHANNEL_MEMBERS_TEMPLATE: &str = "irc://agents/{agent_id}/channels/{encoded_channel}/members";

/// Topic of one channel, addressable on its own.
const CHANNEL_TOPIC_TEMPLATE: &str = "irc://agents/{agent_id}/channels/{encoded_channel}/topic";

/// Compact conversation with one channel or peer.
const TRANSCRIPT_TEMPLATE: &str = "irc://agents/{agent_id}/transcripts/{encoded_channel}";

/// Cursor-page expansion of the per-agent events resource. Subscribing to
/// `irc://agents/{agent_id}/events` and reading this on each notification is a
/// complete delivery loop that needs no tool call.
const EVENT_CURSOR_TEMPLATE: &str = "irc://agents/{agent_id}/events/after/{sequence}";

/// Resources returned by one `resources/list` page. A connected agent
/// contributes eight fixed resources plus four per joined channel, so this
/// keeps a full listing well inside client response limits.
const RESOURCE_PAGE_SIZE: usize = 60;

#[cfg(test)]
const PROMPT_NAMES: &[&str] = &[
    "irc-connect",
    "irc-maintain-attention",
    "irc-watch-mentions",
    "irc-join",
    "irc-summarize-respond",
];

/// The single MCP revision this server implements.
const SUPPORTED_PROTOCOL_VERSIONS: &[ProtocolVersion] = &[ProtocolVersion::V_2026_07_28];

/// Tools that can be run as MCP tasks.
///
/// Only the two DCC operations that genuinely outlive their request qualify.
/// Everything else here completes in one round trip, and wrapping a fast
/// operation in a task handle would cost the caller a poll for no benefit.
///
/// `irc.connect` is deliberately absent. An initial connect is a single attempt
/// bounded by `onboarding.connect_timeout_ms` — the reconnect backoff loop only
/// exists after a connection has been established once — so it cannot outlive
/// its own request, and a task handle would replace a result the caller can use
/// with a poll for one it already has. Its long wait is narrated with progress
/// notifications instead.
const TASK_AUGMENTED_TOOLS: &[&str] = &["irc.dcc.send", DCC_ACCEPT_TOOL];

/// The one task-augmented tool that can also ask its caller a question, and so
/// has to settle it before a task is created.
const DCC_ACCEPT_TOOL: &str = "irc.dcc.accept";

/// Status message a task-augmented DCC call starts with, before its session
/// exists to describe.
const TASK_INITIAL_STATUS: &str = "Negotiating the direct connection.";

/// The steps `irc.history` reports, for the `total` its progress carries.
const HISTORY_PROGRESS_TOTAL: u32 = 3;

/// MCP request handler backed by a shared gateway.
#[derive(Clone)]
pub struct IrcMcpService {
    gateway: Arc<Gateway>,
    tool_router: ToolRouter<Self>,
    prompt_router: PromptRouter<Self>,
    typing_deadlines: Arc<Mutex<BTreeMap<(crate::agent::AgentId, String), Instant>>>,
    callers: CallerPolicy,
}

impl std::fmt::Debug for IrcMcpService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The task ledger holds running futures rather than inspectable data,
        // so the count is the only part of it worth printing.
        formatter
            .debug_struct("IrcMcpService")
            .field("gateway", &self.gateway)
            .field("running_tasks", &self.gateway.tasks().running_task_count())
            .finish_non_exhaustive()
    }
}

impl IrcMcpService {
    /// Create a request handler for a shared gateway, serving one trusted
    /// local caller.
    pub fn new(gateway: Arc<Gateway>) -> Self {
        Self::with_caller_policy(gateway, CallerPolicy::Local)
    }

    /// Create a request handler that identifies callers with an explicit
    /// policy. Shared transports must use this rather than [`Self::new`], so
    /// handles are bound to whoever created them.
    pub fn with_caller_policy(gateway: Arc<Gateway>, callers: CallerPolicy) -> Self {
        Self {
            gateway,
            tool_router: Self::tool_router(),
            prompt_router: Self::prompt_router(),
            typing_deadlines: Arc::new(Mutex::new(BTreeMap::new())),
            callers,
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ConnectPromptInput {
    /// Preferred mythological nickname, or omit to choose one during the workflow.
    nickname: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct WatchMentionsPromptInput {
    /// Opaque handle returned by `irc.connect`.
    agent_id: String,
    /// Optional comma-separated channel or nickname targets; omit for all targets.
    targets: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MaintainAttentionPromptInput {
    /// Opaque handle returned by `irc.connect`.
    agent_id: String,
    /// Optional comma-separated task channels that require complete traffic.
    full_traffic_targets: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct JoinPromptInput {
    /// Opaque handle returned by `irc.connect`.
    agent_id: String,
    /// Channel to join, including its server-advertised prefix.
    channel: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SummarizeRespondPromptInput {
    /// Opaque handle returned by `irc.connect`.
    agent_id: String,
    /// Channel or nickname conversation to read.
    target: String,
    /// Optional user objective that the summary and response should address.
    objective: Option<String>,
}

#[prompt_router(router = "prompt_router")]
impl IrcMcpService {
    /// Connect, read onboarding state, and establish a safe IRC identity.
    #[prompt(
        name = "irc-connect",
        description = "Connect an IRC guest, read the authoritative MOTD, and establish coordination context."
    )]
    async fn prompt_connect(
        &self,
        Parameters(input): Parameters<ConnectPromptInput>,
    ) -> Vec<PromptMessage> {
        let nickname = input.nickname.map_or_else(
            || "Choose three candidates from different mythological traditions, check current/recent names when possible, and keep the most distinctive one.".into(),
            |nickname| format!("Use `{nickname}` as the preferred nickname unless current or recent activity shows that it would recycle another session's identity."),
        );
        vec![PromptMessage::new_text(
            Role::User,
            format!(
                "Establish a new IRC collaboration session. {nickname} Call `irc.connect`, then \
                 read and follow the returned MOTD before participating. If you want to choose the \
                 replacement yourself when the server refuses a name, pass \
                 `nick_conflict_policy: \"elicit\"`: the call then returns an `input_required` \
                 question instead of connecting, and you answer it and re-send the same call. \
                 Otherwise the default bounded suffixing applies. Read the auto-joined channel \
                 topic, announce a concise hello with real task/worktree scope, and preserve the \
                 returned `agent_id` and native resource links for subsequent operations. Do not \
                 invent account registration for an ephemeral guest."
            ),
        )]
    }

    /// Maintain model responsiveness after the foreground turn ends.
    #[prompt(
        name = "irc-maintain-attention",
        description = "Open compound IRC attention, merge it into the client's listen stream, and arrange the one-minute model fallback."
    )]
    async fn prompt_maintain_attention(
        &self,
        Parameters(input): Parameters<MaintainAttentionPromptInput>,
    ) -> Vec<PromptMessage> {
        let targets = input.full_traffic_targets.map_or_else(
            || "Use no full-traffic targets yet; direct/addressed and account-identified human messages still qualify.".into(),
            |targets| format!("Pass these comma-separated task channels as full_traffic_targets: {targets}."),
        );
        vec![PromptMessage::new_text(
            Role::User,
            format!(
                "For IRC agent `{}`, call `irc.attention.open`. {targets} Have the client merge the returned filterAddition under params.notifications in its one existing `subscriptions/listen` request and open or reopen that stream with complete request metadata; matching `notifications/resources/updated` are the asynchronous host notification path. Then, unless the host directly resumes the model from those matching notifications, run the returned ordinary scheduler prompt immediately and in this same conversation with no interval longer than 60 seconds. A scheduled quiet check still consumes model tokens, so do not describe it as a zero-token path. Persist `resume_cursor` only after handling returned events, drain immediately while `has_more` is true, and cancel the recurring task, close the watch, and disconnect when the work ends. Successful tool results may also report bounded activity while the model is already running, but cannot wake it. Do not use top-level MRTR or a task's input_required state as an ambient event channel: both request input needed to continue a specific operation.",
                input.agent_id,
            ),
        )]
    }

    /// Create and consume a mentions-only live watch.
    #[prompt(
        name = "irc-watch-mentions",
        description = "Create a mentions watch, subscribe when supported, and handle addressed IRC messages."
    )]
    async fn prompt_watch_mentions(
        &self,
        Parameters(input): Parameters<WatchMentionsPromptInput>,
    ) -> Vec<PromptMessage> {
        let targets = input.targets.map_or_else(
            || "Watch all targets.".into(),
            |targets| format!("Restrict the watch to these comma-separated targets: {targets}."),
        );
        vec![PromptMessage::new_text(
            Role::User,
            format!(
                "For IRC agent `{}`, set up mention delivery in this order. 1. Call \
                 `irc.watch.create` with `mentions_only: true`. {targets} Keep the returned \
                 `watch_id` and `latest_cursor`, and attach the native watch resource link. 2. Ask \
                 the host to merge that watch URI into the `resourceSubscriptions` of its one \
                 consolidated `subscriptions/listen` stream; the notification is filtered by the watch, so it means \
                 there is something here for you. 3. On each update, call `irc.events.read` with \
                 that `watch_id` and the cursor you last persisted — or read \
                 `irc://watches/{{watch_id}}/events/after/{{stream_id}}/{{sequence}}` for the \
                 compact window. 4. Persist the returned `next_cursor` yourself: the watch holds \
                 no position, so you own it, re-reading a cursor is always safe, and you keep \
                 reading while `has_more` is true. Check `status` on every read and treat anything \
                 other than `current` as records lost. Answer direct messages and channel mentions \
                 in the same location. Without subscriptions, call `irc.events.read` with that \
                 `watch_id` and a positive `wait_ms` as a standing long poll. Note the boundary: \
                 resource notifications and `subscriptions/listen` wake the host application but \
                 cannot force or schedule a model turn. MCP 2026-07-28 has no server-initiated \
                 sampling — it is deprecated by SEP-2577 — and input requests exist only inside an \
                 active client request. For autonomous model participation, use \
                 `irc.attention.open` and its at-most-60-second same-conversation scheduler recipe, \
                 or a host bridge that directly resumes the model from this listen stream.",
                input.agent_id
            ),
        )]
    }

    /// Join a channel and read its instructions before participating.
    #[prompt(
        name = "irc-join",
        description = "Join an IRC channel, read its topic and context resources, then participate safely."
    )]
    async fn prompt_join(
        &self,
        Parameters(input): Parameters<JoinPromptInput>,
    ) -> Vec<PromptMessage> {
        vec![PromptMessage::new_text(
            Role::User,
            format!(
                "Using IRC agent `{}`, call `irc.join` for `{}`. If the channel is keyed, the call \
                 comes back as an `input_required` question asking for the key rather than as a \
                 failure: answer it and re-send the same call, or decline to leave the channel \
                 unjoined. Follow the returned native channel resource link, read the topic before \
                 sending messages, and treat it as channel-specific instruction. Read the recent \
                 transcript/history and known members to avoid duplicating active work, then \
                 announce relevant intent and subscribe to the channel's live resources when \
                 supported.",
                input.agent_id, input.channel
            ),
        )]
    }

    /// Summarize a conversation and prepare or send an appropriate response.
    #[prompt(
        name = "irc-summarize-respond",
        description = "Summarize one IRC conversation and formulate a context-aware response."
    )]
    async fn prompt_summarize_respond(
        &self,
        Parameters(input): Parameters<SummarizeRespondPromptInput>,
    ) -> Vec<PromptMessage> {
        let objective = input.objective.map_or_else(
            || "Identify the most important unanswered question or coordination need.".into(),
            |objective| format!("Prioritize this user objective: {objective}"),
        );
        vec![PromptMessage::new_text(
            Role::User,
            format!(
                "For IRC agent `{}`, read the semantic transcript resource for `{}` or fall back to `irc.history` plus cursor events. Distinguish history replay from live messages. Summarize participants, human directives, decisions, claimed scopes, risks, and open questions. {objective} Draft a concise response grounded only in observed context; send it with `irc.send` only when the selected workflow/user intent authorizes participation, and otherwise present the draft for review.",
                input.agent_id, input.target
            ),
        )]
    }
}

#[tool_router(router = tool_router)]
impl IrcMcpService {
    /// Register a guest and return the complete server MOTD before publishing its handle.
    #[tool(
        name = "irc.connect",
        description = "Connect one mythologically named guest to the configured Ergo server. \
                       With `nick_conflict_policy: \"elicit\"`, a nickname the server refuses \
                       abandons the attempt and returns an input_required question asking which \
                       name to register instead; answering and retrying makes a fresh attempt.",
        output_schema = envelope_schema::<ConnectOutput>(),
        annotations(
            title = "Connect IRC guest",
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn irc_connect(
        &self,
        Parameters(input): Parameters<ConnectInput>,
        context: RequestContext<RoleServer>,
        RequestState(request_state): RequestState,
        InputResponses(responses): InputResponses,
    ) -> Result<CallToolResponse, McpError> {
        let owner = self.callers.identify(&context)?;
        let profile = RequestProfile::from_context(&context);
        let result_detail = input.result_detail;
        let asks = input.nick_conflict_policy == NickConflictPolicy::Elicit;
        // Said before anything is registered: a preference the server would
        // have to clamp is a preference the caller cannot verify it received.
        if let Some(field) = input.activity.out_of_bounds() {
            return Ok(refusal(
                ErrorKind::Validation.as_str(),
                format!(
                    "{field} must be between 0 and {}",
                    crate::mcp::activity::INLINE_MENTIONS_CAP
                ),
                false,
            )
            .into());
        }
        // Bound to the arguments as they arrived, so a retry that changed any of
        // them is a different registration and cannot redeem this exchange.
        let operation = OriginatingOperation::for_tool("irc.connect", &input.salient());

        let chosen = match request_state.as_deref() {
            Some(sealed) => match self.open_nickname_choice(
                &owner,
                &operation,
                sealed,
                responses.as_ref(),
                &input,
            )? {
                Resolution::Ready(nickname) => Some(nickname),
                Resolution::NeedsInput(request) => return Ok(request.into()),
                Resolution::Settled(result) => return Ok(result.into()),
            },
            None => None,
        };
        // A policy whose whole behavior is "ask" cannot be honored by a request
        // that declared no way to answer, and quietly falling back to `suffix`
        // would register a different identity than the caller asked for. Said
        // before connecting, because there is nothing to undo yet.
        if chosen.is_none() && asks && !profile.supports_form_elicitation() {
            return Ok(refusal(
                ErrorKind::Validation.as_str(),
                "nick_conflict_policy \"elicit\" needs a request that declares form elicitation; \
                 use \"suffix\" or \"fail\", or declare the elicitation capability",
                false,
            )
            .into());
        }

        let request = connect_request(&input, chosen);
        Ok(
            match self
                .connect_reporting_progress(owner.clone(), request, &context)
                .await
            {
                Ok(connected) => {
                    let output = ConnectOutput {
                        resources: ResourceUris::for_agent(&connected.agent_id),
                        agent_id: connected.agent_id,
                        nickname: connected.nickname.clone(),
                        nickname_adjusted: connected.nickname_adjusted,
                        registered: true,
                        motd: motd_for_tool_result(connected.motd, result_detail),
                        result_detail,
                        attention: ATTENTION_ONBOARDING,
                    };
                    let summary = if output.motd.text.is_empty() {
                        format!(
                            "Connected {} as {}. Before yielding, call irc.attention.open and \
                             arrange its returned check at least every 60 seconds while connected. \
                             The server has no MOTD.",
                            output.agent_id, output.nickname,
                        )
                    } else {
                        format!(
                            "Connected {} as {}. Before yielding, call irc.attention.open and \
                             arrange its returned check at least every 60 seconds while connected. \
                             Server MOTD:\n{}",
                            output.agent_id, output.nickname, output.motd.text
                        )
                    };
                    let content = agent_resource_links(&output.resources);
                    tool_success_with_content(summary, &output, content).into()
                }
                // The one registration failure a caller can fix by answering.
                // Nothing was published: the actor released its capacity permit
                // and stopped when the attempt failed, so the retry starts from
                // a clean gateway rather than resuming anything.
                Err(GatewayError::NicknameUnavailable {
                    message,
                    attempted_nicknames,
                }) if asks && profile.supports_form_elicitation() => {
                    let pending = connect_nickname::PendingNickname {
                        attempted: attempted_nicknames,
                        detail: message,
                    };
                    self.ask_for_a_nickname(&owner, &operation, &pending, &input)?
                        .into()
                }
                Err(error) => tool_error(error).into(),
            },
        )
    }

    /// Open the single compound attention selection used by host subscriptions
    /// and by the portable one-minute model-scheduler fallback.
    #[tool(
        name = "irc.attention.open",
        description = "Open model attention for one IRC guest, combining direct/addressed messages, \
                       account-identified humans, and complete traffic in selected task targets.",
        output_schema = envelope_schema::<AttentionOpenOutput>(),
        annotations(
            title = "Open IRC model attention",
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn irc_attention_open(
        &self,
        Parameters(input): Parameters<AttentionOpenInput>,
    ) -> CallToolResult {
        let filter = input.filter();
        match self.gateway.create_watch(&input.agent_id, filter).await {
            Ok(created) => {
                let schedule = AttentionSchedule::new(&created.watch, &created.latest_cursor);
                let resources = ResourceUris::for_agent(&created.watch.agent_id);
                let subscription = AttentionSubscription::new(&created.watch, &resources);
                let summary = format!(
                    "Attention opened for {} at {}. Merge the returned filter addition under \
                     params.notifications and open or reopen the client's one subscriptions/listen \
                     stream with complete request metadata. Because that stream wakes the host \
                     rather than the model, also schedule the returned ordinary prompt in this same \
                     conversation at least every 60 seconds unless the host directly resumes the \
                     model on matching updates. Scheduled quiet checks consume model tokens.",
                    created.watch.agent_id, created.watch.uri,
                );
                let link = ContentBlock::ResourceLink(watch_resource_entry(&created.watch));
                let output = AttentionOpenOutput {
                    watch: created.watch,
                    initial_cursor: created.latest_cursor,
                    subscription,
                    schedule,
                    instructions: ATTENTION_ONBOARDING,
                };
                tool_success_with_content(summary, &output, vec![link])
            }
            Err(error) => tool_error(error),
        }
    }

    /// Perform one compact, normally non-blocking scheduled attention check.
    #[tool(
        name = "irc.attention.check",
        description = "Check one model-attention watch with a compact quiet path and a scan \
                       checkpoint that advances through irrelevant traffic.",
        output_schema = envelope_schema::<AttentionCheckOutput>(),
        annotations(
            title = "Check IRC model attention",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn irc_attention_check(
        &self,
        Parameters(input): Parameters<AttentionCheckInput>,
    ) -> CallToolResult {
        let wait = Duration::from_millis(input.wait_ms);
        match self
            .gateway
            .read_attention_events(
                &input.agent_id,
                &input.watch_id,
                input.cursor,
                input.limit,
                wait,
            )
            .await
        {
            Ok(page) => {
                let output = AttentionCheckOutput::from_page(page);
                // Like the opt-in flag on `irc.events.read`, this acknowledges
                // only the courtesy activity hint. It never moves a watch or
                // delivery cursor, so retrying the caller's previous attention
                // cursor remains at-least-once even if this response is lost.
                if input.set_activity_anchor {
                    self.gateway
                        .set_activity_anchor(&input.agent_id, output.resume_cursor.clone())
                        .await;
                }
                let summary = match output.state {
                    AttentionCheckState::Quiet => "quiet".into(),
                    AttentionCheckState::Events => format!(
                        "{} attention event(s){}",
                        output.events.len(),
                        if output.has_more { "; drain again" } else { "" }
                    ),
                    AttentionCheckState::StreamReset => {
                        "stream_reset: report lost continuity and recover".into()
                    }
                    AttentionCheckState::EventGap => {
                        "event_gap: report lost records and recover".into()
                    }
                };
                tool_success(summary, &output)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Disconnect and destroy one actor and all of its direct sessions.
    #[tool(
        name = "irc.disconnect",
        description = "Disconnect one explicit IRC guest and invalidate its process-local handle.",
        output_schema = envelope_schema::<DisconnectOutput>(),
        annotations(
            title = "Disconnect IRC guest",
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_disconnect(
        &self,
        Parameters(input): Parameters<DisconnectInput>,
    ) -> CallToolResult {
        match self.gateway.disconnect(&input.agent_id, input.reason).await {
            Ok(receipt) => {
                let output = DisconnectOutput {
                    agent_id: input.agent_id,
                    disconnected: true,
                    quit_sent: receipt.quit_sent,
                    dcc_sessions_closed: receipt.dcc_sessions_closed,
                };
                tool_success(format!("Disconnected {}.", output.agent_id), &output)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Register a caller-owned selection over one agent's stream.
    #[tool(
        name = "irc.watch.create",
        description = "Create a subscribable watch over one guest's events, returning a resource \
                       link that wakes a host on matching activity and the cursor to read from.",
        output_schema = envelope_schema::<WatchCreateOutput>(),
        annotations(
            title = "Create event watch",
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn irc_watch_create(
        &self,
        Parameters(input): Parameters<WatchCreateInput>,
    ) -> CallToolResult {
        let filter = input.filter();
        match self.gateway.create_watch(&input.agent_id, filter).await {
            Ok(created) => {
                let next_uri = watch_events_uri(&created.watch.watch_id, &created.latest_cursor);
                let summary = format!(
                    "Watching {} at {}. Merge that URI into the client's one \
                     subscriptions/listen stream, then on each notification call \
                     irc.events.read with watch_id {} and the cursor you last persisted, starting \
                     from sequence {}.",
                    created.watch.agent_id,
                    created.watch.uri,
                    created.watch.watch_id,
                    created.latest_cursor.sequence
                );
                let link = ContentBlock::ResourceLink(watch_resource_entry(&created.watch));
                let output = WatchCreateOutput {
                    watch: created.watch,
                    latest_cursor: created.latest_cursor,
                    next_uri,
                    instructions: WATCH_INSTRUCTIONS,
                };
                tool_success_with_content(summary, &output, vec![link])
            }
            Err(error) => tool_error(error),
        }
    }

    #[tool(
        name = "irc.watch.close",
        description = "Release one watch handle and stop its resource notifications.",
        output_schema = envelope_schema::<WatchCloseOutput>(),
        annotations(
            title = "Close event watch",
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn irc_watch_close(
        &self,
        Parameters(input): Parameters<WatchCloseInput>,
    ) -> CallToolResult {
        match self.gateway.close_watch(&input.watch_id) {
            Ok(()) => tool_success(
                format!("Closed watch {}.", input.watch_id),
                &WatchCloseOutput {
                    watch_id: input.watch_id,
                },
            ),
            Err(error) => tool_error(error),
        }
    }

    #[tool(
        name = "irc.status",
        description = "Read connection, identity, protocol, event, and reconnect status for one guest.",
        output_schema = envelope_schema::<StatusOutput>(),
        annotations(
            title = "Read guest status",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn irc_status(
        &self,
        Parameters(input): Parameters<AgentInput>,
        context: RequestContext<RoleServer>,
    ) -> CallToolResult {
        match self.gateway.snapshot(&input.agent_id).await {
            Ok(snapshot) => {
                let output = StatusOutput {
                    advertised_capabilities: snapshot.protocol.capabilities.len(),
                    negotiated_capabilities: snapshot
                        .protocol
                        .capabilities
                        .values()
                        .filter(|capability| capability.status == CapabilityStatus::Negotiated)
                        .count(),
                    events: snapshot.journal,
                    resources: ResourceUris::for_agent(&input.agent_id),
                    state: snapshot.state,
                    result_detail: input.result_detail,
                    caller: Some(caller_profile(&RequestProfile::from_context(&context))),
                };
                let mut output = output;
                output.state.motd = motd_for_tool_result(output.state.motd, output.result_detail);
                let content = agent_resource_links(&output.resources);
                tool_success_with_content(
                    format!(
                        "{} is {:?} as {}.",
                        input.agent_id,
                        output.state.connection_state,
                        output
                            .state
                            .identity
                            .nickname
                            .as_deref()
                            .unwrap_or("unknown")
                    ),
                    &output,
                    content,
                )
            }
            Err(error) => tool_error(error),
        }
    }

    /// Join one channel and wait for a definitive reply when available.
    #[tool(
        name = "irc.join",
        description = "Join one channel using the actor's correlated IRC command path. A join \
                       refused by ERR_BADCHANNELKEY (475) with no `key` supplied returns an \
                       input_required question asking for the key when the request declared form \
                       elicitation; every other rejection is the ordinary structured result.",
        output_schema = envelope_schema::<JoinOutput>(),
        annotations(
            title = "Join channel",
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_join(
        &self,
        Parameters(input): Parameters<JoinInput>,
        context: RequestContext<RoleServer>,
        RequestState(request_state): RequestState,
        InputResponses(responses): InputResponses,
    ) -> Result<CallToolResponse, McpError> {
        let owner = self.callers.identify(&context)?;
        let profile = RequestProfile::from_context(&context);
        let result_detail = input.result_detail;
        if let Err(error) = validate_irc_atom(input.channel.as_str(), "channel") {
            return Ok(tool_error(error).into());
        }
        // Bound to the arguments as they arrived — including the absent `key`
        // that made the question necessary.
        let operation = OriginatingOperation::for_tool("irc.join", &input.salient());

        let key = match request_state.as_deref() {
            Some(sealed) => {
                match self.open_channel_key(
                    &owner,
                    &operation,
                    sealed,
                    responses.as_ref(),
                    &input,
                )? {
                    Resolution::Ready(key) => Some(key),
                    Resolution::NeedsInput(request) => return Ok(request.into()),
                    Resolution::Settled(result) => return Ok(result.into()),
                }
            }
            None => input.key.clone(),
        };
        let mut params = vec![input.channel.to_string()];
        if let Some(key) = key.clone() {
            params.push(key);
        }
        match self
            .execute(
                &input.agent_id,
                OutboundMessage::new("JOIN", params),
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                // One rejection, and only one, is answerable: the channel wants
                // a key this call did not carry. Everything else is a decision
                // about the guest that no answer would change, and is returned
                // exactly as it always was.
                if join_key::needs_key(&result, key.is_some())
                    && profile.supports_form_elicitation()
                {
                    return Ok(self
                        .ask_for_a_channel_key(&owner, &operation, &input, &result)?
                        .into());
                }
                let failure = command_failure(&result);
                let outcome = result.outcome;
                let result = command_result_for_detail(result, result_detail);
                let output = JoinOutput {
                    resource: ResourceUris::channel(&input.agent_id, input.channel.as_str()),
                    channel: input.channel,
                    result,
                };
                let content = vec![channel_resource_link(
                    output.resource.clone(),
                    output.channel.as_str(),
                )];
                Ok(command_tool_result_with_content(
                    format!("JOIN {}: {outcome:?}.", output.channel),
                    &output,
                    failure,
                    content,
                )
                .into())
            }
            Err(error) => Ok(tool_error(error).into()),
        }
    }

    /// Part one channel.
    #[tool(
        name = "irc.part",
        description = "Part one channel and correlate the server echo or rejection.",
        output_schema = envelope_schema::<CommandResult>(),
        annotations(
            title = "Leave channel",
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_part(&self, Parameters(input): Parameters<PartInput>) -> CallToolResult {
        if let Err(error) = validate_irc_atom(input.channel.as_str(), "channel") {
            return tool_error(error);
        }
        let message = input.reason.map_or_else(
            || OutboundMessage::new("PART", vec![input.channel.to_string()]),
            |reason| {
                OutboundMessage::new("PART", vec![input.channel.to_string()]).with_trailing(reason)
            },
        );
        self.execute_result(
            &input.agent_id,
            message,
            CompletionMode::Auto,
            input.timeout_ms,
            "PART",
            input.result_detail,
        )
        .await
    }

    /// Send one logical IRC message, safely splitting only when requested.
    #[tool(
        name = "irc.send",
        description = "Send PRIVMSG, NOTICE, ACTION, or TAGMSG with negotiated-safe semantics.",
        output_schema = envelope_schema::<SendOutput>(),
        annotations(
            title = "Send message",
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn irc_send(&self, Parameters(input): Parameters<SendInput>) -> CallToolResult {
        match self.send_message(input).await {
            Ok(output) => {
                // One logical message can become several lines, and the first
                // one the server did not take is the exchange worth reporting:
                // the rest either preceded it or never had a chance.
                let failed = output
                    .results
                    .iter()
                    .find(|result| {
                        result.outcome != CommandOutcome::Completed
                            && result.outcome != CommandOutcome::SentUnconfirmed
                    })
                    .map(|result| CommandFailure {
                        outcome: result.outcome,
                        result: result.clone(),
                    });
                let summary = send_result_summary(
                    output.line_count,
                    failed.as_ref().map(|failure| failure.outcome),
                );
                command_tool_result(summary, &output, failed)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Read server-backed channel or private-message history.
    #[tool(
        name = "irc.history",
        description = "Read IRCv3 CHATHISTORY, with an explicitly reported legacy/unavailable fallback.",
        output_schema = envelope_schema::<HistoryOutput>(),
        annotations(
            title = "Read channel history",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_history(
        &self,
        Parameters(input): Parameters<HistoryInput>,
        context: RequestContext<RoleServer>,
    ) -> CallToolResult {
        let agent_id = input.agent_id.clone();
        let channel = input.target.channel().cloned();
        let mut progress = ProgressReporter::new(&context, HISTORY_PROGRESS_TOTAL);
        match self.history(input, &mut progress).await {
            Ok(output) => {
                let failure = output.result.as_ref().and_then(command_failure);
                let summary =
                    history_result_summary(failure.as_ref().map(|failure| failure.outcome));
                let resources = ResourceUris::for_agent(&agent_id);
                let mut content = vec![resource_link(resources.events)];
                if let Some(channel) = channel {
                    content.push(channel_resource_link(
                        ResourceUris::channel(&agent_id, channel.as_str()),
                        channel.as_str(),
                    ));
                }
                command_tool_result_with_content(summary, &output, failure, content)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Run one common query with typed required parameters.
    #[tool(
        name = "irc.query",
        description = "Run a typed WHOIS, WHO, NAMES, MODE, MOTD, HELP, or other common IRC query.",
        output_schema = envelope_schema::<CommandResult>(),
        annotations(
            title = "Run server query",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_query(&self, Parameters(input): Parameters<QueryInput>) -> CallToolResult {
        let result_detail = input.result_detail;
        let message = match query_message(input.query) {
            Ok(message) => message,
            Err(error) => return tool_error(error),
        };
        self.execute_result(
            &input.agent_id,
            message,
            CompletionMode::Auto,
            input.timeout_ms,
            "Query",
            result_detail,
        )
        .await
    }

    /// Read a WHOIS profile with a command-specific input and result schema.
    #[tool(
        name = "irc.whois",
        description = "Read one nickname's WHOIS profile as typed fields plus the lossless reply envelope.",
        output_schema = envelope_schema::<WhoisOutput>(),
        annotations(
            title = "Read WHOIS profile",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_whois(&self, Parameters(input): Parameters<WhoisInput>) -> CallToolResult {
        let requested_nickname = input.nickname;
        let message = OutboundMessage::new("WHOIS", vec![requested_nickname.to_string()]);
        match self
            .execute(
                &input.agent_id,
                message,
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let profile = whois_profile(&result);
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let output = WhoisOutput {
                    requested_nickname,
                    profile,
                    result: command_result_for_detail(result, input.result_detail),
                };
                command_tool_result(format!("WHOIS: {outcome:?}."), &output, failure)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Read channel membership with a command-specific input and result schema.
    #[tool(
        name = "irc.names",
        description = "Read channel membership grouped into typed NAMES entries.",
        output_schema = envelope_schema::<NamesOutput>(),
        annotations(
            title = "Read channel names",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_names(&self, Parameters(input): Parameters<NamesInput>) -> CallToolResult {
        let channels: Vec<String> = input.channels.iter().map(ToString::to_string).collect();
        let params = (!channels.is_empty())
            .then(|| channels.join(","))
            .into_iter()
            .collect();
        match self
            .execute(
                &input.agent_id,
                OutboundMessage::new("NAMES", params),
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let channels = names_channels(&result);
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let output = NamesOutput {
                    channels,
                    result: command_result_for_detail(result, input.result_detail),
                };
                command_tool_result(format!("NAMES: {outcome:?}."), &output, failure)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Read the visible channel list with typed entries.
    #[tool(
        name = "irc.list",
        description = "List visible IRC channels as typed channel, member-count, and topic entries.",
        output_schema = envelope_schema::<ListOutput>(),
        annotations(
            title = "List IRC channels",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_list(&self, Parameters(input): Parameters<ListInput>) -> CallToolResult {
        if let Some(mask) = input.mask.as_deref()
            && let Err(error) = validate_irc_atom(mask, "mask")
        {
            return tool_error(error);
        }
        let message = OutboundMessage::new("LIST", input.mask.into_iter().collect());
        match self
            .execute(
                &input.agent_id,
                message,
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let channels = list_channels(&result);
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let output = ListOutput {
                    channels,
                    result: command_result_for_detail(result, input.result_detail),
                };
                command_tool_result(format!("LIST: {outcome:?}."), &output, failure)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Read user or channel modes with typed replies.
    #[tool(
        name = "irc.mode.get",
        description = "Read user, channel, or list modes through a stable typed tool.",
        output_schema = envelope_schema::<ModeGetOutput>(),
        annotations(
            title = "Read IRC modes",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_mode_get(&self, Parameters(input): Parameters<ModeGetInput>) -> CallToolResult {
        if let Some(mode) = input.mode.as_deref()
            && let Err(error) = validate_irc_atom(mode, "mode")
        {
            return tool_error(error);
        }
        let target = input.target;
        let mut params = vec![target.to_string()];
        params.extend(input.mode);
        match self
            .execute(
                &input.agent_id,
                OutboundMessage::new("MODE", params),
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let modes = mode_replies(&result);
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let output = ModeGetOutput {
                    target,
                    modes,
                    result: command_result_for_detail(result, input.result_detail),
                };
                command_tool_result(format!("MODE query: {outcome:?}."), &output, failure)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Read HELP with ordered typed lines.
    #[tool(
        name = "irc.help",
        description = "Read the server HELP index or one subject as ordered typed lines.",
        output_schema = envelope_schema::<HelpOutput>(),
        annotations(
            title = "Read server help",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_help(&self, Parameters(input): Parameters<HelpInput>) -> CallToolResult {
        if let Some(subject) = input.subject.as_deref()
            && let Err(error) = validate_irc_atom(subject, "subject")
        {
            return tool_error(error);
        }
        let subject = input.subject;
        let message = OutboundMessage::new("HELP", subject.clone().into_iter().collect());
        match self
            .execute(
                &input.agent_id,
                message,
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let lines = help_lines(&result);
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let output = HelpOutput {
                    subject,
                    lines,
                    result: command_result_for_detail(result, input.result_detail),
                };
                command_tool_result(format!("HELP: {outcome:?}."), &output, failure)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Read one channel topic with typed metadata.
    #[tool(
        name = "irc.topic.get",
        description = "Read one channel topic and setter metadata through a stable typed tool.",
        output_schema = envelope_schema::<TopicOutput>(),
        annotations(
            title = "Read channel topic",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_topic_get(&self, Parameters(input): Parameters<TopicGetInput>) -> CallToolResult {
        let channel = input.channel;
        let resource = ResourceUris::channel(&input.agent_id, channel.as_str());
        match self
            .execute(
                &input.agent_id,
                OutboundMessage::new("TOPIC", vec![channel.to_string()]),
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let (topic, set_by, set_at) = topic_reply(&result);
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let output = TopicOutput {
                    channel,
                    topic,
                    set_by,
                    set_at,
                    resource: resource.clone(),
                    result: command_result_for_detail(result, input.result_detail),
                };
                command_tool_result_with_content(
                    format!("TOPIC query: {outcome:?}."),
                    &output,
                    failure,
                    vec![channel_resource_link(resource, output.channel.as_str())],
                )
            }
            Err(error) => tool_error(error),
        }
    }

    /// Set or clear one channel topic.
    #[tool(
        name = "irc.topic.set",
        description = "Set or clear one channel topic through a stable typed mutation.",
        output_schema = envelope_schema::<TopicOutput>(),
        annotations(
            title = "Change channel topic",
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_topic_set(&self, Parameters(input): Parameters<TopicSetInput>) -> CallToolResult {
        let channel = input.channel;
        let requested_topic = input.topic;
        let resource = ResourceUris::channel(&input.agent_id, channel.as_str());
        let message = OutboundMessage::new("TOPIC", vec![channel.to_string()])
            .with_trailing(&requested_topic);
        match self
            .execute(
                &input.agent_id,
                message,
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let (confirmed, set_by, set_at) = topic_reply(&result);
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let topic = confirmed.or_else(|| failure.is_none().then_some(requested_topic));
                let output = TopicOutput {
                    channel,
                    topic,
                    set_by,
                    set_at,
                    resource: resource.clone(),
                    result: command_result_for_detail(result, input.result_detail),
                };
                command_tool_result_with_content(
                    format!("TOPIC mutation: {outcome:?}."),
                    &output,
                    failure,
                    vec![channel_resource_link(resource, output.channel.as_str())],
                )
            }
            Err(error) => tool_error(error),
        }
    }

    /// Change this guest's nickname.
    #[tool(
        name = "irc.nick.set",
        description = "Change this guest's nickname through a stable typed mutation.",
        output_schema = envelope_schema::<NickSetOutput>(),
        annotations(
            title = "Change IRC nickname",
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_nick_set(&self, Parameters(input): Parameters<NickSetInput>) -> CallToolResult {
        let nickname = input.nickname;
        match self
            .execute(
                &input.agent_id,
                OutboundMessage::new("NICK", vec![nickname.to_string()]),
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let output = NickSetOutput {
                    nickname,
                    result: command_result_for_detail(result, input.result_detail),
                };
                command_tool_result(format!("NICK: {outcome:?}."), &output, failure)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Set or clear this guest's away state.
    #[tool(
        name = "irc.away.set",
        description = "Set or clear this guest's away state through a stable typed mutation.",
        output_schema = envelope_schema::<AwaySetOutput>(),
        annotations(
            title = "Change IRC away state",
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_away_set(&self, Parameters(input): Parameters<AwaySetInput>) -> CallToolResult {
        let message = input.message.filter(|message| !message.is_empty());
        let outbound = message.as_ref().map_or_else(
            || OutboundMessage::new("AWAY", Vec::new()),
            |message| OutboundMessage::new("AWAY", Vec::new()).with_trailing(message),
        );
        match self
            .execute(
                &input.agent_id,
                outbound,
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let output = AwaySetOutput {
                    away: message.is_some(),
                    message,
                    result: command_result_for_detail(result, input.result_detail),
                };
                command_tool_result(format!("AWAY: {outcome:?}."), &output, failure)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Remove one member from a channel.
    #[tool(
        name = "irc.kick",
        description = "Remove one nickname from a channel through a stable typed mutation. Where \
                       `mcp.confirm_destructive` is enabled, the call first returns an \
                       input_required confirmation and applies nothing until it is answered.",
        output_schema = envelope_schema::<KickOutput>(),
        annotations(
            title = "Kick channel member",
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn irc_kick(
        &self,
        Parameters(input): Parameters<KickInput>,
        context: RequestContext<RoleServer>,
        RequestState(request_state): RequestState,
        InputResponses(responses): InputResponses,
    ) -> Result<CallToolResponse, McpError> {
        let owner = self.callers.identify(&context)?;
        let profile = RequestProfile::from_context(&context);
        match self.confirm_destructive(
            &owner,
            &profile,
            &OriginatingOperation::for_tool("irc.kick", &input.salient()),
            &input.action(),
            request_state.as_deref(),
            responses.as_ref(),
        )? {
            Resolution::Ready(()) => {}
            Resolution::NeedsInput(request) => return Ok(request.into()),
            Resolution::Settled(result) => return Ok(result.into()),
        }

        let channel = input.channel;
        let nickname = input.nickname;
        let resource = ResourceUris::channel(&input.agent_id, channel.as_str());
        let outbound = input.reason.map_or_else(
            || OutboundMessage::new("KICK", vec![channel.to_string(), nickname.to_string()]),
            |reason| {
                OutboundMessage::new("KICK", vec![channel.to_string(), nickname.to_string()])
                    .with_trailing(reason)
            },
        );
        match self
            .execute(
                &input.agent_id,
                outbound,
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let output = KickOutput {
                    channel,
                    nickname,
                    resource: resource.clone(),
                    result: command_result_for_detail(result, input.result_detail),
                };
                Ok(command_tool_result_with_content(
                    format!("KICK: {outcome:?}."),
                    &output,
                    failure,
                    vec![channel_resource_link(resource, output.channel.as_str())],
                )
                .into())
            }
            Err(error) => Ok(tool_error(error).into()),
        }
    }

    /// Invite one nickname to a channel.
    #[tool(
        name = "irc.invite",
        description = "Invite one nickname to a channel through a stable typed mutation.",
        output_schema = envelope_schema::<InviteOutput>(),
        annotations(
            title = "Invite channel member",
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn irc_invite(&self, Parameters(input): Parameters<InviteInput>) -> CallToolResult {
        let channel = input.channel;
        let nickname = input.nickname;
        let resource = ResourceUris::channel(&input.agent_id, channel.as_str());
        let outbound =
            OutboundMessage::new("INVITE", vec![nickname.to_string(), channel.to_string()]);
        match self
            .execute(
                &input.agent_id,
                outbound,
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let output = InviteOutput {
                    nickname,
                    channel,
                    resource: resource.clone(),
                    result: command_result_for_detail(result, input.result_detail),
                };
                command_tool_result_with_content(
                    format!("INVITE: {outcome:?}."),
                    &output,
                    failure,
                    vec![channel_resource_link(resource, output.channel.as_str())],
                )
            }
            Err(error) => tool_error(error),
        }
    }

    /// Mutate the server-side MONITOR list after runtime ISUPPORT validation.
    #[tool(
        name = "irc.monitor.update",
        description = "Add, remove, or clear server-side MONITOR entries after runtime support checks.",
        output_schema = envelope_schema::<MonitorUpdateOutput>(),
        annotations(
            title = "Update IRC monitor list",
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_monitor_update(
        &self,
        Parameters(input): Parameters<MonitorUpdateInput>,
    ) -> CallToolResult {
        let snapshot = match self.gateway.snapshot(&input.agent_id).await {
            Ok(snapshot) => snapshot,
            Err(error) => return tool_error(error),
        };
        if !snapshot
            .protocol
            .isupport
            .keys()
            .any(|name| name.eq_ignore_ascii_case("MONITOR"))
        {
            return tool_error(GatewayError::InvalidMessage(
                "MONITOR is not advertised by this server".into(),
            ));
        }
        let operation = input.operation;
        let nicknames = input.nicknames;
        let (verb, target_list) = match operation {
            MonitorUpdateKind::Add if !nicknames.is_empty() => (
                "+",
                Some(
                    nicknames
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(","),
                ),
            ),
            MonitorUpdateKind::Remove if !nicknames.is_empty() => (
                "-",
                Some(
                    nicknames
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(","),
                ),
            ),
            MonitorUpdateKind::Clear if nicknames.is_empty() => ("C", None),
            MonitorUpdateKind::Add | MonitorUpdateKind::Remove => {
                return tool_error(GatewayError::InvalidMessage(
                    "MONITOR add/remove requires at least one nickname".into(),
                ));
            }
            MonitorUpdateKind::Clear => {
                return tool_error(GatewayError::InvalidMessage(
                    "MONITOR clear does not accept nicknames".into(),
                ));
            }
        };
        let mut params = vec![verb.into()];
        params.extend(target_list);
        match self
            .execute(
                &input.agent_id,
                OutboundMessage::new("MONITOR", params),
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let output = MonitorUpdateOutput {
                    operation,
                    nicknames,
                    result: command_result_for_detail(result, input.result_detail),
                };
                command_tool_result(format!("MONITOR: {outcome:?}."), &output, failure)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Change user or channel modes.
    #[tool(
        name = "irc.mode.set",
        description = "Change user or channel modes through a stable typed mutation.",
        output_schema = envelope_schema::<ModeSetOutput>(),
        annotations(
            title = "Change IRC modes",
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn irc_mode_set(&self, Parameters(input): Parameters<ModeSetInput>) -> CallToolResult {
        if let Err(error) = validate_irc_atom(&input.modes, "modes") {
            return tool_error(error);
        }
        if !input.modes.starts_with(['+', '-']) {
            return tool_error(GatewayError::InvalidMessage(
                "mode mutation must begin with + or -".into(),
            ));
        }
        for argument in &input.arguments {
            if let Err(error) = validate_irc_atom(argument, "mode argument") {
                return tool_error(error);
            }
        }
        let target = input.target;
        let modes = input.modes;
        let arguments = input.arguments;
        let mut params = vec![target.to_string(), modes.clone()];
        params.extend(arguments.iter().cloned());
        let resource = target
            .channel()
            .map(|channel| ResourceUris::channel(&input.agent_id, channel.as_str()));
        match self
            .execute(
                &input.agent_id,
                OutboundMessage::new("MODE", params),
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let output = ModeSetOutput {
                    target,
                    modes,
                    arguments,
                    resource: resource.clone(),
                    result: command_result_for_detail(result, input.result_detail),
                };
                let content = resource
                    .map(|uri| channel_resource_link(uri, output.target.as_str()))
                    .into_iter()
                    .collect();
                command_tool_result_with_content(
                    format!("MODE mutation: {outcome:?}."),
                    &output,
                    failure,
                    content,
                )
            }
            Err(error) => tool_error(error),
        }
    }

    /// Add or remove a lightweight reaction from one server-identified message.
    #[tool(
        name = "irc.reaction.update",
        description = "Add or remove an IRCv3 reaction after checking message-tags and the server's client-tag policy.",
        output_schema = envelope_schema::<ReactionUpdateOutput>(),
        annotations(
            title = "Update message reaction",
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn irc_reaction_update(
        &self,
        Parameters(input): Parameters<ReactionUpdateInput>,
    ) -> CallToolResult {
        if let Err(error) = validate_irc_atom(&input.message_id, "message_id") {
            return tool_error(error);
        }
        if input.reaction.is_empty()
            || input
                .reaction
                .bytes()
                .any(|byte| matches!(byte, b'\0' | b'\r' | b'\n'))
        {
            return tool_error(GatewayError::InvalidMessage(
                "reaction must be non-empty and contain no NUL, CR, or LF".into(),
            ));
        }
        let snapshot = match self.gateway.snapshot(&input.agent_id).await {
            Ok(snapshot) => snapshot,
            Err(error) => return tool_error(error),
        };
        if let Err(error) = require_capability(&snapshot, "message-tags", "IRCv3 reactions") {
            return tool_error(error);
        }
        let reaction_tag = match input.operation {
            ReactionUpdateKind::Add => "draft/react",
            ReactionUpdateKind::Remove => "draft/unreact",
        };
        for tag in ["reply", reaction_tag] {
            if !client_tag_allowed(&snapshot, tag) {
                return tool_error(GatewayError::InvalidMessage(format!(
                    "+{tag} is blocked by the server's CLIENTTAGDENY policy"
                )));
            }
        }

        let target = input.target;
        let message_id = input.message_id;
        let reaction = input.reaction;
        let outbound = OutboundMessage {
            tags: vec![
                Tag::new("+reply", Some(message_id.clone())),
                Tag::new(format!("+{reaction_tag}"), Some(reaction.clone())),
            ],
            command: "TAGMSG".into(),
            params: vec![target.to_string()],
            trailing: None,
        };
        match self
            .execute(
                &input.agent_id,
                outbound,
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let output = ReactionUpdateOutput {
                    target,
                    message_id,
                    reaction,
                    operation: input.operation,
                    result: command_result_for_detail(result, input.result_detail),
                };
                command_tool_result(format!("Reaction update: {outcome:?}."), &output, failure)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Redact one message after exact capability negotiation.
    #[tool(
        name = "irc.message.redact",
        description = "Redact one server-identified message through negotiated IRCv3 message \
                       redaction. Where `mcp.confirm_destructive` is enabled, the call first \
                       returns an input_required confirmation and applies nothing until it is \
                       answered.",
        output_schema = envelope_schema::<MessageRedactOutput>(),
        annotations(
            title = "Redact IRC message",
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn irc_message_redact(
        &self,
        Parameters(input): Parameters<MessageRedactInput>,
        context: RequestContext<RoleServer>,
        RequestState(request_state): RequestState,
        InputResponses(responses): InputResponses,
    ) -> Result<CallToolResponse, McpError> {
        let owner = self.callers.identify(&context)?;
        let profile = RequestProfile::from_context(&context);
        if let Err(error) = validate_irc_atom(&input.message_id, "message_id") {
            return Ok(tool_error(error).into());
        }
        let snapshot = match self.gateway.snapshot(&input.agent_id).await {
            Ok(snapshot) => snapshot,
            Err(error) => return Ok(tool_error(error).into()),
        };
        for (capability, operation) in [
            ("message-tags", "IRCv3 message redaction"),
            ("message-redaction", "IRCv3 message redaction"),
        ] {
            if let Err(error) = require_capability(&snapshot, capability, operation) {
                return Ok(tool_error(error).into());
            }
        }
        // After validation, so nobody is asked to approve a call that was never
        // going to reach the server, and the confirmed arguments are the ones
        // already checked.
        match self.confirm_destructive(
            &owner,
            &profile,
            &OriginatingOperation::for_tool("irc.message.redact", &input.salient()),
            &input.action(),
            request_state.as_deref(),
            responses.as_ref(),
        )? {
            Resolution::Ready(()) => {}
            Resolution::NeedsInput(request) => return Ok(request.into()),
            Resolution::Settled(result) => return Ok(result.into()),
        }

        let target = input.target;
        let message_id = input.message_id;
        let reason = input.reason.filter(|reason| !reason.is_empty());
        let mut outbound =
            OutboundMessage::new("REDACT", vec![target.to_string(), message_id.clone()]);
        outbound.trailing = reason.clone();
        match self
            .execute(
                &input.agent_id,
                outbound,
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let output = MessageRedactOutput {
                    target,
                    message_id,
                    reason,
                    result: command_result_for_detail(result, input.result_detail),
                };
                Ok(command_tool_result(
                    format!("Message redaction: {outcome:?}."),
                    &output,
                    failure,
                )
                .into())
            }
            Err(error) => Ok(tool_error(error).into()),
        }
    }

    /// Read one synchronized conversation read marker.
    #[tool(
        name = "irc.read.get",
        description = "Read one synchronized IRCv3 conversation marker after exact capability negotiation.",
        output_schema = envelope_schema::<ReadMarkerOutput>(),
        annotations(
            title = "Read conversation marker",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_read_get(
        &self,
        Parameters(input): Parameters<ReadMarkerGetInput>,
    ) -> CallToolResult {
        let snapshot = match self.gateway.snapshot(&input.agent_id).await {
            Ok(snapshot) => snapshot,
            Err(error) => return tool_error(error),
        };
        if let Err(error) = require_capability(&snapshot, "read-marker", "IRCv3 read markers") {
            return tool_error(error);
        }
        let target = input.target;
        match self
            .execute(
                &input.agent_id,
                OutboundMessage::new("MARKREAD", vec![target.to_string()]),
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let read_at = read_marker_reply(&result);
                let output = ReadMarkerOutput {
                    target,
                    read_at,
                    result: command_result_for_detail(result, input.result_detail),
                };
                command_tool_result(format!("Read marker query: {outcome:?}."), &output, failure)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Advance one synchronized conversation read marker.
    #[tool(
        name = "irc.read.set",
        description = "Advance one synchronized IRCv3 conversation marker to a previously received server timestamp.",
        output_schema = envelope_schema::<ReadMarkerOutput>(),
        annotations(
            title = "Advance conversation marker",
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_read_set(
        &self,
        Parameters(input): Parameters<ReadMarkerSetInput>,
    ) -> CallToolResult {
        let snapshot = match self.gateway.snapshot(&input.agent_id).await {
            Ok(snapshot) => snapshot,
            Err(error) => return tool_error(error),
        };
        if let Err(error) = require_capability(&snapshot, "read-marker", "IRCv3 read markers") {
            return tool_error(error);
        }
        let target = input.target;
        let requested = input.read_at;
        let outbound = OutboundMessage::new(
            "MARKREAD",
            vec![target.to_string(), format!("timestamp={requested}")],
        );
        match self
            .execute(
                &input.agent_id,
                outbound,
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let read_at = read_marker_reply(&result);
                let output = ReadMarkerOutput {
                    target,
                    read_at,
                    result: command_result_for_detail(result, input.result_detail),
                };
                command_tool_result(
                    format!("Read marker update: {outcome:?}."),
                    &output,
                    failure,
                )
            }
            Err(error) => tool_error(error),
        }
    }

    /// Publish one privacy-sensitive typing state with required throttling.
    #[tool(
        name = "irc.typing.set",
        description = "Publish a privacy-sensitive IRCv3 typing state with negotiated-tag checks and per-target throttling.",
        output_schema = envelope_schema::<TypingSetOutput>(),
        annotations(
            title = "Publish typing state",
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn irc_typing_set(
        &self,
        Parameters(input): Parameters<TypingSetInput>,
    ) -> CallToolResult {
        let snapshot = match self.gateway.snapshot(&input.agent_id).await {
            Ok(snapshot) => snapshot,
            Err(error) => return tool_error(error),
        };
        if let Err(error) = require_capability(&snapshot, "message-tags", "IRCv3 typing") {
            return tool_error(error);
        }
        if !client_tag_allowed(&snapshot, "typing") {
            return tool_error(GatewayError::InvalidMessage(
                "+typing is blocked by the server's CLIENTTAGDENY policy".into(),
            ));
        }

        let target = input.target;
        let throttle_key = (
            input.agent_id.clone(),
            casefold_target(&snapshot, target.as_str()),
        );
        let now = Instant::now();
        {
            let mut deadlines = self.typing_deadlines.lock().await;
            if let Err(retry_after) = claim_typing_slot(&mut deadlines, throttle_key, now) {
                return tool_error(GatewayError::InvalidMessage(format!(
                    "typing notifications are limited to one per target every 3 seconds; retry after {} ms",
                    retry_after.as_millis()
                )));
            }
        }

        let state = input.state;
        let outbound = OutboundMessage {
            tags: vec![Tag::new("+typing", Some(state.as_str().into()))],
            command: "TAGMSG".into(),
            params: vec![target.to_string()],
            trailing: None,
        };
        match self
            .execute(
                &input.agent_id,
                outbound,
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await
        {
            Ok(result) => {
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let output = TypingSetOutput {
                    target,
                    state,
                    result: command_result_for_detail(result, input.result_detail),
                };
                command_tool_result(format!("Typing state: {outcome:?}."), &output, failure)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Execute any syntactically valid structured IRC command.
    #[tool(
        name = "irc.execute",
        description = "Execute a structured IRC command without accepting raw CRLF-delimited lines.",
        output_schema = envelope_schema::<CommandResult>(),
        annotations(
            title = "Execute IRC command",
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn irc_execute(&self, Parameters(input): Parameters<ExecuteInput>) -> CallToolResult {
        let result_detail = input.result_detail;
        let message = OutboundMessage {
            tags: input.tags,
            command: input.command,
            params: input.params,
            trailing: input.trailing,
        };
        let mode = match input.response_mode {
            ResponseMode::Auto => CompletionMode::Auto,
            ResponseMode::Collect => CompletionMode::Collect,
            ResponseMode::FireAndForget => CompletionMode::FireAndForget,
        };
        self.execute_result(
            &input.agent_id,
            message,
            mode,
            input.timeout_ms,
            "IRC command",
            result_detail,
        )
        .await
    }

    /// Read ordered events after a caller-owned cursor, optionally long polling.
    #[tool(
        name = "irc.events.read",
        description = "Read an agent's bounded event journal after an explicit caller-owned cursor, \
                       optionally through a watch's registered selection.",
        output_schema = envelope_schema::<EventsReadOutput>(),
        annotations(
            title = "Read event journal",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn irc_events_read(
        &self,
        Parameters(input): Parameters<EventsReadInput>,
    ) -> CallToolResult {
        let resources = ResourceUris::for_agent(&input.agent_id);
        let wait = Duration::from_millis(input.wait_ms);
        let page = match &input.watch_id {
            Some(watch_id) => {
                if let Some(conflict) = input.conflicting_filter() {
                    return tool_error(GatewayError::InvalidMessage(format!(
                        "watch_id already carries a complete selection, so `{conflict}` must be \
                         omitted; narrow the watch itself or read without watch_id"
                    )));
                }
                self.gateway
                    .read_watch_events(
                        &input.agent_id,
                        watch_id,
                        input.cursor.clone(),
                        input.limit,
                        wait,
                    )
                    .await
            }
            None => {
                self.gateway
                    .read_events(
                        &input.agent_id,
                        input.cursor.clone(),
                        input.limit,
                        wait,
                        input.filter(),
                    )
                    .await
            }
        };
        match page {
            Ok(page) => {
                // The one explicit caller action that moves the activity
                // anchor. It happens after the read succeeded and records
                // exactly the position the caller is being handed, so the
                // counts a later hint reports start where this read stopped.
                if input.set_activity_anchor {
                    self.gateway
                        .set_activity_anchor(&input.agent_id, page.next_cursor.clone())
                        .await;
                }
                tool_success_with_content(
                    format!(
                        "Read {} event(s); cursor is {}.{}",
                        page.events.len(),
                        page.next_cursor.sequence,
                        if page.has_more {
                            " More are already retained: read again from that cursor."
                        } else {
                            ""
                        }
                    ),
                    &page,
                    vec![resource_link(resources.events)],
                )
            }
            Err(error) => tool_error(error),
        }
    }

    /// Offer a direct peer chat.
    #[tool(
        name = "irc.dcc.chat.open",
        description = "Send an ordinary or reverse DCC CHAT offer to one peer.",
        output_schema = envelope_schema::<DccSessionOutput>(),
        annotations(
            title = "Offer direct chat",
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn irc_dcc_chat_open(
        &self,
        Parameters(input): Parameters<DccChatOpenInput>,
    ) -> CallToolResult {
        match self
            .gateway
            .dcc_chat_open(&input.agent_id, input.target.to_string(), input.reverse)
            .await
        {
            Ok(session) => dcc_session_result("DCC CHAT offer written.", &input.agent_id, session),
            Err(error) => tool_error(error),
        }
    }

    /// Queue one active direct-chat line.
    #[tool(
        name = "irc.dcc.chat.send",
        description = "Send one bounded line through an established DCC CHAT socket.",
        output_schema = envelope_schema::<DccChatSendOutput>(),
        annotations(
            title = "Send direct chat line",
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn irc_dcc_chat_send(
        &self,
        Parameters(input): Parameters<DccChatSendInput>,
    ) -> CallToolResult {
        match self
            .gateway
            .dcc_chat_send(&input.agent_id, input.dcc_session_id.clone(), input.text)
            .await
        {
            Ok(()) => tool_success_with_content(
                "DCC CHAT line queued.",
                &DccChatSendOutput {
                    dcc_session_id: input.dcc_session_id,
                    queued: true,
                },
                vec![dcc_resource_link(&input.agent_id)],
            ),
            Err(error) => tool_error(error),
        }
    }

    /// Offer one local file without loading its body into memory.
    #[tool(
        name = "irc.dcc.send",
        description = "Offer and stream one local file through ordinary or reverse DCC SEND. \
                       A client that declares the tasks extension in its request capabilities \
                       receives a task handle that follows the transfer to completion, with \
                       status and cancellation, instead of a result returned once the offer is \
                       written.",
        output_schema = envelope_schema::<DccSessionOutput>(),
        annotations(
            title = "Offer file transfer",
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn irc_dcc_send(&self, Parameters(input): Parameters<DccSendInput>) -> CallToolResult {
        match self
            .gateway
            .dcc_send(
                &input.agent_id,
                input.target.to_string(),
                input.source_path,
                input.filename,
                input.reverse,
            )
            .await
        {
            Ok(session) => dcc_session_result("DCC SEND offer written.", &input.agent_id, session),
            Err(error) => tool_error(error),
        }
    }

    /// Accept one incoming direct offer.
    #[tool(
        name = "irc.dcc.accept",
        description = "Accept one incoming DCC CHAT or SEND offer into a configured receive root \
                       with explicit file conflict behavior. A SEND names `root` and a relative \
                       `destination_path`; omitting either where the server cannot choose returns \
                       an input_required elicitation to answer and retry with. A client that \
                       declares the tasks extension in its request capabilities receives a task \
                       handle that follows a SEND transfer to completion, with status and \
                       cancellation, instead of a result returned once the acceptance is written.",
        output_schema = envelope_schema::<DccSessionOutput>(),
        annotations(
            title = "Accept direct offer",
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn irc_dcc_accept(
        &self,
        Parameters(input): Parameters<DccAcceptInput>,
        context: RequestContext<RoleServer>,
        RequestState(request_state): RequestState,
        InputResponses(responses): InputResponses,
    ) -> Result<CallToolResponse, McpError> {
        let owner = self.callers.identify(&context)?;
        let profile = RequestProfile::from_context(&context);
        let plan = match self
            .plan_dcc_accept(
                &owner,
                &profile,
                &input,
                request_state.as_deref(),
                responses.as_ref(),
            )
            .await?
        {
            DccAcceptResolution::Ready(plan) => plan,
            DccAcceptResolution::NeedsInput(request) => return Ok(request.into()),
            DccAcceptResolution::Settled(result) => return Ok(result.into()),
        };
        Ok(match self
            .gateway
            .dcc_accept(
                &input.agent_id,
                input.dcc_session_id.clone(),
                plan.destination,
                plan.conflict,
            )
            .await
        {
            Ok(session) => dcc_session_result("DCC offer accepted.", &input.agent_id, session),
            Err(error) => tool_error(error),
        }
        .into())
    }

    /// Reject one incoming offer.
    #[tool(
        name = "irc.dcc.reject",
        description = "Reject one incoming offered DCC session.",
        output_schema = envelope_schema::<DccSessionOutput>(),
        annotations(
            title = "Reject direct offer",
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_dcc_reject(
        &self,
        Parameters(input): Parameters<DccSessionInput>,
    ) -> CallToolResult {
        match self
            .gateway
            .dcc_reject(&input.agent_id, input.dcc_session_id)
            .await
        {
            Ok(session) => dcc_session_result("DCC offer rejected.", &input.agent_id, session),
            Err(error) => tool_error(error),
        }
    }

    /// Cancel one non-terminal direct session; terminal cancellation is idempotent.
    #[tool(
        name = "irc.dcc.cancel",
        description = "Cancel one active or offered DCC session and close its direct resources.",
        output_schema = envelope_schema::<DccSessionOutput>(),
        annotations(
            title = "Cancel direct session",
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_dcc_cancel(
        &self,
        Parameters(input): Parameters<DccSessionInput>,
    ) -> CallToolResult {
        match self
            .gateway
            .dcc_cancel(&input.agent_id, input.dcc_session_id)
            .await
        {
            Ok(session) => {
                let summary = format!("DCC session is {}.", session.state);
                dcc_session_result(&summary, &input.agent_id, session)
            }
            Err(error) => tool_error(error),
        }
    }

    /// List retained direct sessions.
    #[tool(
        name = "irc.dcc.list",
        description = "List active and recently terminal DCC sessions for one guest.",
        output_schema = envelope_schema::<DccListOutput>(),
        annotations(
            title = "List direct sessions",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn irc_dcc_list(&self, Parameters(input): Parameters<DccListInput>) -> CallToolResult {
        match self
            .gateway
            .dcc_list(
                &input.agent_id,
                input.state,
                input.kind,
                input.peer.as_deref(),
            )
            .await
        {
            Ok(sessions) => {
                let output = DccListOutput { sessions };
                tool_success_with_content(
                    format!("Found {} DCC session(s).", output.sessions.len()),
                    &output,
                    vec![dcc_resource_link(&input.agent_id)],
                )
            }
            Err(error) => tool_error(error),
        }
    }

    async fn execute(
        &self,
        agent_id: &crate::agent::AgentId,
        message: OutboundMessage,
        mode: CompletionMode,
        timeout_ms: u64,
    ) -> Result<CommandResult, GatewayError> {
        self.gateway
            .execute(agent_id, message, mode, Duration::from_millis(timeout_ms))
            .await
    }

    async fn execute_result(
        &self,
        agent_id: &crate::agent::AgentId,
        message: OutboundMessage,
        mode: CompletionMode,
        timeout_ms: u64,
        operation: &str,
        result_detail: ToolResultDetail,
    ) -> CallToolResult {
        match self.execute(agent_id, message, mode, timeout_ms).await {
            Ok(result) => {
                let outcome = result.outcome;
                let failure = command_failure(&result);
                let result = command_result_for_detail(result, result_detail);
                command_tool_result(format!("{operation}: {outcome:?}."), &result, failure)
            }
            Err(error) => tool_error(error),
        }
    }

    async fn send_message(&self, input: SendInput) -> Result<SendOutput, GatewayError> {
        let result_detail = input.result_detail;
        validate_irc_atom(input.target.as_str(), "target")?;
        let snapshot = self.gateway.snapshot(&input.agent_id).await?;
        if input.reply_to.is_some() && !capability_active(&snapshot, "message-tags") {
            return Err(GatewayError::InvalidMessage(
                "reply_to requires negotiated message-tags".into(),
            ));
        }
        if matches!(input.kind, SendKind::Tagmsg) && !capability_active(&snapshot, "message-tags") {
            return Err(GatewayError::InvalidMessage(
                "TAGMSG requires negotiated message-tags".into(),
            ));
        }
        let messages = build_send_messages(
            &input,
            snapshot.line_budget.max_body_bytes,
            relay_prefix_reservation(&snapshot),
            self.gateway.config().limits.max_message_bytes,
            self.gateway.config().limits.max_message_parts,
        )?;
        let mut results = Vec::with_capacity(messages.len());
        for message in messages {
            results.push(
                self.execute(
                    &input.agent_id,
                    message,
                    CompletionMode::Auto,
                    input.timeout_ms,
                )
                .await?,
            );
        }
        let results: Vec<_> = results
            .into_iter()
            .map(|result| command_result_for_detail(result, result_detail))
            .collect();
        Ok(SendOutput {
            line_count: results.len(),
            results,
        })
    }

    /// Read history, reporting the phases a caller cannot otherwise see.
    ///
    /// The playback request is one correlated command bounded by the caller's
    /// own `timeout_ms`, which is where the whole latency of this tool lives, so
    /// the useful reports are that the request went out and how much came back.
    /// Finer granularity is not available without restructuring correlation: a
    /// CHATHISTORY request produces exactly one batch, and per-record ticks
    /// would need a channel threaded through the command path that every one of
    /// this server's tools shares — for a signal no client can act on.
    async fn history(
        &self,
        input: HistoryInput,
        progress: &mut Option<ProgressReporter>,
    ) -> Result<HistoryOutput, GatewayError> {
        crate::mcp::progress::report(progress, 1, "Checking history availability.").await;
        let snapshot = self.gateway.snapshot(&input.agent_id).await?;
        let native = capability_active(&snapshot, "chathistory");
        let legacy = snapshot.protocol.commands.contains_key("HISTORY");
        let (availability, message) = if native {
            (
                HistoryAvailability::Native,
                history_message(input.target.as_str(), &input.selector, input.limit)?,
            )
        } else if legacy {
            (
                HistoryAvailability::Degraded,
                OutboundMessage::new(
                    "HISTORY",
                    vec![input.target.to_string(), input.limit.to_string()],
                ),
            )
        } else {
            return Ok(HistoryOutput {
                availability: HistoryAvailability::Unavailable,
                result: None,
                events: Vec::new(),
                result_detail: input.result_detail,
            });
        };
        let before = snapshot.journal.latest;
        crate::mcp::progress::report(progress, 2, "Requesting playback from the server.").await;
        let result = self
            .execute(
                &input.agent_id,
                message,
                CompletionMode::Auto,
                input.timeout_ms,
            )
            .await?;
        let mut events = self
            .gateway
            .read_events(
                &input.agent_id,
                Some(before),
                self.gateway.config().limits.max_event_page_size,
                Duration::ZERO,
                EventFilter {
                    command_id: Some(result.command_id.as_str().to_owned()),
                    origin: Some(EventOrigin::History),
                    ..EventFilter::default()
                },
            )
            .await?
            .events;
        if !input.include_non_message_events {
            events.retain(|event| {
                matches!(
                    event.class,
                    EventClass::MessageChannel
                        | EventClass::MessagePrivate
                        | EventClass::MessageAction
                        | EventClass::MessageNotice
                        | EventClass::MessageTagged
                )
            });
        }
        events.truncate(input.limit);
        crate::mcp::progress::report(
            progress,
            HISTORY_PROGRESS_TOTAL,
            format!("Collected {} history records.", events.len()),
        )
        .await;
        let failure = is_failure_outcome(result.outcome);
        let result_detail = if failure {
            ToolResultDetail::Full
        } else {
            input.result_detail
        };
        let result = history_command_result_for_detail(result, result_detail);
        Ok(HistoryOutput {
            availability,
            result: Some(result),
            events,
            result_detail,
        })
    }
}

#[tool_handler(router = self.tool_router)]
#[prompt_handler(router = self.prompt_router)]
impl ServerHandler for IrcMcpService {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_prompts()
                .enable_resources()
                .enable_resources_subscribe()
                .enable_resources_list_changed()
                .enable_completions()
                .enable_tasks()
                .build(),
        )
        // `ServerInfo::new` defaults to whichever revision the SDK calls
        // latest, which trails the one this server implements.
        .with_protocol_version(ProtocolVersion::V_2026_07_28)
        .with_server_info(Implementation::new(
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
        ))
        .with_instructions(MCP_INSTRUCTIONS)
    }

    /// This server speaks exactly one protocol revision.
    ///
    /// Everything it exposes assumes the stateless request model: identity and
    /// capabilities arrive per request, cross-request state is named by an
    /// explicit handle, and there is no handshake to remember. Advertising an
    /// older revision would promise a lifecycle none of that implements, so the
    /// narrower list is the honest one — and it is what makes a per-request
    /// version outside the set an explicit `-32022` instead of a request served
    /// under assumptions the caller does not share.
    fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
        std::borrow::Cow::Borrowed(SUPPORTED_PROTOCOL_VERSIONS)
    }

    /// Route one tool call, running the long DCC operations as MCP tasks for a
    /// caller that can follow one.
    ///
    /// A DCC transfer already has gateway-side session state, progress, and
    /// cancellation; without this the originating tool completed as soon as the
    /// offer was written, leaving the client with no native way to follow the
    /// work it had just started. Defining it in this impl means the
    /// `#[tool_handler]` macro leaves its own `call_tool` out.
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        // One gate for the whole tool surface: every tool but irc.connect
        // names a handle, and a handle the caller does not own must be
        // refused before it reaches the gateway.
        let owner = self.callers.identify(&context)?;
        self.authorize_handles(&owner, &request).await?;

        let profile = RequestProfile::from_context(&context);
        if runs_as_task(&request.name, &profile) {
            // Input first, task second. The tasks extension says pre-creation
            // MRTR exchanges SHOULD be resolved synchronously before a
            // `CreateTaskResult`. Tasks have a distinct later input path via
            // `tasks/get` and `tasks/update`, but this destination is required
            // to decide what work the task represents, so creating first would
            // add a poll/update loop for no benefit.
            if let Some(settled) = self
                .settle_input_before_task(&owner, &profile, &request)
                .await?
            {
                return Ok(settled);
            }
            let service = self.clone();
            let context = context.clone();
            let task =
                self.gateway
                    .tasks()
                    .spawn(owner, TASK_INITIAL_STATUS, move |task_context| {
                        Box::pin(async move {
                            service.run_dcc_task(request, context, task_context).await
                        })
                    });
            return Ok(CreateTaskResult::new(task).into());
        }
        let agent_id = named_agent(&request);
        // The attention check is itself the stronger, cursor-bearing activity
        // answer and is the one result expected every minute on fallback hosts.
        // Repeating a derived hint beside it adds tokens but no information.
        let carries_activity_hint = request.name != "irc.attention.check";
        let call = ToolCallContext::new(self, request, context);
        let mut response = self.tool_router.call(call).await?;
        if let CallToolResponse::Complete(result) = &mut response {
            adopt_unstructured(result);
            if carries_activity_hint {
                self.hint_at_activity(agent_id.as_ref(), result).await;
            }
        }
        Ok(response)
    }

    /// Report one task's state to the caller that created it.
    ///
    /// The owner check is what makes a task id safe to hand out: it is a bearer
    /// token otherwise, and any caller who learned one could read a transfer it
    /// never started. A task belonging to somebody else is refused exactly as a
    /// task that never existed.
    async fn get_task(
        &self,
        request: GetTaskParams,
        context: RequestContext<RoleServer>,
    ) -> Result<GetTaskResult, McpError> {
        let owner = self.callers.identify(&context)?;
        self.gateway
            .tasks()
            .get(&owner, &request.task_id)
            .map(GetTaskResult::new)
    }

    async fn update_task(
        &self,
        request: UpdateTaskParams,
        context: RequestContext<RoleServer>,
    ) -> Result<(), McpError> {
        let owner = self.callers.identify(&context)?;
        self.gateway
            .tasks()
            .update(&owner, &request.task_id, request.input_responses)
    }

    async fn cancel_task(
        &self,
        request: CancelTaskParams,
        context: RequestContext<RoleServer>,
    ) -> Result<(), McpError> {
        let owner = self.callers.identify(&context)?;
        self.gateway.tasks().cancel(&owner, &request.task_id)
    }

    /// List agent resources one bounded page at a time.
    ///
    /// Eight fixed resources plus per-channel resources exist for each agent,
    /// so at the configured agent ceiling an unpaginated reply would be large
    /// enough to break clients that bound response size. The cursor is the
    /// index of the first item on the next page, which is stable because
    /// `agent_ids` is ordered.
    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        let owner = self.callers.identify(&context)?;
        let offset = match request.and_then(|request| request.cursor) {
            None => 0,
            Some(cursor) => cursor.parse::<usize>().map_err(|_| {
                McpError::invalid_params(format!("unrecognized resource cursor: {cursor}"), None)
            })?,
        };

        let mut resources = Vec::new();
        let owned = self.gateway.agent_ids_for(&owner).await;
        for agent_id in &owned {
            // A snapshot is what makes the per-channel resources discoverable
            // and supplies the last-modified hint; an agent that vanished
            // mid-listing simply contributes nothing.
            let Ok(snapshot) = self.gateway.snapshot(agent_id).await else {
                continue;
            };
            resources.extend(
                descriptors_for_agent(agent_id, &snapshot.state, Some(snapshot.state.snapshot_at))
                    .into_iter()
                    .map(ResourceDescriptor::into_resource),
            );
        }
        for watch in self.gateway.watches().list() {
            if owned.contains(&watch.agent_id) {
                resources.push(watch_resource_entry(&watch));
            }
        }

        if offset > resources.len() {
            return Err(McpError::invalid_params(
                format!("resource cursor {offset} is past the end of the list"),
                None,
            ));
        }
        let mut page: Vec<Resource> = resources.split_off(offset);
        let next_cursor = (page.len() > RESOURCE_PAGE_SIZE).then(|| {
            page.truncate(RESOURCE_PAGE_SIZE);
            (offset + RESOURCE_PAGE_SIZE).to_string()
        });

        let mut result = ListResourcesResult::with_all_items(page);
        result.next_cursor = next_cursor;
        Ok(result)
    }

    /// The template set is a fixed single entry, so it is never paginated and
    /// any supplied cursor is meaningless rather than merely ignored.
    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        Ok(ListResourceTemplatesResult::with_all_items(vec![
            ResourceTemplate::new(CHANNEL_STATE_TEMPLATE, "irc-channel-state")
                .with_title("IRC channel state")
                .with_description("Best-effort state for one channel joined by one explicit agent")
                .with_mime_type("application/json"),
            ResourceTemplate::new(CHANNEL_MEMBERS_TEMPLATE, "irc-channel-members")
                .with_title("IRC channel members")
                .with_description("Who is currently in one channel, without its topic or modes")
                .with_mime_type("application/json"),
            ResourceTemplate::new(CHANNEL_TOPIC_TEMPLATE, "irc-channel-topic")
                .with_title("IRC channel topic")
                .with_description(
                    "The current topic of one channel. Channel topics carry that channel's \
                     standing instructions, so this is worth reading on its own.",
                )
                .with_mime_type("application/json"),
            ResourceTemplate::new(TRANSCRIPT_TEMPLATE, "irc-transcript")
                .with_title("IRC conversation transcript")
                .with_description(
                    "Compact conversation with one channel or peer: who said what, and when, \
                     without the lossless protocol detail.",
                )
                .with_mime_type("application/json"),
            ResourceTemplate::new(EVENT_CURSOR_TEMPLATE, "irc-events-after")
                .with_title("IRC events after a cursor")
                .with_description(
                    "Every retained event after the given sequence, with the next cursor to \
                     read from. Subscribe to the agent's events resource and read this on \
                     each notification to consume the journal without polling.",
                )
                .with_mime_type("application/json"),
            ResourceTemplate::new(WATCH_EVENTS_TEMPLATE, "irc-watch-events-after")
                .with_title("Watch events after a position")
                .with_description(
                    "Compact conversational records that one watch selects, after an explicit \
                     position carried in the path. Reading changes nothing on the server, so the \
                     same URI always returns the same window and `next_cursor` advances only over \
                     the events returned.",
                )
                .with_mime_type("application/json"),
        ]))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, McpError> {
        let owner = self.callers.identify(&context)?;
        // A watch lives under its own authority, so it is resolved before any
        // agent lookup. Neither of its resources mutates delivery state: the
        // descriptor reports the selection and the stream's health, and the
        // positioned window takes its position from the path.
        if request.uri.starts_with(WATCH_URI_PREFIX) {
            let watch = WatchUri::from_str(&request.uri)
                .map_err(|error| McpError::resource_not_found(error.to_string(), None))?;
            self.gateway
                .authorize_watch(&owner, &watch.watch_id)
                .await?;
            return match watch.target {
                WatchTarget::Descriptor => {
                    let payload = self
                        .gateway
                        .read_watch(&watch.watch_id)
                        .await
                        .map_err(gateway_read_error)?;
                    json_resource(request.uri, &payload)
                }
                WatchTarget::EventsAfter(cursor) => {
                    let payload = self
                        .gateway
                        .read_watch_window(&watch.watch_id, cursor)
                        .await
                        .map_err(gateway_read_error)?;
                    json_resource(request.uri, &payload)
                }
            };
        }

        let uri = AgentResourceUri::from_str(&request.uri)
            .map_err(|error| McpError::resource_not_found(error.to_string(), None))?;
        self.gateway.authorize_agent(&owner, &uri.agent_id).await?;

        // Only a genuinely absent agent is "not found". Anything else went
        // wrong on our side, and a caller retrying a different URI would be
        // chasing the wrong problem.
        let snapshot = self
            .gateway
            .snapshot(&uri.agent_id)
            .await
            .map_err(|error| match error {
                GatewayError::AgentNotFound(_) => {
                    McpError::resource_not_found(error.to_string(), None)
                }
                other => McpError::internal_error(other.to_string(), None),
            })?;

        // A cursor page is a live journal read rather than a snapshot field, so
        // it is served straight from the gateway. This is the read half of the
        // subscribe-then-read loop, and it deliberately does not long poll: the
        // notification already said there is something to collect. The stream
        // id comes from the snapshot, so the page reports a genuine gap or
        // reset instead of silently restarting the caller.
        // Journal-backed windows are live reads rather than snapshot fields, so
        // each is served straight from the gateway. This is the read half of
        // the subscribe-then-read loop, and it deliberately does not long poll:
        // the notification already said there is something to collect.
        match &uri.kind {
            ResourceKind::EventsAfter(sequence) => {
                let page = self
                    .gateway
                    .read_events(
                        &uri.agent_id,
                        Some(EventCursor {
                            // The stream id comes from the snapshot, so the page
                            // reports a genuine gap or reset instead of silently
                            // restarting the caller.
                            stream_id: snapshot.journal.stream_id.clone(),
                            sequence: *sequence,
                        }),
                        self.gateway.config().limits.max_event_page_size,
                        Duration::ZERO,
                        EventFilter::default(),
                    )
                    .await
                    .map_err(gateway_read_error)?;
                json_resource(request.uri, &page)
            }
            ResourceKind::Inbox => {
                let payload = self
                    .gateway
                    .read_conversation(&uri.agent_id, ConversationWindow::Inbox)
                    .await
                    .map_err(gateway_read_error)?;
                json_resource(request.uri, &ResourcePayload::Inbox(payload))
            }
            ResourceKind::Transcript(target) => {
                let payload = self
                    .gateway
                    .read_conversation(&uri.agent_id, ConversationWindow::Target(target))
                    .await
                    .map_err(gateway_read_error)?;
                json_resource(request.uri, &ResourcePayload::Transcript(payload))
            }
            ResourceKind::Wire => {
                let payload = self
                    .gateway
                    .read_wire(&uri.agent_id)
                    .await
                    .map_err(gateway_read_error)?;
                json_resource(request.uri, &ResourcePayload::Wire(Box::new(payload)))
            }
            _ => {
                let payload = snapshot
                    .resource(&uri)
                    .map_err(|error| McpError::resource_not_found(error.to_string(), None))?;
                json_resource(request.uri, &payload)
            }
        }
    }

    /// Complete the channel-state template arguments from live gateway state.
    ///
    /// Both arguments are drawn from sets the gateway already knows exactly, so
    /// a caller should never have to guess an agent handle or work out the
    /// percent-encoding of a channel name by hand.
    async fn complete(
        &self,
        request: CompleteRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, McpError> {
        let owner = self.callers.identify(&context)?;
        let Reference::Resource(reference) = &request.r#ref else {
            return Ok(CompleteResult::default());
        };
        if reference.uri != CHANNEL_STATE_TEMPLATE {
            return Ok(CompleteResult::default());
        }

        let prefix = request.argument.value.as_str();
        let values = match request.argument.name.as_str() {
            "agent_id" => self
                .gateway
                .agent_ids_for(&owner)
                .await
                .into_iter()
                .map(|agent_id| agent_id.to_string())
                .filter(|candidate| candidate.starts_with(prefix))
                .collect(),
            "encoded_channel" => {
                // Prefer the agent already chosen for the sibling argument, so
                // the offered channels are ones that agent has actually joined.
                let chosen = request
                    .context
                    .as_ref()
                    .and_then(|context| context.arguments.as_ref())
                    .and_then(|arguments| arguments.get("agent_id"))
                    .cloned();
                let mut channels = BTreeSet::new();
                for agent_id in self.gateway.agent_ids_for(&owner).await {
                    if chosen
                        .as_ref()
                        .is_some_and(|wanted| *wanted != agent_id.to_string())
                    {
                        continue;
                    }
                    if let Ok(snapshot) = self.gateway.snapshot(&agent_id).await {
                        channels.extend(
                            snapshot
                                .state
                                .channels
                                .values()
                                .map(|channel| encode_channel_segment(&channel.name)),
                        );
                    }
                }
                channels
                    .into_iter()
                    .filter(|candidate| candidate.starts_with(prefix))
                    .collect()
            }
            _ => Vec::new(),
        };

        CompletionInfo::new(values)
            .map(CompleteResult::new)
            .map_err(|error| McpError::internal_error(error, None))
    }

    fn accepted_subscription_filter(
        &self,
        requested: &SubscriptionFilter,
    ) -> Option<SubscriptionFilter> {
        Some(requested.supported_by(&self.get_info().capabilities))
    }

    async fn listen(&self, context: SubscriptionContext) -> Result<(), McpError> {
        // A subscription outlives any one request, so the identity it was
        // opened under is the one its resynchronization is scoped to.
        let owner = self.callers.identify(context.request_context())?;
        self.authorize_resource_subscriptions(&owner, context.accepted())
            .await?;
        let mut updates = self.gateway.subscribe_resource_updates();
        loop {
            tokio::select! {
                () = context.cancelled() => return Ok(()),
                update = updates.recv() => match update {
                    Ok(uri) if uri == "irc://agents" => {
                        let _ = context.sink().notify_resource_list_changed().await;
                    }
                    Ok(uri) if self.owner_may_observe_resource(&owner, &uri).await => {
                        let _ = context.sink().notify_resource_updated(uri).await;
                    }
                    Ok(_) => {}
                    // Dropping notifications silently leaves a subscriber
                    // believing its last read is still current, which is the
                    // one failure a resource subscription must not have. Every
                    // resource that exists is republished instead. That is safe
                    // to do liberally because no resource read consumes
                    // anything: a subscriber re-reads what it holds and
                    // recovers its own position from the cursor or status in
                    // the payload.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                        tracing::warn!(
                            owner = %owner,
                            missed,
                            "resource notifications lagged; resynchronizing"
                        );
                        self.notify_resynchronization(&context, &owner).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
    }
}

impl IrcMcpService {
    /// Refuse subscriptions to another caller's agent or watch resource.
    ///
    /// The SDK acknowledges the requested filter before entering `listen`, so
    /// this is the earliest request-context-aware authorization point. An
    /// unauthorized filter is closed immediately and can never receive an
    /// update. Individual broadcasts are checked again below so a shared
    /// gateway cannot leak another owner's resource activity.
    async fn authorize_resource_subscriptions(
        &self,
        owner: &OwnerId,
        filter: &SubscriptionFilter,
    ) -> Result<(), McpError> {
        let Some(uris) = filter.resource_subscriptions.as_ref() else {
            return Ok(());
        };
        for uri in uris {
            if uri.starts_with(WATCH_URI_PREFIX) {
                let watch = WatchUri::from_str(uri)
                    .map_err(|error| McpError::resource_not_found(error.to_string(), None))?;
                self.gateway.authorize_watch(owner, &watch.watch_id).await?;
            } else {
                let resource = AgentResourceUri::from_str(uri)
                    .map_err(|error| McpError::resource_not_found(error.to_string(), None))?;
                self.gateway
                    .authorize_agent(owner, &resource.agent_id)
                    .await?;
            }
        }
        Ok(())
    }

    /// Whether one caller may receive an update for a stable resource URI.
    async fn owner_may_observe_resource(&self, owner: &OwnerId, uri: &str) -> bool {
        if uri.starts_with(WATCH_URI_PREFIX) {
            let Ok(watch) = WatchUri::from_str(uri) else {
                return false;
            };
            return self
                .gateway
                .authorize_watch(owner, &watch.watch_id)
                .await
                .is_ok();
        }
        let Ok(resource) = AgentResourceUri::from_str(uri) else {
            return false;
        };
        self.gateway
            .authorize_agent(owner, &resource.agent_id)
            .await
            .is_ok()
    }

    /// Refuse a tool call that names a handle the caller does not own.
    ///
    /// Both handle kinds are checked here rather than in each tool, so a tool
    /// added later cannot forget the check. An unowned handle is reported
    /// exactly as a missing one.
    async fn authorize_handles(
        &self,
        owner: &OwnerId,
        request: &CallToolRequestParams,
    ) -> Result<(), McpError> {
        let Some(arguments) = request.arguments.as_ref() else {
            return Ok(());
        };
        if let Some(agent_id) = named_agent(request) {
            self.gateway.authorize_agent(owner, &agent_id).await?;
        }
        if let Some(watch_id) = arguments
            .get("watch_id")
            .and_then(|value| value.as_str())
            .and_then(|value| WatchId::from_str(value).ok())
        {
            self.gateway.authorize_watch(owner, &watch_id).await?;
        }
        Ok(())
    }

    /// Connect one guest, narrating the wait to a caller that asked to see it.
    ///
    /// Registration is the longest blocking call this server has: transport
    /// setup, capability negotiation, optional authentication, nickname
    /// arbitration and a full MOTD, all inside one `await` bounded by
    /// `onboarding.connect_timeout_ms`. Without this the caller sees a single
    /// opaque pause and cannot distinguish a slow server from a hung one.
    ///
    /// The stages come from the actor, which is the only thing that observes
    /// them and has no MCP peer of its own, so they arrive over a bounded
    /// channel that is drained here alongside the connect itself. A caller that
    /// supplied no progress token creates no channel at all, and the actor then
    /// has nothing to publish to — the silent path costs one `Option` check.
    async fn connect_reporting_progress(
        &self,
        owner: OwnerId,
        request: ConnectRequest,
        context: &RequestContext<RoleServer>,
    ) -> Result<crate::gateway::ConnectedAgent, GatewayError> {
        let Some(mut reporter) = ProgressReporter::new(context, ConnectMilestone::TOTAL) else {
            return self.gateway.connect_as(owner, request, None).await;
        };
        // Sized for the whole sequence, so a stage is never dropped for want of
        // room while this loop is between iterations.
        let (milestones, mut reached) =
            tokio::sync::mpsc::channel(ConnectMilestone::TOTAL as usize);
        let connect = self.gateway.connect_as(owner, request, Some(milestones));
        tokio::pin!(connect);
        loop {
            tokio::select! {
                // Biased so a stage that arrived before the connect resolved is
                // reported rather than raced away by the result.
                biased;
                Some(milestone) = reached.recv() => {
                    reporter.report(milestone.step(), milestone.describe()).await;
                }
                // Progress stops here: the request is over, and on Streamable
                // HTTP its notification stream ends with this result.
                connected = &mut connect => return connected,
            }
        }
    }

    /// Settle any input round trip a task-augmented call still owes, before a
    /// task exists to hide it.
    ///
    /// Returns `None` when the call is ready to run — the ordinary case — and
    /// the response to send back otherwise. The tool re-resolves the same
    /// question inside the task, deterministically reaching the same answer from
    /// the same request fields; running it here first is what keeps a question
    /// on the request that can still carry one.
    async fn settle_input_before_task(
        &self,
        owner: &OwnerId,
        profile: &RequestProfile,
        request: &CallToolRequestParams,
    ) -> Result<Option<CallToolResponse>, McpError> {
        if request.name != DCC_ACCEPT_TOOL {
            return Ok(None);
        }
        let arguments = serde_json::Value::Object(request.arguments.clone().unwrap_or_default());
        let Ok(input) = serde_json::from_value::<DccAcceptInput>(arguments) else {
            // Malformed arguments are the router's error to report, in the one
            // place that phrases them; this decides only about input requests.
            return Ok(None);
        };
        Ok(
            match self
                .plan_dcc_accept(
                    owner,
                    profile,
                    &input,
                    request.request_state.as_deref(),
                    request.input_responses.as_ref(),
                )
                .await?
            {
                DccAcceptResolution::Ready(_) => None,
                DccAcceptResolution::NeedsInput(needed) => Some(needed.into()),
                DccAcceptResolution::Settled(result) => Some(result.into()),
            },
        )
    }

    /// Read a caller's answer to the nickname question, or ask it again.
    ///
    /// Every refusal is in band and leaves nothing connected: a state minted for
    /// another caller or another call, an expired one, a declined answer, and a
    /// name that is not a nickname all produce an ordinary tool result with
    /// `isError`, because the model that issued the call is the one that has to
    /// react.
    fn open_nickname_choice(
        &self,
        owner: &OwnerId,
        operation: &OriginatingOperation,
        sealed: &str,
        responses: Option<&rmcp::model::InputResponses>,
        input: &ConnectInput,
    ) -> Result<Resolution<Nickname>, McpError> {
        let pending: connect_nickname::PendingNickname =
            match self.gateway.request_states().open(sealed, owner, operation) {
                Ok(pending) => pending,
                Err(error) => {
                    return Ok(Resolution::Settled(refusal(
                        ErrorKind::Validation.as_str(),
                        error.message,
                        false,
                    )));
                }
            };
        Ok(match connect_nickname::read_answer(responses)? {
            // A round that answered nothing is asked again rather than refused,
            // which is what the specification requires of a partial response.
            connect_nickname::Answer::Missing => {
                Resolution::NeedsInput(self.ask_for_a_nickname(owner, operation, &pending, input)?)
            }
            connect_nickname::Answer::Declined => Resolution::Settled(declined(format!(
                "The nickname choice for {} was declined; no guest was connected.",
                input.nickname
            ))),
            connect_nickname::Answer::Chosen(chosen) => match Nickname::new(chosen) {
                Ok(nickname) => Resolution::Ready(nickname),
                Err(error) => Resolution::Settled(refusal(
                    ErrorKind::Validation.as_str(),
                    format!("the chosen nickname is not usable: {error}"),
                    false,
                )),
            },
        })
    }

    /// Ask which nickname to register, sealing what the question was about.
    ///
    /// Suggestions are built from the name the server refused last rather than
    /// from the one the call originally asked for, so a second round after a
    /// chosen name also collided proposes variations of *that* name instead of
    /// circling back to the first.
    fn ask_for_a_nickname(
        &self,
        owner: &OwnerId,
        operation: &OriginatingOperation,
        pending: &connect_nickname::PendingNickname,
        input: &ConnectInput,
    ) -> Result<InputRequiredResult, McpError> {
        let base = pending
            .attempted
            .last()
            .and_then(|refused| Nickname::new(refused.clone()).ok())
            .unwrap_or_else(|| input.nickname.clone());
        let suggestions = connect_nickname::suggestions(
            &base,
            &pending.attempted,
            self.gateway.config().onboarding.nickname_attempts,
        );
        let requests = connect_nickname::nickname_requests(pending, &suggestions)?;
        let sealed = self
            .gateway
            .request_states()
            .seal(owner, operation, pending)?;
        Ok(InputRequiredResult::new(Some(requests), Some(sealed)))
    }

    /// Read a caller's answer to the channel-key question, or ask it again.
    ///
    /// Refusals are in band and leave the channel unjoined, which is exactly
    /// what a join that was never re-issued means.
    fn open_channel_key(
        &self,
        owner: &OwnerId,
        operation: &OriginatingOperation,
        sealed: &str,
        responses: Option<&rmcp::model::InputResponses>,
        input: &JoinInput,
    ) -> Result<Resolution<String>, McpError> {
        let channel = input.channel.to_string();
        let pending: join_key::PendingJoin =
            match self.gateway.request_states().open(sealed, owner, operation) {
                Ok(pending) => pending,
                Err(error) => {
                    return Ok(Resolution::Settled(refusal(
                        ErrorKind::Validation.as_str(),
                        error.message,
                        false,
                    )));
                }
            };
        if !pending.matches(&channel) {
            return Ok(Resolution::Settled(refusal(
                ErrorKind::Validation.as_str(),
                "this channel key was supplied for a different channel",
                false,
            )));
        }
        Ok(match join_key::read_answer(responses)? {
            // A round that answered nothing is asked again rather than refused,
            // which is what the specification requires of a partial response.
            join_key::Answer::Missing => {
                Resolution::NeedsInput(self.ask_for_a_key(owner, operation, &channel, None)?)
            }
            join_key::Answer::Declined => Resolution::Settled(declined(format!(
                "The key for {channel} was declined; the channel was not joined."
            ))),
            join_key::Answer::Chosen(key) => Resolution::Ready(key),
        })
    }

    /// Ask for one channel's key after the server refused the join without it.
    fn ask_for_a_channel_key(
        &self,
        owner: &OwnerId,
        operation: &OriginatingOperation,
        input: &JoinInput,
        result: &CommandResult,
    ) -> Result<InputRequiredResult, McpError> {
        self.ask_for_a_key(
            owner,
            operation,
            &input.channel.to_string(),
            join_key::rejection_detail(result).as_deref(),
        )
    }

    /// Ask for one channel's key, sealing which channel it is for.
    fn ask_for_a_key(
        &self,
        owner: &OwnerId,
        operation: &OriginatingOperation,
        channel: &str,
        detail: Option<&str>,
    ) -> Result<InputRequiredResult, McpError> {
        let requests = join_key::key_requests(channel, detail)?;
        let sealed = self.gateway.request_states().seal(
            owner,
            operation,
            &join_key::PendingJoin {
                channel: channel.to_owned(),
            },
        )?;
        Ok(InputRequiredResult::new(Some(requests), Some(sealed)))
    }

    /// Require a human confirmation for one destructive mutation, when the
    /// deployment asked for one.
    ///
    /// Called before anything is written, so every outcome but `Ready` leaves
    /// the channel exactly as it was. A request that cannot be asked is refused
    /// rather than served: the setting exists because somebody decided a model
    /// may not do this alone, and proceeding silently would delete that policy
    /// while appearing to honor it.
    fn confirm_destructive(
        &self,
        owner: &OwnerId,
        profile: &RequestProfile,
        operation: &OriginatingOperation,
        action: &str,
        request_state: Option<&str>,
        responses: Option<&rmcp::model::InputResponses>,
    ) -> Result<Resolution<()>, McpError> {
        if !self.gateway.config().mcp.confirm_destructive {
            return Ok(Resolution::Ready(()));
        }
        let Some(sealed) = request_state else {
            if !profile.supports_form_elicitation() {
                return Ok(Resolution::Settled(refusal(
                    CONFIRMATION_REQUIRED,
                    format!(
                        "this gateway requires a confirmed decision before it will {action}, and \
                         this request declared no form elicitation to ask through; nothing was \
                         applied"
                    ),
                    false,
                )));
            }
            return Ok(Resolution::NeedsInput(
                self.ask_for_confirmation(owner, operation, action)?,
            ));
        };
        let pending: confirm_action::PendingConfirmation =
            match self.gateway.request_states().open(sealed, owner, operation) {
                Ok(pending) => pending,
                Err(error) => {
                    return Ok(Resolution::Settled(refusal(
                        CONFIRMATION_REQUIRED,
                        error.message,
                        false,
                    )));
                }
            };
        if !pending.matches(action) {
            return Ok(Resolution::Settled(refusal(
                CONFIRMATION_REQUIRED,
                "this confirmation was given for a different action",
                false,
            )));
        }
        Ok(match confirm_action::read_answer(responses)? {
            confirm_action::Answer::Confirmed => Resolution::Ready(()),
            confirm_action::Answer::Refused => Resolution::Settled(declined(format!(
                "The request to {action} was not confirmed; nothing was applied."
            ))),
            confirm_action::Answer::Missing => {
                Resolution::NeedsInput(self.ask_for_confirmation(owner, operation, action)?)
            }
        })
    }

    /// Ask a caller to confirm one exact action, sealing the action with it.
    fn ask_for_confirmation(
        &self,
        owner: &OwnerId,
        operation: &OriginatingOperation,
        action: &str,
    ) -> Result<InputRequiredResult, McpError> {
        let requests = confirm_action::confirmation_requests(action)?;
        let sealed = self.gateway.request_states().seal(
            owner,
            operation,
            &confirm_action::PendingConfirmation::for_action(action),
        )?;
        Ok(InputRequiredResult::new(Some(requests), Some(sealed)))
    }

    /// Decide what an `irc.dcc.accept` call still needs, or produce the
    /// validated plan it will run.
    ///
    /// Separated from the tool body on purpose. A task-augmented acceptance must
    /// settle its input round trips *before* a task exists — the tasks extension
    /// recommends resolving pre-creation MRTR synchronously, and this choice is
    /// part of deciding what transfer the task will run — so the task path calls
    /// this and only spawns once it holds an
    /// [`AcceptPlan`](dcc_accept::AcceptPlan).
    ///
    /// Every refusal here is in-band: a bad request state, a state minted for
    /// another offer, and a caller that declined all produce an ordinary tool
    /// result with `isError`, because the model that issued the call is the one
    /// that has to react, and none of them leaves the offer consumed. The
    /// offer's own TTL remains the only thing that retires it.
    pub(crate) async fn plan_dcc_accept(
        &self,
        owner: &OwnerId,
        profile: &RequestProfile,
        input: &DccAcceptInput,
        request_state: Option<&str>,
        responses: Option<&rmcp::model::InputResponses>,
    ) -> Result<DccAcceptResolution, McpError> {
        let offer = match self
            .gateway
            .dcc_session(&input.agent_id, &input.dcc_session_id)
            .await
        {
            Ok(offer) => offer,
            Err(error) => return Ok(DccAcceptResolution::Settled(tool_error(error))),
        };
        // A session that is not a pending incoming offer cannot be accepted at
        // all. Asking where its file should go would be asking about work that
        // is refused a moment later, so this hands the call straight through and
        // lets the session lifecycle give the one authoritative answer.
        if offer.direction != DccDirection::Inbound || offer.state != DccState::Offered {
            return Ok(DccAcceptResolution::Ready(dcc_accept::AcceptPlan {
                destination: None,
                conflict: input.conflict,
            }));
        }
        // Bound to the arguments as they arrived, so a retry that changed any of
        // them is a different operation and cannot redeem this exchange's state.
        let operation = OriginatingOperation::for_tool("irc.dcc.accept", &input.salient());
        let pending = match request_state {
            Some(sealed) => {
                match self
                    .gateway
                    .request_states()
                    .open::<dcc_accept::PendingAccept>(sealed, owner, &operation)
                {
                    Ok(pending) if pending.matches(&offer) => Some(pending),
                    Ok(_) => {
                        return Ok(DccAcceptResolution::Settled(destination_refusal(
                            "this destination choice was made for a different DCC offer",
                        )));
                    }
                    Err(error) => {
                        return Ok(DccAcceptResolution::Settled(destination_refusal(
                            error.message,
                        )));
                    }
                }
            }
            None => None,
        };

        let answer = dcc_accept::read_answer(responses)?;
        if answer == dcc_accept::Answer::Declined {
            return Ok(DccAcceptResolution::Settled(declined_destination(&offer)));
        }
        let chosen = match &answer {
            dcc_accept::Answer::Chosen(choice) => Some(choice),
            _ => None,
        };
        let offered_roots = pending.as_ref().map(|pending| pending.roots.as_slice());

        match dcc_accept::decide(
            &self.gateway.config().dcc,
            &offer,
            input,
            chosen,
            offered_roots,
        ) {
            dcc_accept::Decision::Ready(plan) => Ok(DccAcceptResolution::Ready(plan)),
            dcc_accept::Decision::Refuse(message) => {
                Ok(DccAcceptResolution::Settled(destination_refusal(message)))
            }
            dcc_accept::Decision::Choose {
                roots,
                default_path,
            } => {
                if !profile.supports_form_elicitation() {
                    // Nothing to ask with. The refusal carries the whole choice
                    // so the caller's next attempt can be the explicit one.
                    return Ok(DccAcceptResolution::Settled(unchosen_destination(
                        roots,
                        default_path,
                    )));
                }
                let requests =
                    dcc_accept::destination_requests(&offer, &roots, default_path.as_ref())?;
                let sealed = self.gateway.request_states().seal(
                    owner,
                    &operation,
                    &dcc_accept::PendingAccept::for_offer(&offer, roots),
                )?;
                Ok(DccAcceptResolution::NeedsInput(InputRequiredResult::new(
                    Some(requests),
                    Some(sealed),
                )))
            }
        }
    }

    /// Run one DCC tool and then follow its session to a terminal state.
    ///
    /// The tool call itself only writes the offer, so returning there would
    /// report success for work that has not happened yet. Instead the task
    /// stays alive for the transfer: it republishes byte progress as the task
    /// status, cancels the session cooperatively when the client asks, and
    /// settles with the terminal session and a link to it.
    async fn run_dcc_task(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
        task: TaskContext,
    ) -> Result<CallToolResult, TaskExit> {
        let agent_id = task_agent_id(&request)?;
        let call = ToolCallContext::new(self, request, context);
        let mut started = match self.tool_router.call(call).await? {
            CallToolResponse::Complete(result) => result,
            // A task must not carry an unanswered question, and `call_tool`
            // settles every one it can see before creating this task. Reaching
            // here means the answer stopped being valid in between — a request
            // state that expired between the two resolutions is the only way —
            // and by now the handle is in the client's hands with its stream
            // closed, so there is nowhere left to ask. Failing deterministically
            // with the reason is the honest answer; the recovery is to call the
            // tool again and answer the question it returns.
            CallToolResponse::InputRequired(_) => {
                return Err(TaskExit::Error(McpError::invalid_request(
                    "this operation needs its input resolved before it can run as a task: answer \
                     the input request the tool returns, then call it again",
                    None,
                )));
            }
            // Only this wrapper creates tasks, so a tool cannot have made one,
            // and the enum is `non_exhaustive`: a future response kind this
            // server has not been taught to follow is an internal fault rather
            // than something to guess at.
            other => {
                return Err(TaskExit::Error(McpError::internal_error(
                    format!("a task-augmented tool returned an unexpected response: {other:?}"),
                    None,
                )));
            }
        };
        // A rejected offer, or an accepted chat, is a finished task rather than
        // a transfer to follow. A task's terminal payload is a complete tool
        // result, so it is enveloped and hinted exactly like one returned
        // directly — a caller that followed a transfer for a minute is the
        // caller most likely to have missed something meanwhile.
        adopt_unstructured(&mut started);
        let Some(session_id) = followable_transfer(&started) else {
            self.hint_at_activity(Some(&agent_id), &mut started).await;
            return Ok(started);
        };

        // Stop following before the SDK's expiry sweep would abort this
        // operation and settle the task as `failed: task expired`. A transfer
        // still running at its deadline has not failed — it continues in the
        // gateway whatever this task does — so the honest terminal result is
        // the session as it stands plus where to keep watching it.
        let deadline = tokio::time::Instant::now() + self.gateway.tasks().follow_window();
        loop {
            if task.is_cancel_requested() {
                // Best effort: the peer may already have finished, in which
                // case the terminal state below is the honest answer.
                let _ = self.gateway.dcc_cancel(&agent_id, session_id.clone()).await;
                return Err(TaskExit::Cancelled);
            }
            let session = self
                .gateway
                .dcc_session(&agent_id, &session_id)
                .await
                .map_err(|error| TaskExit::Error(gateway_read_error(error)))?;
            let expired = tokio::time::Instant::now() >= deadline;
            if session.state.is_terminal() || expired {
                let link = describe_resource(
                    &AgentResourceUri {
                        agent_id: agent_id.clone(),
                        kind: ResourceKind::DccSession(session_id),
                    },
                    Some(session.updated_at),
                )
                .into_resource();
                let summary = if expired && !session.state.is_terminal() {
                    unfinished_session_summary(&session)
                } else {
                    terminal_session_summary(&session)
                };
                let mut result = tool_success_with_content(
                    summary,
                    &session,
                    vec![ContentBlock::ResourceLink(link)],
                );
                self.hint_at_activity(Some(&agent_id), &mut result).await;
                return Ok(result);
            }
            task.set_status_message(progress_summary(&session));
            tokio::select! {
                () = task.cancelled() => continue,
                () = tokio::time::sleep_until(deadline) => {}
                () = tokio::time::sleep(TASK_POLL_INTERVAL) => {}
            }
        }
    }

    /// Attach the caller's bounded activity hint to one successful result.
    ///
    /// Every tool but `irc.connect` names an agent, and `call_tool` has already
    /// refused any handle this caller does not own, so naming an agent here is
    /// the whole entitlement check: owner isolation is inherited rather than
    /// re-derived. `irc.connect` carries no hint because the anchor is born
    /// during that very call, `irc.watch.close` carries none because it names a
    /// watch rather than an agent, and `irc.attention.check` is kept compact
    /// because its own cursor-bearing result already says strictly more.
    ///
    /// Failures carry none either. A failure branch has no room for one in the
    /// declared schema, and burying news of a mention inside a report that
    /// something went wrong is not where a reader will look for it.
    async fn hint_at_activity(&self, agent_id: Option<&AgentId>, result: &mut CallToolResult) {
        if result.is_error == Some(true) {
            return;
        }
        let Some(agent_id) = agent_id else {
            return;
        };
        let Some(structured) = result.structured_content.as_mut() else {
            return;
        };
        if let Some(hint) = self.gateway.activity_hint(agent_id).await {
            envelope::attach_activity(structured, &hint);
        }
    }

    /// Republish every live resource after notifications were dropped.
    ///
    /// The list-changed notification comes first: a client that lost updates
    /// may also have missed an agent appearing or going away, so the catalog
    /// itself is resynchronized before the individual URIs within it.
    ///
    /// Republishing a watch URI costs the subscriber nothing but a re-read: the
    /// watch descriptor is immutable and every event window is positioned by the
    /// caller, so this can never consume a backlog on somebody's behalf.
    async fn notify_resynchronization(&self, context: &SubscriptionContext, owner: &OwnerId) {
        let _ = context.sink().notify_resource_list_changed().await;
        let owned = self.gateway.agent_ids_for(owner).await;
        let mut republished = 0_usize;
        for agent_id in &owned {
            let Ok(snapshot) = self.gateway.snapshot(agent_id).await else {
                continue;
            };
            for descriptor in descriptors_for_agent(agent_id, &snapshot.state, None) {
                let _ = context.sink().notify_resource_updated(descriptor.uri).await;
                republished += 1;
            }
        }
        for watch in self.gateway.watches().list() {
            if owned.contains(&watch.agent_id) {
                let _ = context.sink().notify_resource_updated(watch.uri).await;
                republished += 1;
            }
        }
        tracing::debug!(
            owner = %owner,
            agents = owned.len(),
            republished,
            "republished every owned resource after a subscription lag"
        );
    }
}

/// What one round of an input-gated tool call resolved to.
///
/// The three outcomes are the whole vocabulary of an MRTR exchange, and they are
/// the same for every question this server asks: run it, ask, or stop. Sharing
/// one type keeps the tools that use it from inventing different answers to the
/// same situation.
#[derive(Debug)]
pub(crate) enum Resolution<T> {
    /// Every argument is settled; run the operation with this.
    Ready(T),
    /// Ask the caller, then let it retry the same call.
    NeedsInput(InputRequiredResult),
    /// Nothing further will happen; report this. Nothing was applied.
    Settled(CallToolResult),
}

/// What one `irc.dcc.accept` call resolved to before anything was accepted.
pub(crate) type DccAcceptResolution = Resolution<dcc_accept::AcceptPlan>;

/// Error kind of a mutation a deployment requires a person to approve.
const CONFIRMATION_REQUIRED: &str = "confirmation_required";

/// Refuse a call in band, leaving whatever it would have changed untouched.
///
/// Never a JSON-RPC error: the call was well formed and reached the tool, so the
/// answer belongs in a result the model can read and act on.
fn refusal(kind: &str, message: impl Into<String>, retriable: bool) -> CallToolResult {
    failure_result(ToolFailure::refusal(kind, message, retriable))
}

/// Report that the caller was asked and refused.
///
/// A refusal is not a failure and not a success either: nothing happened, and
/// the operation is still there to start again. Saying so as an error result is
/// what stops a model from reading silence as completion. Retriable, because
/// calling again and answering is the recovery.
fn declined(message: impl Into<String>) -> CallToolResult {
    refusal("declined", message, true)
}

/// Refuse an acceptance whose destination cannot be settled.
fn destination_refusal(message: impl Into<String>) -> CallToolResult {
    refusal(
        GatewayError::Dcc(String::new()).kind().as_str(),
        message,
        false,
    )
}

/// Report that the caller was asked where the file should go and refused.
///
/// The offer is still there to accept until its own TTL retires it.
fn declined_destination(offer: &DccSession) -> CallToolResult {
    declined(format!(
        "The destination choice for {} from {} was declined; the offer is still pending.",
        offer.filename.as_deref().unwrap_or("this transfer"),
        offer.peer
    ))
}

/// Refuse an acceptance whose root the caller must name explicitly.
///
/// Reached when several roots are configured, the call named none, and the
/// request declared no way to answer a form. Listing the roots and the default
/// destination makes the retry a single obvious call rather than a guess.
fn unchosen_destination(roots: Vec<String>, default_path: Option<PathBuf>) -> CallToolResult {
    let message = format!(
        "This gateway has more than one DCC receive root; name one in `root`. Configured roots \
         are {}.",
        roots.join(", ")
    );
    failure_result(ToolFailure::unchosen_destination(
        message,
        roots,
        default_path,
    ))
}

/// Whether this call is answered with a task handle instead of a result.
///
/// Tasks in 2026-07-28 are **server-directed**: the server decides, per request,
/// whether a call becomes a task, and the client's only say is whether it
/// declared the extension it would need to follow one. There is no per-call
/// opt-in key and no client-supplied retention window — both belonged to the
/// superseded design, where the client asked for a task by naming the extension
/// in the call's own metadata.
///
/// So the decision is exactly two facts: this is one of the operations that
/// outlives its request, and this request declared the tasks extension in its
/// `_meta` client capabilities. Returning a task to a client that did not
/// declare it is forbidden — the SDK rejects the attempt — and would in any case
/// hand back a handle the client has no method to resolve.
///
/// Written as a free function so both facts can be tested without a live
/// request; the capability half is read through [`RequestProfile`], never from
/// `params.meta`, which is always `None` server-side.
fn runs_as_task(tool: &str, profile: &RequestProfile) -> bool {
    TASK_AUGMENTED_TOOLS.contains(&tool) && profile.declares_extension(TASKS_EXTENSION_ID)
}

/// The agent handle one tool call names, when it names a well-formed one.
///
/// Every tool but `irc.connect` takes an `agent_id`, so this is both the
/// ownership gate's subject and the subject of any activity hint the result
/// carries — the same value read the same way, so the two can never disagree
/// about which agent a call was about.
fn named_agent(request: &CallToolRequestParams) -> Option<AgentId> {
    request
        .arguments
        .as_ref()?
        .get("agent_id")?
        .as_str()
        .and_then(|value| AgentId::from_str(value).ok())
}

/// The agent handle a task-augmented DCC call names.
fn task_agent_id(request: &CallToolRequestParams) -> Result<AgentId, TaskExit> {
    named_agent(request).ok_or_else(|| {
        TaskExit::Error(McpError::invalid_params(
            "a task-augmented DCC call must name a valid agent_id",
            None,
        ))
    })
}

/// The transfer a DCC tool result reports having started, when it started one.
///
/// Only a SEND is followable. `irc.dcc.accept` also accepts CHAT offers, and an
/// accepted chat sits in `active` for as long as the conversation lasts —
/// following it would hold the task open for the whole exchange, withhold the
/// session details the caller needs to say anything, and then report a healthy
/// chat as expired. A chat accept is complete when it is accepted; its activity
/// is `dcc.chat.message` events, not bytes to count.
fn followable_transfer(result: &CallToolResult) -> Option<DccSessionId> {
    let session = result
        .structured_content
        .as_ref()?
        .get("result")?
        .get("session")?;
    if session.get("kind")?.as_str()? != "send" {
        return None;
    }
    session
        .get("id")?
        .as_str()
        .and_then(|value| DccSessionId::from_str(value).ok())
}

/// Human-readable progress for a running direct session.
fn progress_summary(session: &DccSession) -> String {
    match session.total_bytes {
        Some(total) if total > 0 => format!(
            "{:?}: {} of {total} bytes with {}.",
            session.state, session.transferred_bytes, session.peer
        ),
        _ => format!(
            "{:?}: {} bytes with {}.",
            session.state, session.transferred_bytes, session.peer
        ),
    }
}

/// Human-readable outcome for a session still running when its task must end.
///
/// This is a completed task, not a failed one: the task was only ever following
/// the transfer, and the transfer is unaffected by the follower stopping.
fn unfinished_session_summary(session: &DccSession) -> String {
    format!(
        "Direct session with {} is still {:?} after {} bytes; this task stopped following it at \
         its deadline. The session resource remains authoritative.",
        session.peer, session.state, session.transferred_bytes
    )
}

/// Human-readable outcome for a settled direct session.
fn terminal_session_summary(session: &DccSession) -> String {
    match &session.error {
        Some(error) => format!("Direct session with {} failed: {error}", session.peer),
        None => format!(
            "Direct session with {} finished as {:?} after {} bytes.",
            session.peer, session.state, session.transferred_bytes
        ),
    }
}

/// Serialize one payload as the JSON contents of a resource read.
fn json_resource<T: Serialize>(uri: String, payload: &T) -> Result<ReadResourceResponse, McpError> {
    let text = serde_json::to_string_pretty(payload)
        .map_err(|error| McpError::internal_error(error.to_string(), None))?;
    Ok(ReadResourceResult::new(vec![
        ResourceContents::text(text, uri).with_mime_type("application/json"),
    ])
    .into())
}

/// Classify a gateway failure encountered while reading a resource.
///
/// Only a genuinely absent handle is "not found". Anything else went wrong on
/// our side, and a caller retrying a different URI would be chasing the wrong
/// problem.
fn gateway_read_error(error: GatewayError) -> McpError {
    match error {
        GatewayError::AgentNotFound(_) | GatewayError::WatchNotFound(_) => {
            McpError::resource_not_found(error.to_string(), None)
        }
        other => McpError::internal_error(other.to_string(), None),
    }
}

/// Catalog entry for one watch handle.
fn watch_resource_entry(watch: &WatchDescriptor) -> Resource {
    Resource::new(watch.uri.clone(), format!("watch-{}", watch.watch_id))
        .with_title(format!("Watch on {}", watch.agent_id))
        .with_description(
            "This watch's selection, the health of the stream it selects from, and where to read \
             what it selected. Subscribe here; reading it consumes nothing, so the events come \
             from irc.events.read with this watch_id and your own cursor, or from the positioned \
             window URI this resource names.",
        )
        .with_mime_type("application/json")
        .with_annotations(
            Annotations::default()
                .with_audience(vec![Role::Assistant, Role::User])
                .with_priority(1.0),
        )
}

/// The registration one `irc.connect` round will attempt.
///
/// A `chosen` nickname replaces the whole candidate list rather than joining it:
/// it is the answer to a question about names the server already refused, so
/// re-offering those names would spend attempts on known collisions and could
/// register one of them after all. The policy is carried through unchanged, so a
/// chosen name that collides in turn asks again.
fn connect_request(input: &ConnectInput, chosen: Option<Nickname>) -> ConnectRequest {
    let (nickname, nickname_fallbacks) = match chosen {
        Some(nickname) => (nickname, Vec::new()),
        None => (input.nickname.clone(), input.nickname_fallbacks.clone()),
    };
    ConnectRequest {
        nickname,
        nickname_fallbacks,
        nick_conflict_policy: input.nick_conflict_policy,
        username: input.username.clone(),
        real_name: input.real_name.clone(),
        channels: input.channels.iter().cloned().collect::<BTreeSet<_>>(),
        activity: input.activity,
    }
}

fn query_message(query: Query) -> Result<OutboundMessage, GatewayError> {
    let (command, params, trailing) = match query {
        Query::Whois { nickname } => ("WHOIS", vec![nickname], None),
        Query::Whowas { nickname } => ("WHOWAS", vec![nickname], None),
        Query::Who { mask, fields } => {
            let mut params = vec![mask];
            if let Some(fields) = fields {
                params.push(format!("%{fields}"));
            }
            ("WHO", params, None)
        }
        Query::Names { channels } => ("NAMES", vec![channels.join(",")], None),
        Query::List { mask } => ("LIST", mask.into_iter().collect(), None),
        Query::Topic { channel } => ("TOPIC", vec![channel.to_string()], None),
        Query::Mode { target, mode } => {
            let mut params = vec![target];
            params.extend(mode);
            ("MODE", params, None)
        }
        Query::Ison { nicknames } => ("ISON", nicknames, None),
        Query::Userhost { nicknames } => ("USERHOST", nicknames, None),
        Query::Monitor {
            operation: MonitorQuery::List,
        } => ("MONITOR", vec!["L".into()], None),
        Query::Monitor {
            operation: MonitorQuery::Status { nicknames },
        } => ("MONITOR", vec!["S".into(), nicknames.join(",")], None),
        Query::Motd => ("MOTD", Vec::new(), None),
        Query::Version => ("VERSION", Vec::new(), None),
        Query::Time => ("TIME", Vec::new(), None),
        Query::Admin => ("ADMIN", Vec::new(), None),
        Query::Info => ("INFO", Vec::new(), None),
        Query::Lusers => ("LUSERS", Vec::new(), None),
        Query::Stats { selector } => ("STATS", selector.into_iter().collect(), None),
        Query::Links { mask } => ("LINKS", mask.into_iter().collect(), None),
        Query::Help { subject } => ("HELP", subject.into_iter().collect(), None),
    };
    let mut message = OutboundMessage::new(command, params);
    message.trailing = trailing;
    message
        .encode(crate::irc::wire::LineBudget::with_body(usize::MAX))
        .map_err(|error| GatewayError::InvalidMessage(error.to_string()))?;
    Ok(message)
}

/// Classify one correlated exchange, keeping it whole only when it failed.
fn command_failure(result: &CommandResult) -> Option<CommandFailure> {
    is_failure_outcome(result.outcome).then(|| CommandFailure {
        outcome: result.outcome,
        result: result.clone(),
    })
}

fn whois_profile(result: &CommandResult) -> WhoisProfile {
    let mut profile = WhoisProfile::default();
    for reply in &result.replies {
        match reply.command.parse::<u16>().ok() {
            Some(301) => {
                profile.nickname = reply.params.get(1).cloned().or(profile.nickname);
                profile.away_message = reply.trailing.clone();
            }
            Some(311) => {
                profile.nickname = reply.params.get(1).cloned();
                profile.username = reply.params.get(2).cloned();
                profile.hostname = reply.params.get(3).cloned();
                profile.real_name = reply.trailing.clone();
            }
            Some(312) => {
                profile.nickname = reply.params.get(1).cloned().or(profile.nickname);
                profile.server = reply.params.get(2).cloned();
            }
            Some(313) => profile.operator = true,
            Some(317) => {
                profile.idle_seconds = reply.params.get(2).and_then(|value| value.parse().ok());
                profile.signon_timestamp = reply.params.get(3).and_then(|value| value.parse().ok());
            }
            Some(319) => {
                profile.channels.extend(
                    reply
                        .trailing
                        .as_deref()
                        .unwrap_or_default()
                        .split_whitespace()
                        .map(str::to_owned),
                );
            }
            Some(330) => profile.account = reply.params.get(2).cloned(),
            Some(671) => profile.secure = true,
            _ => {}
        }
    }
    profile
}

fn names_channels(result: &CommandResult) -> Vec<NamesChannel> {
    let mut channels: BTreeMap<String, NamesChannel> = BTreeMap::new();
    for reply in &result.replies {
        if reply.command != "353" {
            continue;
        }
        let Some(channel) = reply.params.get(2) else {
            continue;
        };
        let entry = channels
            .entry(channel.clone())
            .or_insert_with(|| NamesChannel {
                channel: channel.clone(),
                visibility: reply.params.get(1).cloned().unwrap_or_default(),
                names: Vec::new(),
            });
        entry.names.extend(
            reply
                .trailing
                .as_deref()
                .unwrap_or_default()
                .split_whitespace()
                .map(str::to_owned),
        );
    }
    channels.into_values().collect()
}

fn list_channels(result: &CommandResult) -> Vec<ChannelListEntry> {
    result
        .replies
        .iter()
        .filter(|reply| reply.command == "322")
        .filter_map(|reply| {
            Some(ChannelListEntry {
                channel: reply.params.get(1)?.clone(),
                visible_users: reply.params.get(2).and_then(|value| value.parse().ok()),
                topic: reply.trailing.clone(),
            })
        })
        .collect()
}

fn mode_replies(result: &CommandResult) -> Vec<ModeReply> {
    result
        .replies
        .iter()
        .filter(|reply| {
            matches!(
                reply.command.as_str(),
                "221" | "324" | "367" | "368" | "348" | "349" | "346" | "347"
            )
        })
        .map(|reply| ModeReply {
            command: reply.command.clone(),
            parameters: reply.params.iter().skip(1).cloned().collect(),
            text: reply.trailing.clone(),
        })
        .collect()
}

fn help_lines(result: &CommandResult) -> Vec<HelpLine> {
    result
        .replies
        .iter()
        .filter(|reply| matches!(reply.command.as_str(), "704" | "705" | "706" | "FAIL"))
        .map(|reply| HelpLine {
            command: reply.command.clone(),
            subject: reply.params.get(1).cloned(),
            text: reply.trailing.clone(),
        })
        .collect()
}

fn topic_reply(result: &CommandResult) -> (Option<String>, Option<String>, Option<u64>) {
    let mut topic = None;
    let mut set_by = None;
    let mut set_at = None;
    for reply in &result.replies {
        match reply.command.as_str() {
            "331" => topic = None,
            "332" | "TOPIC" => topic = reply.trailing.clone(),
            "333" => {
                set_by = reply.params.get(2).cloned();
                set_at = reply.params.get(3).and_then(|value| value.parse().ok());
            }
            _ => {}
        }
    }
    (topic, set_by, set_at)
}

/// Split one logical message into lines that survive relay intact.
///
/// A client writes prefix-less lines, but the body budget applies to the form
/// the server relays, which carries `:nick!user@host ` ahead of the command.
/// `relay_prefix_bytes` reserves that room, because a server that overruns the
/// budget truncates the tail rather than rejecting the line, and the sender
/// sees only a successful write.
fn build_send_messages(
    input: &SendInput,
    max_body_bytes: usize,
    relay_prefix_bytes: usize,
    max_message_bytes: usize,
    max_message_parts: usize,
) -> Result<Vec<OutboundMessage>, GatewayError> {
    let command = match input.kind {
        SendKind::Privmsg | SendKind::Action => "PRIVMSG",
        SendKind::Notice => "NOTICE",
        SendKind::Tagmsg => "TAGMSG",
    };
    if matches!(input.kind, SendKind::Tagmsg) {
        if input.text.as_ref().is_some_and(|text| !text.is_empty()) {
            return Err(GatewayError::InvalidMessage(
                "TAGMSG does not carry text".into(),
            ));
        }
        let message = OutboundMessage {
            tags: message_tags(input),
            command: command.into(),
            params: vec![input.target.to_string()],
            trailing: None,
        };
        message
            .encode(crate::irc::wire::LineBudget::with_body(max_body_bytes))
            .map_err(|error| GatewayError::InvalidMessage(error.to_string()))?;
        return Ok(vec![message]);
    }
    let text = input.text.as_deref().ok_or_else(|| {
        GatewayError::InvalidMessage("text is required for this send kind".into())
    })?;
    if text.len() > max_message_bytes {
        return Err(GatewayError::ResourceLimit(format!(
            "message is {} bytes; the configured limit is {max_message_bytes}",
            text.len()
        )));
    }
    let action_overhead = usize::from(matches!(input.kind, SendKind::Action)) * 9;
    let template = OutboundMessage {
        tags: message_tags(input),
        command: command.into(),
        params: vec![input.target.to_string()],
        trailing: Some(String::new()),
    };
    let available = max_body_bytes
        .checked_sub(
            template
                .body_overhead()
                .saturating_add(action_overhead)
                .saturating_add(relay_prefix_bytes),
        )
        .ok_or_else(|| {
            GatewayError::InvalidMessage("target leaves no room for message text".into())
        })?;
    let chunks = if text.len() <= available {
        vec![text.to_owned()]
    } else {
        match input.multiline {
            MultilinePolicy::Prefer | MultilinePolicy::Split => split_utf8(text, available)?,
            MultilinePolicy::Require => {
                return Err(GatewayError::InvalidMessage(
                    "message is overlong and IRCv3 multiline is not negotiated".into(),
                ));
            }
            MultilinePolicy::RejectIfTooLong => {
                return Err(GatewayError::InvalidMessage(format!(
                    "message is {} bytes but only {available} fit",
                    text.len()
                )));
            }
        }
    };
    if chunks.len() > max_message_parts {
        return Err(GatewayError::ResourceLimit(format!(
            "message needs {} IRC lines; the configured limit is {max_message_parts}",
            chunks.len()
        )));
    }
    chunks
        .into_iter()
        .map(|chunk| {
            let trailing = if matches!(input.kind, SendKind::Action) {
                format!("\u{1}ACTION {chunk}\u{1}")
            } else {
                chunk
            };
            let mut message = template.clone();
            message.trailing = Some(trailing);
            message
                .encode(crate::irc::wire::LineBudget::with_body(max_body_bytes))
                .map_err(|error| GatewayError::InvalidMessage(error.to_string()))?;
            Ok(message)
        })
        .collect()
}

fn message_tags(input: &SendInput) -> Vec<Tag> {
    let mut tags = input.tags.clone();
    if let Some(reply_to) = &input.reply_to {
        tags.push(Tag::new("+reply", Some(reply_to.clone())));
    }
    tags
}

fn split_utf8(text: &str, max_bytes: usize) -> Result<Vec<String>, GatewayError> {
    if max_bytes == 0 {
        return Err(GatewayError::InvalidMessage(
            "active line budget leaves no room for text".into(),
        ));
    }
    let mut remaining = text;
    let mut chunks = Vec::new();
    while !remaining.is_empty() {
        let mut end = remaining.len().min(max_bytes);
        while end > 0 && !remaining.is_char_boundary(end) {
            end -= 1;
        }
        if end == 0 {
            return Err(GatewayError::InvalidMessage(
                "one UTF-8 code point exceeds the active line budget".into(),
            ));
        }
        chunks.push(remaining[..end].to_owned());
        remaining = &remaining[end..];
    }
    Ok(chunks)
}

fn history_message(
    target: &str,
    selector: &HistorySelector,
    limit: usize,
) -> Result<OutboundMessage, GatewayError> {
    validate_irc_atom(target, "history target")?;
    if limit == 0 {
        return Err(GatewayError::InvalidMessage(
            "history limit must be greater than zero".into(),
        ));
    }
    let mut params = match selector {
        HistorySelector::Latest => vec!["LATEST".into(), target.into(), "*".into()],
        HistorySelector::Before { anchor } => {
            vec!["BEFORE".into(), target.into(), history_anchor(anchor)?]
        }
        HistorySelector::After { anchor } => {
            vec!["AFTER".into(), target.into(), history_anchor(anchor)?]
        }
        HistorySelector::Around { anchor } => {
            vec!["AROUND".into(), target.into(), history_anchor(anchor)?]
        }
        HistorySelector::Between { start, end } => vec![
            "BETWEEN".into(),
            target.into(),
            history_anchor(start)?,
            history_anchor(end)?,
        ],
    };
    params.push(limit.to_string());
    Ok(OutboundMessage::new("CHATHISTORY", params))
}

fn history_anchor(anchor: &HistoryAnchor) -> Result<String, GatewayError> {
    match anchor {
        HistoryAnchor::Timestamp(value) => {
            chrono::DateTime::parse_from_rfc3339(value).map_err(|_| {
                GatewayError::InvalidMessage(
                    "history timestamp must be a valid RFC 3339 value".into(),
                )
            })?;
            Ok(format!("timestamp={value}"))
        }
        HistoryAnchor::MessageId(value) => {
            validate_irc_atom(value, "history message ID")?;
            Ok(format!("msgid={value}"))
        }
    }
}

/// Bytes a server prepends to this identity's messages when it relays them.
///
/// The observed hostmask is authoritative, and a self JOIN or CHGHOST records
/// it. Before either is seen the username is still the requested one, which a
/// server may rewrite (commonly by prefixing `~`), so this falls back to the
/// advertised maxima instead. Over-reserving only splits a message earlier than
/// strictly required; under-reserving loses its tail.
fn relay_prefix_reservation(snapshot: &crate::agent::actor::AgentSnapshot) -> usize {
    let identity = &snapshot.state.identity;
    let nickname = identity
        .nickname
        .as_ref()
        .map_or_else(|| isupport_numeric(snapshot, "NICKLEN", 9), String::len);
    let (username, hostname) = match (&identity.username, &identity.hostname) {
        (Some(username), Some(hostname)) => (username.len(), hostname.len()),
        _ => (
            // One further byte covers the `~` an unidentified username gains.
            isupport_numeric(snapshot, "USERLEN", 18).saturating_add(1),
            isupport_numeric(snapshot, "HOSTLEN", 63),
        ),
    };
    // ':' + nickname + '!' + username + '@' + hostname + ' '
    nickname
        .saturating_add(username)
        .saturating_add(hostname)
        .saturating_add(4)
}

/// Numeric ISUPPORT token, or `default` until the server advertises one.
fn isupport_numeric(
    snapshot: &crate::agent::actor::AgentSnapshot,
    name: &str,
    default: usize,
) -> usize {
    snapshot
        .protocol
        .isupport
        .get(name)
        .and_then(|token| token.value.as_deref())
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn capability_active(snapshot: &crate::agent::actor::AgentSnapshot, feature: &str) -> bool {
    snapshot.protocol.capabilities.values().any(|capability| {
        capability.feature == feature && capability.status == CapabilityStatus::Negotiated
    })
}

fn require_capability(
    snapshot: &crate::agent::actor::AgentSnapshot,
    feature: &str,
    operation: &str,
) -> Result<(), GatewayError> {
    if capability_active(snapshot, feature) {
        Ok(())
    } else {
        Err(GatewayError::InvalidMessage(format!(
            "{operation} requires the negotiated {feature} capability"
        )))
    }
}

/// Whether one client-only tag will be relayed under the current ISUPPORT
/// policy. Tag names are compared exactly and omit their leading `+`, as the
/// CLIENTTAGDENY token requires.
fn client_tag_allowed(snapshot: &crate::agent::actor::AgentSnapshot, tag: &str) -> bool {
    let token = snapshot
        .protocol
        .isupport
        .values()
        .find(|token| token.name.eq_ignore_ascii_case("CLIENTTAGDENY"));
    client_tag_allowed_by_policy(token, tag)
}

fn client_tag_allowed_by_policy(
    token: Option<&crate::irc::isupport::IsupportToken>,
    tag: &str,
) -> bool {
    let Some(token) = token else {
        return true;
    };
    if token.negated {
        return true;
    }
    let Some(value) = token.unescaped_value() else {
        return true;
    };
    let entries: Vec<_> = value.split(',').filter(|entry| !entry.is_empty()).collect();
    if entries.first() == Some(&"*") {
        entries
            .iter()
            .skip(1)
            .any(|entry| entry.strip_prefix('-') == Some(tag))
    } else {
        !entries.contains(&tag)
    }
}

fn casefold_target(snapshot: &crate::agent::actor::AgentSnapshot, target: &str) -> String {
    let mapping = snapshot
        .protocol
        .isupport
        .values()
        .find(|token| token.name.eq_ignore_ascii_case("CASEMAPPING"))
        .and_then(|token| token.value.as_deref())
        .map_or_else(
            crate::irc::isupport::CaseMapping::default,
            crate::irc::isupport::CaseMapping::parse,
        );
    mapping.fold(target)
}

fn claim_typing_slot(
    deadlines: &mut BTreeMap<(crate::agent::AgentId, String), Instant>,
    key: (crate::agent::AgentId, String),
    now: Instant,
) -> Result<(), Duration> {
    let interval = Duration::from_secs(3);
    deadlines.retain(|_, sent_at| now.duration_since(*sent_at) < interval);
    if let Some(sent_at) = deadlines.get(&key) {
        return Err(interval.saturating_sub(now.duration_since(*sent_at)));
    }
    deadlines.insert(key, now);
    Ok(())
}

fn read_marker_reply(result: &CommandResult) -> Option<crate::time::Timestamp> {
    result
        .replies
        .iter()
        .rev()
        .find(|reply| reply.command.eq_ignore_ascii_case("MARKREAD"))
        .and_then(|reply| reply.params.get(1).or(reply.trailing.as_ref()))
        .filter(|value| value.as_str() != "*")
        .and_then(|value| value.strip_prefix("timestamp="))
        .and_then(|value| value.parse().ok())
}

fn validate_irc_atom(value: &str, field: &str) -> Result<(), GatewayError> {
    if value.is_empty()
        || value.starts_with(':')
        || value
            .bytes()
            .any(|byte| matches!(byte, b'\0' | b'\r' | b'\n' | b' '))
    {
        Err(GatewayError::InvalidMessage(format!(
            "{field} is empty or contains a forbidden IRC parameter character"
        )))
    } else {
        Ok(())
    }
}

fn dcc_session_result(
    summary: &str,
    agent_id: &crate::agent::AgentId,
    session: DccSession,
) -> CallToolResult {
    tool_success_with_content(
        summary,
        &DccSessionOutput { session },
        vec![dcc_resource_link(agent_id)],
    )
}

/// Keep one presentation copy of the MOTD in normal tool traffic. The stable
/// MOTD resource remains the lossless source for line boundaries and numerics.
fn motd_for_tool_result(
    mut motd: crate::agent::state::MotdState,
    detail: ToolResultDetail,
) -> crate::agent::state::MotdState {
    if detail == ToolResultDetail::Compact {
        motd.lines.clear();
        motd.wire_replies.clear();
    }
    motd
}

/// Lossless replies remain authoritative for ordinary compact command output;
/// omit only the third, derived semantic projection.
fn command_result_for_detail(mut result: CommandResult, detail: ToolResultDetail) -> CommandResult {
    if detail == ToolResultDetail::Compact {
        result.semantic_result = None;
    }
    result
}

/// History events are authoritative in compact history output. Retain the
/// command envelope while removing its equivalent successful reply batch.
fn history_command_result_for_detail(
    mut result: CommandResult,
    detail: ToolResultDetail,
) -> CommandResult {
    if detail == ToolResultDetail::Compact {
        result.replies.clear();
        result.semantic_result = None;
    }
    result
}

fn send_result_summary(line_count: usize, failure: Option<CommandOutcome>) -> String {
    failure.map_or_else(
        || format!("Sent {line_count} IRC line(s)."),
        |outcome| format!("IRC send failed after processing {line_count} line(s): {outcome:?}."),
    )
}

fn history_result_summary(failure: Option<CommandOutcome>) -> String {
    failure.map_or_else(
        || "History query completed.".into(),
        |outcome| format!("History query failed: {outcome:?}."),
    )
}

/// Project one request's declarations into the reportable status field.
///
/// Kept next to the other result shaping so the wire form of a diagnostic stays
/// separate from the per-request evaluation the rest of the service reads.
fn caller_profile(profile: &RequestProfile) -> CallerProfile {
    CallerProfile {
        protocol_version: profile
            .protocol_version()
            .map(|version| version.as_str().to_owned()),
        request_metadata_complete: profile.declares_required_metadata(),
        extensions: profile
            .extension_ids()
            .into_iter()
            .map(str::to_owned)
            .collect(),
        form_elicitation: profile.supports_form_elicitation(),
        progress_requested: profile.progress_token().is_some(),
    }
}

fn tool_success(summary: impl Into<String>, value: &impl Serialize) -> CallToolResult {
    tool_success_with_content(summary, value, Vec::new())
}

/// Succeed with additional native content blocks after the text summary, so a
/// client can recognize a returned resource as something to attach or
/// subscribe to rather than as a URI printed inside JSON.
fn tool_success_with_content(
    summary: impl Into<String>,
    value: &impl Serialize,
    content: Vec<ContentBlock>,
) -> CallToolResult {
    match envelope::success(value) {
        Ok(structured) => structured_result_with_content(summary, structured, false, content),
        Err(error) => unserializable(&error),
    }
}

/// Report one correlated IRC command, successfully completed or not.
///
/// The two cases produce different branches of the same declared schema: a
/// completed command answers with the tool's own output, and a rejected or
/// unacknowledged one answers with the shared failure carrying that exchange
/// whole. The numerics are the actionable part of a rejection, so they travel
/// with it rather than being summarized away.
fn command_tool_result(
    summary: String,
    value: &impl Serialize,
    failure: Option<CommandFailure>,
) -> CallToolResult {
    command_tool_result_with_content(summary, value, failure, Vec::new())
}

fn command_tool_result_with_content(
    summary: String,
    value: &impl Serialize,
    failure: Option<CommandFailure>,
    content: Vec<ContentBlock>,
) -> CallToolResult {
    match failure {
        Some(failure) => failure_result_with_content(
            ToolFailure::from_command(failure.outcome, summary, failure.result),
            content,
        ),
        None => tool_success_with_content(summary, value, content),
    }
}

/// Report one failure in band, with the text summary taken from its message.
fn failure_result(failure: ToolFailure) -> CallToolResult {
    failure_result_with_content(failure, Vec::new())
}

fn failure_result_with_content(failure: ToolFailure, content: Vec<ContentBlock>) -> CallToolResult {
    let summary = failure.message.clone();
    match envelope::failure(&failure) {
        Ok(structured) => structured_result_with_content(summary, structured, true, content),
        Err(error) => unserializable(&error),
    }
}

/// Finish one already-enveloped result: text summary first, then any native
/// content blocks, with `isError` agreeing with the envelope's discriminator.
fn structured_result_with_content(
    summary: impl Into<String>,
    structured: serde_json::Value,
    is_error: bool,
    mut content: Vec<ContentBlock>,
) -> CallToolResult {
    let mut result = if is_error {
        CallToolResult::structured_error(structured)
    } else {
        CallToolResult::structured(structured)
    };
    content.insert(0, ContentBlock::text(summary));
    result.content = content;
    result
}

/// Report a result this server computed but could not serialize.
///
/// Unreachable in practice — every output type is a plain `serde` structure —
/// but it still has to answer inside the declared schema, because a caller
/// validating `structuredContent` would otherwise see the advertised contract
/// broken by the one path that exists to report a broken contract.
fn unserializable(error: &serde_json::Error) -> CallToolResult {
    failure_result(ToolFailure::refusal(
        envelope::INTERNAL_KIND,
        format!("could not serialize typed tool output: {error}"),
        false,
    ))
}

/// Bring a result the SDK produced before the tool ran into the envelope.
///
/// A call whose arguments do not satisfy the published input schema is refused
/// by the generated dispatcher with a text-only error result, which never
/// reaches the funnel below. Left alone it would be the one tool result on this
/// server carrying no `structuredContent` at all — a hole in exactly the
/// guarantee this envelope exists to make — so it is adopted here, keeping its
/// explanation and gaining the shape every other failure has.
fn adopt_unstructured(result: &mut CallToolResult) {
    if result.structured_content.is_some() {
        return;
    }
    let message: Vec<&str> = result
        .content
        .iter()
        .filter_map(|block| block.as_text().map(|text| text.text.as_str()))
        .collect();
    // Not retriable unchanged: the arguments themselves are what the
    // dispatcher refused.
    let failure = ToolFailure::refusal(ErrorKind::Validation.as_str(), message.join(" "), false);
    result.is_error = Some(true);
    result.structured_content = envelope::failure(&failure).ok();
}

/// One correlated command that did not complete, kept whole for the failure it
/// becomes.
struct CommandFailure {
    outcome: CommandOutcome,
    result: CommandResult,
}

fn resource_link(uri: impl Into<String>) -> ContentBlock {
    let uri = uri.into();
    let parsed =
        AgentResourceUri::from_str(&uri).expect("gateway-generated agent resource URI must parse");
    ContentBlock::ResourceLink(crate::mcp::resources::describe(&parsed, None).into_resource())
}

fn agent_resource_links(resources: &ResourceUris) -> Vec<ContentBlock> {
    resources
        .named()
        .into_iter()
        .map(|(_, uri)| resource_link(uri))
        .collect()
}

fn channel_resource_link(uri: impl Into<String>, _channel: &str) -> ContentBlock {
    resource_link(uri)
}

fn dcc_resource_link(agent_id: &crate::agent::AgentId) -> ContentBlock {
    resource_link(ResourceUris::for_agent(agent_id).dcc)
}

fn tool_error(error: GatewayError) -> CallToolResult {
    failure_result(ToolFailure::from_error(&error))
}

const fn is_failure_outcome(outcome: CommandOutcome) -> bool {
    matches!(
        outcome,
        CommandOutcome::Rejected
            | CommandOutcome::TimedOut
            | CommandOutcome::NotWritten
            | CommandOutcome::Indeterminate
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::CallToolRequestParams;

    /// The profile of a request declaring `capabilities`.
    fn declaring(capabilities: rmcp::model::ClientCapabilities) -> RequestProfile {
        let mut meta = rmcp::model::RequestMetaObject::new();
        meta.set_protocol_version(ProtocolVersion::V_2026_07_28);
        meta.set_client_capabilities(capabilities);
        RequestProfile::from_meta(&meta)
    }

    #[test]
    fn a_task_is_created_only_for_a_client_that_declared_the_extension() {
        let declared = declaring(
            rmcp::model::ClientCapabilities::builder()
                .enable_tasks()
                .build(),
        );
        let silent = declaring(rmcp::model::ClientCapabilities::default());

        assert!(runs_as_task("irc.dcc.send", &declared));
        assert!(runs_as_task("irc.dcc.accept", &declared));
        assert!(
            !runs_as_task("irc.dcc.send", &silent),
            "a client with no way to resolve a task handle must get its result directly"
        );
    }

    #[test]
    fn declaring_the_extension_does_not_make_every_call_a_task() {
        // The trigger is the server's, but it applies only to the operations
        // that outlive their request: a fast tool answered with a handle would
        // cost the caller a poll for nothing.
        let declared = declaring(
            rmcp::model::ClientCapabilities::builder()
                .enable_tasks()
                .build(),
        );
        assert!(!runs_as_task("irc.send", &declared));
        assert!(!runs_as_task("irc.dcc.list", &declared));
        assert!(!runs_as_task("irc.connect", &declared));
    }

    #[test]
    fn the_task_trigger_reads_declared_capabilities_and_not_call_metadata() {
        // The superseded design had the client opt one call in by putting the
        // extension id in that call's own `_meta`. Honoring that would also
        // reintroduce the bug it hid: inbound `_meta` is hoisted onto the
        // request context, so `params.meta` is always `None` on the wire and a
        // trigger reading it never fires in production.
        let mut request = CallToolRequestParams::new("irc.dcc.send");
        let mut meta = rmcp::model::RequestMetaObject::new();
        meta.insert(TASKS_EXTENSION_ID.to_string(), serde_json::json!({}));
        request.meta = Some(meta);
        assert!(!runs_as_task(
            &request.name,
            &RequestProfile::from_meta(request.meta.as_ref().expect("metadata"))
        ));
    }

    #[test]
    fn only_the_operations_that_outlive_their_request_can_become_tasks() {
        let service = IrcMcpService::new(Arc::new(Gateway::new(Default::default())));
        let names: BTreeSet<_> = service
            .tool_router
            .list_all()
            .iter()
            .map(|tool| tool.name.to_string())
            .collect();
        for name in TASK_AUGMENTED_TOOLS {
            assert!(
                names.contains(*name),
                "{name} is task-augmented but is not a registered tool"
            );
        }
        // Everything else answers in one round trip, so wrapping it would cost
        // the caller a poll for nothing.
        assert!(!TASK_AUGMENTED_TOOLS.contains(&"irc.send"));
        assert!(!TASK_AUGMENTED_TOOLS.contains(&"irc.dcc.list"));
    }

    #[test]
    fn a_started_transfer_is_followed_by_the_session_its_tool_reported() {
        let session_id = DccSessionId::new();
        // Built through the funnel, so the path this reads is the path the
        // envelope actually produces rather than a hand-written guess at it.
        let result = tool_success(
            "offered",
            &serde_json::json!({
                "session": { "id": session_id.to_string(), "kind": "send" }
            }),
        );
        assert_eq!(followable_transfer(&result), Some(session_id));

        // A tool that started nothing to follow settles the task immediately.
        assert_eq!(
            followable_transfer(&tool_success("nothing", &serde_json::json!({}))),
            None
        );
    }

    #[test]
    fn an_accepted_chat_is_a_finished_task_rather_than_something_to_follow() {
        // `irc.dcc.accept` takes CHAT offers too, and an accepted chat stays
        // `active` for the whole conversation. Following one would hold the
        // task open for its duration, withhold the session details the caller
        // needs to say anything at all, and then report a healthy chat as
        // expired when the deadline passed.
        let chat = tool_success(
            "accepted",
            &serde_json::json!({
                "session": {
                    "id": DccSessionId::new().to_string(),
                    "kind": "chat",
                    "state": "active",
                }
            }),
        );
        assert_eq!(followable_transfer(&chat), None);
    }

    #[test]
    fn initialization_instructions_are_deliberately_small() {
        let service = IrcMcpService::new(Arc::new(Gateway::new(Default::default())));
        let info = service.get_info();
        assert_eq!(info.instructions.as_deref(), Some(MCP_INSTRUCTIONS));
        assert!(!MCP_INSTRUCTIONS.contains("AGENT"));
        assert!(MCP_INSTRUCTIONS.contains("irc.attention.open"));
        assert!(MCP_INSTRUCTIONS.contains("60 seconds"));
    }

    #[test]
    fn the_server_speaks_only_the_stateless_protocol_revision() {
        let service = IrcMcpService::new(Arc::new(Gateway::new(Default::default())));
        assert_eq!(
            service.supported_protocol_versions().as_ref(),
            [ProtocolVersion::V_2026_07_28]
        );
        assert_eq!(
            service.get_info().protocol_version,
            ProtocolVersion::V_2026_07_28,
            "the advertised version must match what the handler will accept"
        );
    }

    #[test]
    fn status_reports_the_declarations_its_own_request_carried() {
        let mut meta = rmcp::model::RequestMetaObject::new();
        meta.set_protocol_version(ProtocolVersion::V_2026_07_28);
        let mut capabilities = rmcp::model::ClientCapabilities::builder()
            .enable_tasks()
            .build();
        capabilities.elicitation = Some(rmcp::model::ElicitationCapability::new());
        meta.set_client_capabilities(capabilities);
        let reported = caller_profile(&RequestProfile::from_meta(&meta));
        assert_eq!(reported.protocol_version.as_deref(), Some("2026-07-28"));
        assert!(reported.request_metadata_complete);
        assert_eq!(reported.extensions, [TASKS_EXTENSION_ID]);
        assert!(reported.form_elicitation);
        assert!(!reported.progress_requested);

        // A caller that declared nothing sees exactly that, which is how a host
        // whose metadata never arrives can tell the difference.
        let silent = caller_profile(&RequestProfile::default());
        assert_eq!(silent.protocol_version, None);
        assert!(!silent.request_metadata_complete);
        assert!(silent.extensions.is_empty());
        assert!(!silent.form_elicitation);
    }

    #[test]
    fn history_references_are_encoded_with_the_required_type_prefix() {
        assert_eq!(
            history_anchor(&HistoryAnchor::Timestamp("2026-08-17T00:00:00Z".into()))
                .expect("timestamp"),
            "timestamp=2026-08-17T00:00:00Z"
        );
        assert_eq!(
            history_anchor(&HistoryAnchor::MessageId("abc".into())).expect("message ID"),
            "msgid=abc"
        );
    }

    #[test]
    fn client_tag_deny_honors_catch_all_exemptions_and_exact_names() {
        use crate::irc::isupport::IsupportToken;

        assert!(client_tag_allowed_by_policy(None, "typing"));
        let selective = IsupportToken::parse("CLIENTTAGDENY=typing,draft/react");
        assert!(!client_tag_allowed_by_policy(Some(&selective), "typing"));
        assert!(!client_tag_allowed_by_policy(
            Some(&selective),
            "draft/react"
        ));
        assert!(client_tag_allowed_by_policy(
            Some(&selective),
            "draft/unreact"
        ));

        let catch_all = IsupportToken::parse("CLIENTTAGDENY=*,-typing");
        assert!(client_tag_allowed_by_policy(Some(&catch_all), "typing"));
        assert!(!client_tag_allowed_by_policy(
            Some(&catch_all),
            "draft/react"
        ));
    }

    #[test]
    fn typing_slots_are_throttled_per_agent_and_casefolded_target() {
        let agent_id = crate::agent::AgentId::new();
        let key = (agent_id, "#room".into());
        let mut deadlines = BTreeMap::new();
        let now = Instant::now();

        assert!(claim_typing_slot(&mut deadlines, key.clone(), now).is_ok());
        let retry_after =
            claim_typing_slot(&mut deadlines, key.clone(), now + Duration::from_secs(1))
                .expect_err("second update should be throttled");
        assert_eq!(retry_after, Duration::from_secs(2));
        assert!(claim_typing_slot(&mut deadlines, key, now + Duration::from_secs(3)).is_ok());
    }

    #[test]
    fn stable_tool_list_is_exact_and_schema_backed() {
        let service = IrcMcpService::new(Arc::new(Gateway::new(Default::default())));
        let tools = service.tool_router.list_all();
        let names: BTreeSet<_> = tools.iter().map(|tool| tool.name.as_ref()).collect();
        assert_eq!(names, TOOL_NAMES.iter().copied().collect());
        for tool in &tools {
            // Every tool declares the same envelope over its own output, so a
            // client can read `ok` before it knows which tool answered. The
            // instances are validated against these schemas in
            // `crate::tests::output_schema`.
            let schema = tool
                .output_schema
                .as_ref()
                .unwrap_or_else(|| panic!("{} declares an output schema", tool.name));
            let branches = schema["oneOf"]
                .as_array()
                .unwrap_or_else(|| panic!("{}: two discriminated branches", tool.name));
            assert_eq!(branches.len(), 2, "{}", tool.name);
            assert_eq!(
                branches[0]["properties"]["ok"]["const"],
                serde_json::json!(true),
                "{}",
                tool.name
            );
            assert_eq!(
                branches[1]["properties"]["ok"]["const"],
                serde_json::json!(false),
                "{}",
                tool.name
            );
        }
        let _ = CallToolRequestParams::new("irc.status");
    }

    #[test]
    fn stable_prompt_list_is_exact_and_advertised() {
        let service = IrcMcpService::new(Arc::new(Gateway::new(Default::default())));
        let names: BTreeSet<_> = service
            .prompt_router
            .list_all()
            .into_iter()
            .map(|prompt| prompt.name)
            .collect();
        assert_eq!(
            names,
            PROMPT_NAMES.iter().map(|name| (*name).to_owned()).collect()
        );
        assert!(service.get_info().capabilities.prompts.is_some());
    }

    /// A watch already names a complete selection, so combining it with the
    /// single-value filters is refused with an explanation rather than resolved
    /// into a third selection nobody asked for.
    #[tokio::test]
    async fn a_watch_selection_and_the_single_value_filters_cannot_be_combined() {
        let service = IrcMcpService::new(Arc::new(Gateway::new(Default::default())));
        let mut input = EventsReadInput {
            agent_id: crate::agent::AgentId::new(),
            cursor: None,
            limit: default_event_limit(),
            wait_ms: 0,
            watch_id: Some(WatchId::new()),
            command_id: None,
            class: Some(EventClass::MessageChannel),
            target: None,
            direction: None,
            origin: None,
            verbosity: None,
            mentions_me: None,
            set_activity_anchor: false,
        };
        assert_eq!(input.conflicting_filter(), Some("class"));
        let refused = service.irc_events_read(Parameters(input.clone())).await;
        assert_eq!(refused.is_error, Some(true));
        let message = refused.content[0].as_text().expect("summary").text.clone();
        assert!(
            message.contains("`class` must be omitted"),
            "the refusal must name the offending field: {message}"
        );

        // Without a watch the same filter is ordinary, and the two together are
        // the only rejected shape.
        input.watch_id = None;
        assert_eq!(input.conflicting_filter(), Some("class"));
        input.class = None;
        input.watch_id = Some(WatchId::new());
        assert_eq!(input.conflicting_filter(), None);
    }

    #[tokio::test]
    async fn workflow_prompts_preserve_realtime_host_boundaries() {
        let service = IrcMcpService::new(Arc::new(Gateway::new(Default::default())));
        let messages = service
            .prompt_watch_mentions(Parameters(WatchMentionsPromptInput {
                agent_id: "agent-example".into(),
                targets: Some("#control".into()),
            }))
            .await;
        let text = messages[0].content.as_text().expect("prompt text");
        // One concrete sequence, in order, ending with who owns the cursor.
        for step in [
            "irc.watch.create",
            "subscriptions/listen",
            "resourceSubscriptions",
            "irc.events.read",
            "watch_id",
            "next_cursor",
            "wait_ms",
        ] {
            assert!(text.text.contains(step), "the prompt omits {step}");
        }
        // The corrected protocol claim: a notification wakes the host and
        // nothing in this protocol version can schedule a model turn.
        assert!(text.text.contains("cannot force or schedule a model turn"));
        assert!(text.text.contains("SEP-2577"));
        assert!(
            !text.text.contains("durable cursor"),
            "the watch no longer holds a position for the caller"
        );

        let attention = service
            .prompt_maintain_attention(Parameters(MaintainAttentionPromptInput {
                agent_id: "agent-example".into(),
                full_traffic_targets: Some("#project".into()),
            }))
            .await;
        let text = attention[0].content.as_text().expect("attention prompt");
        for boundary in [
            "irc.attention.open",
            "subscriptions/listen",
            "notifications/resources/updated",
            "60 seconds",
            "consumes model tokens",
            "top-level MRTR",
            "task's input_required",
        ] {
            assert!(
                text.text.contains(boundary),
                "attention prompt omits {boundary}"
            );
        }
    }

    #[test]
    fn native_resource_links_accompany_structured_tool_output() {
        let agent_id = crate::agent::AgentId::new();
        let resources = ResourceUris::for_agent(&agent_id);
        let links = agent_resource_links(&resources);
        assert_eq!(links.len(), resources.named().len());
        assert!(
            links
                .iter()
                .all(|block| matches!(block, ContentBlock::ResourceLink(_)))
        );

        let result = tool_success_with_content(
            "connected",
            &serde_json::json!({"agent_id": agent_id}),
            links,
        );
        assert_eq!(result.content.len(), resources.named().len() + 1);
        assert_eq!(
            result.content[0].as_text().expect("summary text").text,
            "connected"
        );
        let linked_uris: BTreeSet<_> = result.content[1..]
            .iter()
            .map(|block| match block {
                ContentBlock::ResourceLink(resource) => {
                    assert!(resource.annotations.is_some());
                    resource.uri.as_str()
                }
                other => panic!("expected resource link, got {other:?}"),
            })
            .collect();
        assert_eq!(
            linked_uris,
            resources.named().into_iter().map(|(_, uri)| uri).collect()
        );
        assert!(result.structured_content.is_some());
    }

    fn command_result_with(lines: &[&'static [u8]]) -> CommandResult {
        CommandResult {
            command_id: crate::irc::correlation::CommandId::new(),
            agent_id: crate::agent::AgentId::new(),
            command: "TEST".into(),
            outcome: CommandOutcome::Completed,
            written: true,
            acknowledged: true,
            retriable: false,
            label: None,
            replies: lines
                .iter()
                .map(|line| {
                    crate::irc::wire::WireMessage::parse(bytes::Bytes::from_static(line))
                        .expect("wire reply")
                })
                .collect(),
            semantic_result: None,
            warnings: Vec::new(),
            first_event_cursor: None,
        }
    }

    #[test]
    fn typed_query_projections_extract_standard_reply_fields() {
        let whois = command_result_with(&[
            b":irc.example 311 Me Athena user host * :Athena Example",
            b":irc.example 317 Me Athena 42 1700000000 :seconds idle, signon time",
            b":irc.example 319 Me Athena :@#control +#rust",
            b":irc.example 330 Me Athena account :is logged in as",
            b":irc.example 671 Me Athena :is using a secure connection",
        ]);
        let profile = whois_profile(&whois);
        assert_eq!(profile.nickname.as_deref(), Some("Athena"));
        assert_eq!(profile.username.as_deref(), Some("user"));
        assert_eq!(profile.hostname.as_deref(), Some("host"));
        assert_eq!(profile.real_name.as_deref(), Some("Athena Example"));
        assert_eq!(profile.idle_seconds, Some(42));
        assert_eq!(profile.signon_timestamp, Some(1_700_000_000));
        assert_eq!(profile.channels, ["@#control", "+#rust"]);
        assert_eq!(profile.account.as_deref(), Some("account"));
        assert!(profile.secure);

        let names = command_result_with(&[
            b":irc.example 353 Me = #control :@grant Athena",
            b":irc.example 353 Me = #control :Ninshubur",
            b":irc.example 366 Me #control :End of NAMES",
        ]);
        let channels = names_channels(&names);
        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].channel, "#control");
        assert_eq!(channels[0].visibility, "=");
        assert_eq!(channels[0].names, ["@grant", "Athena", "Ninshubur"]);

        let topic = command_result_with(&[
            b":irc.example 332 Me #control :Coordinate here",
            b":irc.example 333 Me #control grant 1700000001",
        ]);
        assert_eq!(
            topic_reply(&topic),
            (
                Some("Coordinate here".into()),
                Some("grant".into()),
                Some(1_700_000_001)
            )
        );
    }

    #[test]
    fn typed_read_marker_projection_uses_the_server_confirmed_value() {
        let result = command_result_with(&[
            b":irc.example MARKREAD #control timestamp=2026-08-17T07:00:00.123Z",
        ]);
        assert_eq!(
            read_marker_reply(&result).map(|value| value.to_rfc3339()),
            Some("2026-08-17T07:00:00.123Z".into())
        );

        let unknown = command_result_with(&[b":irc.example MARKREAD #control *"]);
        assert_eq!(read_marker_reply(&unknown), None);
    }

    /// Description of one top-level property of a tool's input schema.
    fn property_description(
        schema: &serde_json::Map<String, serde_json::Value>,
        name: &str,
    ) -> Option<String> {
        schema
            .get("properties")?
            .get(name)?
            .get("description")?
            .as_str()
            .map(str::to_owned)
    }

    #[test]
    fn every_handle_property_tells_the_caller_where_the_handle_comes_from() {
        let service = IrcMcpService::new(Arc::new(Gateway::new(Default::default())));
        let mut checked = 0_usize;
        for tool in service.tool_router.list_all() {
            // Each handle names the tool that mints it, so a caller reading one
            // schema never has to search for where the value comes from.
            for property in ["agent_id", "watch_id"] {
                let Some(description) = property_description(&tool.input_schema, property) else {
                    continue;
                };
                let source = match (tool.name.as_ref(), property) {
                    ("irc.attention.check", "watch_id") => "irc.attention.open",
                    (_, "agent_id") => "irc.connect",
                    (_, "watch_id") => "irc.watch.create",
                    _ => unreachable!(),
                };
                assert!(
                    description.contains(source),
                    "{}: {property} description does not name its source: {description:?}",
                    tool.name
                );
                checked += 1;
            }
        }
        // Every tool takes a handle except irc.connect, which mints one.
        // irc.events.read and irc.attention.check each take both their agent
        // and their watch, yielding one more checked property than tool names.
        assert_eq!(checked, TOOL_NAMES.len() + 1);
    }

    #[test]
    fn accepting_a_transfer_advertises_the_root_that_carries_its_authority() {
        // A model can only choose a destination it can see in the schema, and
        // the two properties have to say what they are for: one names a
        // configured root, the other is relative to it and can never be
        // absolute.
        let service = IrcMcpService::new(Arc::new(Gateway::new(Default::default())));
        let accept = service
            .tool_router
            .list_all()
            .into_iter()
            .find(|tool| tool.name == "irc.dcc.accept")
            .expect("irc.dcc.accept is published");

        let root = property_description(&accept.input_schema, "root").expect("root is an input");
        assert!(root.contains("receive_roots"), "{root:?}");
        let destination = property_description(&accept.input_schema, "destination_path")
            .expect("destination_path is an input");
        assert!(destination.contains("relative"), "{destination:?}");
        assert!(
            destination.contains("Absolute paths are refused"),
            "{destination:?}"
        );
        assert!(
            accept
                .description
                .as_deref()
                .expect("description")
                .contains("input_required"),
            "the description must tell a caller the choice can be asked for"
        );
    }

    #[test]
    fn caller_supplied_deadlines_publish_the_bound_they_are_rejected_against() {
        let service = IrcMcpService::new(Arc::new(Gateway::new(Default::default())));
        let limit = crate::config::GatewayLimits::default();
        assert_eq!(limit.max_command_timeout_ms, 30_000);
        assert_eq!(limit.max_event_wait_ms, 30_000);
        let mut checked = 0_usize;
        for tool in service.tool_router.list_all() {
            for field in ["timeout_ms", "wait_ms"] {
                let Some(property) = tool
                    .input_schema
                    .get("properties")
                    .and_then(|properties| properties.get(field))
                else {
                    continue;
                };
                let description = property
                    .get("description")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_else(|| panic!("{}: {field} has no description", tool.name));
                assert!(
                    description.contains("30000"),
                    "{}: {field} does not publish its bound: {description:?}",
                    tool.name
                );
                checked += 1;
            }
        }
        assert!(
            checked >= 7,
            "expected every bounded deadline, saw {checked}"
        );
    }

    #[test]
    fn send_text_schema_states_which_kinds_require_it() {
        let service = IrcMcpService::new(Arc::new(Gateway::new(Default::default())));
        let send = service
            .tool_router
            .list_all()
            .into_iter()
            .find(|tool| tool.name == "irc.send")
            .expect("irc.send");
        let description = property_description(&send.input_schema, "text").expect("text");
        assert!(description.contains("tagmsg"), "{description:?}");
        // The prose must match what build_send_messages actually enforces.
        let tagmsg = SendInput {
            agent_id: crate::agent::AgentId::new(),
            target: "Athena".parse().expect("target"),
            kind: SendKind::Tagmsg,
            text: Some("body".into()),
            tags: Vec::new(),
            reply_to: None,
            multiline: MultilinePolicy::Prefer,
            timeout_ms: 1_000,
            result_detail: ToolResultDetail::Full,
        };
        assert!(matches!(
            build_send_messages(&tagmsg, 512, 0, 512, 1),
            Err(GatewayError::InvalidMessage(_))
        ));
        let empty = SendInput {
            text: None,
            ..tagmsg
        };
        assert!(build_send_messages(&empty, 512, 0, 512, 1).is_ok());
    }

    #[test]
    fn enum_backed_inputs_publish_their_exact_json_tokens() {
        let service = IrcMcpService::new(Arc::new(Gateway::new(Default::default())));
        let tools = service.tool_router.list_all();
        let description = |tool_name: &str, property: &str| {
            let tool = tools
                .iter()
                .find(|tool| tool.name == tool_name)
                .unwrap_or_else(|| panic!("missing {tool_name}"));
            property_description(&tool.input_schema, property)
                .unwrap_or_else(|| panic!("missing {tool_name}.{property} description"))
        };

        for (tool, property, tokens) in [
            (
                "irc.connect",
                "nick_conflict_policy",
                &["suffix", "fail", "elicit"][..],
            ),
            (
                "irc.send",
                "kind",
                &["privmsg", "notice", "action", "tagmsg"][..],
            ),
            (
                "irc.send",
                "multiline",
                &["require", "prefer", "split", "reject_if_too_long"][..],
            ),
            (
                "irc.execute",
                "response_mode",
                &["auto", "collect", "fire_and_forget"][..],
            ),
            ("irc.connect", "result_detail", &["compact", "full"][..]),
            ("irc.status", "result_detail", &["compact", "full"][..]),
            ("irc.join", "result_detail", &["compact", "full"][..]),
            ("irc.part", "result_detail", &["compact", "full"][..]),
            ("irc.send", "result_detail", &["compact", "full"][..]),
            ("irc.history", "result_detail", &["compact", "full"][..]),
            ("irc.query", "result_detail", &["compact", "full"][..]),
            ("irc.execute", "result_detail", &["compact", "full"][..]),
        ] {
            let description = description(tool, property);
            for token in tokens {
                assert!(
                    description.contains(token),
                    "{tool}.{property} does not publish {token:?}: {description:?}"
                );
            }
        }

        let history = description("irc.history", "selector");
        for token in ["latest", "before", "anchor", "timestamp", "value"] {
            assert!(history.contains(token), "irc.history.selector: {history:?}");
        }
        let query = description("irc.query", "query");
        for token in ["names", "channels", "topic", "channel"] {
            assert!(query.contains(token), "irc.query.query: {query:?}");
        }
    }

    #[test]
    fn compact_motd_keeps_instructions_but_drops_duplicate_protocol_forms() {
        let wire = crate::irc::wire::WireMessage::parse(bytes::Bytes::from_static(
            b":irc.example 372 Athena :- Read the rules",
        ))
        .expect("wire MOTD");
        let motd = crate::agent::state::MotdState {
            lines: vec!["Read the rules".into()],
            text: "Read the rules".into(),
            wire_replies: vec![wire],
            ..Default::default()
        };

        let compact = motd_for_tool_result(motd.clone(), ToolResultDetail::Compact);
        assert_eq!(compact.text, "Read the rules");
        assert!(compact.lines.is_empty());
        assert!(compact.wire_replies.is_empty());

        let full = motd_for_tool_result(motd, ToolResultDetail::Full);
        assert_eq!(full.lines, ["Read the rules"]);
        assert_eq!(full.wire_replies.len(), 1);
    }

    #[test]
    fn compact_history_keeps_the_envelope_without_repeating_event_data() {
        let wire = crate::irc::wire::WireMessage::parse(bytes::Bytes::from_static(
            b":Athena!u@h PRIVMSG #control :hello",
        ))
        .expect("history wire");
        let result = CommandResult {
            command_id: crate::irc::correlation::CommandId::new(),
            agent_id: crate::agent::AgentId::new(),
            command: "CHATHISTORY".into(),
            outcome: CommandOutcome::Completed,
            written: true,
            acknowledged: true,
            retriable: false,
            label: Some("history-label".into()),
            replies: vec![wire],
            semantic_result: Some(Vec::new()),
            warnings: Vec::new(),
            first_event_cursor: None,
        };

        let compact_command = command_result_for_detail(result.clone(), ToolResultDetail::Compact);
        assert_eq!(compact_command.replies.len(), 1);
        assert!(compact_command.semantic_result.is_none());

        let compact = history_command_result_for_detail(result.clone(), ToolResultDetail::Compact);
        assert_eq!(compact.command, "CHATHISTORY");
        assert!(compact.replies.is_empty());
        assert!(compact.semantic_result.is_none());

        let full = history_command_result_for_detail(result, ToolResultDetail::Full);
        assert_eq!(full.replies.len(), 1);
        assert!(full.semantic_result.is_some());
    }

    #[test]
    fn ordinary_command_detail_defaults_to_full_for_existing_callers() {
        let agent_id = crate::agent::AgentId::new();
        let input: ExecuteInput = serde_json::from_value(serde_json::json!({
            "agent_id": agent_id.as_str(),
            "command": "VERSION"
        }))
        .expect("execute input");
        assert_eq!(input.result_detail, ToolResultDetail::Full);
    }

    /// One rejected exchange, so a failure branch has something to carry.
    fn rejected(outcome: CommandOutcome) -> CommandFailure {
        let mut result = command_result_with(&[b":irc.example 475 Me #keyed :Cannot join channel"]);
        result.outcome = outcome;
        result.retriable = false;
        CommandFailure { outcome, result }
    }

    #[test]
    fn rejected_send_and_history_summaries_are_not_success_phrased() {
        let send = send_result_summary(1, Some(CommandOutcome::Rejected));
        assert_eq!(
            send,
            "IRC send failed after processing 1 line(s): Rejected."
        );
        assert!(!send.contains("Sent 1"));
        let send_result = command_tool_result(
            send.clone(),
            &serde_json::json!({"outcome": "rejected"}),
            Some(rejected(CommandOutcome::Rejected)),
        );
        assert_eq!(send_result.is_error, Some(true));
        assert_eq!(send_result.content[0].as_text().expect("text").text, send);

        let history = history_result_summary(Some(CommandOutcome::TimedOut));
        assert_eq!(history, "History query failed: TimedOut.");
        assert!(!history.contains("completed"));
        let history_result = command_tool_result(
            history.clone(),
            &serde_json::json!({"outcome": "timed_out"}),
            Some(rejected(CommandOutcome::TimedOut)),
        );
        assert_eq!(history_result.is_error, Some(true));
        assert_eq!(
            history_result.content[0].as_text().expect("text").text,
            history
        );
    }

    /// A command the server refused takes the failure branch, and takes its
    /// numerics with it: the 475 is the actionable part of the rejection, and
    /// dropping it to fit a shared shape would make the shared shape useless.
    #[test]
    fn a_rejected_command_reports_the_failure_branch_with_its_exchange_intact() {
        let result = command_tool_result(
            "JOIN #keyed: Rejected.".into(),
            &serde_json::json!({"channel": "#keyed"}),
            Some(rejected(CommandOutcome::Rejected)),
        );
        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect("an enveloped failure");
        assert_eq!(structured["ok"], serde_json::json!(false));
        assert_eq!(structured["error"]["kind"], "rejected");
        assert_eq!(structured["error"]["message"], "JOIN #keyed: Rejected.");
        assert_eq!(
            structured["error"]["command_result"]["replies"][0]["command"],
            "475"
        );
        assert!(
            structured.get("result").is_none(),
            "a failure branch never carries the success branch too: {structured}"
        );

        // The same call, completed, is the success branch and says nothing
        // about failure at all.
        let completed = command_tool_result(
            "JOIN #open: Completed.".into(),
            &serde_json::json!({"channel": "#open"}),
            None,
        );
        assert_eq!(completed.is_error, Some(false));
        let structured = completed.structured_content.expect("an enveloped success");
        assert_eq!(structured["ok"], serde_json::json!(true));
        assert_eq!(structured["result"]["channel"], "#open");
        assert!(structured.get("error").is_none(), "{structured}");
    }

    #[test]
    fn utf8_splitting_is_lossless_and_bounded() {
        let chunks = split_utf8("Māui🙂Athena", 5).expect("split");
        assert!(chunks.iter().all(|chunk| chunk.len() <= 5));
        assert_eq!(chunks.concat(), "Māui🙂Athena");
    }

    #[test]
    fn message_building_uses_the_stable_reply_tag_and_logical_bounds() {
        let input = SendInput {
            agent_id: crate::agent::AgentId::new(),
            target: "Athena".parse().expect("target"),
            kind: SendKind::Privmsg,
            text: Some("hello".into()),
            tags: Vec::new(),
            reply_to: Some("message-id".into()),
            multiline: MultilinePolicy::Split,
            timeout_ms: 1_000,
            result_detail: ToolResultDetail::Full,
        };
        let messages = build_send_messages(&input, 512, 0, 5, 1).expect("message");
        assert_eq!(messages[0].tags[0].key, "+reply");
        assert!(matches!(
            build_send_messages(&input, 512, 4, 4, 1),
            Err(GatewayError::ResourceLimit(_))
        ));
    }

    /// Bytes the relayed line spends outside the message text itself.
    fn relayed_overhead(prefix: &str, target: &str) -> usize {
        // ':' nick!user@host ' ' + "PRIVMSG" + ' ' + target + " :" + CRLF
        prefix.len() + 1 + "PRIVMSG".len() + 1 + target.len() + 2 + 2
    }

    #[test]
    fn outbound_text_reserves_the_relayed_source_prefix() {
        let prefix = "Mnemosyne!~mcp-agent-f372cdb8-@127.0.0.1";
        let target = "#control";
        let reservation = prefix.len() + 4;
        let text = "y".repeat(463);
        let input = SendInput {
            agent_id: crate::agent::AgentId::new(),
            target: target.parse().expect("target"),
            kind: SendKind::Privmsg,
            text: Some(text.clone()),
            tags: Vec::new(),
            reply_to: None,
            multiline: MultilinePolicy::Split,
            timeout_ms: 1_000,
            result_detail: ToolResultDetail::Full,
        };

        // Without the reservation this text fits one line, which is exactly how
        // the tail used to be lost: 463 bytes clears the 492-byte prefix-less
        // budget, then the relayed line overruns 512 and the server trims it.
        let unreserved = build_send_messages(&input, 512, 0, 64 * 1024, 256).expect("unreserved");
        assert_eq!(unreserved.len(), 1);
        assert!(relayed_overhead(prefix, target) + text.len() > 512);

        let messages = build_send_messages(&input, 512, reservation, 64 * 1024, 256).expect("send");
        assert_eq!(messages.len(), 2);
        let carried: String = messages
            .iter()
            .map(|message| message.trailing.clone().expect("trailing"))
            .collect();
        assert_eq!(carried, text);
        for message in &messages {
            let body = message.trailing.as_deref().expect("trailing").len();
            assert!(
                relayed_overhead(prefix, target) + body <= 512,
                "relayed line must fit the server's body budget"
            );
        }
    }

    #[test]
    fn an_action_reserves_its_ctcp_wrapper_alongside_the_prefix() {
        let prefix = "Mnemosyne!~mcp-agent-f372cdb8-@127.0.0.1";
        let target = "#control";
        let input = SendInput {
            agent_id: crate::agent::AgentId::new(),
            target: target.parse().expect("target"),
            kind: SendKind::Action,
            text: Some("z".repeat(600)),
            tags: Vec::new(),
            reply_to: None,
            multiline: MultilinePolicy::Split,
            timeout_ms: 1_000,
            result_detail: ToolResultDetail::Full,
        };
        let messages =
            build_send_messages(&input, 512, prefix.len() + 4, 64 * 1024, 256).expect("send");
        for message in &messages {
            let body = message.trailing.as_deref().expect("trailing").len();
            assert!(relayed_overhead(prefix, target) + body <= 512);
        }
    }

    #[test]
    fn a_reservation_that_swallows_the_budget_is_rejected_not_truncated() {
        let input = SendInput {
            agent_id: crate::agent::AgentId::new(),
            target: "#control".parse().expect("target"),
            kind: SendKind::Privmsg,
            text: Some("hello".into()),
            tags: Vec::new(),
            reply_to: None,
            multiline: MultilinePolicy::Split,
            timeout_ms: 1_000,
            result_detail: ToolResultDetail::Full,
        };
        assert!(matches!(
            build_send_messages(&input, 512, 512, 64 * 1024, 256),
            Err(GatewayError::InvalidMessage(_))
        ));
    }
}
