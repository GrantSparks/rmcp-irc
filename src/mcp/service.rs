//! One `rmcp` handler type shared by stdio and Streamable HTTP.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::{
        router::{prompt::PromptRouter, tool::ToolRouter},
        tool::{ToolCallContext, schema_for_output},
        wrapper::Parameters,
    },
    model::{
        Annotations, CallToolRequestParams, CallToolResponse, CallToolResult, CancelTaskParams,
        CompleteRequestParams, CompleteResult, CompletionInfo, ContentBlock, CreateTaskResult,
        GetTaskParams, GetTaskResult, Implementation, ListResourceTemplatesResult,
        ListResourcesResult, PaginatedRequestParams, PromptMessage, ReadResourceRequestParams,
        ReadResourceResponse, ReadResourceResult, Reference, Resource, ResourceContents,
        ResourceTemplate, Role, ServerCapabilities, ServerInfo, SubscriptionFilter,
        TASKS_EXTENSION_ID, UpdateTaskParams,
    },
    prompt, prompt_handler, prompt_router,
    service::{RequestContext, RoleServer, SubscriptionContext},
    task_manager::{TaskContext, TaskExit, TaskManager, TaskOptions},
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    MCP_INSTRUCTIONS,
    agent::AgentId,
    agent::{
        actor::CompletionMode,
        journal::{EventClass, EventCursor, EventFilter, EventOrigin},
    },
    dcc::session::{DccSession, DccSessionId},
    error::GatewayError,
    gateway::{ConnectRequest, ConversationWindow, Gateway},
    irc::{
        capabilities::CapabilityStatus,
        correlation::{CommandOutcome, CommandResult},
        wire::{OutboundMessage, Tag},
    },
    mcp::{
        authorization::{CallerPolicy, OwnerId},
        resources::{
            AgentResourceUri, ResourceDescriptor, ResourceKind, ResourcePayload, ResourceUris,
            describe as describe_resource, descriptors_for_agent, encode_channel_segment,
        },
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
    "irc-watch-mentions",
    "irc-join",
    "irc-summarize-respond",
];

/// Tools that can be run as MCP tasks.
///
/// Only the two DCC operations that genuinely outlive their request qualify.
/// Everything else here completes in one round trip, and wrapping a fast
/// operation in a task handle would cost the caller a poll for no benefit.
const TASK_AUGMENTED_TOOLS: &[&str] = &["irc.dcc.send", "irc.dcc.accept"];

/// How often a running transfer republishes its progress as a task status
/// message. Slow enough not to churn, fast enough that a stalled transfer is
/// visible well before its deadline.
const TASK_PROGRESS_INTERVAL: Duration = Duration::from_millis(500);

/// MCP request handler backed by a shared gateway.
#[derive(Clone)]
pub struct IrcMcpService {
    gateway: Arc<Gateway>,
    tool_router: ToolRouter<Self>,
    prompt_router: PromptRouter<Self>,
    typing_deadlines: Arc<Mutex<BTreeMap<(crate::agent::AgentId, String), Instant>>>,
    tasks: TaskManager,
    callers: CallerPolicy,
}

impl std::fmt::Debug for IrcMcpService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The task manager holds running futures rather than inspectable data,
        // so the count is the only part of it worth printing.
        formatter
            .debug_struct("IrcMcpService")
            .field("gateway", &self.gateway)
            .field("running_tasks", &self.tasks.running_task_count())
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
            tasks: TaskManager::new(),
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
                "Establish a new IRC collaboration session. {nickname} Call `irc.connect`, then read and follow the returned MOTD before participating. Read the auto-joined channel topic, announce a concise hello with real task/worktree scope, and preserve the returned `agent_id` and native resource links for subsequent operations. Do not invent account registration for an ephemeral guest."
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
                 the host to call `subscriptions/listen` with that watch URI in \
                 `resourceSubscriptions`; the notification is filtered by the watch, so it means \
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
                 active client request. Autonomous participation belongs to the host's scheduler \
                 or a separately configured direct LLM integration, not to this relay contract.",
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
                "Using IRC agent `{}`, call `irc.join` for `{}`. Follow the returned native channel resource link, read the topic before sending messages, and treat it as channel-specific instruction. Read the recent transcript/history and known members to avoid duplicating active work, then announce relevant intent and subscribe to the channel's live resources when supported.",
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
        description = "Connect one mythologically named guest to the configured Ergo server.",
        output_schema = schema_for_output::<ConnectOutput>(),
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
    ) -> Result<CallToolResult, McpError> {
        let owner = self.callers.identify(&context)?;
        let result_detail = input.result_detail;
        let request = match connect_request(input) {
            Ok(request) => request,
            Err(error) => return Ok(tool_error(error)),
        };
        Ok(match self.gateway.connect_as(owner, request).await {
            Ok(connected) => {
                let output = ConnectOutput {
                    resources: ResourceUris::for_agent(&connected.agent_id),
                    agent_id: connected.agent_id,
                    nickname: connected.nickname.clone(),
                    nickname_adjusted: connected.nickname_adjusted,
                    registered: true,
                    motd: motd_for_tool_result(connected.motd, result_detail),
                    result_detail,
                };
                let summary = if output.motd.text.is_empty() {
                    format!(
                        "Connected {} as {}. The server has no MOTD.",
                        output.agent_id, output.nickname
                    )
                } else {
                    format!(
                        "Connected {} as {}. Server MOTD:\n{}",
                        output.agent_id, output.nickname, output.motd.text
                    )
                };
                let content = agent_resource_links(&output.resources);
                tool_success_with_content(summary, &output, content)
            }
            Err(error) => tool_error(error),
        })
    }

    /// Disconnect and destroy one actor and all of its direct sessions.
    #[tool(
        name = "irc.disconnect",
        description = "Disconnect one explicit IRC guest and invalidate its process-local handle.",
        output_schema = schema_for_output::<DisconnectOutput>(),
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
        output_schema = schema_for_output::<WatchCreateOutput>(),
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
                    "Watching {} at {}. Subscribe to that URI, then on each notification call \
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
        output_schema = schema_for_output::<WatchCloseOutput>(),
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
        output_schema = schema_for_output::<StatusOutput>(),
        annotations(
            title = "Read guest status",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn irc_status(&self, Parameters(input): Parameters<AgentInput>) -> CallToolResult {
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
        description = "Join one channel using the actor's correlated IRC command path.",
        output_schema = schema_for_output::<JoinOutput>(),
        annotations(
            title = "Join channel",
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_join(&self, Parameters(input): Parameters<JoinInput>) -> CallToolResult {
        let result_detail = input.result_detail;
        if let Err(error) = validate_irc_atom(input.channel.as_str(), "channel") {
            return tool_error(error);
        }
        let mut params = vec![input.channel.to_string()];
        if let Some(key) = input.key {
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
                let failure = is_failure_outcome(result.outcome).then_some(result.outcome);
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
                command_tool_result_with_content(
                    format!("JOIN {}: {outcome:?}.", output.channel),
                    &output,
                    failure,
                    content,
                )
            }
            Err(error) => tool_error(error),
        }
    }

    /// Part one channel.
    #[tool(
        name = "irc.part",
        description = "Part one channel and correlate the server echo or rejection.",
        output_schema = schema_for_output::<CommandResult>(),
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
        output_schema = schema_for_output::<SendOutput>(),
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
                let failed = output
                    .results
                    .iter()
                    .find(|result| {
                        result.outcome != CommandOutcome::Completed
                            && result.outcome != CommandOutcome::SentUnconfirmed
                    })
                    .map(|result| result.outcome);
                let summary = send_result_summary(output.line_count, failed);
                command_tool_result(summary, &output, failed)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Read server-backed channel or private-message history.
    #[tool(
        name = "irc.history",
        description = "Read IRCv3 CHATHISTORY, with an explicitly reported legacy/unavailable fallback.",
        output_schema = schema_for_output::<HistoryOutput>(),
        annotations(
            title = "Read channel history",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn irc_history(&self, Parameters(input): Parameters<HistoryInput>) -> CallToolResult {
        let agent_id = input.agent_id.clone();
        let channel = input.target.channel().cloned();
        match self.history(input).await {
            Ok(output) => {
                let outcome = output.result.as_ref().map(|result| result.outcome);
                let failure = outcome.filter(|outcome| is_failure_outcome(*outcome));
                let summary = history_result_summary(failure);
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
        output_schema = schema_for_output::<CommandResult>(),
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
        output_schema = schema_for_output::<WhoisOutput>(),
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
        output_schema = schema_for_output::<NamesOutput>(),
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
        output_schema = schema_for_output::<ListOutput>(),
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
        output_schema = schema_for_output::<ModeGetOutput>(),
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
        output_schema = schema_for_output::<HelpOutput>(),
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
        output_schema = schema_for_output::<TopicOutput>(),
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
        output_schema = schema_for_output::<TopicOutput>(),
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
        output_schema = schema_for_output::<NickSetOutput>(),
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
        output_schema = schema_for_output::<AwaySetOutput>(),
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
        description = "Remove one nickname from a channel through a stable typed mutation.",
        output_schema = schema_for_output::<KickOutput>(),
        annotations(
            title = "Kick channel member",
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn irc_kick(&self, Parameters(input): Parameters<KickInput>) -> CallToolResult {
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
                command_tool_result_with_content(
                    format!("KICK: {outcome:?}."),
                    &output,
                    failure,
                    vec![channel_resource_link(resource, output.channel.as_str())],
                )
            }
            Err(error) => tool_error(error),
        }
    }

    /// Invite one nickname to a channel.
    #[tool(
        name = "irc.invite",
        description = "Invite one nickname to a channel through a stable typed mutation.",
        output_schema = schema_for_output::<InviteOutput>(),
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
        output_schema = schema_for_output::<MonitorUpdateOutput>(),
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
        output_schema = schema_for_output::<ModeSetOutput>(),
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
        output_schema = schema_for_output::<ReactionUpdateOutput>(),
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
        description = "Redact one server-identified message through negotiated IRCv3 message redaction.",
        output_schema = schema_for_output::<MessageRedactOutput>(),
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
    ) -> CallToolResult {
        if let Err(error) = validate_irc_atom(&input.message_id, "message_id") {
            return tool_error(error);
        }
        let snapshot = match self.gateway.snapshot(&input.agent_id).await {
            Ok(snapshot) => snapshot,
            Err(error) => return tool_error(error),
        };
        for (capability, operation) in [
            ("message-tags", "IRCv3 message redaction"),
            ("message-redaction", "IRCv3 message redaction"),
        ] {
            if let Err(error) = require_capability(&snapshot, capability, operation) {
                return tool_error(error);
            }
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
                command_tool_result(format!("Message redaction: {outcome:?}."), &output, failure)
            }
            Err(error) => tool_error(error),
        }
    }

    /// Read one synchronized conversation read marker.
    #[tool(
        name = "irc.read.get",
        description = "Read one synchronized IRCv3 conversation marker after exact capability negotiation.",
        output_schema = schema_for_output::<ReadMarkerOutput>(),
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
        output_schema = schema_for_output::<ReadMarkerOutput>(),
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
        output_schema = schema_for_output::<TypingSetOutput>(),
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
        output_schema = schema_for_output::<CommandResult>(),
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
        output_schema = schema_for_output::<EventsReadOutput>(),
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
            Ok(page) => tool_success_with_content(
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
            ),
            Err(error) => tool_error(error),
        }
    }

    /// Offer a direct peer chat.
    #[tool(
        name = "irc.dcc.chat.open",
        description = "Send an ordinary or reverse DCC CHAT offer to one peer.",
        output_schema = schema_for_output::<DccSessionOutput>(),
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
        output_schema = schema_for_output::<DccChatSendOutput>(),
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
                       Request the tasks extension on the call to follow the transfer as a task \
                       with progress and cancellation instead of returning once the offer is \
                       written.",
        output_schema = schema_for_output::<DccSessionOutput>(),
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
        description = "Accept one incoming DCC CHAT or SEND offer with explicit file conflict \
                       behavior. Request the tasks extension on the call to follow the transfer \
                       as a task with progress and cancellation instead of returning once the \
                       acceptance is written.",
        output_schema = schema_for_output::<DccSessionOutput>(),
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
    ) -> CallToolResult {
        match self
            .gateway
            .dcc_accept(
                &input.agent_id,
                input.dcc_session_id,
                input.destination_path,
                input.conflict,
            )
            .await
        {
            Ok(session) => dcc_session_result("DCC offer accepted.", &input.agent_id, session),
            Err(error) => tool_error(error),
        }
    }

    /// Reject one incoming offer.
    #[tool(
        name = "irc.dcc.reject",
        description = "Reject one incoming offered DCC session.",
        output_schema = schema_for_output::<DccSessionOutput>(),
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
        output_schema = schema_for_output::<DccSessionOutput>(),
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
        output_schema = schema_for_output::<DccListOutput>(),
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
                let failure = is_failure_outcome(outcome).then_some(outcome);
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

    async fn history(&self, input: HistoryInput) -> Result<HistoryOutput, GatewayError> {
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
        .with_server_info(Implementation::new(
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
        ))
        .with_instructions(MCP_INSTRUCTIONS)
    }

    /// Route one tool call, running the long DCC operations as MCP tasks when
    /// the caller asked for that and can follow one.
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

        if TASK_AUGMENTED_TOOLS.contains(&request.name.as_ref())
            && let Some(options) = requested_task_options(&request)
        {
            let service = self.clone();
            let context = context.clone();
            let task = self.tasks.spawn(options, move |task_context| {
                Box::pin(async move { service.run_dcc_task(request, context, task_context).await })
            });
            return Ok(CreateTaskResult::new(task).into());
        }
        let call = ToolCallContext::new(self, request, context);
        self.tool_router.call(call).await
    }

    async fn get_task(
        &self,
        request: GetTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<GetTaskResult, McpError> {
        self.tasks
            .get_task(&request.task_id)
            .map(GetTaskResult::new)
    }

    async fn update_task(
        &self,
        request: UpdateTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<(), McpError> {
        self.tasks
            .update_task(&request.task_id, request.input_responses)
    }

    async fn cancel_task(
        &self,
        request: CancelTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<(), McpError> {
        self.tasks.cancel_task(&request.task_id)
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
        if let Some(agent_id) = arguments
            .get("agent_id")
            .and_then(|value| value.as_str())
            .and_then(|value| AgentId::from_str(value).ok())
        {
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
        let started = match self.tool_router.call(call).await? {
            CallToolResponse::Complete(result) => result,
            // Neither is reachable: no tool in this service asks for client
            // input or answers with a task of its own, and only this wrapper
            // can create one.
            other => {
                return Err(TaskExit::Error(McpError::internal_error(
                    format!("a task-augmented tool returned an unexpected response: {other:?}"),
                    None,
                )));
            }
        };
        // A rejected offer is a finished task, not a transfer to follow.
        let Some(session_id) = started_session_id(&started) else {
            return Ok(started);
        };

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
            if session.state.is_terminal() {
                let link = describe_resource(
                    &AgentResourceUri {
                        agent_id: agent_id.clone(),
                        kind: ResourceKind::DccSession(session_id),
                    },
                    Some(session.updated_at),
                )
                .into_resource();
                let summary = terminal_session_summary(&session);
                return Ok(tool_success_with_content(
                    summary,
                    &session,
                    vec![ContentBlock::ResourceLink(link)],
                ));
            }
            task.set_status_message(progress_summary(&session));
            tokio::select! {
                () = task.cancelled() => continue,
                () = tokio::time::sleep(TASK_PROGRESS_INTERVAL) => {}
            }
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

/// Task options when the caller asked for this call to run as a task.
///
/// Opting in is the client's decision, signalled by the tasks extension key in
/// request metadata. A caller that did not ask still gets the ordinary
/// synchronous result, so nothing that worked before starts returning a handle
/// the caller cannot follow.
fn requested_task_options(request: &CallToolRequestParams) -> Option<TaskOptions> {
    let requested = request.meta.as_ref()?.get(TASKS_EXTENSION_ID)?;
    let mut options = TaskOptions::new()
        .with_poll_interval_ms(TASK_PROGRESS_INTERVAL.as_millis() as u64)
        .with_status_message("Negotiating the direct connection.");
    // A caller may name its own retention window; anything else just takes the
    // default rather than failing a call over an advisory hint.
    if let Some(ttl_ms) = requested.get("ttl").and_then(serde_json::Value::as_u64) {
        options = options.with_ttl_ms(ttl_ms);
    }
    Some(options)
}

/// The agent handle a task-augmented DCC call names.
fn task_agent_id(request: &CallToolRequestParams) -> Result<AgentId, TaskExit> {
    request
        .arguments
        .as_ref()
        .and_then(|arguments| arguments.get("agent_id"))
        .and_then(|value| value.as_str())
        .and_then(|value| AgentId::from_str(value).ok())
        .ok_or_else(|| {
            TaskExit::Error(McpError::invalid_params(
                "a task-augmented DCC call must name a valid agent_id",
                None,
            ))
        })
}

/// The session a DCC tool result reports having started, when it started one.
fn started_session_id(result: &CallToolResult) -> Option<DccSessionId> {
    result
        .structured_content
        .as_ref()?
        .get("session")?
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

fn connect_request(input: ConnectInput) -> Result<ConnectRequest, GatewayError> {
    let nickname = input.nickname;
    let nickname_fallbacks = input.nickname_fallbacks;
    let channels: BTreeSet<_> = input.channels.into_iter().collect();
    Ok(ConnectRequest {
        nickname,
        nickname_fallbacks,
        nick_conflict_policy: input.nick_conflict_policy,
        username: input.username,
        real_name: input.real_name,
        channels,
    })
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

fn command_failure(result: &CommandResult) -> Option<CommandOutcome> {
    is_failure_outcome(result.outcome).then_some(result.outcome)
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

fn tool_success(summary: impl Into<String>, value: &impl Serialize) -> CallToolResult {
    structured_result(summary, value, false)
}

/// Succeed with additional native content blocks after the text summary, so a
/// client can recognize a returned resource as something to attach or
/// subscribe to rather than as a URI printed inside JSON.
fn tool_success_with_content(
    summary: impl Into<String>,
    value: &impl Serialize,
    content: Vec<ContentBlock>,
) -> CallToolResult {
    structured_result_with_content(summary, value, false, content)
}

fn command_tool_result(
    summary: String,
    value: &impl Serialize,
    failure: Option<CommandOutcome>,
) -> CallToolResult {
    structured_result(summary, value, failure.is_some())
}

fn command_tool_result_with_content(
    summary: String,
    value: &impl Serialize,
    failure: Option<CommandOutcome>,
    content: Vec<ContentBlock>,
) -> CallToolResult {
    structured_result_with_content(summary, value, failure.is_some(), content)
}

fn structured_result(
    summary: impl Into<String>,
    value: &impl Serialize,
    is_error: bool,
) -> CallToolResult {
    structured_result_with_content(summary, value, is_error, Vec::new())
}

fn structured_result_with_content(
    summary: impl Into<String>,
    value: &impl Serialize,
    is_error: bool,
    mut content: Vec<ContentBlock>,
) -> CallToolResult {
    match serde_json::to_value(value) {
        Ok(value) => {
            let mut result = if is_error {
                CallToolResult::structured_error(value)
            } else {
                CallToolResult::structured(value)
            };
            content.insert(0, ContentBlock::text(summary));
            result.content = content;
            result
        }
        Err(error) => CallToolResult::error(vec![ContentBlock::text(format!(
            "could not serialize typed tool output: {error}"
        ))]),
    }
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
    let output = ToolErrorOutput {
        kind: error.kind().as_str().into(),
        message: error.to_string(),
        retriable: error.retriable(),
    };
    structured_result(output.message.clone(), &output, true)
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

    #[test]
    fn only_a_caller_that_asked_for_a_task_gets_one() {
        let mut request = CallToolRequestParams::new("irc.dcc.send");
        assert!(
            requested_task_options(&request).is_none(),
            "a call with no task metadata must still complete synchronously"
        );

        let mut meta = rmcp::model::RequestMetaObject::new();
        meta.insert(TASKS_EXTENSION_ID.to_string(), serde_json::json!({}));
        request.meta = Some(meta);
        let options = requested_task_options(&request).expect("opted in");
        assert_eq!(
            options.ttl_ms,
            Some(rmcp::task_manager::DEFAULT_TASK_TTL_MS),
            "an opt-in without a ttl takes the default rather than failing"
        );

        let mut meta = rmcp::model::RequestMetaObject::new();
        meta.insert(
            TASKS_EXTENSION_ID.to_string(),
            serde_json::json!({ "ttl": 60_000 }),
        );
        request.meta = Some(meta);
        assert_eq!(
            requested_task_options(&request).expect("opted in").ttl_ms,
            Some(60_000)
        );
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
        let result = CallToolResult::structured(serde_json::json!({
            "session": { "id": session_id.to_string() }
        }));
        assert_eq!(started_session_id(&result), Some(session_id));

        // A tool that started nothing to follow settles the task immediately.
        assert_eq!(
            started_session_id(&CallToolResult::structured(serde_json::json!({}))),
            None
        );
    }

    #[test]
    fn initialization_instructions_are_deliberately_small() {
        let service = IrcMcpService::new(Arc::new(Gateway::new(Default::default())));
        let info = service.get_info();
        assert_eq!(info.instructions.as_deref(), Some(MCP_INSTRUCTIONS));
        assert!(!MCP_INSTRUCTIONS.contains("AGENT"));
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
        assert!(tools.iter().all(|tool| tool.output_schema.is_some()));
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
            for (property, source) in [
                ("agent_id", "irc.connect"),
                ("watch_id", "irc.watch.create"),
            ] {
                let Some(description) = property_description(&tool.input_schema, property) else {
                    continue;
                };
                assert!(
                    description.contains(source),
                    "{}: {property} description does not name its source: {description:?}",
                    tool.name
                );
                checked += 1;
            }
        }
        // Every tool takes a handle except irc.connect, which mints one, and
        // irc.events.read takes both: an agent and, optionally, the watch whose
        // selection it should read through.
        assert_eq!(checked, TOOL_NAMES.len());
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
                &["suffix", "fail"][..],
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
            Some(CommandOutcome::Rejected),
        );
        assert_eq!(send_result.is_error, Some(true));
        assert_eq!(send_result.content[0].as_text().expect("text").text, send);

        let history = history_result_summary(Some(CommandOutcome::TimedOut));
        assert_eq!(history, "History query failed: TimedOut.");
        assert!(!history.contains("completed"));
        let history_result = command_tool_result(
            history.clone(),
            &serde_json::json!({"outcome": "timed_out"}),
            Some(CommandOutcome::TimedOut),
        );
        assert_eq!(history_result.is_error, Some(true));
        assert_eq!(
            history_result.content[0].as_text().expect("text").text,
            history
        );
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
