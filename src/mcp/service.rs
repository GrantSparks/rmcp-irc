//! One `rmcp` handler type shared by stdio and Streamable HTTP.

use std::{collections::BTreeSet, str::FromStr, sync::Arc, time::Duration};

use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::{router::tool::ToolRouter, tool::schema_for_output, wrapper::Parameters},
    model::{
        CallToolResult, CompleteRequestParams, CompleteResult, CompletionInfo, ContentBlock,
        Implementation, ListResourceTemplatesResult, ListResourcesResult, PaginatedRequestParams,
        ReadResourceRequestParams, ReadResourceResponse, ReadResourceResult, Reference, Resource,
        ResourceContents, ResourceTemplate, ServerCapabilities, ServerInfo, SubscriptionFilter,
    },
    service::{RequestContext, RoleServer, SubscriptionContext},
    tool, tool_handler, tool_router,
};
use serde::Serialize;

use crate::{
    MCP_INSTRUCTIONS,
    agent::{
        actor::CompletionMode,
        journal::{EventClass, EventCursor, EventFilter, EventOrigin},
    },
    dcc::session::DccSession,
    error::GatewayError,
    gateway::{ConnectRequest, Gateway},
    irc::{
        capabilities::CapabilityStatus,
        correlation::{CommandOutcome, CommandResult},
        wire::{OutboundMessage, Tag},
    },
    mcp::{
        resources::{AgentResourceUri, ResourceKind, ResourceUris, encode_channel_segment},
        tools::*,
    },
};

/// The one resource template this server exposes. Declared once so the
/// template listing and its argument completion cannot drift apart.
const CHANNEL_STATE_TEMPLATE: &str = "irc://agents/{agent_id}/channels/{encoded_channel}";

/// Cursor-page expansion of the per-agent events resource. Subscribing to
/// `irc://agents/{agent_id}/events` and reading this on each notification is a
/// complete delivery loop that needs no tool call.
const EVENT_CURSOR_TEMPLATE: &str = "irc://agents/{agent_id}/events/after/{sequence}";

/// Resources returned by one `resources/list` page. Six resources exist per
/// connected agent, so this keeps a full listing well inside client response
/// limits even at the configured agent ceiling.
const RESOURCE_PAGE_SIZE: usize = 60;

/// MCP request handler backed by a shared gateway.
#[derive(Clone, Debug)]
pub struct IrcMcpService {
    gateway: Arc<Gateway>,
    tool_router: ToolRouter<Self>,
}

impl IrcMcpService {
    /// Create a request handler for a shared gateway.
    pub fn new(gateway: Arc<Gateway>) -> Self {
        Self {
            gateway,
            tool_router: Self::tool_router(),
        }
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
    async fn irc_connect(&self, Parameters(input): Parameters<ConnectInput>) -> CallToolResult {
        let result_detail = input.result_detail;
        let request = match connect_request(input) {
            Ok(request) => request,
            Err(error) => return tool_error(error),
        };
        match self.gateway.connect(request).await {
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
                tool_success(summary, &output)
            }
            Err(error) => tool_error(error),
        }
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

    /// Read a consistent status snapshot.
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
                tool_success(
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
                command_tool_result(
                    format!("JOIN {}: {outcome:?}.", input.channel),
                    &JoinOutput {
                        resource: ResourceUris::channel(&input.agent_id, input.channel.as_str()),
                        channel: input.channel,
                        result,
                    },
                    failure,
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
        match self.history(input).await {
            Ok(output) => {
                let outcome = output.result.as_ref().map(|result| result.outcome);
                let failure = outcome.filter(|outcome| is_failure_outcome(*outcome));
                let summary = history_result_summary(failure);
                command_tool_result(summary, &output, failure)
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
        description = "Read an agent's bounded event journal after an explicit caller-owned cursor.",
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
        let filter = input.filter();
        match self
            .gateway
            .read_events(
                &input.agent_id,
                input.cursor,
                input.limit,
                Duration::from_millis(input.wait_ms),
                filter,
            )
            .await
        {
            Ok(page) => tool_success(
                format!(
                    "Read {} event(s); cursor is {}.",
                    page.events.len(),
                    page.next_cursor.sequence
                ),
                &page,
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
            Ok(session) => dcc_session_result("DCC CHAT offer written.", session),
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
            Ok(()) => tool_success(
                "DCC CHAT line queued.",
                &DccChatSendOutput {
                    dcc_session_id: input.dcc_session_id,
                    queued: true,
                },
            ),
            Err(error) => tool_error(error),
        }
    }

    /// Offer one local file without loading its body into memory.
    #[tool(
        name = "irc.dcc.send",
        description = "Offer and stream one local file through ordinary or reverse DCC SEND.",
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
            Ok(session) => dcc_session_result("DCC SEND offer written.", session),
            Err(error) => tool_error(error),
        }
    }

    /// Accept one incoming direct offer.
    #[tool(
        name = "irc.dcc.accept",
        description = "Accept one incoming DCC CHAT or SEND offer with explicit file conflict behavior.",
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
            Ok(session) => dcc_session_result("DCC offer accepted.", session),
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
            Ok(session) => dcc_session_result("DCC offer rejected.", session),
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
                dcc_session_result(&summary, session)
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
                tool_success(
                    format!("Found {} DCC session(s).", output.sessions.len()),
                    &output,
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
impl ServerHandler for IrcMcpService {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_resources_subscribe()
                .enable_resources_list_changed()
                .enable_completions()
                .build(),
        )
        .with_server_info(Implementation::new(
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
        ))
        .with_instructions(MCP_INSTRUCTIONS)
    }

    /// List agent resources one bounded page at a time.
    ///
    /// Six resources exist per connected agent, so at the configured agent
    /// ceiling an unpaginated reply would be large enough to break clients that
    /// bound response size. The cursor is the index of the first item on the
    /// next page, which is stable because `agent_ids` is ordered.
    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        let offset = match request.and_then(|request| request.cursor) {
            None => 0,
            Some(cursor) => cursor.parse::<usize>().map_err(|_| {
                McpError::invalid_params(format!("unrecognized resource cursor: {cursor}"), None)
            })?,
        };

        let mut resources = Vec::new();
        for agent_id in self.gateway.agent_ids().await {
            let uris = ResourceUris::for_agent(&agent_id);
            resources.extend(uris.named().into_iter().map(|(name, uri)| {
                Resource::new(uri, format!("{}-{name}", agent_id.as_str()))
                    .with_title(format!("IRC {name} for {agent_id}"))
                    .with_description(format!("In-memory {name} snapshot for {agent_id}"))
                    .with_mime_type("application/json")
            }));
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
            ResourceTemplate::new(EVENT_CURSOR_TEMPLATE, "irc-events-after")
                .with_title("IRC events after a cursor")
                .with_description(
                    "Every retained event after the given sequence, with the next cursor to \
                     read from. Subscribe to the agent's events resource and read this on \
                     each notification to consume the journal without polling.",
                )
                .with_mime_type("application/json"),
        ]))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, McpError> {
        let uri = AgentResourceUri::from_str(&request.uri)
            .map_err(|error| McpError::resource_not_found(error.to_string(), None))?;

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
        let text = if let ResourceKind::EventsAfter(sequence) = uri.kind {
            let page = self
                .gateway
                .read_events(
                    &uri.agent_id,
                    Some(EventCursor {
                        stream_id: snapshot.journal.stream_id.clone(),
                        sequence,
                    }),
                    self.gateway.config().limits.max_event_page_size,
                    Duration::ZERO,
                    EventFilter::default(),
                )
                .await
                .map_err(|error| McpError::internal_error(error.to_string(), None))?;
            serde_json::to_string_pretty(&page)
        } else {
            let payload = snapshot
                .resource(&uri)
                .map_err(|error| McpError::resource_not_found(error.to_string(), None))?;
            serde_json::to_string_pretty(&payload)
        }
        .map_err(|error| McpError::internal_error(error.to_string(), None))?;

        Ok(ReadResourceResult::new(vec![
            ResourceContents::text(text, request.uri).with_mime_type("application/json"),
        ])
        .into())
    }

    /// Complete the channel-state template arguments from live gateway state.
    ///
    /// Both arguments are drawn from sets the gateway already knows exactly, so
    /// a caller should never have to guess an agent handle or work out the
    /// percent-encoding of a channel name by hand.
    async fn complete(
        &self,
        request: CompleteRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, McpError> {
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
                .agent_ids()
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
                for agent_id in self.gateway.agent_ids().await {
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
        let mut updates = self.gateway.subscribe_resource_updates();
        loop {
            tokio::select! {
                () = context.cancelled() => return Ok(()),
                update = updates.recv() => match update {
                    Ok(uri) if uri == "irc://agents" => {
                        let _ = context.sink().notify_resource_list_changed().await;
                    }
                    Ok(uri) => {
                        let _ = context.sink().notify_resource_updated(uri).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
    }
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

fn dcc_session_result(summary: &str, session: DccSession) -> CallToolResult {
    tool_success(summary, &DccSessionOutput { session })
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

fn command_tool_result(
    summary: String,
    value: &impl Serialize,
    failure: Option<CommandOutcome>,
) -> CallToolResult {
    structured_result(summary, value, failure.is_some())
}

fn structured_result(
    summary: impl Into<String>,
    value: &impl Serialize,
    is_error: bool,
) -> CallToolResult {
    match serde_json::to_value(value) {
        Ok(value) => {
            let mut result = if is_error {
                CallToolResult::structured_error(value)
            } else {
                CallToolResult::structured(value)
            };
            result.content = vec![ContentBlock::text(summary)];
            result
        }
        Err(error) => CallToolResult::error(vec![ContentBlock::text(format!(
            "could not serialize typed tool output: {error}"
        ))]),
    }
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
    fn stable_tool_list_is_exact_and_schema_backed() {
        let service = IrcMcpService::new(Arc::new(Gateway::new(Default::default())));
        let tools = service.tool_router.list_all();
        let names: BTreeSet<_> = tools.iter().map(|tool| tool.name.as_ref()).collect();
        assert_eq!(names, TOOL_NAMES.iter().copied().collect());
        assert!(tools.iter().all(|tool| tool.output_schema.is_some()));
        let _ = CallToolRequestParams::new("irc.status");
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
            let Some(description) = property_description(&tool.input_schema, "agent_id") else {
                continue;
            };
            assert!(
                description.contains("irc.connect"),
                "{}: agent_id description does not name its source: {description:?}",
                tool.name
            );
            checked += 1;
        }
        // Every tool except irc.connect itself takes a handle.
        assert_eq!(checked, TOOL_NAMES.len() - 1);
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
