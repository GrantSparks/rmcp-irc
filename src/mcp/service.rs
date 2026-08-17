//! One `rmcp` handler type shared by stdio and Streamable HTTP.

use std::{collections::BTreeSet, str::FromStr, sync::Arc, time::Duration};

use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::{router::tool::ToolRouter, tool::schema_for_output, wrapper::Parameters},
    model::{
        CallToolResult, ContentBlock, Implementation, ListResourceTemplatesResult,
        ListResourcesResult, PaginatedRequestParams, ReadResourceRequestParams,
        ReadResourceResponse, ReadResourceResult, Resource, ResourceContents, ResourceTemplate,
        ServerCapabilities, ServerInfo, SubscriptionFilter,
    },
    service::{RequestContext, RoleServer, SubscriptionContext},
    tool, tool_handler, tool_router,
};
use serde::Serialize;

use crate::{
    MCP_INSTRUCTIONS,
    agent::{
        actor::CompletionMode,
        journal::{EventClass, EventFilter, EventOrigin},
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
        resources::{AgentResourceUri, ResourceUris},
        tools::*,
    },
};

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
        output_schema = schema_for_output::<ConnectOutput>()
    )]
    async fn irc_connect(&self, Parameters(input): Parameters<ConnectInput>) -> CallToolResult {
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
                    motd: connected.motd,
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
        output_schema = schema_for_output::<DisconnectOutput>()
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
        output_schema = schema_for_output::<StatusOutput>()
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
                };
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
        output_schema = schema_for_output::<JoinOutput>()
    )]
    async fn irc_join(&self, Parameters(input): Parameters<JoinInput>) -> CallToolResult {
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
                command_tool_result(
                    format!("JOIN {}: {:?}.", input.channel, result.outcome),
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
        output_schema = schema_for_output::<CommandResult>()
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
        )
        .await
    }

    /// Send one logical IRC message, safely splitting only when requested.
    #[tool(
        name = "irc.send",
        description = "Send PRIVMSG, NOTICE, ACTION, or TAGMSG with negotiated-safe semantics.",
        output_schema = schema_for_output::<SendOutput>()
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
                command_tool_result(
                    format!("Sent {} IRC line(s).", output.line_count),
                    &output,
                    failed,
                )
            }
            Err(error) => tool_error(error),
        }
    }

    /// Read server-backed channel or private-message history.
    #[tool(
        name = "irc.history",
        description = "Read IRCv3 CHATHISTORY, with an explicitly reported legacy/unavailable fallback.",
        output_schema = schema_for_output::<HistoryOutput>()
    )]
    async fn irc_history(&self, Parameters(input): Parameters<HistoryInput>) -> CallToolResult {
        match self.history(input).await {
            Ok(output) => {
                let outcome = output.result.as_ref().map(|result| result.outcome);
                command_tool_result(
                    "History query completed.".into(),
                    &output,
                    outcome.filter(|outcome| is_failure_outcome(*outcome)),
                )
            }
            Err(error) => tool_error(error),
        }
    }

    /// Run one common query with typed required parameters.
    #[tool(
        name = "irc.query",
        description = "Run a typed WHOIS, WHO, NAMES, MODE, MOTD, HELP, or other common IRC query.",
        output_schema = schema_for_output::<CommandResult>()
    )]
    async fn irc_query(&self, Parameters(input): Parameters<QueryInput>) -> CallToolResult {
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
        )
        .await
    }

    /// Execute any syntactically valid structured IRC command.
    #[tool(
        name = "irc.execute",
        description = "Execute a structured IRC command without accepting raw CRLF-delimited lines.",
        output_schema = schema_for_output::<CommandResult>()
    )]
    async fn irc_execute(&self, Parameters(input): Parameters<ExecuteInput>) -> CallToolResult {
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
        )
        .await
    }

    /// Read ordered events after a caller-owned cursor, optionally long polling.
    #[tool(
        name = "irc.events.read",
        description = "Read an agent's bounded event journal after an explicit caller-owned cursor.",
        output_schema = schema_for_output::<EventsReadOutput>()
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
        output_schema = schema_for_output::<DccSessionOutput>()
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
        output_schema = schema_for_output::<DccChatSendOutput>()
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
        output_schema = schema_for_output::<DccSessionOutput>()
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
        output_schema = schema_for_output::<DccSessionOutput>()
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
        output_schema = schema_for_output::<DccSessionOutput>()
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
        output_schema = schema_for_output::<DccSessionOutput>()
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
        output_schema = schema_for_output::<DccListOutput>()
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
    ) -> CallToolResult {
        match self.execute(agent_id, message, mode, timeout_ms).await {
            Ok(result) => command_tool_result(
                format!("{operation}: {:?}.", result.outcome),
                &result,
                is_failure_outcome(result.outcome).then_some(result.outcome),
            ),
            Err(error) => tool_error(error),
        }
    }

    async fn send_message(&self, input: SendInput) -> Result<SendOutput, GatewayError> {
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
        Ok(HistoryOutput {
            availability,
            result: Some(result),
            events,
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
                .build(),
        )
        .with_server_info(Implementation::new(
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
        ))
        .with_instructions(MCP_INSTRUCTIONS)
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
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
        Ok(ListResourcesResult::with_all_items(resources))
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        Ok(ListResourceTemplatesResult::with_all_items(vec![
            ResourceTemplate::new(
                "irc://agents/{agent_id}/channels/{encoded_channel}",
                "irc-channel-state",
            )
            .with_title("IRC channel state")
            .with_description("Best-effort state for one channel joined by one explicit agent")
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
        let snapshot = self
            .gateway
            .snapshot(&uri.agent_id)
            .await
            .map_err(|error| McpError::resource_not_found(error.to_string(), None))?;
        let payload = snapshot
            .resource(&uri)
            .map_err(|error| McpError::resource_not_found(error.to_string(), None))?;
        let text = serde_json::to_string_pretty(&payload)
            .map_err(|error| McpError::internal_error(error.to_string(), None))?;
        Ok(ReadResourceResult::new(vec![
            ResourceContents::text(text, request.uri).with_mime_type("application/json"),
        ])
        .into())
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

fn build_send_messages(
    input: &SendInput,
    max_body_bytes: usize,
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
        .checked_sub(template.body_overhead().saturating_add(action_overhead))
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
        };
        let messages = build_send_messages(&input, 512, 5, 1).expect("message");
        assert_eq!(messages[0].tags[0].key, "+reply");
        assert!(matches!(
            build_send_messages(&input, 512, 4, 1),
            Err(GatewayError::ResourceLimit(_))
        ));
    }
}
