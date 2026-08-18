//! Typed configuration loading and validation.

use std::{
    collections::BTreeSet,
    env, fs,
    net::IpAddr,
    path::{Path, PathBuf},
};

use secrecy::SecretString;
use serde::{Deserialize, Serialize};

use crate::error::{GatewayError, Result};

/// Process configuration.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// IRC endpoint used by every guest agent.
    pub irc: IrcEndpoint,
    /// Text and defaults used while creating guest identities.
    pub onboarding: OnboardingConfig,
    /// Reconnect policy for agent actors.
    pub reconnect: ReconnectConfig,
    /// In-memory resource limits.
    pub limits: GatewayLimits,
    /// Direct-client-to-client networking configuration.
    pub dcc: DccConfig,
    /// Policy for the MCP surface itself, rather than for IRC.
    pub mcp: McpConfig,
}

impl Config {
    /// Load and validate a TOML configuration file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let source = fs::read_to_string(path).map_err(|error| {
            GatewayError::Configuration(format!("could not read {}: {error}", path.display()))
        })?;
        let config: Self = toml::from_str(&source).map_err(|error| {
            GatewayError::Configuration(format!("could not parse {}: {error}", path.display()))
        })?;
        config.validate()?;
        Ok(config)
    }

    /// Validate relationships between configuration values.
    pub fn validate(&self) -> Result<()> {
        if self.irc.host.trim().is_empty() {
            return Err(GatewayError::Configuration(
                "irc.host must not be empty".into(),
            ));
        }
        if self.irc.port == 0 {
            return Err(GatewayError::Configuration(
                "irc.port must be greater than zero".into(),
            ));
        }
        if self.onboarding.nickname_attempts == 0 {
            return Err(GatewayError::Configuration(
                "onboarding.nickname_attempts must be greater than zero".into(),
            ));
        }
        if self.onboarding.connect_timeout_ms == 0 {
            return Err(GatewayError::Configuration(
                "onboarding.connect_timeout_ms must be greater than zero".into(),
            ));
        }
        if self.onboarding.nickname_instruction.trim().is_empty() {
            return Err(GatewayError::Configuration(
                "onboarding.nickname_instruction must not be empty".into(),
            ));
        }
        validate_template(
            &self.onboarding.username_template,
            "onboarding.username_template",
            &["{agent_id}", "{agent_short}", "{client}"],
        )?;
        validate_template(
            &self.onboarding.real_name_template,
            "onboarding.real_name_template",
            &[
                "{agent_id}",
                "{agent_short}",
                "{nickname}",
                "{client}",
                "{client_full}",
            ],
        )?;
        for channel in &self.onboarding.initial_channels {
            if channel.trim().is_empty()
                || channel
                    .bytes()
                    .any(|byte| matches!(byte, b'\0' | b'\r' | b'\n' | b' ' | b','))
            {
                return Err(GatewayError::Configuration(format!(
                    "onboarding.initial_channels contains an invalid IRC channel: {channel:?}"
                )));
            }
        }
        if self.reconnect.initial_delay_ms > self.reconnect.max_delay_ms {
            return Err(GatewayError::Configuration(
                "reconnect.initial_delay_ms must not exceed max_delay_ms".into(),
            ));
        }
        if !self.reconnect.multiplier.is_finite() || self.reconnect.multiplier < 1.0 {
            return Err(GatewayError::Configuration(
                "reconnect.multiplier must be finite and at least 1.0".into(),
            ));
        }
        if !self.reconnect.jitter.is_finite() || !(0.0..=1.0).contains(&self.reconnect.jitter) {
            return Err(GatewayError::Configuration(
                "reconnect.jitter must be between 0.0 and 1.0".into(),
            ));
        }
        if self.limits.max_agents == 0
            || self.limits.command_queue == 0
            || self.limits.pending_commands == 0
            || self.limits.active_batches == 0
            || self.limits.replies_per_command == 0
            || self.limits.event_count == 0
            || self.limits.event_bytes == 0
            || self.limits.max_message_bytes == 0
            || self.limits.max_message_parts == 0
            || self.limits.max_command_timeout_ms == 0
            || self.limits.max_event_wait_ms == 0
            || self.limits.max_event_page_size == 0
            || self.limits.max_watches_per_owner == 0
            || self.limits.watch_ttl_ms == 0
        {
            return Err(GatewayError::Configuration(
                "gateway count, byte, queue, and timeout limits must be greater than zero".into(),
            ));
        }
        if self.limits.max_line_bytes < 512 {
            return Err(GatewayError::Configuration(
                "limits.max_line_bytes must permit the traditional 512-byte IRC line".into(),
            ));
        }
        if self.dcc.port_start == 0 || self.dcc.port_start > self.dcc.port_end {
            return Err(GatewayError::Configuration(
                "dcc.port_start must be non-zero and less than or equal to dcc.port_end".into(),
            ));
        }
        if self.dcc.download_directory.as_os_str().is_empty() {
            return Err(GatewayError::Configuration(
                "dcc.download_directory must not be empty".into(),
            ));
        }
        validate_receive_roots(&self.dcc.receive_roots)?;
        if self.dcc.max_sessions == 0
            || self.dcc.max_offers_per_peer == 0
            || self.dcc.transfer_buffer_bytes == 0
            || self.dcc.max_transfer_bytes == 0
            || self.dcc.offer_ttl_ms == 0
            || self.dcc.connect_timeout_ms == 0
            || self.dcc.idle_timeout_ms == 0
            || self.dcc.chat_queue == 0
            || self.dcc.chat_line_bytes == 0
        {
            return Err(GatewayError::Configuration(
                "DCC session, offer, transfer, queue, and timeout bounds must be greater than zero"
                    .into(),
            ));
        }
        if let Some(reference) = &self.irc.server_password {
            reference.validate("irc.server_password")?;
        }
        if let Some(sasl) = &self.irc.sasl {
            if sasl.username.trim().is_empty() {
                return Err(GatewayError::Configuration(
                    "irc.sasl.username must not be empty".into(),
                ));
            }
            sasl.password.validate("irc.sasl.password")?;
        }
        Ok(())
    }

    /// Resolve optional endpoint credentials immediately before connecting.
    pub fn resolve_credentials(&self) -> Result<ResolvedCredentials> {
        Ok(ResolvedCredentials {
            server_password: self
                .irc
                .server_password
                .as_ref()
                .map(CredentialRef::resolve)
                .transpose()?,
            sasl: self
                .irc
                .sasl
                .as_ref()
                .map(|sasl| {
                    Ok(ResolvedSasl {
                        username: sasl.username.clone(),
                        password: sasl.password.resolve()?,
                    })
                })
                .transpose()?,
        })
    }
}

/// Reject a receive-root list that could not be selected from unambiguously.
///
/// A root name reaches this process from a tool argument and leaves it in an
/// elicitation form, so it is constrained to an identifier charset: anything
/// that needed quoting, or that differs from another name only in case, would
/// make the caller's choice ambiguous at exactly the moment the choice is the
/// only thing standing between a peer's file and the filesystem. Startup is the
/// right place to refuse, because a misdeclared root cannot be corrected from a
/// tool call.
fn validate_receive_roots(roots: &[DccReceiveRoot]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for root in roots {
        if root.name.is_empty()
            || root.name.len() > 64
            || !root
                .name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(GatewayError::Configuration(format!(
                "dcc.receive_roots name must be 1-64 characters of letters, digits, '_', '-', or \
                 '.': {:?}",
                root.name
            )));
        }
        if root.path.as_os_str().is_empty() {
            return Err(GatewayError::Configuration(format!(
                "dcc.receive_roots path must not be empty for root {:?}",
                root.name
            )));
        }
        if !seen.insert(root.name.to_ascii_lowercase()) {
            return Err(GatewayError::Configuration(format!(
                "dcc.receive_roots names must be unique regardless of case: {:?}",
                root.name
            )));
        }
    }
    Ok(())
}

fn validate_template(value: &str, field: &str, placeholders: &[&str]) -> Result<()> {
    if value.trim().is_empty() {
        return Err(GatewayError::Configuration(format!(
            "{field} must not be empty"
        )));
    }
    let mut remainder = value.to_owned();
    for placeholder in placeholders {
        remainder = remainder.replace(placeholder, "");
    }
    if remainder.contains('{') || remainder.contains('}') {
        return Err(GatewayError::Configuration(format!(
            "{field} contains an unknown or unmatched placeholder"
        )));
    }
    Ok(())
}

/// Network transport used for the configured Ergo endpoint.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IrcTransport {
    /// Unencrypted TCP, suitable for a trusted internal network.
    #[default]
    Plain,
    /// TLS with certificates validated against native trust roots.
    Tls,
}

/// Connection details for the existing Ergo server.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IrcEndpoint {
    /// DNS name or IP address.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// Plain TCP or TLS, according to the endpoint.
    pub transport: IrcTransport,
    /// Optional server PASS value referenced indirectly through an environment variable.
    pub server_password: Option<CredentialRef>,
    /// Optional SASL PLAIN configuration when required by the endpoint.
    pub sasl: Option<SaslConfig>,
}

impl Default for IrcEndpoint {
    fn default() -> Self {
        Self {
            host: "irc".into(),
            port: 6667,
            transport: IrcTransport::Plain,
            server_password: None,
            sasl: None,
        }
    }
}

/// Environment variable containing a credential.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialRef {
    /// Name of an environment variable containing the credential.
    pub env: String,
}

impl std::fmt::Debug for CredentialRef {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialRef")
            .field("env", &self.env)
            .finish_non_exhaustive()
    }
}

impl CredentialRef {
    fn validate(&self, field: &str) -> Result<()> {
        if self.env.trim().is_empty() {
            return Err(GatewayError::Configuration(format!(
                "{field}.env must not be empty"
            )));
        }
        Ok(())
    }

    fn resolve(&self) -> Result<SecretString> {
        let value = env::var(&self.env).map_err(|error| {
            GatewayError::Configuration(format!(
                "credential environment variable {} is unavailable: {error}",
                self.env
            ))
        })?;
        Ok(SecretString::from(value))
    }
}

/// Optional SASL PLAIN credentials required by an IRC endpoint.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SaslConfig {
    /// SASL authentication identity.
    pub username: String,
    /// Password supplied through an environment-variable reference.
    pub password: CredentialRef,
}

/// Endpoint credentials resolved for one connection attempt.
#[derive(Debug)]
pub struct ResolvedCredentials {
    /// Optional IRC PASS value.
    pub server_password: Option<SecretString>,
    /// Optional SASL PLAIN identity.
    pub sasl: Option<ResolvedSasl>,
}

/// Resolved SASL PLAIN identity whose password remains redacted in Debug.
#[derive(Debug)]
pub struct ResolvedSasl {
    /// Authentication identity.
    pub username: String,
    /// Secret password.
    pub password: SecretString,
}

/// Guest registration defaults and initial joins.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct OnboardingConfig {
    /// Short instruction sent to MCP clients before they create an agent.
    pub nickname_instruction: String,
    /// Guest username template.
    pub username_template: String,
    /// Guest real-name template.
    pub real_name_template: String,
    /// Channels joined after successful registration.
    pub initial_channels: Vec<String>,
    /// Maximum nickname candidates attempted during initial registration.
    pub nickname_attempts: usize,
    /// Deadline for registration and the initial MOTD.
    pub connect_timeout_ms: u64,
}

impl Default for OnboardingConfig {
    fn default() -> Self {
        Self {
            nickname_instruction: "Choose a nickname based on a mythological character.".into(),
            username_template: "{client}-{agent_short}".into(),
            real_name_template: "rmcp-irc guest {nickname} [{client_full}]".into(),
            initial_channels: Vec::new(),
            nickname_attempts: 8,
            connect_timeout_ms: 15_000,
        }
    }
}

impl OnboardingConfig {
    /// Expand the default username for one provisional agent.
    pub fn username(&self, agent_id: &str, client: Option<&ClientIdentity>) -> String {
        self.username_template
            .replace("{agent_id}", agent_id)
            .replace("{agent_short}", &agent_short(agent_id))
            .replace("{client}", &ClientIdentity::label(client))
    }

    /// Expand the default real name for one provisional agent.
    pub fn real_name(
        &self,
        agent_id: &str,
        nickname: &str,
        client: Option<&ClientIdentity>,
    ) -> String {
        self.real_name_template
            .replace("{agent_id}", agent_id)
            .replace("{agent_short}", &agent_short(agent_id))
            .replace("{nickname}", nickname)
            .replace("{client}", &ClientIdentity::label(client))
            .replace("{client_full}", &ClientIdentity::full(client))
    }
}

/// Longest client label kept in a username.
///
/// A server truncates `USER` to its own `USERLEN` (commonly around 20) and the
/// agent discriminator has to survive alongside it, so the runtime name is
/// clamped here rather than left for the server to cut at an arbitrary point.
const CLIENT_LABEL_MAX: usize = 12;

/// Hex characters of the agent handle kept by `{agent_short}`.
const AGENT_SHORT_LEN: usize = 6;

/// Who is driving this MCP session, as declared in the `initialize` handshake.
///
/// This is the only place the runtime identifies itself. The nickname is chosen
/// by the model and the agent handle is a random UUID, so without this a human
/// reading `WHOIS` cannot tell a Claude Code session from a Codex one.
#[derive(Clone, Debug)]
pub struct ClientIdentity {
    /// `clientInfo.name`, verbatim.
    pub name: String,
    /// `clientInfo.version`, verbatim.
    pub version: String,
}

impl ClientIdentity {
    /// Wire-safe short form for the username field.
    ///
    /// Unknown clients collapse to `mcp` so the field never renders empty and
    /// the resulting username stays a legal IRC user string.
    fn label(client: Option<&Self>) -> String {
        let Some(client) = client else {
            return "mcp".into();
        };
        let sanitized = sanitize_label(&client.name);
        if sanitized.is_empty() {
            "mcp".into()
        } else {
            sanitized
        }
    }

    /// Human-facing form for the real-name field, which permits spaces.
    fn full(client: Option<&Self>) -> String {
        let Some(client) = client else {
            return "unidentified MCP client".into();
        };
        let name = sanitize_realname_fragment(&client.name);
        let version = sanitize_realname_fragment(&client.version);
        match (name.is_empty(), version.is_empty()) {
            (true, _) => "unidentified MCP client".into(),
            (false, true) => name,
            (false, false) => format!("{name} {version}"),
        }
    }
}

/// Reduce a client name to lowercase alphanumerics and single hyphens.
fn sanitize_label(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if !out.ends_with('-') && !out.is_empty() {
            out.push('-');
        }
        if out.len() >= CLIENT_LABEL_MAX {
            break;
        }
    }
    out.truncate(CLIENT_LABEL_MAX);
    out.trim_matches('-').to_owned()
}

/// Strip only what cannot ride in a real name; spaces are legal there.
fn sanitize_realname_fragment(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !matches!(ch, '\0' | '\r' | '\n'))
        .collect::<String>()
        .trim()
        .to_owned()
}

/// The distinguishing tail of an agent handle.
///
/// Handles look like `agent-104f9e07-...`; the constant prefix carries no
/// information, so `{agent_short}` starts after it.
fn agent_short(agent_id: &str) -> String {
    agent_id
        .strip_prefix("agent-")
        .unwrap_or(agent_id)
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(AGENT_SHORT_LEN)
        .collect()
}

/// Exponential reconnect policy.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReconnectConfig {
    /// Initial delay before reconnecting.
    pub initial_delay_ms: u64,
    /// Maximum reconnect delay.
    pub max_delay_ms: u64,
    /// Exponential multiplier.
    pub multiplier: f64,
    /// Random jitter fraction in the range 0.0 through 1.0.
    pub jitter: f64,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay_ms: 500,
            max_delay_ms: 30_000,
            multiplier: 2.0,
            jitter: 0.2,
        }
    }
}

/// Policy for how the MCP surface behaves, independently of IRC.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct McpConfig {
    /// Require an explicit human confirmation before `irc.kick` or
    /// `irc.message.redact` takes effect.
    ///
    /// Off by default, because a gateway that asks a question its caller cannot
    /// answer is a gateway whose destructive tools do not work. Turning it on
    /// is an operator decision that a person — not a model — approves each of
    /// those two mutations, and it is enforced by refusing the call when the
    /// request declared no way to answer: silently proceeding would defeat the
    /// only reason the setting exists.
    pub confirm_destructive: bool,
    /// Attach the bounded activity hint to successful tool results that name an
    /// agent.
    ///
    /// On by default: for a host that does not subscribe, or subscribes without
    /// starting a model turn, this is the only way news reaches the model
    /// without it spending a whole turn on a long poll. The hint costs a few
    /// hundred tokens at its worst and moves nothing, so the reason to turn it
    /// off is a deployment that has decided its own delivery loop is enough —
    /// never the mere presence of a subscription, which proves a host can be
    /// woken and nothing about whether it will think.
    pub activity_hints: bool,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            confirm_destructive: false,
            activity_hints: true,
        }
    }
}

/// Bounded resources owned by the process or one agent actor.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct GatewayLimits {
    /// Simultaneously published guest actors in HTTP mode.
    pub max_agents: usize,
    /// Commands waiting for one agent actor.
    pub command_queue: usize,
    /// Commands awaiting correlated completion per actor.
    pub pending_commands: usize,
    /// Simultaneously open IRC batches per actor.
    pub active_batches: usize,
    /// Replies retained by one command collector.
    pub replies_per_command: usize,
    /// Events retained per actor.
    pub event_count: usize,
    /// Serialized event bytes retained per actor.
    pub event_bytes: usize,
    /// Largest inbound line accepted before server-specific negotiation.
    pub max_line_bytes: usize,
    /// Largest logical message accepted before line splitting.
    pub max_message_bytes: usize,
    /// Largest number of IRC lines emitted for one logical message.
    pub max_message_parts: usize,
    /// Largest caller-selected command collector deadline.
    pub max_command_timeout_ms: u64,
    /// Largest caller-selected event long-poll duration.
    pub max_event_wait_ms: u64,
    /// Largest number of events returned by one read.
    pub max_event_page_size: usize,
    /// Watch handles one caller may hold at once, across all of its agents.
    pub max_watches_per_owner: usize,
    /// How long a watch handle survives without being used. Refreshed whenever
    /// a caller reads the watch or its events, so an actively consumed watch
    /// never lapses.
    pub watch_ttl_ms: u64,
}

impl Default for GatewayLimits {
    fn default() -> Self {
        Self {
            max_agents: 64,
            command_queue: 256,
            pending_commands: 128,
            active_batches: 64,
            replies_per_command: 512,
            event_count: 4_096,
            event_bytes: 4 * 1024 * 1024,
            max_line_bytes: 8 * 1024,
            max_message_bytes: 64 * 1024,
            max_message_parts: 256,
            max_command_timeout_ms: 30_000,
            max_event_wait_ms: 30_000,
            max_event_page_size: 1_000,
            max_watches_per_owner: 32,
            watch_ttl_ms: 3_600_000,
        }
    }
}

/// Name of the root `download_directory` seeds when no root is declared.
pub const DEFAULT_RECEIVE_ROOT: &str = "downloads";

/// One named directory incoming DCC files may be received into.
///
/// The name is the whole authority a tool call carries: a caller chooses *which*
/// configured root a file lands in and where beneath it, never the root's path.
/// That keeps the set of writable directories an operator decision while leaving
/// the destination an explicit, replayable argument.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DccReceiveRoot {
    /// Identifier `irc.dcc.accept` names to select this root.
    pub name: String,
    /// Directory every file received into this root is confined beneath.
    /// Relative paths resolve from the gateway process working directory.
    pub path: PathBuf,
}

/// DCC networking and filesystem behavior.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DccConfig {
    /// Address used by DCC listeners.
    pub bind_address: IpAddr,
    /// Address advertised to peers, or automatic discovery when absent.
    pub advertised_address: Option<IpAddr>,
    /// First port available to DCC listeners.
    pub port_start: u16,
    /// Last port available to DCC listeners.
    pub port_end: u16,
    /// Directory that seeds the default receive root when `receive_roots` is
    /// empty. It is not a fallback for an explicit root: declaring roots
    /// replaces it entirely.
    pub download_directory: PathBuf,
    /// Named roots incoming files may be received into, in the order an
    /// operator declared them. Empty means `download_directory` seeds one root
    /// named [`DEFAULT_RECEIVE_ROOT`]; read the effective list through
    /// [`DccConfig::receive_roots`].
    pub receive_roots: Vec<DccReceiveRoot>,
    /// Maximum active DCC sessions per IRC agent.
    pub max_sessions: usize,
    /// Maximum simultaneous incoming offers retained from one nickname.
    pub max_offers_per_peer: usize,
    /// Buffer used while streaming transfers.
    pub transfer_buffer_bytes: usize,
    /// Maximum bytes accepted for one incoming file transfer.
    pub max_transfer_bytes: u64,
    /// How long an offer and its listener remain available for peer acceptance.
    pub offer_ttl_ms: u64,
    /// Deadline while actively connecting to a peer endpoint.
    pub connect_timeout_ms: u64,
    /// Idle direct-session timeout.
    pub idle_timeout_ms: u64,
    /// Accept incoming CHAT offers without an MCP call.
    pub automatic_accept_chat: bool,
    /// Accept incoming SEND offers without an MCP call.
    pub automatic_accept_send: bool,
    /// Permit loopback, private, link-local, and carrier-grade NAT peer addresses.
    pub allow_private_addresses: bool,
    /// Buffered outbound lines per active DCC CHAT session.
    pub chat_queue: usize,
    /// Largest DCC CHAT line including its LF terminator.
    pub chat_line_bytes: usize,
}

impl DccConfig {
    /// The named roots incoming files may be received into.
    ///
    /// An empty `receive_roots` list means the configuration never declared
    /// any — which every configuration written before named roots existed did —
    /// so `download_directory` seeds a single root named
    /// [`DEFAULT_RECEIVE_ROOT`] and those deployments keep exactly the one root
    /// they had. Declaring roots explicitly *replaces* that default rather than
    /// adding to it, which is what makes the declared list the complete
    /// filesystem authority instead of a hint layered over a hidden one.
    ///
    /// Never empty: a receive always has somewhere to resolve against, and code
    /// downstream of here does not have to carry an "unless there are no roots"
    /// branch that could only fail late.
    pub fn receive_roots(&self) -> std::borrow::Cow<'_, [DccReceiveRoot]> {
        if self.receive_roots.is_empty() {
            std::borrow::Cow::Owned(vec![DccReceiveRoot {
                name: DEFAULT_RECEIVE_ROOT.to_owned(),
                path: self.download_directory.clone(),
            }])
        } else {
            std::borrow::Cow::Borrowed(&self.receive_roots)
        }
    }

    /// Look one configured root up by the name a caller supplied.
    pub fn receive_root(&self, name: &str) -> Option<DccReceiveRoot> {
        self.receive_roots()
            .iter()
            .find(|root| root.name == name)
            .cloned()
    }

    /// The root used when nobody chose one and nobody has to.
    ///
    /// That is the first declared root, which is also the only root whenever
    /// there is exactly one. Automatic acceptance uses it because there is no
    /// caller present to ask.
    pub fn default_receive_root(&self) -> DccReceiveRoot {
        self.receive_roots()
            .first()
            .cloned()
            .expect("receive_roots is never empty")
    }

    /// Configured root names in declaration order.
    ///
    /// This is the choice a caller has to make, so it is also what an
    /// elicitation offers and what a refusal lists.
    pub fn receive_root_names(&self) -> Vec<String> {
        self.receive_roots()
            .iter()
            .map(|root| root.name.clone())
            .collect()
    }
}

impl Default for DccConfig {
    fn default() -> Self {
        Self {
            bind_address: IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            advertised_address: None,
            port_start: 50_000,
            port_end: 50_100,
            download_directory: PathBuf::from(DEFAULT_RECEIVE_ROOT),
            receive_roots: Vec::new(),
            max_sessions: 16,
            max_offers_per_peer: 4,
            transfer_buffer_bytes: 64 * 1024,
            max_transfer_bytes: 1024 * 1024 * 1024,
            offer_ttl_ms: 120_000,
            connect_timeout_ms: 15_000,
            idle_timeout_ms: 300_000,
            automatic_accept_chat: false,
            automatic_accept_send: false,
            allow_private_addresses: false,
            chat_queue: 64,
            chat_line_bytes: 8 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        Config::default().validate().expect("default config");
    }

    /// Build a configuration whose only non-default part is its receive roots.
    fn with_roots(roots: Vec<(&str, &str)>) -> Config {
        let mut config = Config::default();
        config.dcc.receive_roots = roots
            .into_iter()
            .map(|(name, path)| DccReceiveRoot {
                name: name.to_owned(),
                path: PathBuf::from(path),
            })
            .collect();
        config
    }

    #[test]
    fn the_download_directory_seeds_the_default_receive_root() {
        // A configuration written before named roots existed must keep exactly
        // the one root it had, wherever that root pointed.
        let mut config = Config::default();
        config.dcc.download_directory = PathBuf::from("/srv/incoming");
        config.validate().expect("valid");

        let roots = config.dcc.receive_roots();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].name, DEFAULT_RECEIVE_ROOT);
        assert_eq!(roots[0].path, PathBuf::from("/srv/incoming"));
        assert_eq!(
            config.dcc.default_receive_root().path,
            PathBuf::from("/srv/incoming")
        );
        assert_eq!(config.dcc.receive_root_names(), vec!["downloads"]);
    }

    #[test]
    fn declared_receive_roots_replace_the_seeded_default() {
        let config = with_roots(vec![("media", "/srv/media"), ("docs", "/srv/docs")]);
        config.validate().expect("valid");

        assert_eq!(config.dcc.receive_root_names(), vec!["media", "docs"]);
        assert_eq!(config.dcc.receive_root(DEFAULT_RECEIVE_ROOT), None);
        // The first declared root is the one an unattended acceptance uses.
        assert_eq!(config.dcc.default_receive_root().name, "media");
        assert_eq!(
            config.dcc.receive_root("docs").expect("docs").path,
            PathBuf::from("/srv/docs")
        );
    }

    #[test]
    fn a_receive_root_list_that_cannot_be_chosen_from_is_refused_at_startup() {
        for (roots, expected) in [
            (vec![("", "/srv/in")], "1-64 characters"),
            (vec![("has space", "/srv/in")], "1-64 characters"),
            (vec![("../escape", "/srv/in")], "1-64 characters"),
            (vec![("media", "")], "must not be empty"),
            (
                vec![("media", "/srv/a"), ("media", "/srv/b")],
                "must be unique",
            ),
            (
                vec![("media", "/srv/a"), ("MEDIA", "/srv/b")],
                "must be unique",
            ),
        ] {
            let error = with_roots(roots.clone())
                .validate()
                .expect_err("must be refused");
            assert!(
                error.to_string().contains(expected),
                "unexpected error for {roots:?}: {error}"
            );
        }
    }

    #[test]
    fn debug_output_does_not_resolve_credentials() {
        let reference = CredentialRef {
            env: "ERGO_PASSWORD".into(),
        };
        assert_eq!(
            format!("{reference:?}"),
            "CredentialRef { env: \"ERGO_PASSWORD\", .. }"
        );
    }

    fn claude() -> ClientIdentity {
        ClientIdentity {
            name: "claude-code".into(),
            version: "2.1.0".into(),
        }
    }

    #[test]
    fn default_username_names_the_runtime_and_stays_within_userlen() {
        let onboarding = OnboardingConfig::default();
        let username = onboarding.username("agent-104f9e07-6d49-4474-a402", Some(&claude()));

        assert_eq!(username, "claude-code-104f9e");
        // Ergo truncates USER at its USERLEN; staying under keeps the
        // discriminator, which is the half that makes the name unique.
        assert!(username.len() <= 19, "username too long: {username}");
    }

    #[test]
    fn default_username_distinguishes_two_runtimes_sharing_a_handle_prefix() {
        let onboarding = OnboardingConfig::default();
        let codex = ClientIdentity {
            name: "codex".into(),
            version: "0.9".into(),
        };

        assert_eq!(
            onboarding.username("agent-104f9e07-6d49", Some(&codex)),
            "codex-104f9e"
        );
        assert_eq!(
            onboarding.username("agent-104f9e07-6d49", Some(&claude())),
            "claude-code-104f9e"
        );
    }

    #[test]
    fn an_unidentified_client_still_yields_a_legal_username() {
        let onboarding = OnboardingConfig::default();
        let username = onboarding.username("agent-104f9e07", None);

        assert_eq!(username, "mcp-104f9e");
        assert!(!username.contains(' '));
        assert!(!username.is_empty());
    }

    #[test]
    fn real_name_carries_the_runtime_and_its_version() {
        let onboarding = OnboardingConfig::default();

        assert_eq!(
            onboarding.real_name("agent-104f9e07", "Ilmarinen", Some(&claude())),
            "rmcp-irc guest Ilmarinen [claude-code 2.1.0]"
        );
        assert_eq!(
            onboarding.real_name("agent-104f9e07", "Ilmarinen", None),
            "rmcp-irc guest Ilmarinen [unidentified MCP client]"
        );
    }

    #[test]
    fn client_labels_are_sanitized_and_clamped() {
        // Spaces and punctuation cannot ride in a username, and an
        // over-long runtime name must not eat the whole field.
        assert_eq!(sanitize_label("Visual Studio Code"), "visual-studi");
        assert_eq!(sanitize_label("Claude_Code/2"), "claude-code");
        assert_eq!(sanitize_label("!!!"), "");
        assert!(sanitize_label("Visual Studio Code").len() <= CLIENT_LABEL_MAX);
    }

    #[test]
    fn a_real_name_never_carries_wire_breaking_characters() {
        let hostile = ClientIdentity {
            name: "evil\r\nQUIT".into(),
            version: "1.0".into(),
        };
        let real_name = onboarding_default().real_name("agent-1", "Nick", Some(&hostile));

        assert!(!real_name.contains('\r'));
        assert!(!real_name.contains('\n'));
    }

    fn onboarding_default() -> OnboardingConfig {
        OnboardingConfig::default()
    }

    #[test]
    fn templates_accept_the_client_placeholders() {
        let mut config = Config::default();
        config.onboarding.username_template = "{client}-{agent_short}".into();
        config.onboarding.real_name_template = "{nickname} via {client_full}".into();

        assert!(config.validate().is_ok());
    }
}
