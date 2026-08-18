//! Foreground IRC collaborator backed by a persistent repository-working Codex thread.

#[path = "../responder/mod.rs"]
mod responder;

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use responder::RunConfig;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "irc-codex-responder", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Own one IRC/MCP connection and one persistent repository-working Codex thread.
    Run(RunArgs),
}

#[derive(Debug, Args)]
struct RunArgs {
    /// Streamable HTTP MCP endpoint.
    #[arg(long)]
    mcp_url: String,
    /// Private persistent responder profile.
    #[arg(long)]
    state_dir: PathBuf,
    /// Repository workspace Codex may inspect and modify.
    #[arg(long)]
    workspace: PathBuf,
    /// Ordered mythological nickname candidate; supply exactly three times.
    #[arg(long = "nickname-candidate", value_name = "NAME", action = clap::ArgAction::Append)]
    nickname_candidates: Vec<String>,
    /// Coordination purpose included in the IRC identity context.
    #[arg(long)]
    purpose: String,
    /// Development-container/location label included in the hello.
    #[arg(long)]
    location: String,
    /// Target whose complete inbound conversation should wake the responder.
    #[arg(long = "full-traffic-target", value_name = "TARGET", action = clap::ArgAction::Append)]
    full_traffic_targets: Vec<String>,
    /// Channel to which validated replies may be sent.
    #[arg(
        long = "allowed-channel",
        value_name = "TARGET",
        action = clap::ArgAction::Append,
        default_value = "#control"
    )]
    allowed_channels: Vec<String>,
    /// Environment variable containing an optional MCP bearer token.
    #[arg(long)]
    bearer_token_env: Option<String>,
    /// Codex CLI executable (0.147.0 or later).
    #[arg(long, default_value = "codex")]
    codex_command: PathBuf,
    /// Optional Codex model override; omitted inherits the configured default.
    #[arg(long)]
    model: Option<String>,
    /// Codex reasoning effort.
    #[arg(long, default_value = "low")]
    effort: String,
    /// Allow repository turns to access the network.
    #[arg(long)]
    network_access: bool,
    /// Maximum duration of one repository-working turn.
    #[arg(long, default_value_t = 1800)]
    turn_timeout_seconds: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| anyhow::anyhow!("initialize tracing: {error}"))?;

    match Cli::parse().command {
        Command::Run(args) => {
            responder::run(RunConfig {
                mcp_url: args.mcp_url,
                state_dir: args.state_dir,
                workspace: args.workspace,
                nickname_candidates: args.nickname_candidates,
                purpose: args.purpose,
                location: args.location,
                full_traffic_targets: args.full_traffic_targets,
                allowed_channels: args.allowed_channels,
                bearer_token_env: args.bearer_token_env,
                codex_command: args.codex_command,
                model: args.model,
                effort: args.effort,
                network_access: args.network_access,
                turn_timeout: Duration::from_secs(args.turn_timeout_seconds),
            })
            .await
        }
    }
}
