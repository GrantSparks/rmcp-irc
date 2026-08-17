//! irc-mcp command-line entry point.

mod agent;
mod config;
mod dcc;
mod error;
mod gateway;
mod irc;
mod mcp;
mod time;

#[cfg(test)]
mod tests;

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use crate::{
    config::Config,
    gateway::Gateway,
    mcp::{authorization::CallerPolicy, service::IrcMcpService},
};
use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};
use rmcp::{
    ServiceExt,
    service::ServerInitializeError,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

/// Instructions sent to every MCP client during initialization.
pub const MCP_INSTRUCTIONS: &str = "Choose a nickname based on a mythological character. \
Call irc.connect with that nickname. Read and follow the IRC server's returned MOTD before \
participating.";

#[derive(Debug, Parser)]
#[command(name = "irc-mcp", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve the same MCP handler over stdio or Streamable HTTP.
    Serve(ServeArgs),
}

#[derive(Debug, Args)]
struct ServeArgs {
    /// MCP transport.
    #[arg(long, value_enum, default_value_t = Transport::Stdio)]
    transport: Transport,
    /// Streamable HTTP listen address.
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
    /// Optional TOML configuration.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Permit unauthenticated HTTP on a non-loopback listen address.
    #[arg(long, requires = "allow_host")]
    allow_unauthenticated_network: bool,
    /// Host or host:port accepted by the HTTP DNS-rebinding guard; repeatable.
    #[arg(long = "allow-host", value_name = "HOST")]
    allow_host: Vec<String>,
    /// Browser Origin accepted by the HTTP cross-origin guard; repeatable.
    #[arg(long = "allow-origin", value_name = "ORIGIN")]
    allow_origin: Vec<String>,
    /// Bearer credential accepted on the HTTP endpoint; repeatable. Each
    /// credential is a distinct caller identity, and handles created by one are
    /// invisible to the others. Without any, the endpoint is trusted and every
    /// caller shares one local owner, so configure credentials before sharing
    /// it.
    #[arg(long = "http-bearer-token", value_name = "TOKEN")]
    http_bearer_token: Vec<String>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Transport {
    /// MCP over stdin/stdout.
    Stdio,
    /// Stateless Streamable HTTP at /mcp.
    Http,
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
        Command::Serve(args) => serve(args).await,
    }
}

async fn serve(args: ServeArgs) -> anyhow::Result<()> {
    let config = match args.config.as_ref() {
        Some(path) => Config::load(path)?,
        None => {
            let config = Config::default();
            config.validate()?;
            config
        }
    };
    let gateway = Arc::new(Gateway::new(config));

    let result = match args.transport {
        Transport::Stdio => {
            tracing::info!("serving MCP over stdio");
            match IrcMcpService::new(gateway.clone())
                .serve(rmcp::transport::stdio())
                .await
            {
                Ok(server) => {
                    server.waiting().await.context("stdio MCP service")?;
                }
                Err(ServerInitializeError::ConnectionClosed(_)) => {
                    tracing::debug!("stdin closed before MCP initialization");
                }
                Err(error) => return Err(error).context("start stdio MCP service"),
            }
            Ok(())
        }
        Transport::Http => {
            let (allowed_hosts, allowed_origins) = http_security(&args)?;
            let callers = CallerPolicy::http(&args.http_bearer_token);
            serve_http(
                gateway.clone(),
                args.listen,
                allowed_hosts,
                allowed_origins,
                callers,
            )
            .await
        }
    };
    let stopped = gateway
        .shutdown_all(Some("MCP gateway shutting down".into()))
        .await;
    tracing::info!(agents = stopped, "IRC agents stopped");
    result
}

fn http_security(args: &ServeArgs) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    if !args.listen.ip().is_loopback() && !args.allow_unauthenticated_network {
        anyhow::bail!(
            "refusing unauthenticated HTTP on {}; pass --allow-unauthenticated-network and at least one --allow-host only on a trusted network",
            args.listen
        );
    }
    if args.allow_host.iter().any(|host| host.trim().is_empty()) {
        anyhow::bail!("--allow-host must not be empty");
    }
    if args
        .http_bearer_token
        .iter()
        .any(|token| token.trim().is_empty())
    {
        anyhow::bail!("--http-bearer-token must not be empty");
    }
    for origin in &args.allow_origin {
        let valid = origin == "null"
            || origin.parse::<axum::http::Uri>().is_ok_and(|uri| {
                uri.scheme().is_some()
                    && uri.authority().is_some()
                    && uri.path() == "/"
                    && uri.query().is_none()
            });
        if !valid {
            anyhow::bail!(
                "invalid --allow-origin {origin:?}; expected SCHEME://HOST[:PORT] or null"
            );
        }
    }
    let mut allowed_hosts = vec!["localhost".into(), "127.0.0.1".into(), "::1".into()];
    if !args.listen.ip().is_unspecified() {
        allowed_hosts.push(args.listen.ip().to_string());
    }
    allowed_hosts.extend(args.allow_host.iter().cloned());
    allowed_hosts.sort();
    allowed_hosts.dedup();

    // A non-empty impossible default means browser requests carrying Origin
    // are denied unless the operator explicitly opts an origin in.
    let mut allowed_origins = if args.allow_origin.is_empty() {
        vec!["https://origin.invalid".into()]
    } else {
        args.allow_origin.clone()
    };
    allowed_origins.sort();
    allowed_origins.dedup();
    Ok((allowed_hosts, allowed_origins))
}

/// Assemble the Streamable HTTP router this binary serves.
///
/// Separate from [`serve_http`] so tests can drive the real router — the same
/// transport validation, the same per-request handler factory, the same
/// middleware — through one in-process request instead of a socket.
pub(crate) fn http_service(
    gateway: Arc<Gateway>,
    allowed_hosts: Vec<String>,
    allowed_origins: Vec<String>,
    callers: CallerPolicy,
    cancellation: CancellationToken,
) -> axum::Router {
    let service: StreamableHttpService<IrcMcpService, LocalSessionManager> =
        StreamableHttpService::new(
            move || {
                Ok(IrcMcpService::with_caller_policy(
                    gateway.clone(),
                    callers.clone(),
                ))
            },
            Default::default(),
            StreamableHttpServerConfig::default()
                .with_legacy_session_mode(false)
                // This server implements only 2026-07-28, so a request that
                // does not declare its protocol version and capabilities is
                // rejected at the transport rather than dispatched under
                // guessed defaults.
                .with_stateless_protocol_metadata_required(true)
                .with_allowed_hosts(allowed_hosts)
                .with_allowed_origins(allowed_origins)
                .with_cancellation_token(cancellation),
        );
    // Every response carries per-caller IRC state, so no shared cache may
    // retain or reuse it for anyone else.
    axum::Router::new()
        .nest_service("/mcp", service)
        .layer(axum::middleware::from_fn(mark_responses_private))
}

async fn serve_http(
    gateway: Arc<Gateway>,
    listen: SocketAddr,
    allowed_hosts: Vec<String>,
    allowed_origins: Vec<String>,
    callers: CallerPolicy,
) -> anyhow::Result<()> {
    let cancellation = CancellationToken::new();
    let router = http_service(
        gateway,
        allowed_hosts,
        allowed_origins,
        callers,
        cancellation.child_token(),
    );
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("bind Streamable HTTP listener at {listen}"))?;
    tracing::info!(address = %listen, endpoint = "/mcp", "serving Streamable HTTP MCP");

    axum::serve(listener, router)
        .with_graceful_shutdown({
            let cancellation = cancellation.clone();
            async move {
                shutdown_signal().await;
                cancellation.cancel();
            }
        })
        .await
        .context("Streamable HTTP MCP service")?;
    Ok(())
}

/// Mark every HTTP response as private and uncacheable.
///
/// A resource read answers with whatever the calling identity owns, so a
/// response cached by an intermediary and replayed to another caller would
/// hand them somebody else's conversation.
async fn mark_responses_private(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("private, no-store"),
    );
    response
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(error) => {
            tracing::warn!(%error, "could not install SIGTERM handler");
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                tracing::warn!(%error, "could not install Ctrl-C handler");
            }
        }
        _ = terminate.recv() => {}
    }
}

#[cfg(test)]
mod cli_tests {
    use super::*;

    fn http_args(listen: &str) -> ServeArgs {
        ServeArgs {
            transport: Transport::Http,
            listen: listen.parse().expect("listen address"),
            config: None,
            allow_unauthenticated_network: false,
            allow_host: Vec::new(),
            allow_origin: Vec::new(),
            http_bearer_token: Vec::new(),
        }
    }

    #[test]
    fn remote_http_requires_explicit_trust_and_a_host() {
        let mut args = http_args("0.0.0.0:8080");
        assert!(http_security(&args).is_err());
        args.allow_unauthenticated_network = true;
        args.allow_host.push("irc".into());
        let (hosts, origins) = http_security(&args).expect("trusted network");
        assert!(hosts.iter().any(|host| host == "irc"));
        assert_eq!(origins, ["https://origin.invalid"]);
    }

    #[test]
    fn loopback_http_is_safe_by_default() {
        let args = http_args("127.0.0.1:8080");
        let (hosts, _) = http_security(&args).expect("loopback");
        assert!(hosts.iter().any(|host| host == "127.0.0.1"));
    }

    #[test]
    fn allowed_origins_are_exact_origins_not_urls() {
        let mut args = http_args("127.0.0.1:8080");
        args.allow_origin.push("https://client.example".into());
        assert!(http_security(&args).is_ok());
        args.allow_origin[0] = "https://client.example/path".into();
        assert!(http_security(&args).is_err());
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "could not install Ctrl-C handler");
    }
}
