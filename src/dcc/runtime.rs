//! Direct-socket setup shared by DCC CHAT and SEND tasks.

use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use rand::RngExt;
use tokio::net::{TcpListener, TcpStream, UdpSocket};

use crate::{
    config::{DccConfig, IrcEndpoint},
    error::{GatewayError, Result},
};

/// Bound direct listener and the endpoint advertised to an IRC peer.
#[derive(Debug)]
pub struct DirectListener {
    /// Socket accepting one peer.
    pub listener: TcpListener,
    /// Externally meaningful endpoint paired with the bound port.
    pub advertised_endpoint: SocketAddr,
}

/// Bind one unused configured DCC port, starting at a randomized offset.
pub async fn bind_listener(config: &DccConfig, irc: &IrcEndpoint) -> Result<DirectListener> {
    let port_count = u32::from(config.port_end) - u32::from(config.port_start) + 1;
    let offset = rand::rng().random_range(0..port_count);
    let advertised_address = advertised_address(config, irc).await?;
    let mut last_error = None;
    for position in 0..port_count {
        let relative = (offset + position) % port_count;
        let port = u16::try_from(u32::from(config.port_start) + relative)
            .expect("configured DCC port range contains u16 ports");
        match TcpListener::bind(SocketAddr::new(config.bind_address, port)).await {
            Ok(listener) => {
                return Ok(DirectListener {
                    listener,
                    advertised_endpoint: SocketAddr::new(advertised_address, port),
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                last_error = Some(error);
            }
            Err(source) => return Err(GatewayError::io("bind DCC listener", source)),
        }
    }
    Err(GatewayError::ResourceLimit(format!(
        "no DCC listener port is available in {}-{}{}",
        config.port_start,
        config.port_end,
        last_error
            .map(|error| format!(": {error}"))
            .unwrap_or_default()
    )))
}

/// Establish a direct peer socket within the configured connection deadline.
pub async fn connect(endpoint: SocketAddr, timeout: Duration) -> Result<TcpStream> {
    tokio::time::timeout(timeout, TcpStream::connect(endpoint))
        .await
        .map_err(|_| GatewayError::TimedOut(format!("connect to DCC peer {endpoint}")))?
        .map_err(|source| GatewayError::io("connect to DCC peer", source))
}

/// Accept one direct peer within the configured connection deadline.
pub async fn accept(listener: TcpListener, timeout: Duration) -> Result<TcpStream> {
    let (stream, _) = tokio::time::timeout(timeout, listener.accept())
        .await
        .map_err(|_| GatewayError::TimedOut("accept DCC peer".into()))?
        .map_err(|source| GatewayError::io("accept DCC peer", source))?;
    Ok(stream)
}

async fn advertised_address(config: &DccConfig, irc: &IrcEndpoint) -> Result<IpAddr> {
    if let Some(address) = config.advertised_address {
        return Ok(address);
    }
    if !config.bind_address.is_unspecified() {
        return Ok(config.bind_address);
    }

    // A connected UDP socket performs route selection without sending data,
    // yielding the interface address used to reach the IRC endpoint.
    let remote = tokio::net::lookup_host((irc.host.as_str(), irc.port))
        .await
        .map_err(|source| GatewayError::io("resolve IRC endpoint for DCC", source))?
        .next()
        .ok_or_else(|| GatewayError::Configuration("IRC endpoint resolved to no address".into()))?;
    let bind = if remote.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind)
        .await
        .map_err(|source| GatewayError::io("bind DCC route probe", source))?;
    socket
        .connect(remote)
        .await
        .map_err(|source| GatewayError::io("select DCC route", source))?;
    socket
        .local_addr()
        .map(|address| address.ip())
        .map_err(|source| GatewayError::io("inspect DCC route", source))
}
