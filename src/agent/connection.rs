//! Native TCP/TLS connector used by every agent actor.

use std::{pin::Pin, sync::Arc};

use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_rustls::TlsConnector;

use crate::{
    config::{IrcEndpoint, IrcTransport},
    error::{GatewayError, Result},
};

/// Async stream requirements shared by plain TCP and TLS connections.
pub trait IrcStream: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> IrcStream for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

/// Type-erased upstream connection owned by one actor.
pub type BoxedIrcStream = Pin<Box<dyn IrcStream>>;

/// Open and configure one native upstream IRC connection.
pub async fn connect(endpoint: &IrcEndpoint) -> Result<BoxedIrcStream> {
    let address = (endpoint.host.as_str(), endpoint.port);
    let tcp = TcpStream::connect(address)
        .await
        .map_err(|source| GatewayError::io("connect to IRC endpoint", source))?;
    tcp.set_nodelay(true)
        .map_err(|source| GatewayError::io("configure IRC TCP socket", source))?;

    match endpoint.transport {
        IrcTransport::Plain => Ok(Box::pin(tcp)),
        IrcTransport::Tls => connect_tls(endpoint, tcp).await,
    }
}

async fn connect_tls(endpoint: &IrcEndpoint, tcp: TcpStream) -> Result<BoxedIrcStream> {
    let native = rustls_native_certs::load_native_certs();
    let mut roots = RootCertStore::empty();
    let (added, ignored) = roots.add_parsable_certificates(native.certs);
    if added == 0 {
        return Err(GatewayError::Tls(
            "the platform trust store contained no usable certificates".into(),
        ));
    }
    if ignored > 0 || !native.errors.is_empty() {
        tracing::warn!(
            ignored,
            errors = native.errors.len(),
            "some native CA certificates were unusable"
        );
    }

    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| GatewayError::Tls(format!("select TLS protocol versions: {error}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server_name = ServerName::try_from(endpoint.host.clone())
        .map_err(|error| GatewayError::Tls(format!("invalid TLS server name: {error}")))?;
    let stream = TlsConnector::from(Arc::new(config))
        .connect(server_name, tcp)
        .await
        .map_err(|error| GatewayError::Tls(error.to_string()))?;
    Ok(Box::pin(stream))
}
