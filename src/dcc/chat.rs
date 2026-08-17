//! Bounded DCC CHAT data-plane runtime.

use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    sync::mpsc,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use super::session::DccSessionId;

/// One line sent or received over DCC CHAT.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct DccChatLine {
    /// Owning DCC session.
    pub session_id: DccSessionId,
    /// Text without the line terminator.
    pub text: String,
}

/// Runtime event delivered back to the owning agent actor.
#[derive(Debug)]
pub enum DccChatEvent {
    /// Peer sent one valid UTF-8 line.
    Inbound(DccChatLine),
    /// Gateway wrote one line to the peer.
    Outbound(DccChatLine),
    /// Peer closed the direct socket cleanly.
    Closed(DccSessionId),
    /// Session ended with a safe error.
    Failed {
        /// Owning session.
        session_id: DccSessionId,
        /// Safe explanation.
        error: String,
    },
}

/// Cloneable writer/cancellation handle for one active direct chat.
#[derive(Clone, Debug)]
pub struct DccChatHandle {
    session_id: DccSessionId,
    outbound: mpsc::Sender<String>,
    cancellation: CancellationToken,
    max_line_bytes: usize,
}

impl DccChatHandle {
    /// Queue one line for the direct peer.
    pub async fn send(&self, text: impl Into<String>) -> Result<(), DccChatError> {
        let text = text.into();
        validate_line(&text, self.max_line_bytes)?;
        self.outbound
            .send(text)
            .await
            .map_err(|_| DccChatError::Closed(self.session_id.clone()))
    }

    /// Cancel the direct session.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
}

/// Spawn a DCC CHAT reader/writer around an established socket.
pub fn spawn_chat<S>(
    stream: S,
    session_id: DccSessionId,
    queue_capacity: usize,
    max_line_bytes: usize,
    idle_timeout: Duration,
    events: mpsc::Sender<DccChatEvent>,
) -> Result<(DccChatHandle, JoinHandle<()>), DccChatError>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    if queue_capacity == 0 || max_line_bytes == 0 || idle_timeout.is_zero() {
        return Err(DccChatError::InvalidBounds);
    }
    let (outbound, receiver) = mpsc::channel(queue_capacity);
    let cancellation = CancellationToken::new();
    let handle = DccChatHandle {
        session_id: session_id.clone(),
        outbound,
        cancellation: cancellation.clone(),
        max_line_bytes,
    };
    let task = tokio::spawn(run_chat(
        stream,
        session_id,
        receiver,
        max_line_bytes,
        idle_timeout,
        cancellation,
        events,
    ));
    Ok((handle, task))
}

async fn run_chat<S>(
    stream: S,
    session_id: DccSessionId,
    mut outbound: mpsc::Receiver<String>,
    max_line_bytes: usize,
    idle_timeout: Duration,
    cancellation: CancellationToken,
    events: mpsc::Sender<DccChatEvent>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let mut incoming = Vec::with_capacity(max_line_bytes.min(4096));
    loop {
        incoming.clear();
        let action = tokio::time::timeout(idle_timeout, async {
            tokio::select! {
                () = cancellation.cancelled() => ChatAction::Cancelled,
                line = outbound.recv() => ChatAction::Outbound(line),
                read = read_bounded_line(&mut reader, &mut incoming, max_line_bytes) => ChatAction::Inbound(read),
            }
        })
        .await;
        let result = match action {
            Err(_) => Err(DccChatError::IdleTimeout),
            Ok(ChatAction::Cancelled | ChatAction::Outbound(None)) => break,
            Ok(ChatAction::Outbound(Some(text))) => {
                match tokio::time::timeout(idle_timeout, write_line(&mut writer, &text)).await {
                    Ok(result) => result.map(|()| {
                        DccChatEvent::Outbound(DccChatLine {
                            session_id: session_id.clone(),
                            text,
                        })
                    }),
                    Err(_) => Err(DccChatError::IdleTimeout),
                }
            }
            Ok(ChatAction::Inbound(Ok(0))) => {
                let _ = events.send(DccChatEvent::Closed(session_id.clone())).await;
                break;
            }
            Ok(ChatAction::Inbound(Ok(_))) => decode_line(&incoming, max_line_bytes).map(|text| {
                DccChatEvent::Inbound(DccChatLine {
                    session_id: session_id.clone(),
                    text,
                })
            }),
            Ok(ChatAction::Inbound(Err(source))) => Err(DccChatError::Io(source)),
        };
        match result {
            Ok(event) => {
                if events.send(event).await.is_err() {
                    break;
                }
            }
            Err(error) => {
                let _ = events
                    .send(DccChatEvent::Failed {
                        session_id: session_id.clone(),
                        error: error.to_string(),
                    })
                    .await;
                break;
            }
        }
    }
}

enum ChatAction {
    Cancelled,
    Outbound(Option<String>),
    Inbound(std::io::Result<usize>),
}

async fn read_bounded_line(
    reader: &mut (impl AsyncBufRead + Unpin),
    output: &mut Vec<u8>,
    max_line_bytes: usize,
) -> std::io::Result<usize> {
    // `read_until` has no allocation bound of its own. Consume buffered slices
    // directly and retain at most one byte beyond the accepted line size.
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(output.len());
        }
        let remaining = max_line_bytes
            .saturating_add(1)
            .saturating_sub(output.len());
        let inspected = available.len().min(remaining);
        let newline = available[..inspected]
            .iter()
            .position(|byte| *byte == b'\n');
        let consumed = newline.map_or(inspected, |index| index + 1);
        output.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if newline.is_some() || output.len() > max_line_bytes {
            return Ok(output.len());
        }
    }
}

async fn write_line(
    writer: &mut (impl AsyncWrite + Unpin),
    text: &str,
) -> Result<(), DccChatError> {
    writer
        .write_all(text.as_bytes())
        .await
        .map_err(DccChatError::Io)?;
    writer.write_all(b"\n").await.map_err(DccChatError::Io)?;
    writer.flush().await.map_err(DccChatError::Io)
}

fn decode_line(input: &[u8], max_line_bytes: usize) -> Result<String, DccChatError> {
    if input.len() > max_line_bytes {
        return Err(DccChatError::LineTooLong {
            actual: input.len(),
            limit: max_line_bytes,
        });
    }
    let mut line = input;
    if line.last() == Some(&b'\n') {
        line = &line[..line.len() - 1];
    }
    if line.last() == Some(&b'\r') {
        line = &line[..line.len() - 1];
    }
    let text = std::str::from_utf8(line).map_err(|_| DccChatError::InvalidUtf8)?;
    validate_line(text, max_line_bytes)?;
    Ok(text.to_owned())
}

fn validate_line(text: &str, max_line_bytes: usize) -> Result<(), DccChatError> {
    if text
        .bytes()
        .any(|byte| matches!(byte, b'\0' | b'\r' | b'\n'))
    {
        return Err(DccChatError::ControlCharacter);
    }
    let actual = text.len().saturating_add(1);
    if actual > max_line_bytes {
        return Err(DccChatError::LineTooLong {
            actual,
            limit: max_line_bytes,
        });
    }
    Ok(())
}

/// DCC CHAT validation or runtime error.
#[derive(Debug, thiserror::Error)]
pub enum DccChatError {
    /// Queue/line/timeout bounds must be non-zero.
    #[error("DCC CHAT bounds must be greater than zero")]
    InvalidBounds,
    /// Line contains a wire delimiter.
    #[error("DCC CHAT text contains NUL, CR, or LF")]
    ControlCharacter,
    /// Peer line is not valid UTF-8.
    #[error("DCC CHAT line is not valid UTF-8")]
    InvalidUtf8,
    /// Line exceeds the configured bound.
    #[error("DCC CHAT line requires {actual} bytes but the limit is {limit}")]
    LineTooLong {
        /// Encoded bytes including LF.
        actual: usize,
        /// Configured limit.
        limit: usize,
    },
    /// Direct session is no longer active.
    #[error("DCC CHAT session is closed: {0}")]
    Closed(DccSessionId),
    /// No direct data arrived within the configured idle bound.
    #[error("DCC CHAT session reached its idle timeout")]
    IdleTimeout,
    /// Direct socket failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn duplex_chat_emits_both_directions() {
        let (left, mut right) = tokio::io::duplex(1024);
        let (events, mut received) = mpsc::channel(8);
        let (handle, task) = spawn_chat(
            left,
            DccSessionId::new(),
            4,
            128,
            Duration::from_secs(1),
            events,
        )
        .expect("spawn");

        handle.send("outbound").await.expect("send");
        let mut wire = [0; 9];
        right.read_exact(&mut wire).await.expect("read outbound");
        assert_eq!(&wire, b"outbound\n");
        assert!(matches!(
            received.recv().await,
            Some(DccChatEvent::Outbound(_))
        ));

        right.write_all(b"inbound\n").await.expect("write inbound");
        assert!(matches!(
            received.recv().await,
            Some(DccChatEvent::Inbound(_))
        ));
        handle.cancel();
        task.await.expect("task");
    }
}
