//! IRC line framing and negotiated length bounds for Tokio streams.
//!
//! Malformed inbound lines become [`InboundFrame::Malformed`] so decoding can
//! resume at the next boundary. Only I/O and outbound encoding return errors.

use bytes::{Buf, BytesMut, buf::BufMut};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

use super::wire::{LineBudget, OutboundMessage, WireEncodeError, WireMessage, encode_with_label};

/// One decoded unit from the socket.
#[derive(Clone, Debug)]
pub enum InboundFrame {
    /// A line the wire model accepted, complete, partial, or invalid.
    Message(Box<WireMessage>),
    /// A line the framing layer refused, retained for journaling.
    Malformed(MalformedLine),
}

/// A line that could not become a [`WireMessage`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MalformedLine {
    /// Why the line was refused.
    pub reason: MalformedReason,
    /// Bytes observed before the line was discarded, truncated to the limit.
    pub observed_bytes: Vec<u8>,
}

/// Why a line was refused by the framing layer.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum MalformedReason {
    /// The line exceeded the active limit and was skipped to the next boundary.
    TooLong {
        /// Bytes observed for this line, which may be a lower bound.
        actual: usize,
        /// Active bound.
        limit: usize,
    },
    /// The line contained a NUL or a stray delimiter.
    EmbeddedControl,
}

/// An outbound command with its optional gateway correlation label.
#[derive(Clone, Debug)]
pub struct OutboundFrame {
    /// Structured command supplied by the caller.
    pub message: OutboundMessage,
    /// Gateway-owned label when `labeled-response` is negotiated.
    pub label: Option<String>,
}

impl From<OutboundMessage> for OutboundFrame {
    fn from(message: OutboundMessage) -> Self {
        Self {
            message,
            label: None,
        }
    }
}

/// Line-oriented IRC codec.
#[derive(Clone, Debug)]
pub struct IrcCodec {
    budget: LineBudget,
    ceiling: usize,
    skipping: bool,
    skipped_bytes: usize,
}

impl IrcCodec {
    /// Use the traditional limit until ISUPPORT advertises a larger value.
    #[cfg(test)]
    pub fn traditional() -> Self {
        Self::new(LineBudget::TRADITIONAL, usize::MAX)
    }

    /// Create a codec with an active budget and a configured hard ceiling.
    ///
    /// The ceiling comes from local configuration and is never raised by a
    /// server advertisement; the active budget is the smaller of the two.
    pub fn new(budget: LineBudget, ceiling: usize) -> Self {
        let mut codec = Self {
            budget,
            ceiling,
            skipping: false,
            skipped_bytes: 0,
        };
        codec.set_budget(budget);
        codec
    }

    /// Update the active budget after protocol discovery, honoring the ceiling.
    pub fn set_budget(&mut self, budget: LineBudget) {
        self.budget = LineBudget {
            max_body_bytes: budget.max_body_bytes.min(self.ceiling),
            max_tag_bytes: budget.max_tag_bytes.min(self.ceiling),
        };
    }

    /// Active budget, after the configured ceiling has been applied.
    pub const fn budget(&self) -> LineBudget {
        self.budget
    }

    /// Largest complete line the framing layer will accept.
    const fn max_frame_bytes(&self) -> usize {
        self.budget.max_total_bytes()
    }

    /// Discard bytes until the next line boundary after an overlong line.
    fn resume_after_overlong(&mut self, source: &mut BytesMut) -> Option<MalformedLine> {
        let limit = self.max_frame_bytes();
        match source.iter().position(|byte| *byte == b'\n') {
            Some(newline) => {
                self.skipped_bytes += newline;
                let actual = self.skipped_bytes;
                source.advance(newline + 1);
                self.skipping = false;
                self.skipped_bytes = 0;
                Some(MalformedLine {
                    reason: MalformedReason::TooLong { actual, limit },
                    observed_bytes: Vec::new(),
                })
            }
            None => {
                self.skipped_bytes += source.len();
                source.clear();
                None
            }
        }
    }
}

impl Decoder for IrcCodec {
    type Item = InboundFrame;
    type Error = std::io::Error;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            if self.skipping {
                match self.resume_after_overlong(source) {
                    Some(malformed) => return Ok(Some(InboundFrame::Malformed(malformed))),
                    None => return Ok(None),
                }
            }

            let limit = self.max_frame_bytes();
            let Some(newline) = source.iter().position(|byte| *byte == b'\n') else {
                if source.len() > limit {
                    // The line is already too long without a boundary in sight.
                    self.skipping = true;
                    self.skipped_bytes = source.len();
                    source.clear();
                    continue;
                }
                return Ok(None);
            };

            let framed_len = newline + 1;
            if framed_len > limit {
                let observed = source.split_to(framed_len);
                return Ok(Some(InboundFrame::Malformed(MalformedLine {
                    reason: MalformedReason::TooLong {
                        actual: framed_len,
                        limit,
                    },
                    observed_bytes: observed[..observed.len().min(limit)].to_vec(),
                })));
            }

            let mut line = source.split_to(framed_len);
            line.truncate(line.len() - 1);
            if line.last() == Some(&b'\r') {
                line.truncate(line.len() - 1);
            }
            if line.is_empty() {
                // Servers may send blank lines as keepalive padding.
                continue;
            }
            return Ok(Some(match WireMessage::parse(line.clone().freeze()) {
                Ok(message) => InboundFrame::Message(Box::new(message)),
                Err(_) => InboundFrame::Malformed(MalformedLine {
                    reason: MalformedReason::EmbeddedControl,
                    observed_bytes: line.to_vec(),
                }),
            }));
        }
    }

    fn decode_eof(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(frame) = self.decode(source)? {
            return Ok(Some(frame));
        }
        if source.is_empty() {
            return Ok(None);
        }
        // A final line without CRLF is still worth delivering.
        let line = source.split().freeze();
        Ok(Some(match WireMessage::parse(line.clone()) {
            Ok(message) => InboundFrame::Message(Box::new(message)),
            Err(_) => InboundFrame::Malformed(MalformedLine {
                reason: MalformedReason::EmbeddedControl,
                observed_bytes: line.to_vec(),
            }),
        }))
    }
}

impl Encoder<OutboundFrame> for IrcCodec {
    type Error = CodecError;

    fn encode(
        &mut self,
        item: OutboundFrame,
        destination: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let line = encode_with_label(&item.message, item.label.as_deref(), self.budget)
            .map_err(CodecError::Encode)?;
        destination.reserve(line.len());
        destination.put_slice(&line);
        Ok(())
    }
}

impl Encoder<OutboundMessage> for IrcCodec {
    type Error = CodecError;

    fn encode(
        &mut self,
        item: OutboundMessage,
        destination: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.encode(OutboundFrame::from(item), destination)
    }
}

/// Outbound encoding failure. Inbound framing failures are frames, not errors.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// Socket I/O failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Outbound validation or encoding failed.
    #[error(transparent)]
    Encode(#[from] WireEncodeError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(frame: Option<InboundFrame>) -> WireMessage {
        match frame.expect("frame") {
            InboundFrame::Message(message) => *message,
            InboundFrame::Malformed(malformed) => {
                panic!("unexpected malformed line: {malformed:?}")
            }
        }
    }

    fn malformed(frame: Option<InboundFrame>) -> MalformedLine {
        match frame.expect("frame") {
            InboundFrame::Malformed(line) => line,
            InboundFrame::Message(message) => panic!("unexpected message: {}", message.command),
        }
    }

    #[test]
    fn decodes_fragmented_lines_without_losing_the_remainder() {
        let mut codec = IrcCodec::traditional();
        let mut source = BytesMut::from(&b"PING :one\r"[..]);
        assert!(codec.decode(&mut source).expect("partial").is_none());
        source.extend_from_slice(b"\nPING :two\r\n");
        assert_eq!(
            message(codec.decode(&mut source).expect("first")).command,
            "PING"
        );
        assert_eq!(
            message(codec.decode(&mut source).expect("second"))
                .trailing
                .as_deref(),
            Some("two")
        );
        assert!(source.is_empty());
    }

    #[test]
    fn an_overlong_line_is_reported_without_killing_the_stream() {
        let mut codec = IrcCodec::new(LineBudget::with_body(64), usize::MAX);
        let mut source = BytesMut::new();
        source.extend_from_slice(b"PRIVMSG #room :");
        source.extend_from_slice(&vec![b'x'; 5_000]);
        source.extend_from_slice(b"\r\nPING :after\r\n");

        let line = malformed(codec.decode(&mut source).expect("malformed"));
        assert!(matches!(line.reason, MalformedReason::TooLong { .. }));
        assert_eq!(
            message(codec.decode(&mut source).expect("recovered")).command,
            "PING"
        );
    }

    #[test]
    fn an_overlong_line_split_across_reads_does_not_resync_mid_line() {
        let mut codec = IrcCodec::new(LineBudget::with_body(32), usize::MAX);
        let mut source = BytesMut::new();
        source.extend_from_slice(b"PRIVMSG #room :");
        source.extend_from_slice(&vec![b'x'; 9_000]);
        assert!(codec.decode(&mut source).expect("still skipping").is_none());

        // The tail of the discarded line must never be parsed as a new line.
        source.extend_from_slice(&[b'x'; 40]);
        source.extend_from_slice(b" TRAILINGGARBAGE\r\nPING :after\r\n");
        let line = malformed(codec.decode(&mut source).expect("malformed"));
        assert!(matches!(line.reason, MalformedReason::TooLong { .. }));
        assert_eq!(
            message(codec.decode(&mut source).expect("recovered")).command,
            "PING"
        );
    }

    #[test]
    fn a_line_with_a_nul_is_reported_rather_than_parsed() {
        let mut codec = IrcCodec::traditional();
        let mut source = BytesMut::from(&b"PRIVMSG #room :bad\0line\r\nPING :after\r\n"[..]);
        assert_eq!(
            malformed(codec.decode(&mut source).expect("malformed")).reason,
            MalformedReason::EmbeddedControl
        );
        assert_eq!(
            message(codec.decode(&mut source).expect("recovered")).command,
            "PING"
        );
    }

    #[test]
    fn tagged_lines_above_the_body_limit_are_accepted() {
        let mut codec = IrcCodec::traditional();
        let mut source = BytesMut::new();
        source.extend_from_slice(b"@id=");
        source.extend_from_slice(&vec![b'a'; 700]);
        source.extend_from_slice(b" :server PRIVMSG #room :hello\r\n");
        let decoded = message(codec.decode(&mut source).expect("tagged"));
        assert_eq!(decoded.command, "PRIVMSG");
        assert_eq!(decoded.tag_value("id").map(str::len), Some(700));
    }

    #[test]
    fn the_configured_ceiling_bounds_a_server_advertisement() {
        let mut codec = IrcCodec::new(LineBudget::TRADITIONAL, 1_024);
        codec.set_budget(LineBudget::with_body(65_536));
        assert_eq!(codec.budget().max_body_bytes, 1_024);
        assert_eq!(codec.budget().max_tag_bytes, 1_024);
    }

    #[test]
    fn encoding_attaches_the_gateway_label() {
        let mut codec = IrcCodec::traditional();
        let mut destination = BytesMut::new();
        codec
            .encode(
                OutboundFrame {
                    message: OutboundMessage::new("WHOIS", vec!["alice".into()]),
                    label: Some("cmd_7".into()),
                },
                &mut destination,
            )
            .expect("encode");
        assert_eq!(&destination[..], b"@label=cmd_7 WHOIS alice\r\n");
    }

    #[test]
    fn decode_eof_delivers_a_final_unterminated_line() {
        let mut codec = IrcCodec::traditional();
        let mut source = BytesMut::from(&b"PING :last"[..]);
        assert_eq!(
            message(codec.decode_eof(&mut source).expect("eof"))
                .trailing
                .as_deref(),
            Some("last")
        );
        assert!(codec.decode_eof(&mut source).expect("drained").is_none());
    }
}
