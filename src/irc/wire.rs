//! Lossless IRC wire model and structured outbound encoder.
//!
//! Every message retains its exact bytes. Non-UTF-8 lines recover as much ASCII
//! framing as possible and expose their original bytes as base64.

use std::collections::HashSet;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use ircv3_parse::components::TagValue as ParsedTagValue;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Outcome of parsing the semantic portions of one raw line.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParseStatus {
    /// The complete line was valid UTF-8 and every component parsed.
    Complete,
    /// Structurally valid components were recovered, but some bytes were not
    /// representable as UTF-8 and are available only in the raw forms.
    Partial,
    /// No command could be recovered; only the raw bytes are meaningful.
    Invalid,
}

/// One IRCv3 message tag, preserving flag versus explicit empty value.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
pub struct Tag {
    /// Case-sensitive opaque key.
    pub key: String,
    /// Unescaped value. None is a flag; an empty string is an explicit value.
    pub value: Option<String>,
    /// Exact escaped value as it appeared on the wire, when one was present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_value: Option<String>,
}

impl Tag {
    /// Build a tag from an already-unescaped value.
    pub fn new(key: impl Into<String>, value: Option<String>) -> Self {
        Self {
            key: key.into(),
            value,
            raw_value: None,
        }
    }

    /// Build a tag from the exact wire form, unescaping the value.
    pub fn from_wire(key: impl Into<String>, raw_value: Option<&str>) -> Self {
        Self {
            key: key.into(),
            value: raw_value.map(unescape_tag_value),
            raw_value: raw_value.map(str::to_owned),
        }
    }
}

/// Parsed source prefix without guessing whether a bare name is a nick or server.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
pub struct Prefix {
    /// Complete prefix without the leading colon.
    pub raw: String,
    /// Name, nickname, or server component.
    pub name: String,
    /// Optional user component.
    pub user: Option<String>,
    /// Optional host component.
    pub host: Option<String>,
}

impl Prefix {
    /// Split a prefix body on the IRC `nick!user@host` boundaries.
    fn parse(raw: &str) -> Self {
        let (name_user, host) = match raw.split_once('@') {
            Some((left, host)) => (left, Some(host.to_owned())),
            None => (raw, None),
        };
        let (name, user) = match name_user.split_once('!') {
            Some((name, user)) => (name, Some(user.to_owned())),
            None => (name_user, None),
        };
        Self {
            raw: raw.to_owned(),
            name: name.to_owned(),
            user,
            host,
        }
    }
}

/// Owned representation used by every receive and event path.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct WireMessage {
    /// Exact raw bytes without CRLF, retained in memory.
    #[serde(skip)]
    #[schemars(skip)]
    pub raw_bytes: Vec<u8>,
    /// Exact text when the line is valid UTF-8.
    pub raw: Option<String>,
    /// Base64 form when the line is not valid UTF-8.
    pub raw_base64: Option<String>,
    /// Whether semantic parsing completed.
    pub parse_status: ParseStatus,
    /// Ordered IRCv3 tags.
    pub tags: Vec<Tag>,
    /// Optional source prefix.
    pub prefix: Option<Prefix>,
    /// Case-preserved command or numeric.
    pub command: String,
    /// Middle parameters.
    pub params: Vec<String>,
    /// Trailing parameter, including an explicitly empty value.
    pub trailing: Option<String>,
}

impl WireMessage {
    /// Parse one line after the framing layer has removed CRLF.
    pub fn parse(raw: Bytes) -> Result<Self, WireParseError> {
        if raw
            .iter()
            .any(|byte| matches!(*byte, b'\0' | b'\r' | b'\n'))
        {
            return Err(WireParseError::EmbeddedControl);
        }
        match std::str::from_utf8(&raw) {
            Ok(text) => Ok(Self::parse_utf8(&raw, text)
                .unwrap_or_else(|| Self::recover_bytes(&raw, Some(text)))),
            Err(_) => Ok(Self::recover_bytes(&raw, None)),
        }
    }

    /// Parse a line already known to be valid UTF-8.
    ///
    /// Returns `None` when the strict parser refuses the line, leaving the
    /// caller to recover whatever framing the bytes still carry.
    fn parse_utf8(raw: &Bytes, text: &str) -> Option<Self> {
        let parsed = ircv3_parse::parse(text).ok()?;
        let tags = parsed.tags().map_or_else(Vec::new, |tags| {
            tags.iter()
                .map(|(key, value)| match value {
                    ParsedTagValue::Flag => Tag {
                        key: key.to_owned(),
                        value: None,
                        raw_value: None,
                    },
                    ParsedTagValue::Empty => Tag::from_wire(key, Some("")),
                    ParsedTagValue::Value(value) => Tag::from_wire(key, Some(value)),
                })
                .collect()
        });
        let prefix = parsed.source().map(|source| Prefix {
            raw: source.as_str().to_owned(),
            name: source.name.to_owned(),
            user: source.user.map(str::to_owned),
            host: source.host.map(str::to_owned),
        });
        let parsed_params = parsed.params();

        Some(Self {
            raw_bytes: raw.to_vec(),
            raw: Some(text.to_owned()),
            raw_base64: None,
            parse_status: ParseStatus::Complete,
            tags,
            prefix,
            command: parsed.command().as_str().to_owned(),
            params: parsed_params.middles.iter().map(str::to_owned).collect(),
            trailing: parsed_params.trailing.raw().map(str::to_owned),
        })
    }

    /// Recover the structurally valid framing of a line that is not UTF-8.
    ///
    /// IRC framing is ASCII, so components can still be split on bytes. Exact
    /// input remains available in `raw_bytes` and `raw_base64`.
    fn recover_bytes(raw: &Bytes, text: Option<&str>) -> Self {
        let mut rest: &[u8] = raw;
        let mut tags = Vec::new();
        let mut prefix = None;

        if let Some(body) = rest.strip_prefix(b"@") {
            let (segment, remainder) = split_space(body);
            tags = parse_tag_bytes(segment);
            rest = remainder;
        }
        if let Some(body) = rest.strip_prefix(b":") {
            let (segment, remainder) = split_space(body);
            prefix = Some(Prefix::parse(&lossy(segment)));
            rest = remainder;
        }

        let (command_bytes, mut remainder) = split_space(rest);
        let command = lossy(command_bytes);
        let mut params = Vec::new();
        let mut trailing = None;
        while !remainder.is_empty() {
            if let Some(body) = remainder.strip_prefix(b":") {
                trailing = Some(lossy(body));
                break;
            }
            let (param, next) = split_space(remainder);
            if !param.is_empty() {
                params.push(lossy(param));
            }
            remainder = next;
        }

        Self {
            raw_bytes: raw.to_vec(),
            raw: text.map(str::to_owned),
            raw_base64: text.is_none().then(|| STANDARD.encode(raw)),
            parse_status: if command.is_empty() {
                ParseStatus::Invalid
            } else {
                ParseStatus::Partial
            },
            tags,
            prefix,
            command,
            params,
            trailing,
        }
    }

    /// Whether this line is a numeric reply, returning its value.
    pub fn numeric(&self) -> Option<u16> {
        if self.command.len() == 3 && self.command.bytes().all(|byte| byte.is_ascii_digit()) {
            self.command.parse().ok()
        } else {
            None
        }
    }

    /// Value of the first tag with this exact key, if it carries one.
    pub fn tag_value(&self, key: &str) -> Option<&str> {
        self.tags
            .iter()
            .find(|tag| tag.key == key)
            .and_then(|tag| tag.value.as_deref())
    }
}

/// Split one space-delimited component, returning it and the remainder.
fn split_space(input: &[u8]) -> (&[u8], &[u8]) {
    input
        .iter()
        .position(|byte| *byte == b' ')
        .map_or((input, &input[input.len()..]), |index| {
            (&input[..index], &input[index + 1..])
        })
}

fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn parse_tag_bytes(segment: &[u8]) -> Vec<Tag> {
    segment
        .split(|byte| *byte == b';')
        .filter(|item| !item.is_empty())
        .map(|item| {
            item.iter().position(|byte| *byte == b'=').map_or_else(
                || Tag {
                    key: lossy(item),
                    value: None,
                    raw_value: None,
                },
                |index| Tag::from_wire(lossy(&item[..index]), Some(&lossy(&item[index + 1..]))),
            )
        })
        .collect()
}

/// Structured outbound command accepted from higher layers.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
pub struct OutboundMessage {
    /// Ordered caller-supplied tags.
    #[serde(default)]
    pub tags: Vec<Tag>,
    /// IRC command or numeric; never a complete raw line.
    pub command: String,
    /// Middle parameters.
    #[serde(default)]
    pub params: Vec<String>,
    /// Optional trailing parameter.
    pub trailing: Option<String>,
}

/// Byte budgets applied to one encoded line.
///
/// IRCv3 message tags are budgeted separately from the message body: the
/// traditional 512-byte limit covers everything from the source prefix to the
/// terminating CRLF, while the tag section has its own larger allowance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LineBudget {
    /// Largest body, including CRLF, permitted by the server.
    pub max_body_bytes: usize,
    /// Largest tag section, including the leading `@` and trailing space.
    pub max_tag_bytes: usize,
}

impl LineBudget {
    /// Traditional body limit with the IRCv3 client tag allowance.
    pub const TRADITIONAL: Self = Self {
        max_body_bytes: 512,
        max_tag_bytes: 4_096,
    };

    /// Budget with an explicit body limit and the IRCv3 tag allowance.
    pub const fn with_body(max_body_bytes: usize) -> Self {
        Self {
            max_body_bytes,
            max_tag_bytes: Self::TRADITIONAL.max_tag_bytes,
        }
    }

    /// Largest complete line this budget can accept.
    pub const fn max_total_bytes(self) -> usize {
        self.max_body_bytes.saturating_add(self.max_tag_bytes)
    }
}

impl Default for LineBudget {
    fn default() -> Self {
        Self::TRADITIONAL
    }
}

impl OutboundMessage {
    /// Create a command with middle parameters and no tags or trailing.
    pub fn new(command: impl Into<String>, params: Vec<String>) -> Self {
        Self {
            tags: Vec::new(),
            command: command.into(),
            params,
            trailing: None,
        }
    }

    /// Attach a trailing parameter.
    #[must_use]
    pub fn with_trailing(mut self, trailing: impl Into<String>) -> Self {
        self.trailing = Some(trailing.into());
        self
    }

    /// Attach one tag carrying an unescaped value.
    #[must_use]
    #[cfg(test)]
    pub fn with_tag(mut self, key: impl Into<String>, value: Option<String>) -> Self {
        self.tags.push(Tag::new(key, value));
        self
    }

    /// Validate and encode a complete IRC line including CRLF.
    pub fn encode(&self, budget: LineBudget) -> Result<Bytes, WireEncodeError> {
        validate_command(&self.command)?;
        let tag_section = self.encode_tags()?;
        if tag_section.len() > budget.max_tag_bytes {
            return Err(WireEncodeError::TagsTooLong {
                actual: tag_section.len(),
                limit: budget.max_tag_bytes,
            });
        }

        let mut body = String::new();
        body.push_str(&self.command);
        for param in &self.params {
            validate_middle_param(param)?;
            body.push(' ');
            body.push_str(param);
        }
        if let Some(trailing) = &self.trailing {
            validate_no_wire_controls("trailing parameter", trailing)?;
            body.push_str(" :");
            body.push_str(trailing);
        }

        let body_with_crlf = body.len().saturating_add(2);
        if body_with_crlf > budget.max_body_bytes {
            return Err(WireEncodeError::LineTooLong {
                actual: body_with_crlf,
                limit: budget.max_body_bytes,
            });
        }

        let mut output = String::with_capacity(tag_section.len() + body_with_crlf);
        output.push_str(&tag_section);
        output.push_str(&body);
        output.push_str("\r\n");
        Ok(Bytes::from(output))
    }

    /// Bytes of body, excluding tags and CRLF, that this command already uses.
    ///
    /// Callers that must fit text into one line use this to compute how much
    /// room a trailing parameter has before the encoder would reject it.
    pub fn body_overhead(&self) -> usize {
        let mut overhead = self.command.len();
        for param in &self.params {
            overhead += param.len() + 1;
        }
        if self.trailing.is_some() {
            overhead += 2;
        }
        overhead + 2
    }

    fn encode_tags(&self) -> Result<String, WireEncodeError> {
        if self.tags.is_empty() {
            return Ok(String::new());
        }
        let mut keys = HashSet::with_capacity(self.tags.len());
        let mut output = String::from("@");
        for (index, tag) in self.tags.iter().enumerate() {
            validate_tag_key(&tag.key)?;
            if matches!(tag.key.as_str(), "label" | "batch") {
                return Err(WireEncodeError::ReservedTag(tag.key.clone()));
            }
            if !keys.insert(tag.key.as_str()) {
                return Err(WireEncodeError::DuplicateTag(tag.key.clone()));
            }
            if index > 0 {
                output.push(';');
            }
            output.push_str(&tag.key);
            if let Some(value) = &tag.value {
                validate_no_wire_controls("tag value", value)?;
                output.push('=');
                escape_tag_value(value, &mut output);
            }
        }
        output.push(' ');
        Ok(output)
    }
}

/// Encode a gateway-owned correlation tag alongside caller tags.
///
/// The encoder rejects `label` and `batch` from callers because the gateway
/// owns them; this is the only path that may add them.
pub fn encode_with_label(
    message: &OutboundMessage,
    label: Option<&str>,
    budget: LineBudget,
) -> Result<Bytes, WireEncodeError> {
    let Some(label) = label else {
        return message.encode(budget);
    };
    validate_no_wire_controls("label", label)?;
    if label.is_empty() || label.len() > MAX_LABEL_BYTES {
        return Err(WireEncodeError::InvalidLabel(label.to_owned()));
    }
    let mut prefixed = message.clone();
    prefixed.tags.insert(
        0,
        Tag {
            key: "__label".into(),
            value: Some(label.to_owned()),
            raw_value: None,
        },
    );
    let encoded = prefixed.encode(budget)?;
    let text = String::from_utf8(encoded.to_vec()).map_err(|_| WireEncodeError::LabelEncoding)?;
    Ok(Bytes::from(text.replacen("@__label=", "@label=", 1)))
}

/// Largest correlation label the gateway will generate or accept.
pub const MAX_LABEL_BYTES: usize = 64;

fn validate_command(command: &str) -> Result<(), WireEncodeError> {
    let valid = !command.is_empty()
        && (command.bytes().all(|byte| byte.is_ascii_alphabetic())
            || (command.len() == 3 && command.bytes().all(|byte| byte.is_ascii_digit())));
    if valid {
        Ok(())
    } else {
        Err(WireEncodeError::InvalidCommand(command.to_owned()))
    }
}

fn validate_tag_key(key: &str) -> Result<(), WireEncodeError> {
    if key.is_empty()
        || key
            .bytes()
            .any(|byte| matches!(byte, b'\0' | b'\r' | b'\n' | b' ' | b';' | b'='))
    {
        Err(WireEncodeError::InvalidTagKey(key.to_owned()))
    } else {
        Ok(())
    }
}

fn validate_middle_param(param: &str) -> Result<(), WireEncodeError> {
    if param.is_empty()
        || param.starts_with(':')
        || param
            .bytes()
            .any(|byte| matches!(byte, b'\0' | b'\r' | b'\n' | b' '))
    {
        Err(WireEncodeError::InvalidMiddleParameter(param.to_owned()))
    } else {
        Ok(())
    }
}

fn validate_no_wire_controls(field: &'static str, value: &str) -> Result<(), WireEncodeError> {
    if value
        .bytes()
        .any(|byte| matches!(byte, b'\0' | b'\r' | b'\n'))
    {
        Err(WireEncodeError::ControlCharacter(field))
    } else {
        Ok(())
    }
}

fn escape_tag_value(value: &str, output: &mut String) {
    for character in value.chars() {
        match character {
            ';' => output.push_str(r"\:"),
            ' ' => output.push_str(r"\s"),
            '\\' => output.push_str(r"\\"),
            '\r' => output.push_str(r"\r"),
            '\n' => output.push_str(r"\n"),
            other => output.push(other),
        }
    }
}

/// Reverse IRCv3 tag-value escaping, dropping a lone trailing backslash.
fn unescape_tag_value(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut characters = value.chars();
    while let Some(character) = characters.next() {
        if character != '\\' {
            output.push(character);
            continue;
        }
        match characters.next() {
            Some(':') => output.push(';'),
            Some('s') => output.push(' '),
            Some('\\') => output.push('\\'),
            Some('r') => output.push('\r'),
            Some('n') => output.push('\n'),
            Some(other) => output.push(other),
            None => {}
        }
    }
    output
}

/// Receive-side parse failure.
#[derive(Debug, thiserror::Error)]
pub enum WireParseError {
    /// The framing layer leaked a forbidden delimiter or NUL.
    #[error("raw IRC line contains NUL, CR, or LF")]
    EmbeddedControl,
}

/// Structured outbound validation or encoding failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireEncodeError {
    /// Command is neither alphabetic nor a three-digit numeric.
    #[error("invalid IRC command: {0}")]
    InvalidCommand(String),
    /// Tag key cannot be represented.
    #[error("invalid IRC tag key: {0}")]
    InvalidTagKey(String),
    /// The caller repeated a tag key.
    #[error("duplicate IRC tag key: {0}")]
    DuplicateTag(String),
    /// The gateway owns this tag for protocol correlation.
    #[error("IRC tag is reserved by the gateway: {0}")]
    ReservedTag(String),
    /// A middle parameter is structurally invalid.
    #[error("invalid IRC middle parameter: {0}")]
    InvalidMiddleParameter(String),
    /// A field contains a forbidden control character.
    #[error("{0} contains NUL, CR, or LF")]
    ControlCharacter(&'static str),
    /// The gateway generated an unusable correlation label.
    #[error("invalid IRC correlation label: {0}")]
    InvalidLabel(String),
    /// A labeled line could not be reassembled as text.
    #[error("labeled IRC line could not be encoded as UTF-8")]
    LabelEncoding,
    /// Encoded body exceeds the negotiated limit.
    #[error("IRC line body requires {actual} bytes but the server permits {limit}")]
    LineTooLong {
        /// Encoded body bytes including CRLF.
        actual: usize,
        /// Negotiated body limit.
        limit: usize,
    },
    /// Encoded tag section exceeds the negotiated tag allowance.
    #[error("IRC tags require {actual} bytes but the server permits {limit}")]
    TagsTooLong {
        /// Encoded tag bytes including the leading marker and separator.
        actual: usize,
        /// Negotiated tag limit.
        limit: usize,
    },
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn parser_preserves_unknown_tags_command_and_raw_text() {
        let input =
            Bytes::from_static(b"@vendor/key=a\\sb;flag :nick!user@host FUTURE #room :hello world");
        let message = WireMessage::parse(input.clone()).expect("parse");
        assert_eq!(message.raw_bytes, input);
        assert_eq!(message.parse_status, ParseStatus::Complete);
        assert_eq!(message.command, "FUTURE");
        assert_eq!(message.params, ["#room"]);
        assert_eq!(message.trailing.as_deref(), Some("hello world"));
        assert_eq!(message.tags[0].value.as_deref(), Some("a b"));
        assert_eq!(message.tags[0].raw_value.as_deref(), Some(r"a\sb"));
        assert_eq!(message.tags[1].value, None);
    }

    #[test]
    fn invalid_utf8_recovers_framing_and_is_retained_as_base64() {
        let input = Bytes::from_static(
            b"@time=2026-01-01T00:00:00Z :nick!user@host PRIVMSG #room :caf\xe9 time",
        );
        let message = WireMessage::parse(input.clone()).expect("retain");
        assert_eq!(message.parse_status, ParseStatus::Partial);
        assert!(message.raw.is_none());
        assert_eq!(message.command, "PRIVMSG");
        assert_eq!(message.params, ["#room"]);
        assert_eq!(message.tag_value("time"), Some("2026-01-01T00:00:00Z"));
        assert_eq!(
            message.prefix.as_ref().map(|prefix| prefix.name.as_str()),
            Some("nick")
        );
        assert!(message.trailing.expect("trailing").ends_with(" time"));
        assert_eq!(
            STANDARD
                .decode(message.raw_base64.expect("base64"))
                .expect("decode"),
            input
        );
    }

    #[test]
    fn a_line_without_a_recoverable_command_is_invalid() {
        let message = WireMessage::parse(Bytes::from_static(b"")).expect("empty");
        assert_eq!(message.parse_status, ParseStatus::Invalid);
        assert!(message.command.is_empty());
    }

    #[test]
    fn a_utf8_line_the_strict_parser_refuses_still_recovers_its_framing() {
        let message = WireMessage::parse(Bytes::from_static(b"@only=tags ")).expect("recover");
        assert_eq!(message.parse_status, ParseStatus::Invalid);
        assert_eq!(message.tag_value("only"), Some("tags"));
        assert!(message.raw.is_some());
        assert!(message.raw_base64.is_none());
    }

    #[test]
    fn numeric_replies_are_recognized_only_as_three_digits() {
        let parse = |line: &'static [u8]| WireMessage::parse(Bytes::from_static(line)).expect("ok");
        assert_eq!(parse(b":server 001 nick :hi").numeric(), Some(1));
        assert_eq!(parse(b":server PRIVMSG #room :hi").numeric(), None);
    }

    #[test]
    fn encoder_rejects_reserved_and_duplicate_tags() {
        let message = OutboundMessage {
            tags: vec![Tag::new("label", Some("caller".into()))],
            command: "WHOIS".into(),
            params: vec!["alice".into()],
            trailing: None,
        };
        assert_eq!(
            message.encode(LineBudget::TRADITIONAL),
            Err(WireEncodeError::ReservedTag("label".into()))
        );
    }

    #[test]
    fn encoder_preserves_utf8_and_escapes_tag_values() {
        let message = OutboundMessage {
            tags: vec![Tag::new(
                "+draft/example",
                Some(r"semi; space \ slash".into()),
            )],
            command: "PRIVMSG".into(),
            params: vec!["#room".into()],
            trailing: Some("hello 🦀".into()),
        };
        assert_eq!(
            message.encode(LineBudget::TRADITIONAL).expect("encode"),
            Bytes::from_static(
                b"@+draft/example=semi\\:\\sspace\\s\\\\\\sslash PRIVMSG #room :hello \xf0\x9f\xa6\x80\r\n"
            )
        );
    }

    #[test]
    fn escaping_round_trips_through_the_parser() {
        let original = "semi; space \\ slash";
        let encoded = OutboundMessage {
            tags: vec![Tag::new("+example", Some(original.into()))],
            command: "PRIVMSG".into(),
            params: vec!["#room".into()],
            trailing: Some("body".into()),
        }
        .encode(LineBudget::TRADITIONAL)
        .expect("encode");
        let line = encoded.slice(..encoded.len() - 2);
        let parsed = WireMessage::parse(line).expect("parse");
        assert_eq!(parsed.tag_value("+example"), Some(original));
    }

    #[test]
    fn tags_are_budgeted_separately_from_the_body() {
        let message = OutboundMessage {
            tags: vec![Tag::new("+bulk", Some("x".repeat(600)))],
            command: "PRIVMSG".into(),
            params: vec!["#room".into()],
            trailing: Some("short".into()),
        };
        assert!(message.encode(LineBudget::TRADITIONAL).is_ok());
        assert_eq!(
            message.encode(LineBudget {
                max_body_bytes: 512,
                max_tag_bytes: 128,
            }),
            Err(WireEncodeError::TagsTooLong {
                actual: 608,
                limit: 128,
            })
        );
    }

    #[test]
    fn body_length_is_measured_without_the_tag_section() {
        let message = OutboundMessage {
            tags: vec![Tag::new("+bulk", Some("x".repeat(600)))],
            command: "PRIVMSG".into(),
            params: vec!["#room".into()],
            trailing: Some("y".repeat(500)),
        };
        assert_eq!(
            message.encode(LineBudget::TRADITIONAL),
            Err(WireEncodeError::LineTooLong {
                actual: 517,
                limit: 512,
            })
        );
    }

    #[test]
    fn only_the_gateway_may_attach_a_correlation_label() {
        let message = OutboundMessage::new("WHOIS", vec!["alice".into()]);
        let encoded =
            encode_with_label(&message, Some("cmd_1"), LineBudget::TRADITIONAL).expect("encode");
        assert_eq!(encoded, Bytes::from_static(b"@label=cmd_1 WHOIS alice\r\n"));
        assert_eq!(
            encode_with_label(&message, Some(&"x".repeat(65)), LineBudget::TRADITIONAL),
            Err(WireEncodeError::InvalidLabel("x".repeat(65)))
        );
    }

    #[test]
    fn body_overhead_reports_the_room_left_for_text() {
        let message = OutboundMessage::new("PRIVMSG", vec!["#room".into()]).with_trailing("");
        assert_eq!(message.body_overhead(), "PRIVMSG #room :\r\n".len());
    }

    proptest! {
        #[test]
        fn structured_messages_round_trip_without_losing_utf8(
            command in "[A-Z]{1,12}",
            params in prop::collection::vec("[#A-Za-z0-9_/-]{1,24}", 0..5),
            trailing_chars in prop::option::of(prop::collection::vec(
                any::<char>().prop_filter("IRC wire controls are not encodable", |value| {
                    !matches!(*value, '\0' | '\r' | '\n')
                }),
                0..64,
            )),
        ) {
            let trailing = trailing_chars.map(|characters| characters.into_iter().collect::<String>());
            let outbound = OutboundMessage {
                tags: Vec::new(),
                command: command.clone(),
                params: params.clone(),
                trailing: trailing.clone(),
            };
            let encoded = outbound
                .encode(LineBudget::with_body(8 * 1024))
                .expect("generated message encodes");
            let parsed = WireMessage::parse(encoded.slice(..encoded.len() - 2))
                .expect("encoded message parses");
            prop_assert_eq!(parsed.command, command);
            prop_assert_eq!(parsed.params, params);
            prop_assert_eq!(parsed.trailing, trailing);
        }
    }
}
