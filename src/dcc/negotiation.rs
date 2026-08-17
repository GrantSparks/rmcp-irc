//! CTCP and DCC negotiation parsing/encoding.

use std::net::{IpAddr, Ipv4Addr};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Parsed CTCP body from an IRC PRIVMSG or NOTICE.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
pub struct CtcpMessage {
    /// Uppercase CTCP command.
    pub command: String,
    /// Optional command body.
    pub body: Option<String>,
}

impl CtcpMessage {
    /// Parse one SOH-delimited CTCP message without altering the IRC text.
    pub fn parse(text: &str) -> Option<Self> {
        let body = text.strip_prefix('\u{1}')?.strip_suffix('\u{1}')?;
        let (command, rest) = body
            .split_once(' ')
            .map_or((body, None), |(command, rest)| (command, Some(rest)));
        if command.is_empty() {
            return None;
        }
        Some(Self {
            command: command.to_ascii_uppercase(),
            body: rest.map(str::to_owned),
        })
    }

    /// Encode a CTCP message for an IRC trailing parameter.
    pub fn encode(&self) -> Result<String, DccNegotiationError> {
        if self.command.is_empty()
            || !self
                .command
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric())
        {
            return Err(DccNegotiationError::InvalidCtcpCommand(
                self.command.clone(),
            ));
        }
        if self.body.as_ref().is_some_and(|body| {
            body.bytes()
                .any(|byte| matches!(byte, b'\0' | b'\r' | b'\n' | 0x01))
        }) {
            return Err(DccNegotiationError::InvalidCtcpBody);
        }
        let mut output = format!("\u{1}{}", self.command.to_ascii_uppercase());
        if let Some(body) = &self.body {
            output.push(' ');
            output.push_str(body);
        }
        output.push('\u{1}');
        Ok(output)
    }
}

/// DCC variant extracted from a CTCP DCC body.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DccOffer {
    /// DCC CHAT endpoint offer.
    Chat {
        /// Address token as advertised.
        address: String,
        /// Port, or zero for a reverse offer.
        port: u16,
        /// Optional reverse-DCC token.
        token: Option<String>,
    },
    /// DCC SEND file offer.
    Send {
        /// Advertised filename.
        filename: String,
        /// Address token as advertised.
        address: String,
        /// Port, or zero for a reverse offer.
        port: u16,
        /// File size when advertised.
        size: Option<u64>,
        /// Optional reverse-DCC token.
        token: Option<String>,
    },
    /// Request to resume a partial transfer.
    Resume {
        /// Advertised filename.
        filename: String,
        /// Original offer port.
        port: u16,
        /// Requested byte offset.
        position: u64,
        /// Optional reverse-DCC token.
        token: Option<String>,
    },
    /// Acceptance of a resume request.
    Accept {
        /// Advertised filename.
        filename: String,
        /// Original offer port.
        port: u16,
        /// Accepted byte offset.
        position: u64,
        /// Optional reverse-DCC token.
        token: Option<String>,
    },
}

impl DccOffer {
    /// Parse the body following the CTCP `DCC` command.
    pub fn parse(body: &str) -> Result<Self, DccNegotiationError> {
        let fields = tokenize(body)?;
        let Some(kind) = fields.first() else {
            return Err(DccNegotiationError::MissingField("variant"));
        };
        match kind.to_ascii_uppercase().as_str() {
            "CHAT" => parse_chat(&fields),
            "SEND" => parse_send(&fields),
            "RESUME" => parse_resume(&fields, false),
            "ACCEPT" => parse_resume(&fields, true),
            other => Err(DccNegotiationError::UnsupportedVariant(other.into())),
        }
    }

    /// Encode this offer as a CTCP body beginning with `DCC`.
    pub fn encode_ctcp(&self) -> Result<String, DccNegotiationError> {
        let body = match self {
            Self::Chat {
                address,
                port,
                token,
            } => {
                validate_atom("address", address)?;
                let mut body = format!("CHAT chat {address} {port}");
                push_token(&mut body, token.as_deref())?;
                body
            }
            Self::Send {
                filename,
                address,
                port,
                size,
                token,
            } => {
                validate_atom("address", address)?;
                let mut body = format!("SEND {} {address} {port}", encode_filename(filename)?);
                if let Some(size) = size {
                    body.push_str(&format!(" {size}"));
                }
                push_token(&mut body, token.as_deref())?;
                body
            }
            Self::Resume {
                filename,
                port,
                position,
                token,
            } => encode_resume("RESUME", filename, *port, *position, token.as_deref())?,
            Self::Accept {
                filename,
                port,
                position,
                token,
            } => encode_resume("ACCEPT", filename, *port, *position, token.as_deref())?,
        };
        CtcpMessage {
            command: "DCC".into(),
            body: Some(body),
        }
        .encode()
    }
}

/// Convert a DCC address token to an IP address.
pub fn parse_address(value: &str) -> Result<IpAddr, DccNegotiationError> {
    if let Ok(address) = value.parse::<IpAddr>() {
        return Ok(address);
    }
    let numeric = value
        .parse::<u32>()
        .map_err(|_| DccNegotiationError::InvalidAddress(value.into()))?;
    Ok(IpAddr::V4(Ipv4Addr::from(numeric)))
}

/// Encode an IP address in an interoperable DCC token.
pub fn encode_address(address: IpAddr) -> String {
    match address {
        IpAddr::V4(address) => u32::from(address).to_string(),
        IpAddr::V6(address) => address.to_string(),
    }
}

fn parse_chat(fields: &[String]) -> Result<DccOffer, DccNegotiationError> {
    let offset = usize::from(
        fields
            .get(1)
            .is_some_and(|value| value.eq_ignore_ascii_case("chat")),
    );
    let address = required(fields, 1 + offset, "address")?.clone();
    let port = parse_field(required(fields, 2 + offset, "port")?, "port")?;
    let token = fields.get(3 + offset).cloned();
    ensure_no_extra(fields, 4 + offset)?;
    validate_reverse(port, token.as_deref())?;
    Ok(DccOffer::Chat {
        address,
        port,
        token,
    })
}

fn parse_send(fields: &[String]) -> Result<DccOffer, DccNegotiationError> {
    let filename = required(fields, 1, "filename")?.clone();
    validate_filename(&filename)?;
    let address = required(fields, 2, "address")?.clone();
    let port = parse_field(required(fields, 3, "port")?, "port")?;
    let size = fields
        .get(4)
        .map(|value| parse_field(value, "size"))
        .transpose()?;
    let token = fields.get(5).cloned();
    ensure_no_extra(fields, 6)?;
    validate_reverse(port, token.as_deref())?;
    Ok(DccOffer::Send {
        filename,
        address,
        port,
        size,
        token,
    })
}

fn parse_resume(fields: &[String], accept: bool) -> Result<DccOffer, DccNegotiationError> {
    let filename = required(fields, 1, "filename")?.clone();
    validate_filename(&filename)?;
    let port = parse_field(required(fields, 2, "port")?, "port")?;
    let position = parse_field(required(fields, 3, "position")?, "position")?;
    let token = fields.get(4).cloned();
    ensure_no_extra(fields, 5)?;
    if accept {
        Ok(DccOffer::Accept {
            filename,
            port,
            position,
            token,
        })
    } else {
        Ok(DccOffer::Resume {
            filename,
            port,
            position,
            token,
        })
    }
}

fn encode_resume(
    kind: &str,
    filename: &str,
    port: u16,
    position: u64,
    token: Option<&str>,
) -> Result<String, DccNegotiationError> {
    let mut body = format!("{kind} {} {port} {position}", encode_filename(filename)?);
    push_token(&mut body, token)?;
    Ok(body)
}

fn validate_reverse(port: u16, token: Option<&str>) -> Result<(), DccNegotiationError> {
    if port == 0 && token.is_none() {
        return Err(DccNegotiationError::MissingReverseToken);
    }
    Ok(())
}

fn required<'a>(
    fields: &'a [String],
    index: usize,
    name: &'static str,
) -> Result<&'a String, DccNegotiationError> {
    fields
        .get(index)
        .ok_or(DccNegotiationError::MissingField(name))
}

fn parse_field<T: std::str::FromStr>(
    value: &str,
    field: &'static str,
) -> Result<T, DccNegotiationError> {
    value
        .parse()
        .map_err(|_| DccNegotiationError::InvalidField {
            field,
            value: value.into(),
        })
}

fn ensure_no_extra(fields: &[String], allowed: usize) -> Result<(), DccNegotiationError> {
    if fields.len() > allowed {
        Err(DccNegotiationError::UnexpectedFields(
            fields[allowed..].to_vec(),
        ))
    } else {
        Ok(())
    }
}

fn push_token(output: &mut String, token: Option<&str>) -> Result<(), DccNegotiationError> {
    if let Some(token) = token {
        validate_atom("token", token)?;
        output.push(' ');
        output.push_str(token);
    }
    Ok(())
}

fn encode_filename(filename: &str) -> Result<String, DccNegotiationError> {
    validate_filename(filename)?;
    if filename.contains(' ') || filename.contains('\t') || filename.contains('"') {
        if filename.contains('"') {
            return Err(DccNegotiationError::InvalidFilename(filename.into()));
        }
        Ok(format!("\"{filename}\""))
    } else {
        Ok(filename.into())
    }
}

/// Require an advertised filename to be exactly one ordinary path component.
pub fn validate_filename(filename: &str) -> Result<(), DccNegotiationError> {
    use std::path::Component;

    let mut components = std::path::Path::new(filename).components();
    let one_normal_component =
        matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none();
    if filename.is_empty()
        || filename == "."
        || filename == ".."
        || filename.contains('\\')
        || filename
            .bytes()
            .any(|byte| matches!(byte, b'\0' | b'\r' | b'\n'))
        || !one_normal_component
    {
        Err(DccNegotiationError::InvalidFilename(filename.into()))
    } else {
        Ok(())
    }
}

fn validate_atom(field: &'static str, value: &str) -> Result<(), DccNegotiationError> {
    if value.is_empty()
        || value
            .bytes()
            .any(|byte| matches!(byte, b'\0' | b'\r' | b'\n' | b' ' | b'\t' | 0x01))
    {
        return Err(DccNegotiationError::InvalidField {
            field,
            value: value.into(),
        });
    }
    Ok(())
}

fn tokenize(input: &str) -> Result<Vec<String>, DccNegotiationError> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for character in input.chars() {
        match character {
            '"' => quoted = !quoted,
            ' ' | '\t' if !quoted => {
                if !current.is_empty() {
                    fields.push(std::mem::take(&mut current));
                }
            }
            '\0' | '\r' | '\n' | '\u{1}' => return Err(DccNegotiationError::InvalidCtcpBody),
            other => current.push(other),
        }
    }
    if quoted {
        return Err(DccNegotiationError::UnterminatedQuote);
    }
    if !current.is_empty() {
        fields.push(current);
    }
    Ok(fields)
}

/// Malformed or unsupported DCC negotiation.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum DccNegotiationError {
    /// CTCP command is not representable.
    #[error("invalid CTCP command: {0}")]
    InvalidCtcpCommand(String),
    /// CTCP body contains a wire delimiter.
    #[error("CTCP body contains NUL, CR, LF, or SOH")]
    InvalidCtcpBody,
    /// DCC variant is not implemented.
    #[error("unsupported DCC variant: {0}")]
    UnsupportedVariant(String),
    /// Required positional field is absent.
    #[error("DCC offer is missing {0}")]
    MissingField(&'static str),
    /// Positional field could not be parsed.
    #[error("invalid DCC {field}: {value}")]
    InvalidField {
        /// Field name.
        field: &'static str,
        /// Rejected value.
        value: String,
    },
    /// Offer contains unexpected trailing fields.
    #[error("unexpected DCC fields: {0:?}")]
    UnexpectedFields(Vec<String>),
    /// Quoted filename was not closed.
    #[error("unterminated quoted DCC filename")]
    UnterminatedQuote,
    /// Filename could expose a path or break quoting.
    #[error("invalid DCC filename: {0}")]
    InvalidFilename(String),
    /// Reverse offers require an opaque token.
    #[error("reverse DCC offer uses port zero without a token")]
    MissingReverseToken,
    /// Address token is neither an IP literal nor a 32-bit DCC IPv4 integer.
    #[error("invalid DCC address: {0}")]
    InvalidAddress(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_reencodes_quoted_send() {
        let offer = DccOffer::parse("SEND \"report final.txt\" 2130706433 5000 42").expect("offer");
        assert_eq!(
            offer,
            DccOffer::Send {
                filename: "report final.txt".into(),
                address: "2130706433".into(),
                port: 5000,
                size: Some(42),
                token: None,
            }
        );
        assert_eq!(
            offer.encode_ctcp().expect("encode"),
            "\u{1}DCC SEND \"report final.txt\" 2130706433 5000 42\u{1}"
        );
    }

    #[test]
    fn ordinary_reject_and_cancel_are_not_fabricated_dcc_variants() {
        for body in ["REJECT SEND report.txt 5000", "CANCEL CHAT 5000"] {
            assert!(matches!(
                DccOffer::parse(body),
                Err(DccNegotiationError::UnsupportedVariant(_))
            ));
        }
    }

    #[test]
    fn numeric_ipv4_is_network_order() {
        assert_eq!(
            parse_address("2130706433").expect("address"),
            "127.0.0.1".parse::<IpAddr>().expect("literal")
        );
    }

    #[test]
    fn reverse_offer_requires_token() {
        assert_eq!(
            DccOffer::parse("CHAT chat 2130706433 0"),
            Err(DccNegotiationError::MissingReverseToken)
        );
    }

    #[test]
    fn send_filenames_cannot_escape_a_download_directory() {
        for filename in ["../secret", "/tmp/secret", ".", "..", r"dir\secret"] {
            let body = format!("SEND {filename} 203.0.113.1 5000 42");
            assert!(matches!(
                DccOffer::parse(&body),
                Err(DccNegotiationError::InvalidFilename(_))
            ));
        }
    }
}
