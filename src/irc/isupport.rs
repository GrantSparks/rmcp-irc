//! Lossless parsing and accumulation of RPL_ISUPPORT tokens.
//!
//! Tokens are kept exactly as the server sent them. The typed accessors are a
//! convenience over that record, never a replacement: an unknown token is
//! still stored and exposed, and a malformed value falls back to the
//! traditional default rather than failing the connection.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::wire::LineBudget;

/// One 005 token, including unknown and negated values.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
pub struct IsupportToken {
    /// Complete token as sent by the server.
    pub raw: String,
    /// Token name without a leading minus sign.
    pub name: String,
    /// Optional value after equals, including an explicitly empty string.
    pub value: Option<String>,
    /// Whether the server negated this token.
    pub negated: bool,
}

impl IsupportToken {
    /// Parse one middle parameter from RPL_ISUPPORT.
    pub fn parse(raw: &str) -> Self {
        let (negated, body) = raw
            .strip_prefix('-')
            .map_or((false, raw), |body| (true, body));
        let (name, value) = body
            .split_once('=')
            .map_or((body, None), |(name, value)| (name, Some(value.to_owned())));
        Self {
            raw: raw.to_owned(),
            name: name.to_owned(),
            value,
            negated,
        }
    }

    /// Value with `\xHH` sequences resolved, as ISUPPORT escaping defines.
    pub fn unescaped_value(&self) -> Option<String> {
        self.value.as_deref().map(unescape_isupport_value)
    }
}

/// Channel membership prefix pairing a mode letter with its symbol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MembershipPrefix {
    /// Mode letter, such as `o`.
    pub mode: char,
    /// Status symbol, such as `@`.
    pub symbol: char,
}

/// How the server compares nicknames and channel names.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CaseMapping {
    /// ASCII plus `{}|^` folded onto `[]\~`.
    #[default]
    Rfc1459,
    /// RFC 1459 without `^`/`~` folding.
    Rfc1459Strict,
    /// Plain ASCII case folding.
    Ascii,
    /// A mapping this gateway does not implement; comparison stays exact.
    Unknown,
}

impl CaseMapping {
    /// Parse the advertised spelling.
    pub fn parse(value: &str) -> Self {
        match value.to_ascii_lowercase().as_str() {
            "rfc1459" => Self::Rfc1459,
            "rfc1459-strict" => Self::Rfc1459Strict,
            "ascii" => Self::Ascii,
            _ => Self::Unknown,
        }
    }

    /// Fold one name for comparison under this mapping.
    pub fn fold(self, value: &str) -> String {
        value
            .chars()
            .map(|character| match (self, character) {
                (Self::Unknown, _) => character,
                (_, 'A'..='Z') => character.to_ascii_lowercase(),
                (Self::Rfc1459, '[') => '{',
                (Self::Rfc1459, ']') => '}',
                (Self::Rfc1459, '\\') => '|',
                (Self::Rfc1459, '~') => '^',
                (Self::Rfc1459Strict, '[') => '{',
                (Self::Rfc1459Strict, ']') => '}',
                (Self::Rfc1459Strict, '\\') => '|',
                _ => character,
            })
            .collect()
    }

    /// Whether two names refer to the same target under this mapping.
    pub fn same(self, left: &str, right: &str) -> bool {
        self.fold(left) == self.fold(right)
    }
}

/// Accumulated ISUPPORT state for one connection.
#[derive(Clone, Debug, Default)]
pub struct IsupportRegistry {
    tokens: BTreeMap<String, IsupportToken>,
}

impl IsupportRegistry {
    /// Empty registry using traditional defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply the middle parameters of one RPL_ISUPPORT reply.
    ///
    /// The first middle is the client's own nickname and the trailing text is
    /// human-readable, so callers pass only the token parameters between them.
    pub fn apply_tokens<'a>(&mut self, tokens: impl IntoIterator<Item = &'a str>) {
        for raw in tokens {
            if raw.is_empty() {
                continue;
            }
            let token = IsupportToken::parse(raw);
            if token.negated {
                self.tokens.remove(&token.name);
            } else {
                self.tokens.insert(token.name.clone(), token);
            }
        }
    }

    /// Clear every token, as a reconnect requires.
    pub fn reset(&mut self) {
        self.tokens.clear();
    }

    /// Exact token record, if the server advertised it.
    pub fn token(&self, name: &str) -> Option<&IsupportToken> {
        self.tokens.get(name)
    }

    /// Every advertised token, keyed by case-preserved name.
    pub fn tokens(&self) -> &BTreeMap<String, IsupportToken> {
        &self.tokens
    }

    /// Raw values in the shape the compatibility resource publishes.
    pub fn as_resource_map(&self) -> BTreeMap<String, Option<String>> {
        self.tokens
            .iter()
            .map(|(name, token)| (name.clone(), token.value.clone()))
            .collect()
    }

    fn numeric(&self, name: &str, default: usize) -> usize {
        self.token(name)
            .and_then(|token| token.value.as_deref())
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    /// Largest nickname the server accepts, in bytes.
    ///
    /// Falls back to the RFC 1459 value of 9. Registration code should use
    /// [`Self::advertised_nick_len`] because `RPL_ISUPPORT` follows welcome.
    pub fn nick_len(&self) -> usize {
        self.advertised_nick_len().unwrap_or(9)
    }

    /// Nickname limit the server actually advertised, if it has done so.
    ///
    /// This is `None` until `RPL_ISUPPORT`, which normally follows successful
    /// registration.
    pub fn advertised_nick_len(&self) -> Option<usize> {
        self.token("NICKLEN")
            .and_then(|token| token.value.as_deref())
            .and_then(|value| value.parse().ok())
    }

    /// Largest channel name the server accepts, in bytes.
    pub fn channel_len(&self) -> usize {
        self.numeric("CHANNELLEN", 200)
    }

    /// Largest topic the server accepts, in bytes.
    pub fn topic_len(&self) -> usize {
        self.numeric("TOPICLEN", 390)
    }

    /// Largest number of channels one JOIN may name, if bounded.
    pub fn max_targets(&self, command: &str) -> Option<usize> {
        let value = self.token("TARGMAX")?.value.as_deref()?;
        value.split(',').find_map(|entry| {
            let (name, limit) = entry.split_once(':')?;
            if !name.eq_ignore_ascii_case(command) {
                return None;
            }
            limit.parse().ok()
        })
    }

    /// Line budget implied by the advertised limits.
    ///
    /// `LINELEN` covers the message body including CRLF. Servers that do not
    /// advertise it keep the traditional 512-byte body.
    pub fn line_budget(&self) -> LineBudget {
        LineBudget {
            max_body_bytes: self.numeric("LINELEN", LineBudget::TRADITIONAL.max_body_bytes),
            max_tag_bytes: self
                .numeric("CLIENTTAGDENY_MAXTAGLEN", 0)
                .max(self.numeric("MAXTAGLEN", LineBudget::TRADITIONAL.max_tag_bytes)),
        }
    }

    /// Advertised case mapping, defaulting to RFC 1459.
    pub fn case_mapping(&self) -> CaseMapping {
        self.token("CASEMAPPING")
            .and_then(|token| token.value.as_deref())
            .map_or(CaseMapping::Rfc1459, CaseMapping::parse)
    }

    /// Characters that introduce a channel name.
    pub fn channel_types(&self) -> Vec<char> {
        self.token("CHANTYPES")
            .and_then(|token| token.value.as_deref())
            .unwrap_or("#&")
            .chars()
            .collect()
    }

    /// Whether a target names a channel under the advertised prefixes.
    pub fn is_channel(&self, target: &str) -> bool {
        target
            .chars()
            .next()
            .is_some_and(|first| self.channel_types().contains(&first))
    }

    /// Membership prefixes in descending order of privilege.
    pub fn membership_prefixes(&self) -> Vec<MembershipPrefix> {
        let Some(value) = self
            .token("PREFIX")
            .and_then(|token| token.value.as_deref())
        else {
            return vec![
                MembershipPrefix {
                    mode: 'o',
                    symbol: '@',
                },
                MembershipPrefix {
                    mode: 'v',
                    symbol: '+',
                },
            ];
        };
        let Some(modes) = value.strip_prefix('(') else {
            return Vec::new();
        };
        let Some((modes, symbols)) = modes.split_once(')') else {
            return Vec::new();
        };
        modes
            .chars()
            .zip(symbols.chars())
            .map(|(mode, symbol)| MembershipPrefix { mode, symbol })
            .collect()
    }

    /// Split the four CHANMODES groups, each possibly empty.
    pub fn channel_modes(&self) -> [String; 4] {
        let value = self
            .token("CHANMODES")
            .and_then(|token| token.value.as_deref())
            .unwrap_or_default();
        let mut groups = value.split(',');
        [
            groups.next().unwrap_or_default().to_owned(),
            groups.next().unwrap_or_default().to_owned(),
            groups.next().unwrap_or_default().to_owned(),
            groups.next().unwrap_or_default().to_owned(),
        ]
    }

    /// Whether the server advertised this token at all.
    pub fn supports(&self, name: &str) -> bool {
        self.tokens.contains_key(name)
    }
}

fn unescape_isupport_value(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(index) = rest.find("\\x") {
        output.push_str(&rest[..index]);
        let hex = rest.get(index + 2..index + 4);
        match hex.and_then(|hex| u8::from_str_radix(hex, 16).ok()) {
            Some(byte) => {
                output.push(char::from(byte));
                rest = &rest[index + 4..];
            }
            None => {
                output.push_str("\\x");
                rest = &rest[index + 2..];
            }
        }
    }
    output.push_str(rest);
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_unknown_and_empty_values() {
        assert_eq!(
            IsupportToken::parse("VENDOR="),
            IsupportToken {
                raw: "VENDOR=".into(),
                name: "VENDOR".into(),
                value: Some(String::new()),
                negated: false,
            }
        );
        assert!(IsupportToken::parse("-MONITOR").negated);
    }

    #[test]
    fn negation_removes_a_previously_advertised_token() {
        let mut registry = IsupportRegistry::new();
        registry.apply_tokens(["MONITOR=100", "NICKLEN=32"]);
        assert!(registry.supports("MONITOR"));
        registry.apply_tokens(["-MONITOR"]);
        assert!(!registry.supports("MONITOR"));
        assert_eq!(registry.nick_len(), 32);
    }

    #[test]
    fn traditional_defaults_apply_until_the_server_advertises() {
        let registry = IsupportRegistry::new();
        assert_eq!(registry.nick_len(), 9);
        assert_eq!(registry.channel_len(), 200);
        assert_eq!(registry.line_budget(), LineBudget::TRADITIONAL);
        assert_eq!(registry.case_mapping(), CaseMapping::Rfc1459);
        assert!(registry.is_channel("#room"));
        assert!(!registry.is_channel("nick"));
    }

    #[test]
    fn an_unparsable_value_falls_back_instead_of_failing() {
        let mut registry = IsupportRegistry::new();
        registry.apply_tokens(["NICKLEN=lots", "LINELEN="]);
        assert_eq!(registry.nick_len(), 9);
        assert_eq!(
            registry.line_budget().max_body_bytes,
            LineBudget::TRADITIONAL.max_body_bytes
        );
    }

    #[test]
    fn advertised_line_length_drives_the_budget() {
        let mut registry = IsupportRegistry::new();
        registry.apply_tokens(["LINELEN=1024", "MAXTAGLEN=8191"]);
        assert_eq!(
            registry.line_budget(),
            LineBudget {
                max_body_bytes: 1_024,
                max_tag_bytes: 8_191,
            }
        );
    }

    #[test]
    fn membership_prefixes_and_channel_modes_are_split() {
        let mut registry = IsupportRegistry::new();
        registry.apply_tokens(["PREFIX=(qaohv)~&@%+", "CHANMODES=beI,k,l,imnpst"]);
        let prefixes = registry.membership_prefixes();
        assert_eq!(prefixes.len(), 5);
        assert_eq!(prefixes[0].mode, 'q');
        assert_eq!(prefixes[0].symbol, '~');
        assert_eq!(registry.channel_modes()[0], "beI");
        assert_eq!(registry.channel_modes()[3], "imnpst");
    }

    #[test]
    fn case_mapping_folds_the_traditional_bracket_characters() {
        let mut registry = IsupportRegistry::new();
        registry.apply_tokens(["CASEMAPPING=rfc1459"]);
        let mapping = registry.case_mapping();
        assert!(mapping.same("Nick[]", "nick{}"));
        assert!(mapping.same("#Room", "#room"));

        registry.apply_tokens(["CASEMAPPING=ascii"]);
        assert!(!registry.case_mapping().same("Nick[]", "nick{}"));
    }

    #[test]
    fn targmax_reports_only_the_named_command() {
        let mut registry = IsupportRegistry::new();
        registry.apply_tokens(["TARGMAX=PRIVMSG:4,JOIN:,NOTICE:3"]);
        assert_eq!(registry.max_targets("PRIVMSG"), Some(4));
        assert_eq!(registry.max_targets("JOIN"), None);
        assert_eq!(registry.max_targets("WHO"), None);
    }

    #[test]
    fn isupport_values_unescape_hex_sequences() {
        let token = IsupportToken::parse(r"NETWORK=Example\x20Net");
        assert_eq!(token.unescaped_value().as_deref(), Some("Example Net"));
        assert_eq!(token.value.as_deref(), Some(r"Example\x20Net"));
    }
}
