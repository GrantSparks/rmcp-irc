//! Validated IRC channel and user targets.
//!
//! [`Target`] uses the server's `CHANTYPES` advertisement so delivery, state,
//! and history code share one target classification.

use std::{fmt, str::FromStr};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{isupport::IsupportRegistry, registration::Nickname};

/// Channel prefixes assumed before the server advertises `CHANTYPES`.
pub const DEFAULT_CHANNEL_TYPES: &[char] = &['#', '&', '+', '!'];

/// A structurally valid IRC channel name.
///
/// The server's `CHANNELLEN` and exact prefix set are authoritative, so this
/// type enforces only what is portable: a channel prefix followed by bytes that
/// cannot break framing or the target list.
#[derive(
    Clone, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct ChannelName(String);

impl ChannelName {
    /// Validate a channel name against the portable prefix set.
    pub fn new(value: impl Into<String>) -> Result<Self, TargetError> {
        Self::with_prefixes(value, DEFAULT_CHANNEL_TYPES)
    }

    /// Validate a channel name against a server's advertised prefixes.
    pub fn with_prefixes(value: impl Into<String>, prefixes: &[char]) -> Result<Self, TargetError> {
        let value = value.into();
        let Some(first) = value.chars().next() else {
            return Err(TargetError::Empty);
        };
        if !prefixes.contains(&first) {
            return Err(TargetError::NotAChannel(first));
        }
        if let Some(character) = value
            .chars()
            .find(|character| matches!(*character, ' ' | ',' | '\u{7}' | '\0' | '\r' | '\n'))
        {
            return Err(TargetError::InvalidCharacter(character));
        }
        Ok(Self(value))
    }

    /// Borrow the channel name.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Encoded length, which is what a server's `CHANNELLEN` bounds.
    pub fn len_bytes(&self) -> usize {
        self.0.len()
    }

    /// Prefix character that introduced this channel.
    pub fn prefix(&self) -> char {
        self.0.chars().next().unwrap_or('#')
    }

    /// Whether two names refer to the same channel under a case mapping.
    pub fn same(&self, other: &Self, isupport: &IsupportRegistry) -> bool {
        isupport.case_mapping().same(&self.0, &other.0)
    }
}

impl fmt::Display for ChannelName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for ChannelName {
    type Err = TargetError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for ChannelName {
    type Error = TargetError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ChannelName> for String {
    fn from(channel: ChannelName) -> Self {
        channel.0
    }
}

/// Where a message is addressed: a channel, or one user.
///
/// Serialized as the plain wire string, so the MCP schema stays a string while
/// the Rust side keeps the distinction.
#[derive(
    Clone, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(try_from = "String", into = "String")]
pub enum Target {
    /// A channel, by any advertised prefix.
    Channel(ChannelName),
    /// A single user.
    User(Nickname),
}

impl Target {
    /// Classify a target using the portable channel prefixes.
    ///
    /// Use [`Self::parse_with`] once the server has advertised `CHANTYPES`;
    /// a server may offer prefixes this default does not know.
    pub fn parse(value: impl Into<String>) -> Result<Self, TargetError> {
        Self::parse_with_prefixes(value, DEFAULT_CHANNEL_TYPES)
    }

    /// Classify a target using the server's advertised channel prefixes.
    pub fn parse_with(
        value: impl Into<String>,
        isupport: &IsupportRegistry,
    ) -> Result<Self, TargetError> {
        Self::parse_with_prefixes(value, &isupport.channel_types())
    }

    fn parse_with_prefixes(
        value: impl Into<String>,
        prefixes: &[char],
    ) -> Result<Self, TargetError> {
        let value = value.into();
        let Some(first) = value.chars().next() else {
            return Err(TargetError::Empty);
        };
        if prefixes.contains(&first) {
            return ChannelName::with_prefixes(value, prefixes).map(Self::Channel);
        }
        Nickname::new(value)
            .map(Self::User)
            .map_err(TargetError::Nickname)
    }

    /// Borrow the wire form.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Channel(channel) => channel.as_str(),
            Self::User(nickname) => nickname.as_str(),
        }
    }

    /// Channel name when this target is a channel.
    pub const fn channel(&self) -> Option<&ChannelName> {
        match self {
            Self::Channel(channel) => Some(channel),
            Self::User(_) => None,
        }
    }

    /// Nickname when this target is a single user.
    pub const fn user(&self) -> Option<&Nickname> {
        match self {
            Self::User(nickname) => Some(nickname),
            Self::Channel(_) => None,
        }
    }

    /// Whether this target is a channel.
    pub const fn is_channel(&self) -> bool {
        matches!(self, Self::Channel(_))
    }
}

impl fmt::Display for Target {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for Target {
    type Err = TargetError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for Target {
    type Error = TargetError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<Target> for String {
    fn from(target: Target) -> Self {
        match target {
            Target::Channel(channel) => channel.into(),
            Target::User(nickname) => nickname.into(),
        }
    }
}

/// Why a target could not be represented.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum TargetError {
    /// The value had no characters.
    #[error("target is empty")]
    Empty,
    /// A channel name did not start with a channel prefix.
    #[error("channel name cannot start with {0:?}")]
    NotAChannel(char),
    /// A channel name contained a character that breaks framing or lists.
    #[error("channel name cannot contain {0:?}")]
    InvalidCharacter(char),
    /// The value was not a channel and not a valid nickname.
    #[error("target is neither a channel nor a valid nickname: {0}")]
    Nickname(#[from] super::registration::NicknameError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channels_require_a_prefix_and_safe_characters() {
        assert!(ChannelName::new("#room").is_ok());
        assert!(ChannelName::new("&local").is_ok());
        assert_eq!(ChannelName::new(""), Err(TargetError::Empty));
        assert_eq!(ChannelName::new("room"), Err(TargetError::NotAChannel('r')));
        assert_eq!(
            ChannelName::new("#a b"),
            Err(TargetError::InvalidCharacter(' '))
        );
        // A comma would split one target into two on the wire.
        assert_eq!(
            ChannelName::new("#a,b"),
            Err(TargetError::InvalidCharacter(','))
        );
    }

    #[test]
    fn targets_classify_channels_and_users() {
        let channel = Target::parse("#room").expect("channel");
        assert!(channel.is_channel());
        assert_eq!(channel.channel().map(ChannelName::as_str), Some("#room"));
        assert!(channel.user().is_none());

        let user = Target::parse("Kuebiko").expect("user");
        assert!(!user.is_channel());
        assert_eq!(user.user().map(Nickname::as_str), Some("Kuebiko"));
    }

    #[test]
    fn advertised_prefixes_decide_classification() {
        let mut isupport = IsupportRegistry::new();
        isupport.apply_tokens(["CHANTYPES=#"]);
        assert!(Target::parse_with("&local", &isupport).is_err());
        assert!(
            Target::parse_with("#room", &isupport)
                .expect("channel")
                .is_channel()
        );

        let mut wide = IsupportRegistry::new();
        wide.apply_tokens(["CHANTYPES=#&"]);
        assert!(
            Target::parse_with("&local", &wide)
                .expect("channel")
                .is_channel()
        );
    }

    #[test]
    fn targets_round_trip_as_plain_strings() {
        for value in ["#room", "Kuebiko"] {
            let target = Target::parse(value).expect("target");
            let json = serde_json::to_string(&target).expect("serialize");
            assert_eq!(json, format!("\"{value}\""));
            let parsed: Target = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(parsed, target);
            assert_eq!(parsed.to_string(), value);
        }
    }

    #[test]
    fn an_unrepresentable_target_is_refused_at_the_edge() {
        assert!(Target::parse("has space").is_err());
        assert!(Target::parse("").is_err());
    }
}
