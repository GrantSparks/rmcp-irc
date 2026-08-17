//! Nickname validation and bounded conflict handling during registration.

use std::{collections::HashSet, fmt, str::FromStr};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::isupport::CaseMapping;

/// A structurally valid IRC nickname.
///
/// RFC 2812 allows a letter or one of the "special" characters to lead, then
/// letters, digits, hyphens, and specials. Servers vary in what they accept
/// beyond that, so this type enforces the portable core and leaves the rest to
/// the server, whose reply remains authoritative.
#[derive(
    Clone, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct Nickname(String);

impl Nickname {
    /// Validate a candidate nickname.
    pub fn new(value: impl Into<String>) -> Result<Self, NicknameError> {
        let value = value.into();
        let mut characters = value.chars();
        let Some(first) = characters.next() else {
            return Err(NicknameError::Empty);
        };
        if !is_nickname_start(first) {
            return Err(NicknameError::InvalidStart(first));
        }
        if let Some(character) = characters.find(|character| !is_nickname_character(*character)) {
            return Err(NicknameError::InvalidCharacter(character));
        }
        Ok(Self(value))
    }

    /// Borrow the nickname text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Encoded length, which is what a server's NICKLEN bounds.
    pub fn len_bytes(&self) -> usize {
        self.0.len()
    }

    /// Whether two nicknames name the same user under a case mapping.
    pub fn same(&self, other: &Self, mapping: CaseMapping) -> bool {
        mapping.same(&self.0, &other.0)
    }

    /// Shorten to at most `max_bytes`, never splitting a code point.
    ///
    /// Returns `None` when nothing valid survives the truncation.
    pub fn truncated(&self, max_bytes: usize) -> Option<Self> {
        if self.0.len() <= max_bytes {
            return Some(self.clone());
        }
        let mut end = max_bytes;
        while end > 0 && !self.0.is_char_boundary(end) {
            end -= 1;
        }
        Self::new(&self.0[..end]).ok()
    }
}

impl fmt::Display for Nickname {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for Nickname {
    type Err = NicknameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for Nickname {
    type Error = NicknameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<Nickname> for String {
    fn from(nickname: Nickname) -> Self {
        nickname.0
    }
}

/// Why a candidate nickname was refused before it reached the wire.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum NicknameError {
    /// The candidate had no characters.
    #[error("nickname is empty")]
    Empty,
    /// The first character cannot start a nickname.
    #[error("nickname cannot start with {0:?}")]
    InvalidStart(char),
    /// A later character is not permitted in a nickname.
    #[error("nickname cannot contain {0:?}")]
    InvalidCharacter(char),
}

const SPECIAL: [char; 9] = ['[', ']', '\\', '`', '_', '^', '{', '|', '}'];

fn is_nickname_start(character: char) -> bool {
    character.is_alphabetic() || SPECIAL.contains(&character)
}

fn is_nickname_character(character: char) -> bool {
    character.is_alphanumeric() || character == '-' || SPECIAL.contains(&character)
}

/// Behavior after caller-supplied nickname candidates collide.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NickConflictPolicy {
    /// Generate bounded suffixed forms of the requested name.
    #[default]
    Suffix,
    /// Return the first collision as a terminal tool error.
    Fail,
}

/// Bounded, ordered plan for acquiring a nickname during registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NicknamePlan {
    requested: Nickname,
    candidates: Vec<Nickname>,
    attempted: usize,
}

impl NicknamePlan {
    /// Build the ordered candidate list.
    ///
    /// Candidates are deduplicated and capped at `max_attempts`. Pass `None`
    /// for `nick_len` until `RPL_ISUPPORT` advertises a limit; applying the
    /// traditional fallback during registration could alter an accepted name.
    pub fn new(
        requested: &Nickname,
        fallbacks: &[Nickname],
        policy: NickConflictPolicy,
        nick_len: impl Into<Option<usize>>,
        max_attempts: usize,
    ) -> Self {
        let nick_len = nick_len.into();
        let mut candidates = Vec::new();
        let mut seen = HashSet::new();
        if max_attempts == 0 {
            return Self {
                requested: requested.clone(),
                candidates,
                attempted: 0,
            };
        }

        push_candidate(
            &mut candidates,
            &mut seen,
            bounded(requested, nick_len),
            max_attempts,
        );
        for fallback in fallbacks {
            push_candidate(
                &mut candidates,
                &mut seen,
                bounded(fallback, nick_len),
                max_attempts,
            );
        }

        if policy == NickConflictPolicy::Suffix {
            for suffix_number in 2..max_attempts.saturating_add(2) {
                if candidates.len() == max_attempts {
                    break;
                }
                let suffix = suffix_number.to_string();
                let base = match nick_len {
                    Some(limit) => {
                        let Some(base) =
                            requested.truncated(limit.saturating_sub(suffix.len() + 1))
                        else {
                            // No valid base remains for a suffixed candidate.
                            break;
                        };
                        base
                    }
                    None => requested.clone(),
                };
                push_candidate(
                    &mut candidates,
                    &mut seen,
                    Nickname::new(format!("{base}_{suffix}")).ok(),
                    max_attempts,
                );
            }
        }

        Self {
            requested: requested.clone(),
            candidates,
            attempted: 0,
        }
    }

    /// Nickname the caller originally asked for.
    #[cfg(test)]
    pub const fn requested(&self) -> &Nickname {
        &self.requested
    }

    /// Ordered candidates this plan will try.
    #[cfg(test)]
    pub fn candidates(&self) -> &[Nickname] {
        &self.candidates
    }

    /// Next candidate to send, consuming one attempt.
    pub fn next_candidate(&mut self) -> Option<&Nickname> {
        let candidate = self.candidates.get(self.attempted);
        if candidate.is_some() {
            self.attempted += 1;
        }
        candidate
    }

    /// Whether every candidate has been attempted.
    #[cfg(test)]
    pub fn is_exhausted(&self) -> bool {
        self.attempted >= self.candidates.len()
    }

    /// Whether a fallback, suffix, or advertised `NICKLEN` changed the name.
    pub fn adjusted(&self) -> bool {
        self.attempted > 1
            || self
                .candidates
                .first()
                .is_some_and(|candidate| *candidate != self.requested)
    }
}

fn bounded(nickname: &Nickname, nick_len: Option<usize>) -> Option<Nickname> {
    match nick_len {
        Some(limit) => nickname.truncated(limit),
        None => Some(nickname.clone()),
    }
}

fn push_candidate(
    candidates: &mut Vec<Nickname>,
    seen: &mut HashSet<String>,
    candidate: Option<Nickname>,
    max_attempts: usize,
) {
    if candidates.len() == max_attempts {
        return;
    }
    let Some(candidate) = candidate else {
        return;
    };
    if seen.insert(candidate.as_str().to_owned()) {
        candidates.push(candidate);
    }
}

/// A registration-time numeric that bears on nickname acquisition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NicknameRejection {
    /// 433: the nickname is already in use.
    InUse,
    /// 436: a collision killed the nickname.
    Collision,
    /// 437: the nickname or channel is temporarily unavailable.
    Unavailable,
    /// 432: the server considers the nickname malformed.
    Erroneous,
    /// 431: no nickname was supplied.
    Missing,
}

impl NicknameRejection {
    /// Classify a numeric reply, if it concerns the nickname.
    pub const fn from_numeric(numeric: u16) -> Option<Self> {
        match numeric {
            433 => Some(Self::InUse),
            436 => Some(Self::Collision),
            437 => Some(Self::Unavailable),
            432 => Some(Self::Erroneous),
            431 => Some(Self::Missing),
            _ => None,
        }
    }

    /// Whether trying the next candidate is worthwhile.
    ///
    /// A malformed name is not retried by suffixing, because the next
    /// candidate would share the property the server objected to.
    pub const fn is_retriable(self) -> bool {
        matches!(self, Self::InUse | Self::Collision | Self::Unavailable)
    }
}

/// Nickname length this server accepts, from its own advertisement.
#[cfg(test)]
mod tests {
    use super::*;

    fn nick(value: &str) -> Nickname {
        Nickname::new(value).expect("valid nickname")
    }

    #[test]
    fn nicknames_reject_structurally_invalid_values() {
        assert_eq!(Nickname::new(""), Err(NicknameError::Empty));
        assert_eq!(
            Nickname::new("2fast"),
            Err(NicknameError::InvalidStart('2'))
        );
        assert_eq!(
            Nickname::new("has space"),
            Err(NicknameError::InvalidCharacter(' '))
        );
        assert_eq!(
            Nickname::new("bad!nick"),
            Err(NicknameError::InvalidCharacter('!'))
        );
        assert!(Nickname::new("[Kuebiko]").is_ok());
        assert!(Nickname::new("Māui-2").is_ok());
    }

    #[test]
    fn suffixes_respect_utf8_byte_boundaries_and_attempt_limit() {
        let plan = NicknamePlan::new(
            &nick("MāuiMessenger"),
            &[],
            NickConflictPolicy::Suffix,
            8,
            3,
        );
        let candidates: Vec<&str> = plan.candidates().iter().map(Nickname::as_str).collect();
        assert_eq!(candidates, ["MāuiMes", "MāuiM_2", "MāuiM_3"]);
        assert!(plan.candidates().iter().all(|nick| nick.len_bytes() <= 8));
    }

    #[test]
    fn an_unadvertised_limit_never_shortens_the_requested_nickname() {
        let plan = NicknamePlan::new(
            &nick("Vafthrudnir"),
            &[],
            NickConflictPolicy::Suffix,
            None,
            4,
        );
        assert_eq!(plan.candidates()[0].as_str(), "Vafthrudnir");
        assert!(!plan.adjusted());
        assert!(
            plan.candidates()
                .iter()
                .all(|candidate| candidate.as_str().starts_with("Vafthrudnir"))
        );
    }

    #[test]
    fn an_advertised_limit_is_still_honored() {
        let plan = NicknamePlan::new(
            &nick("Vafthrudnir"),
            &[],
            NickConflictPolicy::Suffix,
            Some(9),
            3,
        );
        assert_eq!(plan.candidates()[0].as_str(), "Vafthrudn");
        assert!(plan.candidates().iter().all(|c| c.len_bytes() <= 9));
    }

    #[test]
    fn truncation_reports_the_nickname_as_adjusted() {
        let plan = NicknamePlan::new(&nick("SeshatLedger"), &[], NickConflictPolicy::Fail, 9, 4);
        assert_ne!(plan.candidates()[0], *plan.requested());
        assert!(plan.adjusted());

        let untouched = NicknamePlan::new(&nick("Kuebiko"), &[], NickConflictPolicy::Fail, 9, 4);
        assert!(!untouched.adjusted());
    }

    #[test]
    fn agent_names_from_the_live_network_survive_registration() {
        for name in [
            "GoibniuForge",
            "HermesErgo",
            "SeshatLedger",
            "NisabaCodex",
            "Vafthrudnir",
        ] {
            let plan = NicknamePlan::new(&nick(name), &[], NickConflictPolicy::Suffix, None, 3);
            assert_eq!(plan.candidates()[0].as_str(), name, "{name} was altered");
        }
    }

    #[test]
    fn explicit_fail_policy_does_not_add_suffixes() {
        let plan = NicknamePlan::new(&nick("Athena"), &[], NickConflictPolicy::Fail, 30, 8);
        assert_eq!(plan.candidates().len(), 1);
        assert_eq!(plan.candidates()[0].as_str(), "Athena");
    }

    #[test]
    fn short_limits_never_produce_an_invalid_candidate() {
        let plan = NicknamePlan::new(&nick("Athena"), &[], NickConflictPolicy::Suffix, 1, 8);
        assert_eq!(plan.candidates().len(), 1);
        assert_eq!(plan.candidates()[0].as_str(), "A");
        assert!(
            plan.candidates()
                .iter()
                .all(|candidate| Nickname::new(candidate.as_str()).is_ok())
        );
    }

    #[test]
    fn fallbacks_precede_generated_suffixes_and_deduplicate() {
        let plan = NicknamePlan::new(
            &nick("Vili"),
            &[nick("Ve"), nick("Vili")],
            NickConflictPolicy::Suffix,
            30,
            4,
        );
        let candidates: Vec<&str> = plan.candidates().iter().map(Nickname::as_str).collect();
        assert_eq!(candidates, ["Vili", "Ve", "Vili_2", "Vili_3"]);
    }

    #[test]
    fn a_plan_reports_exhaustion_and_adjustment() {
        let mut plan = NicknamePlan::new(&nick("Vili"), &[], NickConflictPolicy::Fail, 30, 1);
        assert_eq!(plan.next_candidate().map(Nickname::as_str), Some("Vili"));
        assert!(!plan.adjusted());
        assert!(plan.is_exhausted());
        assert!(plan.next_candidate().is_none());
    }

    #[test]
    fn only_transient_rejections_are_retried() {
        assert_eq!(
            NicknameRejection::from_numeric(433),
            Some(NicknameRejection::InUse)
        );
        assert!(
            NicknameRejection::from_numeric(437)
                .expect("437")
                .is_retriable()
        );
        assert!(
            !NicknameRejection::from_numeric(432)
                .expect("432")
                .is_retriable()
        );
        assert_eq!(NicknameRejection::from_numeric(1), None);
    }

    #[test]
    fn nicknames_compare_under_the_server_case_mapping() {
        assert!(nick("Kuebiko").same(&nick("kuebiko"), CaseMapping::Rfc1459));
        assert!(nick("nick[]").same(&nick("nick{}"), CaseMapping::Rfc1459));
        assert!(!nick("nick[]").same(&nick("nick{}"), CaseMapping::Ascii));
    }
}
