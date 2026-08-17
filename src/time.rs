//! UTC timestamps serialized as RFC 3339 with millisecond precision.

use std::{fmt, str::FromStr};

use chrono::{DateTime, ParseError, SecondsFormat, Utc};
use schemars::{JsonSchema, Schema, json_schema};
use serde::{Deserialize, Serialize};

/// A UTC instant rendered as stable millisecond RFC 3339 text.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Timestamp(DateTime<Utc>);

impl Timestamp {
    /// Current UTC time.
    pub fn now() -> Self {
        Self(Utc::now())
    }

    /// Wrap an existing instant.
    pub const fn from_datetime(value: DateTime<Utc>) -> Self {
        Self(value)
    }

    /// The underlying instant.
    pub const fn as_datetime(self) -> DateTime<Utc> {
        self.0
    }

    /// Build an instant from Unix epoch seconds, as used by `RPL_TOPICWHOTIME`.
    pub fn from_unix_seconds(seconds: i64) -> Option<Self> {
        DateTime::from_timestamp(seconds, 0).map(Self)
    }

    /// Stable millisecond RFC 3339 rendering, as used on the wire.
    pub fn to_rfc3339(self) -> String {
        self.0.to_rfc3339_opts(SecondsFormat::Millis, true)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_rfc3339())
    }
}

impl FromStr for Timestamp {
    type Err = ParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        DateTime::parse_from_rfc3339(value).map(|value| Self(value.with_timezone(&Utc)))
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(value: DateTime<Utc>) -> Self {
        Self(value)
    }
}

impl From<Timestamp> for DateTime<Utc> {
    fn from(value: Timestamp) -> Self {
        value.0
    }
}

impl Serialize for Timestamp {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_rfc3339())
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for Timestamp {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Timestamp".into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "format": "date-time",
            "description": "UTC instant in RFC 3339 form with millisecond precision.",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wire_form_is_millisecond_rfc3339() {
        let timestamp: Timestamp = "2026-08-17T01:02:03.456Z".parse().expect("parse");
        assert_eq!(timestamp.to_rfc3339(), "2026-08-17T01:02:03.456Z");
        assert_eq!(
            serde_json::to_string(&timestamp).expect("serialize"),
            "\"2026-08-17T01:02:03.456Z\""
        );
    }

    #[test]
    fn timestamps_round_trip_through_serde() {
        let original = Timestamp::now();
        let json = serde_json::to_string(&original).expect("serialize");
        let parsed: Timestamp = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.to_rfc3339(), original.to_rfc3339());
    }

    #[test]
    fn ordering_is_chronological_not_lexical() {
        let earlier: Timestamp = "2026-08-17T09:00:00.000Z".parse().expect("parse");
        let later: Timestamp = "2026-08-17T10:00:00.000Z".parse().expect("parse");
        assert!(earlier < later);
        let same_as_earlier: Timestamp = "2026-08-17T11:00:00.000+02:00".parse().expect("parse");
        assert_eq!(same_as_earlier, earlier);
        assert!(same_as_earlier < later);
    }

    #[test]
    fn epoch_seconds_and_rfc3339_are_both_supported_but_distinct() {
        let from_epoch = Timestamp::from_unix_seconds(1_776_000_000).expect("epoch");
        assert_eq!(from_epoch.to_rfc3339(), "2026-04-12T13:20:00.000Z");
        assert!("1776000000".parse::<Timestamp>().is_err());
    }

    #[test]
    fn a_malformed_instant_is_refused() {
        assert!("not a time".parse::<Timestamp>().is_err());
        assert!(serde_json::from_str::<Timestamp>("\"2026-13-45T99:99:99Z\"").is_err());
    }
}
