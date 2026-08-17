//! Typed anchors for server-side `CHATHISTORY` requests.

use std::fmt;

use crate::time::Timestamp;

/// A point in a channel's history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HistoryReference {
    /// A server-assigned message identifier.
    MessageId(String),
    /// An instant, used when no message ID is available.
    At(Timestamp),
}

impl HistoryReference {
    /// Wire form accepted by `CHATHISTORY` selectors.
    pub fn to_wire(&self) -> String {
        match self {
            Self::MessageId(id) => format!("msgid={id}"),
            Self::At(timestamp) => format!("timestamp={}", timestamp.to_rfc3339()),
        }
    }

    /// Prefer a message ID, falling back to a server-supplied time.
    ///
    /// A message ID identifies one line exactly; a timestamp can collide with
    /// other lines in the same millisecond, so it is only used when the server
    /// gave no identifier.
    pub fn best(message_id: Option<&str>, server_time: Option<Timestamp>) -> Option<Self> {
        message_id.map_or_else(
            || server_time.map(Self::At),
            |id| Some(Self::MessageId(id.to_owned())),
        )
    }
}

impl fmt::Display for HistoryReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_wire())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instant() -> Timestamp {
        "2026-08-17T01:02:03.456Z".parse().expect("instant")
    }

    #[test]
    fn each_reference_renders_its_own_wire_form() {
        assert_eq!(
            HistoryReference::MessageId("abc123".into()).to_wire(),
            "msgid=abc123"
        );
        assert_eq!(
            HistoryReference::At(instant()).to_wire(),
            "timestamp=2026-08-17T01:02:03.456Z"
        );
    }

    #[test]
    fn a_message_id_is_preferred_over_a_timestamp() {
        assert_eq!(
            HistoryReference::best(Some("abc"), Some(instant())),
            Some(HistoryReference::MessageId("abc".into()))
        );
        assert_eq!(
            HistoryReference::best(None, Some(instant())),
            Some(HistoryReference::At(instant()))
        );
        assert_eq!(HistoryReference::best(None, None), None);
    }
}
