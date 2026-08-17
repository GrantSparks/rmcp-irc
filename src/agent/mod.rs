//! One supervised actor per upstream IRC identity.

pub mod actor;
pub mod connection;
pub mod journal;
pub mod reconnect;
pub mod state;

use std::{fmt, str::FromStr};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Opaque in-memory routing handle for one guest IRC identity.
#[derive(Clone, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct AgentId(String);

impl AgentId {
    /// Create a fresh process-local handle.
    pub fn new() -> Self {
        Self(format!("agent-{}", Uuid::new_v4()))
    }

    /// Borrow the wire representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for AgentId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for AgentId {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some(uuid) = value.strip_prefix("agent-") else {
            return Err("agent handle must start with agent-");
        };
        Uuid::parse_str(uuid).map_err(|_| "agent handle contains an invalid UUID")?;
        Ok(Self(value.to_owned()))
    }
}

impl<'de> Deserialize<'de> for AgentId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}
