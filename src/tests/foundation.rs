//! Cross-module contract tests.

use std::collections::HashSet;

use crate::{
    MCP_INSTRUCTIONS,
    config::Config,
    irc::commands::{COMMANDS, spec_for},
    mcp::tools::TOOL_NAMES,
};

#[test]
fn example_configuration_matches_the_typed_model() {
    Config::load("config/example.toml").expect("example config should remain runnable");
}

#[test]
fn stable_tool_names_are_unique_and_namespaced() {
    let unique: HashSet<_> = TOOL_NAMES.iter().copied().collect();
    assert_eq!(unique.len(), TOOL_NAMES.len());
    assert!(TOOL_NAMES.iter().all(|name| name.starts_with("irc.")));
}

#[test]
fn command_registry_names_are_unique() {
    let unique: HashSet<_> = COMMANDS.iter().map(|spec| spec.name).collect();
    assert_eq!(unique.len(), COMMANDS.len());
    for name in unique {
        assert!(spec_for(name).is_some());
    }
}

#[test]
fn onboarding_defers_collaboration_rules_to_the_motd() {
    assert!(MCP_INSTRUCTIONS.contains("mythological"));
    assert!(MCP_INSTRUCTIONS.contains("irc.connect"));
    assert!(MCP_INSTRUCTIONS.contains("MOTD"));
    assert!(MCP_INSTRUCTIONS.contains("irc.attention.open"));
    assert!(MCP_INSTRUCTIONS.contains("60 seconds"));
    assert!(!MCP_INSTRUCTIONS.contains("#control"));
}
