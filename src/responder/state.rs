//! Durable responder state and its process-lifetime lock.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::output::ReplyAction;

/// Current on-disk state schema.
pub const STATE_SCHEMA_VERSION: u32 = 2;
/// Dynamic-tool registry installed in newly created App Server threads.
pub const TOOL_REGISTRY_VERSION: u32 = 2;

/// An MCP journal cursor kept opaque except for its stable wire fields.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Cursor {
    /// Gateway-created stream identifier.
    pub stream_id: String,
    /// Last handled sequence within that stream.
    pub sequence: u64,
}

/// Subscription fields returned by `irc.attention.open`.
///
/// The gateway's `filterAddition` recipe and the `subscriptions/listen` wire
/// shape are camelCase, so this persistence mirror must be too — snake_case
/// field names would silently deserialize both fields to `None`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredSubscriptionFilter {
    /// Whether resource-list changes were accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources_list_changed: Option<bool>,
    /// Exact resource URIs requested on the consolidated stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_subscriptions: Option<Vec<String>>,
}

/// Valid model output that must finish dispatching before its cursor commits.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PendingOutbox {
    /// Reply actions in required send order.
    pub actions: Vec<ReplyAction>,
    /// Index of the first action not durably recorded as sent.
    pub next_unsent_action: usize,
    /// Cursor to commit only after every action is sent.
    pub resume_cursor: Cursor,
    /// Whether this outbox completes the one-time bootstrap turn.
    pub completes_bootstrap: bool,
}

/// Everything required to resume one adapter-owned identity and thread.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResponderState {
    /// On-disk schema discriminator.
    pub schema_version: u32,
    /// MCP endpoint this profile is permanently bound to.
    pub endpoint: String,
    /// Canonical repository workspace this thread may modify.
    #[serde(default)]
    pub workspace: String,
    /// Adapter-created App Server thread. Never supplied by an operator.
    pub thread_id: Option<String>,
    /// Version of the dynamic IRC tools persisted with `thread_id`.
    #[serde(default)]
    pub tool_registry_version: u32,
    /// Current in-memory IRC gateway handle, if it remains live.
    pub agent_id: Option<String>,
    /// Current attention watch handle, if it remains live.
    pub watch_id: Option<String>,
    /// Nickname accepted by the IRC server on the current/recent connection.
    pub accepted_nickname: Option<String>,
    /// Candidates the model chose for itself in this profile's naming turn,
    /// kept so a crash before the first registration preserves the intended
    /// identity. Absent in profiles saved before self-naming existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chosen_candidates: Vec<String>,
    /// Server-returned consolidated subscription filter.
    pub subscription_filter: Option<StoredSubscriptionFilter>,
    /// Resource update that is allowed to wake the model.
    pub model_resume_resource: Option<String>,
    /// Last cursor whose reply outbox completed.
    pub committed_cursor: Option<Cursor>,
    /// Whether the required hello was successfully dispatched.
    pub bootstrap_complete: bool,
    /// Validated replies not yet completely dispatched.
    pub pending_outbox: Option<PendingOutbox>,
}

impl ResponderState {
    /// Make a fresh profile permanently bound to one endpoint and workspace.
    pub fn fresh(endpoint: String, workspace: String) -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            endpoint,
            workspace,
            thread_id: None,
            tool_registry_version: TOOL_REGISTRY_VERSION,
            agent_id: None,
            watch_id: None,
            accepted_nickname: None,
            chosen_candidates: Vec::new(),
            subscription_filter: None,
            model_resume_resource: None,
            committed_cursor: None,
            bootstrap_complete: false,
            pending_outbox: None,
        }
    }

    fn validate(&self, endpoint: &str, workspace: &str) -> anyhow::Result<()> {
        ensure!(
            self.schema_version == STATE_SCHEMA_VERSION,
            "unsupported responder state schema {}; this binary supports {}",
            self.schema_version,
            STATE_SCHEMA_VERSION
        );
        ensure!(
            self.endpoint == endpoint,
            "state profile is bound to MCP endpoint {:?}, not {:?}; choose another --state-dir",
            self.endpoint,
            endpoint
        );
        ensure!(
            self.workspace == workspace,
            "state profile is bound to workspace {:?}, not {:?}; choose another --state-dir",
            self.workspace,
            workspace
        );
        if let Some(outbox) = &self.pending_outbox {
            ensure!(
                outbox.next_unsent_action <= outbox.actions.len(),
                "pending outbox checkpoint is invalid"
            );
        }
        Ok(())
    }
}

/// Exclusive profile access plus atomic state persistence.
pub struct StateStore {
    directory: PathBuf,
    state_path: PathBuf,
    endpoint: String,
    workspace: String,
    _lock: File,
}

impl StateStore {
    /// Open/create a private state directory and take its lifetime lock.
    pub fn open(
        directory: &Path,
        endpoint: &str,
        workspace: &str,
    ) -> anyhow::Result<(Self, ResponderState)> {
        create_private_directory(directory)?;
        let lock_path = directory.join("responder.lock");
        let lock = private_file_options()
            .read(true)
            .write(true)
            .create(true)
            .open(&lock_path)
            .with_context(|| format!("open responder lock {}", lock_path.display()))?;
        set_private_file_mode(&lock_path)?;
        lock.try_lock_exclusive().with_context(|| {
            format!(
                "lock responder profile {}; another responder may already be running",
                directory.display()
            )
        })?;

        let store = Self {
            directory: directory.to_path_buf(),
            state_path: directory.join("state.json"),
            endpoint: endpoint.to_owned(),
            workspace: workspace.to_owned(),
            _lock: lock,
        };
        let state = if store.state_path.exists() {
            set_private_file_mode(&store.state_path)?;
            let bytes = fs::read(&store.state_path)
                .with_context(|| format!("read {}", store.state_path.display()))?;
            let state: ResponderState = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse {}", store.state_path.display()))?;
            state.validate(endpoint, workspace)?;
            state
        } else {
            ResponderState::fresh(endpoint.to_owned(), workspace.to_owned())
        };
        Ok((store, state))
    }

    /// Root directory holding this profile and its isolated Codex home.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Replace `state.json` atomically and fsync both file and directory.
    pub fn save(&self, state: &ResponderState) -> anyhow::Result<()> {
        state.validate(&self.endpoint, &self.workspace)?;
        let temporary = self
            .directory
            .join(format!(".state.json.{}.tmp", Uuid::new_v4()));
        let file = private_file_options()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .with_context(|| format!("create {}", temporary.display()))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, state).context("serialize responder state")?;
        writer.write_all(b"\n").context("finish responder state")?;
        writer.flush().context("flush responder state")?;
        writer
            .get_ref()
            .sync_all()
            .context("sync responder state")?;
        fs::rename(&temporary, &self.state_path).with_context(|| {
            format!(
                "publish responder state {} -> {}",
                temporary.display(),
                self.state_path.display()
            )
        })?;
        File::open(&self.directory)
            .and_then(|directory| directory.sync_all())
            .with_context(|| format!("sync {}", self.directory.display()))?;
        Ok(())
    }
}

/// Create a directory whose contents are not visible to other local users.
pub fn create_private_directory(path: &Path) -> anyhow::Result<()> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        ensure!(
            !metadata.file_type().is_symlink(),
            "refusing symlinked private directory {}",
            path.display()
        );
    }
    fs::create_dir_all(path).with_context(|| format!("create {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("chmod 0700 {}", path.display()))?;
    }
    Ok(())
}

fn private_file_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

#[cfg(unix)]
fn set_private_file_mode(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod 0600 {}", path.display()))
}

#[cfg(not(unix))]
fn set_private_file_mode(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // A concurrently spawning test child (the scripted Codex stand-ins)
    // inherits every open descriptor for its fork-to-exec window, which can
    // briefly keep a just-dropped profile flock alive. Wait that window out
    // so the assertion sees the binding error, not the transient lock.
    fn open_after_lock_release(directory: &Path, endpoint: &str, workspace: &str) -> anyhow::Error {
        for _ in 0..200 {
            let error = StateStore::open(directory, endpoint, workspace)
                .err()
                .expect("open must fail");
            if !error.to_string().contains("another responder") {
                return error;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("profile lock was never released");
    }

    #[test]
    fn a_profile_is_bound_to_one_endpoint_and_lock_holder() {
        let directory = tempfile::tempdir().expect("tempdir");
        let (store, mut state) = StateStore::open(
            directory.path(),
            "http://irc:8080/mcp",
            "/workspace/project",
        )
        .expect("first lock");
        store.save(&state).expect("save");
        let error = StateStore::open(
            directory.path(),
            "http://irc:8080/mcp",
            "/workspace/project",
        )
        .err()
        .expect("second lock must fail");
        assert!(error.to_string().contains("another responder"), "{error:#}");
        state.workspace = "/workspace/other".into();
        let error = store
            .save(&state)
            .expect_err("in-memory workspace rebinding must fail");
        assert!(
            error.to_string().contains("bound to workspace"),
            "{error:#}"
        );
        drop(store);
        let error = open_after_lock_release(
            directory.path(),
            "http://elsewhere/mcp",
            "/workspace/project",
        );
        assert!(
            error.to_string().contains("bound to MCP endpoint"),
            "{error:#}"
        );

        let error =
            open_after_lock_release(directory.path(), "http://irc:8080/mcp", "/workspace/other");
        assert!(
            error.to_string().contains("bound to workspace"),
            "{error:#}"
        );
    }

    #[test]
    fn subscription_filter_uses_the_gateway_camel_case_wire_shape() {
        // Mirrors the exact filterAddition irc.attention.open returns.
        let filter: StoredSubscriptionFilter = serde_json::from_value(serde_json::json!({
            "resourcesListChanged": true,
            "resourceSubscriptions": ["irc://watches/example", "irc://agents/a/status"]
        }))
        .expect("parse gateway filterAddition");
        assert_eq!(filter.resources_list_changed, Some(true));
        assert!(
            filter
                .resource_subscriptions
                .as_deref()
                .expect("subscribed URIs")
                .contains(&"irc://watches/example".to_owned())
        );
        let round_trip = serde_json::to_value(&filter).expect("serialize filter");
        assert_eq!(
            round_trip["resourceSubscriptions"][0],
            "irc://watches/example"
        );
    }

    #[test]
    fn an_invalid_outbox_checkpoint_is_rejected() {
        let mut state = ResponderState::fresh("http://irc/mcp".into(), "/workspace/project".into());
        state.pending_outbox = Some(PendingOutbox {
            actions: Vec::new(),
            next_unsent_action: 1,
            resume_cursor: Cursor {
                stream_id: "stream".into(),
                sequence: 4,
            },
            completes_bootstrap: false,
        });
        assert!(
            state
                .validate("http://irc/mcp", "/workspace/project")
                .is_err()
        );
    }

    #[test]
    fn schema_one_profiles_report_the_migration_error_before_workspace_binding() {
        let state = ResponderState::fresh("http://irc/mcp".into(), "/workspace/project".into());
        let mut value = serde_json::to_value(state).expect("serialize state");
        value["schema_version"] = serde_json::json!(1);
        value.as_object_mut().unwrap().remove("workspace");
        let old: ResponderState = serde_json::from_value(value).expect("parse schema one state");
        let error = old
            .validate("http://irc/mcp", "/workspace/project")
            .expect_err("schema one must be rejected");
        assert!(
            error
                .to_string()
                .contains("unsupported responder state schema 1"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_cannot_be_used_as_private_state() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("real");
        fs::create_dir(&destination).expect("destination");
        let link = directory.path().join("profile");
        symlink(&destination, &link).expect("symlink");
        let error = StateStore::open(&link, "http://irc/mcp", "/workspace/project")
            .err()
            .expect("symlink must fail");
        assert!(error.to_string().contains("symlinked"), "{error:#}");
    }
}
