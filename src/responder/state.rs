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
pub const STATE_SCHEMA_VERSION: u32 = 1;

/// An MCP journal cursor kept opaque except for its stable wire fields.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Cursor {
    /// Gateway-created stream identifier.
    pub stream_id: String,
    /// Last handled sequence within that stream.
    pub sequence: u64,
}

/// Subscription fields returned by `irc.attention.open`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
    /// Adapter-created App Server thread. Never supplied by an operator.
    pub thread_id: Option<String>,
    /// Current in-memory IRC gateway handle, if it remains live.
    pub agent_id: Option<String>,
    /// Current attention watch handle, if it remains live.
    pub watch_id: Option<String>,
    /// Nickname accepted by the IRC server on the current/recent connection.
    pub accepted_nickname: Option<String>,
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
    /// Make a fresh profile permanently bound to `endpoint`.
    pub fn fresh(endpoint: String) -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            endpoint,
            thread_id: None,
            agent_id: None,
            watch_id: None,
            accepted_nickname: None,
            subscription_filter: None,
            model_resume_resource: None,
            committed_cursor: None,
            bootstrap_complete: false,
            pending_outbox: None,
        }
    }

    fn validate(&self, endpoint: &str) -> anyhow::Result<()> {
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
    _lock: File,
}

impl StateStore {
    /// Open/create a private state directory and take its lifetime lock.
    pub fn open(directory: &Path, endpoint: &str) -> anyhow::Result<(Self, ResponderState)> {
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
            _lock: lock,
        };
        let state = if store.state_path.exists() {
            set_private_file_mode(&store.state_path)?;
            let bytes = fs::read(&store.state_path)
                .with_context(|| format!("read {}", store.state_path.display()))?;
            let state: ResponderState = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse {}", store.state_path.display()))?;
            state.validate(endpoint)?;
            state
        } else {
            ResponderState::fresh(endpoint.to_owned())
        };
        Ok((store, state))
    }

    /// Root directory holding this profile and its isolated Codex home.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Replace `state.json` atomically and fsync both file and directory.
    pub fn save(&self, state: &ResponderState) -> anyhow::Result<()> {
        state.validate(&state.endpoint)?;
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

    #[test]
    fn a_profile_is_bound_to_one_endpoint_and_lock_holder() {
        let directory = tempfile::tempdir().expect("tempdir");
        let (store, state) =
            StateStore::open(directory.path(), "http://irc:8080/mcp").expect("first lock");
        store.save(&state).expect("save");
        let error = StateStore::open(directory.path(), "http://irc:8080/mcp")
            .err()
            .expect("second lock must fail");
        assert!(error.to_string().contains("another responder"), "{error:#}");
        drop(store);
        let error = StateStore::open(directory.path(), "http://elsewhere/mcp")
            .err()
            .expect("endpoint change must fail");
        assert!(
            error.to_string().contains("bound to MCP endpoint"),
            "{error:#}"
        );
    }

    #[test]
    fn an_invalid_outbox_checkpoint_is_rejected() {
        let mut state = ResponderState::fresh("http://irc/mcp".into());
        state.pending_outbox = Some(PendingOutbox {
            actions: Vec::new(),
            next_unsent_action: 1,
            resume_cursor: Cursor {
                stream_id: "stream".into(),
                sequence: 4,
            },
            completes_bootstrap: false,
        });
        assert!(state.validate("http://irc/mcp").is_err());
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
        let error = StateStore::open(&link, "http://irc/mcp")
            .err()
            .expect("symlink must fail");
        assert!(error.to_string().contains("symlinked"), "{error:#}");
    }
}
