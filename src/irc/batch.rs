//! Tracking for nested and interleaved IRCv3 batches.

use std::collections::HashMap;

/// Active IRCv3 batch metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActiveBatch {
    /// Batch identifier without the leading plus or minus.
    pub id: String,
    /// Opaque batch type.
    pub kind: String,
    /// Optional parent batch from the batch tag.
    pub parent: Option<String>,
}

/// Actor-local active batch table.
#[derive(Debug)]
pub struct BatchTracker {
    max_active: usize,
    active: HashMap<String, ActiveBatch>,
}

impl BatchTracker {
    /// Create an empty tracker.
    pub fn new(max_active: usize) -> Self {
        assert!(max_active > 0, "active batch bound must be non-zero");
        Self {
            max_active,
            active: HashMap::new(),
        }
    }

    /// Start a batch, allowing interleaving and validated nesting.
    pub fn start(
        &mut self,
        id: impl Into<String>,
        kind: impl Into<String>,
        parent: Option<String>,
    ) -> Result<(), BatchError> {
        let id = id.into();
        if self.active.contains_key(&id) {
            return Err(BatchError::Duplicate(id));
        }
        if self.active.len() >= self.max_active {
            return Err(BatchError::Limit(self.max_active));
        }
        if let Some(parent) = &parent
            && !self.active.contains_key(parent)
        {
            return Err(BatchError::UnknownParent(parent.clone()));
        }
        self.active.insert(
            id.clone(),
            ActiveBatch {
                id,
                kind: kind.into(),
                parent,
            },
        );
        Ok(())
    }

    /// End a batch after all nested children have ended.
    pub fn end(&mut self, id: &str) -> Result<ActiveBatch, BatchError> {
        if self
            .active
            .values()
            .any(|batch| batch.parent.as_deref() == Some(id))
        {
            return Err(BatchError::OpenChildren(id.to_owned()));
        }
        self.active
            .remove(id)
            .ok_or_else(|| BatchError::Unknown(id.to_owned()))
    }

    /// Resolve a batch identifier referenced by an IRCv3 tag.
    pub fn get(&self, id: &str) -> Option<&ActiveBatch> {
        self.active.get(id)
    }

    /// Resolve one active batch followed by each active parent.
    ///
    /// The result starts with `id`. Cloned metadata lets a caller retain the
    /// lineage while ending the referenced batch.
    pub fn lineage(&self, id: &str) -> Vec<ActiveBatch> {
        let mut lineage = Vec::new();
        let mut current = Some(id);
        while let Some(id) = current {
            let Some(batch) = self.active.get(id) else {
                break;
            };
            lineage.push(batch.clone());
            current = batch.parent.as_deref();
        }
        lineage
    }

    /// Drop connection-local batch state after a socket reconnect.
    pub fn clear(&mut self) {
        self.active.clear();
    }
}

/// Invalid or resource-exhausted batch transition.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BatchError {
    /// Identifier is already active.
    #[error("batch identifier is already active: {0}")]
    Duplicate(String),
    /// Parent tag references no active batch.
    #[error("batch parent is not active: {0}")]
    UnknownParent(String),
    /// End marker references no active batch.
    #[error("batch identifier is not active: {0}")]
    Unknown(String),
    /// Parent ended before a child.
    #[error("batch still has active children: {0}")]
    OpenChildren(String),
    /// Configured active-batch bound was reached.
    #[error("active batch limit reached: {0}")]
    Limit(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supports_nested_and_interleaved_batches() {
        let mut tracker = BatchTracker::new(4);
        tracker.start("a", "chathistory", None).expect("a");
        tracker.start("b", "other", None).expect("b");
        tracker
            .start("child", "multiline", Some("a".into()))
            .expect("child");
        assert_eq!(
            tracker
                .lineage("child")
                .iter()
                .map(|batch| batch.kind.as_str())
                .collect::<Vec<_>>(),
            ["multiline", "chathistory"]
        );
        assert_eq!(tracker.end("a"), Err(BatchError::OpenChildren("a".into())));
        tracker.end("b").expect("interleaved");
        tracker.end("child").expect("child");
        tracker.end("a").expect("parent");
    }
}
