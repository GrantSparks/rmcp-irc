//! One task ledger, shared by every request and bound to its callers.
//!
//! MCP tasks are the only handle a client has on work that outlives the call
//! that started it: after a `CreateTaskResult` the originating response stream
//! is finished, and everything the client learns afterwards arrives through
//! `tasks/get`, `tasks/update`, and `tasks/cancel` — each of which is a
//! *separate request*. On Streamable HTTP each of those requests builds its own
//! handler instance, so a task store owned by the handler is a store the next
//! request cannot see: the task id in the client's hand resolves to nothing,
//! the work keeps running unobserved, and cancellation has nowhere to land.
//!
//! The store therefore lives on the gateway — the one object every per-request
//! handler shares — and this type is that store. It wraps a single
//! [`TaskManager`] and adds the one thing the SDK deliberately leaves to the
//! server: who is allowed to look.
//!
//! ## Owner binding
//!
//! A task id is high-entropy, but the specification is explicit that
//! unguessable is not authorized: ids handed to clients are bearer-shaped, and
//! a server that does not bind them to a principal lets any caller who learns
//! one read another caller's result and cancel another caller's transfer. Every
//! task here records the [`OwnerId`] that created it, and a lookup by anyone
//! else fails **exactly** as an unknown id does — same code, same message. That
//! is the same reasoning as [`crate::mcp::authorization::not_authorized`]: a
//! distinguishable "not yours" is an oracle for which ids exist.
//!
//! ## Lifetime
//!
//! Tasks are process-lifetime. Nothing here is written to disk, because the DCC
//! transfer a task follows does not survive a restart either — a task id that
//! outlived the work would be a handle onto nothing. After a restart every
//! previously issued id is simply unknown, which is the deterministic answer the
//! specification allows for an expired task and the only honest one available.
//!
//! ## Keeping the owner map in step
//!
//! The SDK decides when a task stops being readable, so this map must never
//! decide it first: a record dropped while its task still resolves would lock a
//! caller out of their own result, reporting "unknown" for something the server
//! is still holding. The SDK is therefore the authority, and every operation
//! here asks it first — which also matters because the SDK has *no background
//! sweeper*. It expires tasks opportunistically, from its own entry points
//! only, so a poll that this module answered without calling through would be a
//! poll that never ran the sweep that eventually stops a runaway operation.
//!
//! Records are therefore never retired on a clock of this module's own. A clock
//! cannot work: the SDK fails an overdue task and then holds it one further
//! window *from when it settled*, and settling is only noticed when something
//! sweeps, so the moment an id stops resolving is not a fixed offset from when
//! it was created. Any independently computed deadline eventually disagrees,
//! and the direction it disagrees in is the one that loses somebody their
//! result.
//!
//! Instead a record is dropped exactly when the SDK reports its id unknown.
//! That covers every task anyone polls. For tasks nobody polls again, [`spawn`]
//! reconciles: records old enough that the SDK *could* have released them are
//! re-checked against it, and only the ones it has actually forgotten are
//! dropped. Age is a filter on what is worth asking about, never the answer.
//!
//! [`spawn`]: TaskLedger::spawn

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use rmcp::{
    ErrorData as McpError,
    model::{DetailedTask, Task},
    task_manager::{DEFAULT_TASK_TTL_MS, TaskContext, TaskFuture, TaskManager, TaskOptions},
};

use crate::mcp::authorization::OwnerId;

/// How long a task stays observable before the SDK fails it as expired.
///
/// The SDK default, restated here because the follow window and the recheck
/// interval are both derived from it and the three must not drift.
pub const TASK_TTL: Duration = Duration::from_millis(DEFAULT_TASK_TTL_MS);

/// How often a running task republishes its progress, and the poll cadence
/// advertised to clients.
///
/// Slow enough not to churn, fast enough that a stalled transfer is visible
/// well before its deadline.
pub const TASK_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// TTL windows after which a record is worth re-checking against the SDK.
///
/// Two is when the SDK's own arithmetic says an id would have been released if
/// it had been swept on schedule, so it is the earliest point at which asking
/// is likely to be productive. Nothing is deleted on this timer — see the note
/// on keeping the map in step.
const RECHECK_WINDOWS: u32 = 2;

/// The caller that owns one task, and when to ask whether it still exists.
#[derive(Clone, Debug)]
struct OwnerRecord {
    owner: OwnerId,
    recheck_at: Instant,
}

/// Every task this process is running, and who may observe each one.
#[derive(Clone)]
pub struct TaskLedger {
    manager: TaskManager,
    owners: Arc<Mutex<HashMap<String, OwnerRecord>>>,
    ttl: Duration,
}

impl std::fmt::Debug for TaskLedger {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The manager holds running futures rather than inspectable data, and
        // the owner map holds credentials-derived identities, so neither belongs
        // in a debug print of the gateway.
        formatter
            .debug_struct("TaskLedger")
            .field("running_tasks", &self.manager.running_task_count())
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl Default for TaskLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskLedger {
    /// Create the ledger with the standard retention window.
    pub fn new() -> Self {
        Self::with_ttl(TASK_TTL)
    }

    /// Create a ledger whose tasks expire after `ttl`.
    ///
    /// The window is a parameter so a test can watch a task expire without
    /// waiting out the production TTL; the retention arithmetic is identical
    /// either way.
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            manager: TaskManager::new(),
            owners: Arc::new(Mutex::new(HashMap::new())),
            ttl,
        }
    }

    /// Number of task operations still running.
    pub fn running_task_count(&self) -> usize {
        self.manager.running_task_count()
    }

    /// How long an operation may keep running before it must settle itself.
    ///
    /// Shorter than the TTL on purpose. When the SDK's sweep finds a task still
    /// running past its deadline it aborts the operation and settles the task
    /// `failed: task expired`, which for a transfer that is merely slow is a
    /// lie — the transfer is fine and continues in the gateway regardless.
    /// Settling first lets the operation report what actually happened instead
    /// of having that verdict imposed on it.
    pub fn follow_window(&self) -> Duration {
        self.ttl.mul_f32(0.8)
    }

    /// Start one operation as a task owned by `owner`.
    ///
    /// The returned [`Task`] is already resolvable through [`Self::get`], which
    /// is what lets the caller answer with a `CreateTaskResult`: the
    /// specification only permits that once the task is durably created.
    pub fn spawn<F>(&self, owner: OwnerId, status_message: &str, make_future: F) -> Task
    where
        F: FnOnce(TaskContext) -> TaskFuture,
    {
        let task = self
            .manager
            .spawn(self.options(status_message), make_future);
        self.lock().insert(
            task.task_id.clone(),
            OwnerRecord {
                owner,
                recheck_at: Instant::now() + RECHECK_WINDOWS * self.ttl,
            },
        );
        self.reconcile();
        task
    }

    /// Serve `tasks/get` for one caller.
    pub fn get(&self, owner: &OwnerId, task_id: &str) -> Result<DetailedTask, McpError> {
        let task = self.resolve(task_id)?;
        self.authorize(owner, task_id)?;
        Ok(task)
    }

    /// Serve `tasks/update` for one caller.
    pub fn update(
        &self,
        owner: &OwnerId,
        task_id: &str,
        input_responses: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> Result<(), McpError> {
        self.resolve(task_id)?;
        self.authorize(owner, task_id)?;
        self.manager.update_task(task_id, input_responses)
    }

    /// Serve `tasks/cancel` for one caller.
    pub fn cancel(&self, owner: &OwnerId, task_id: &str) -> Result<(), McpError> {
        self.resolve(task_id)?;
        self.authorize(owner, task_id)?;
        self.manager.cancel_task(task_id)
    }

    /// Ask the SDK whether this task still exists, forgetting it if not.
    ///
    /// Every operation goes through here before anything else, for two reasons.
    /// It makes the SDK the single authority on whether an id resolves, so this
    /// module can never report "unknown" for a task the server is still
    /// holding. And because the SDK expires tasks only from its own entry
    /// points, calling through is what keeps a caller's polls able to retire an
    /// operation that has outlived its deadline — an ownership check that
    /// answered without it would leave a runaway task with nothing to stop it.
    ///
    /// The lookup is read-only, so doing it before the ownership check cannot
    /// change anything on behalf of a caller who turns out not to own the task.
    fn resolve(&self, task_id: &str) -> Result<DetailedTask, McpError> {
        self.manager.get_task(task_id).inspect_err(|_| {
            self.lock().remove(task_id);
        })
    }

    /// Options every task created here runs with.
    ///
    /// The TTL is the ledger's own, so the advertised `ttlMs`, the SDK's expiry
    /// sweep, and this module's owner-record retention can never disagree.
    /// There is deliberately no per-call override: 2026-07-28 tasks are
    /// server-directed, so a client neither asks for a task nor names how long
    /// one should live.
    fn options(&self, status_message: &str) -> TaskOptions {
        TaskOptions::new()
            .with_ttl_ms(self.ttl.as_millis() as u64)
            .with_poll_interval_ms(TASK_POLL_INTERVAL.as_millis() as u64)
            .with_status_message(status_message)
    }

    /// Confirm `owner` created `task_id`, or refuse as if it never existed.
    fn authorize(&self, owner: &OwnerId, task_id: &str) -> Result<(), McpError> {
        match self.lock().get(task_id) {
            Some(record) if &record.owner == owner => Ok(()),
            _ => Err(unknown_task(task_id)),
        }
    }

    /// Drop records the SDK has confirmed it no longer serves.
    ///
    /// Only records old enough to be worth asking about are checked, and the
    /// SDK is asked one id at a time outside this module's lock — so the common
    /// case costs a comparison per record, and a record is never removed on
    /// suspicion. The check doubles as a sweep, which is how a task nobody polls
    /// still gets retired.
    fn reconcile(&self) {
        let now = Instant::now();
        let candidates: Vec<String> = {
            let owners = self.lock();
            owners
                .iter()
                .filter(|(_, record)| record.recheck_at <= now)
                .map(|(task_id, _)| task_id.clone())
                .collect()
        };
        let forgotten: Vec<String> = candidates
            .into_iter()
            .filter(|task_id| self.manager.get_task(task_id).is_err())
            .collect();
        if forgotten.is_empty() {
            return;
        }
        let mut owners = self.lock();
        for task_id in forgotten {
            owners.remove(&task_id);
        }
    }

    /// Take the owner map, recovering from a panic in another holder.
    ///
    /// Nothing under this lock can leave a record half-written — the guarded
    /// section is a map insert or a lookup — so a poisoned lock means an
    /// unrelated panic, and refusing every later task over it would turn one
    /// unrelated fault into a permanently broken task surface.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, OwnerRecord>> {
        self.owners.lock().unwrap_or_else(|poisoned| {
            self.owners.clear_poison();
            poisoned.into_inner()
        })
    }
}

/// The error a caller gets for a task id this server will not resolve.
///
/// Byte-identical to the SDK's own unknown-task error, which is the point: an
/// expired task, a task from a previous process, and another caller's task must
/// be one answer, or the difference between them is information.
fn unknown_task(task_id: &str) -> McpError {
    McpError::invalid_params(format!("unknown task: {task_id}"), None)
}

#[cfg(test)]
mod tests {
    use rmcp::model::{CallToolResult, TaskStatus};

    use super::*;

    /// A task that runs until it is cancelled or its TTL elapses.
    fn pending(ledger: &TaskLedger, owner: OwnerId) -> String {
        ledger
            .spawn(owner, "waiting", |context| {
                Box::pin(async move {
                    context.cancelled().await;
                    Err(rmcp::task_manager::TaskExit::Cancelled)
                })
            })
            .task_id
    }

    #[tokio::test]
    async fn the_owner_that_created_a_task_is_the_only_caller_that_can_see_it() {
        let ledger = TaskLedger::new();
        let mine = OwnerId::from_bearer("mine");
        let theirs = OwnerId::from_bearer("theirs");
        let task_id = pending(&ledger, mine.clone());

        assert_eq!(
            ledger
                .get(&mine, &task_id)
                .expect("my own task")
                .task
                .status,
            TaskStatus::Working
        );
        let refused = ledger
            .get(&theirs, &task_id)
            .expect_err("another caller's task");
        assert_eq!(refused.code.0, -32602);
        assert_eq!(refused.message, format!("unknown task: {task_id}"));
    }

    #[tokio::test]
    async fn a_task_nobody_owns_is_refused_exactly_as_the_sdk_refuses_an_unknown_one() {
        // The two errors must not be distinguishable: any difference tells a
        // caller which ids exist and belong to somebody else.
        let ledger = TaskLedger::new();
        let manager = TaskManager::new();
        let invented = "3f1a5c8e-0000-4000-8000-000000000000";
        let ours = ledger
            .get(&OwnerId::local(), invented)
            .expect_err("no such task");
        let sdk = manager.get_task(invented).expect_err("no such task");
        assert_eq!(ours.code, sdk.code);
        assert_eq!(ours.message, sdk.message);
        assert_eq!(ours.data, sdk.data);
    }

    #[tokio::test]
    async fn wrong_owner_cannot_cancel_or_answer_another_callers_task() {
        let ledger = TaskLedger::new();
        let mine = OwnerId::from_bearer("mine");
        let theirs = OwnerId::from_bearer("theirs");
        let task_id = pending(&ledger, mine.clone());

        assert!(ledger.cancel(&theirs, &task_id).is_err());
        assert!(ledger.update(&theirs, &task_id, []).is_err());
        // The refusal must not have reached the operation.
        assert_eq!(
            ledger.get(&mine, &task_id).expect("still mine").task.status,
            TaskStatus::Working
        );
        ledger.cancel(&mine, &task_id).expect("my own task");
    }

    #[tokio::test]
    async fn an_expired_task_becomes_unknown_to_the_caller_that_created_it() {
        let ttl = Duration::from_millis(200);
        let ledger = TaskLedger::with_ttl(ttl);
        let owner = OwnerId::local();
        let task_id = pending(&ledger, owner.clone());
        assert!(ledger.get(&owner, &task_id).is_ok());

        // Past one TTL the task has failed but is still readable, so a poller
        // that was mid-interval still learns the outcome.
        tokio::time::sleep(ttl + Duration::from_millis(50)).await;
        assert_eq!(
            ledger
                .get(&owner, &task_id)
                .expect("still readable")
                .task
                .status,
            TaskStatus::Failed
        );

        // Past the retention window the id is simply unknown, and says so in
        // the same words a never-issued id does.
        tokio::time::sleep(ttl + Duration::from_millis(100)).await;
        let gone = ledger.get(&owner, &task_id).expect_err("retired");
        assert_eq!(gone.message, format!("unknown task: {task_id}"));
        assert!(
            !ledger.lock().contains_key(&task_id),
            "the owner record follows the task the SDK forgot, rather than lingering on a clock"
        );
    }

    #[tokio::test]
    async fn a_task_the_sdk_still_serves_is_never_reported_unknown_by_the_ledger() {
        // The SDK has no background sweeper: it expires tasks only from its own
        // entry points. A ledger that retired an owner record on a clock of its
        // own could therefore answer "unknown" for a result the SDK is still
        // holding — losing a caller their own completed transfer. Asking the
        // SDK first is what makes that impossible.
        let ttl = Duration::from_millis(150);
        let ledger = TaskLedger::with_ttl(ttl);
        let owner = OwnerId::local();
        let task_id = ledger
            .spawn(owner.clone(), "slow", |_| {
                Box::pin(async {
                    // Settles well past its own TTL, with nothing polling in
                    // between to make the SDK notice.
                    tokio::time::sleep(Duration::from_millis(400)).await;
                    Ok(CallToolResult::success(Vec::new()))
                })
            })
            .task_id;

        tokio::time::sleep(Duration::from_millis(500)).await;
        let settled = ledger
            .get(&owner, &task_id)
            .expect("a result the SDK still holds must still reach its owner");
        assert!(settled.task.status.is_terminal());
    }

    #[tokio::test]
    async fn owner_records_do_not_accumulate_for_the_life_of_the_process() {
        let ledger = TaskLedger::with_ttl(Duration::from_millis(10));
        let owner = OwnerId::local();
        for _ in 0..4 {
            ledger.spawn(owner.clone(), "done", |_| {
                Box::pin(async { Ok(CallToolResult::success(Vec::new())) })
            });
        }
        assert_eq!(ledger.lock().len(), 4);
        // Nothing ever polls these, so reconciliation on the next spawn is the
        // only thing that can retire them.
        tokio::time::sleep(Duration::from_millis(10) * (RECHECK_WINDOWS + 2)).await;
        ledger.spawn(owner, "done", |_| {
            Box::pin(async { Ok(CallToolResult::success(Vec::new())) })
        });
        assert_eq!(ledger.lock().len(), 1);
    }

    #[tokio::test]
    async fn an_operation_settles_itself_before_the_sdk_would_call_it_expired() {
        // The SDK's sweep aborts an overdue operation and settles the task
        // `failed: task expired`, which for work that is merely slow is a lie.
        // The follow window is what gives an operation the chance to report
        // what actually happened instead.
        let ledger = TaskLedger::with_ttl(Duration::from_secs(300));
        assert!(ledger.follow_window() < Duration::from_secs(300));
        assert!(ledger.follow_window() > Duration::from_secs(120));
    }

    #[tokio::test]
    async fn a_fresh_ledger_knows_nothing_of_another_ledgers_tasks() {
        // This is the restart contract: task state is process-lifetime, so an
        // id minted by one process is unknown to the next.
        let owner = OwnerId::local();
        let before = TaskLedger::new();
        let task_id = pending(&before, owner.clone());
        let after = TaskLedger::new();
        assert_eq!(
            after
                .get(&owner, &task_id)
                .expect_err("a restart forgets every task")
                .message,
            format!("unknown task: {task_id}")
        );
    }

    #[tokio::test]
    async fn every_task_advertises_the_ledgers_own_retention_and_poll_cadence() {
        let ledger = TaskLedger::with_ttl(Duration::from_secs(30));
        let task = ledger.spawn(OwnerId::local(), "negotiating", |_| {
            Box::pin(async { Ok(CallToolResult::success(Vec::new())) })
        });
        assert_eq!(task.ttl_ms, Some(30_000));
        assert_eq!(
            task.poll_interval_ms,
            Some(TASK_POLL_INTERVAL.as_millis() as u64)
        );
        assert_eq!(task.status_message.as_deref(), Some("negotiating"));
    }
}
