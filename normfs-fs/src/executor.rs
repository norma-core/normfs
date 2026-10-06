//! Blocking I/O stays off the async runtime's threads.

use std::fs::File;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, oneshot};

use crate::plan::Plan;
use crate::{FsError, PublishReport};

/// Runs before replying so cancellation cannot separate bookkeeping from publication.
/// A panic is logged without changing the committed result.
pub type Accounting = Box<dyn FnOnce(&PublishReport) + Send + 'static>;

pub(crate) struct Resources {
    /// The open file for `Append` and `Restore`; the executor never closes it.
    pub file: Option<Arc<File>>,
    pub runs: Vec<Bytes>,
    /// With this off the fsync steps are reported done without being done,
    /// and nothing proved about durability applies to the plan.
    pub sync: bool,
}

pub(crate) struct Finished {
    pub plan: Plan,
    /// The file a `Create` plan opened, still open, when it reached `Done`.
    pub file: Option<File>,
    /// A `Remove` plan found nothing to unlink.
    pub absent: bool,
}

pub(crate) struct PlanJob {
    pub plan: Plan,
    pub res: Resources,
    pub then: Option<Accounting>,
    pub reply: oneshot::Sender<Result<Finished, FsError>>,
}

pub(crate) struct Job {
    pub task: Task,
    pub permit: OwnedSemaphorePermit,
}

pub(crate) enum Task {
    Plan(PlanJob),
    Blocking(Box<dyn FnOnce() + Send + 'static>),
}

pub(crate) trait Executor: Send + Sync {
    /// Submitted jobs finish even if the caller drops the reply future.
    fn submit(&self, job: Job) -> Result<(), FsError>;
    fn name(&self) -> &'static str;
}
