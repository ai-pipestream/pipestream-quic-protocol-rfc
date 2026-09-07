//! A single bounded owner for one durable client journal. No SQLite operation,
//! journal open/audit, or final file-owner destruction runs on the async caller.
pub use pipestream_core::v2::client::{
    Creation, Intent, JournalError, JournalLimits, ObservedWork, RetainedReference, ScopeMember,
    ScopeObservation,
};
use pipestream_core::{
    persistence::{PhysicalLimits, PhysicalUsage},
    v2::{client::Journal as Store, *},
};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock, mpsc},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch};

mod operations;
mod ownership;
#[cfg(test)]
mod tests;

type Result<T> = std::result::Result<T, JournalError>;
type Job = Box<dyn FnOnce(&Store) + Send>;
static WORKERS: OnceLock<Arc<Semaphore>> = OnceLock::new();
const MAX_WORKERS: usize = 64;

fn error(code: ErrorCode, detail: &'static str) -> JournalError {
    Error { code, detail }.into()
}

#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// Total queued, executing and completed-but-undelivered operations.
    /// Each argument/result is additionally bounded by the typed record/page
    /// limits. This count is not a whole-process memory guarantee.
    pub in_flight: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self { in_flight: 16 }
    }
}
impl Options {
    fn validate(self) -> Result<()> {
        if !(1..=32).contains(&self.in_flight) {
            return Err(error(
                ErrorCode::LimitExceeded,
                "journal in-flight limit must be 1..=32",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Exit {
    Running,
    Stopped,
    Failed,
}

struct Inner {
    // Taking the only sender closes acceptance atomically with enqueue. The
    // worker drains already accepted jobs before dropping the store.
    sender: Mutex<Option<mpsc::SyncSender<Job>>>,
    exit: watch::Sender<Exit>,
    slots: Arc<Semaphore>,
    options: Options,
    creation: Creation,
}

/// Cloneable async handles sharing one worker and one exclusive journal owner.
///
/// Cancellation of an accepted call discards its reply, not its operation. The
/// same immutable intent remains the recovery identity even if its local commit
/// finishes after the caller disappears. No caller-supplied callbacks run here.
#[derive(Clone)]
pub struct Journal {
    inner: Arc<Inner>,
}

struct Reply<T> {
    result: Result<T>,
    // Keep capacity until the caller actually receives or discards this reply,
    // not just until the database write finishes.
    _slot: OwnedSemaphorePermit,
}

impl Journal {
    /// Explicit new history. Does not replace an existing journal.
    pub async fn initialize(
        path: PathBuf,
        creation: Creation,
        limits: JournalLimits,
        physical: PhysicalLimits,
        options: Options,
    ) -> Result<Self> {
        Self::start(path, creation, limits, physical, options, true).await
    }

    /// Existing history with trusted expected configuration. Missing,
    /// incompatible or corrupted history refuses; there is no automatic reset.
    pub async fn open(
        path: PathBuf,
        creation: Creation,
        limits: JournalLimits,
        physical: PhysicalLimits,
        options: Options,
    ) -> Result<Self> {
        Self::start(path, creation, limits, physical, options, false).await
    }

    async fn start(
        path: PathBuf,
        creation: Creation,
        limits: JournalLimits,
        physical: PhysicalLimits,
        options: Options,
        initialize: bool,
    ) -> Result<Self> {
        options.validate()?;
        creation.request(Id(1))?; // bounded validation before cloning configuration
        let worker_slot = WORKERS
            .get_or_init(|| Arc::new(Semaphore::new(MAX_WORKERS)))
            .clone()
            .try_acquire_owned()
            .map_err(|_| error(ErrorCode::LimitExceeded, "client journal worker ceiling"))?;
        let (sender, receiver) = mpsc::sync_channel::<Job>(options.in_flight);
        let (ready, opened) = oneshot::channel();
        let (exit, _) = watch::channel(Exit::Running);
        let worker_exit = exit.clone();
        let handle = Self {
            inner: Arc::new(Inner {
                sender: Mutex::new(Some(sender)),
                exit,
                slots: Arc::new(Semaphore::new(options.in_flight)),
                options,
                creation: creation.clone(),
            }),
        };
        std::thread::Builder::new()
            .name("pipestream-v2-client-journal".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let _worker_slot = worker_slot;
                    let _lease = match ownership::Lease::acquire(&path) {
                        Ok(lease) => lease,
                        Err(error) => {
                            let _ = ready.send(Err(error));
                            return;
                        }
                    };
                    let store = if initialize {
                        Store::initialize(&path, creation, limits, physical)
                    } else {
                        Store::open(&path, creation, limits, physical)
                    };
                    let store = match store {
                        Ok(store) => store,
                        Err(error) => {
                            let _ = ready.send(Err(error));
                            return;
                        }
                    };
                    if ready.send(Ok(())).is_err() {
                        return;
                    }
                    for job in receiver {
                        job(&store);
                    }
                    // Store and its physical owner are destroyed on this thread
                    // before the stopped notification, including on unwind.
                }));
                worker_exit.send_replace(if result.is_ok() {
                    Exit::Stopped
                } else {
                    Exit::Failed
                });
            })?;
        let outcome = opened
            .await
            .map_err(|_| {
                error(
                    ErrorCode::InternalError,
                    "journal worker failed during open",
                )
            })
            .and_then(|outcome| outcome);
        if let Err(error) = outcome {
            // Readiness can fail before the worker finishes unwinding its
            // lease/store. Do not report a finished open attempt until those
            // owners are gone; an immediate retry must not race that cleanup.
            handle.close();
            let _ = handle.closed().await;
            return Err(error);
        }
        Ok(handle)
    }

    pub fn creation(&self) -> &Creation {
        &self.inner.creation
    }

    /// Locally retained operation/reply slots, not server outstanding requests.
    pub fn in_flight(&self) -> usize {
        self.inner.options.in_flight - self.inner.slots.available_permits()
    }

    /// Refuse further calls on every clone and drain accepted operations.
    /// Nonblocking; this does not cancel work at an authority or imply coverage.
    pub fn close(&self) {
        self.inner
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    /// Wait until accepted operations and exclusive store-owner destruction
    /// have finished on the worker.
    /// A caller may put a timeout around this wait; timing out does not stop a
    /// running commit or authorize reopening/replacing its files.
    pub async fn closed(&self) -> Result<()> {
        let mut exit = self.inner.exit.subscribe();
        loop {
            match *exit.borrow_and_update() {
                Exit::Stopped => return Ok(()),
                Exit::Failed => {
                    return Err(error(ErrorCode::InternalError, "journal worker panicked"));
                }
                Exit::Running => {}
            }
            exit.changed()
                .await
                .map_err(|_| error(ErrorCode::InternalError, "journal worker status lost"))?;
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.close();
        self.closed().await
    }

    async fn execute<T: Send + 'static>(
        &self,
        run: impl FnOnce(&Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let slot = self.inner.slots.clone().try_acquire_owned().map_err(|_| {
            error(
                ErrorCode::LimitExceeded,
                "journal operation capacity exhausted",
            )
        })?;
        let (send, receive) = oneshot::channel();
        let job: Job = Box::new(move |store| {
            let result = run(store);
            let _ = send.send(Reply {
                result,
                _slot: slot,
            });
        });
        {
            let sender = self
                .inner
                .sender
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            sender
                .as_ref()
                .ok_or_else(|| error(ErrorCode::Cancelled, "journal is closing"))?
                .try_send(job)
                .map_err(|failure| match failure {
                    mpsc::TrySendError::Full(_) => {
                        error(ErrorCode::LimitExceeded, "journal queue full")
                    }
                    mpsc::TrySendError::Disconnected(_) => {
                        error(ErrorCode::InternalError, "journal worker unavailable")
                    }
                })?;
        }
        let reply = receive
            .await
            .map_err(|_| error(ErrorCode::InternalError, "journal operation interrupted"))?;
        reply.result
    }
}
