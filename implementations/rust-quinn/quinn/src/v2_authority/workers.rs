//! Fixed file-I/O workers, separate from the control dispatcher's metadata pool.
//! Queued closures own all resource pins until execution ends even if the async
//! receiver disappears. Dropping the pool closes its queue, not a durable job.
use super::{Error, ErrorCode, error};
use std::sync::{Arc, Mutex, mpsc};
use tokio::sync::oneshot;

type Job = Box<dyn FnOnce() + Send>;
#[derive(Clone)]
pub(crate) struct Workers {
    sender: mpsc::SyncSender<Job>,
}
impl Workers {
    pub(crate) fn new(threads: usize, queued: usize) -> Result<Self, Error> {
        if !(1..=32).contains(&threads) || !(1..=256).contains(&queued) {
            return Err(error(
                ErrorCode::LimitExceeded,
                "invalid file worker limits",
            ));
        }
        // At most one ordinary job and one returned-value destructor per live
        // transfer. Both retain its lease, which reserves these queue slots.
        let (sender, receiver) = mpsc::sync_channel::<Job>(queued * 2);
        let receiver = Arc::new(Mutex::new(receiver));
        for index in 0..threads {
            let receiver = receiver.clone();
            std::thread::Builder::new()
                .name(format!("pipestream-v2-file-{index}"))
                .spawn(move || {
                    loop {
                        let job = {
                            let Ok(receiver) = receiver.lock() else {
                                return;
                            };
                            receiver.recv()
                        };
                        let Ok(job) = job else {
                            return;
                        };
                        // A bad callback must not quietly reduce the fixed pool's
                        // capacity. The reply channel closes and reports the fault.
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                    }
                })
                .map_err(|_| error(ErrorCode::InternalError, "file worker could not start"))?;
        }
        Ok(Self { sender })
    }
    pub(crate) async fn run<T: Send + 'static>(
        &self,
        job: impl FnOnce() -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Box::new(move || {
                let _ = send.send(job());
            }))
            .map_err(|failure| match failure {
                mpsc::TrySendError::Full(_) => {
                    error(ErrorCode::LimitExceeded, "file worker queue full")
                }
                mpsc::TrySendError::Disconnected(_) => {
                    error(ErrorCode::InternalError, "file worker pool stopped")
                }
            })?;
        receive
            .await
            .map_err(|_| error(ErrorCode::InternalError, "file operation failed"))?
    }
}

/// File-owning state must be destroyed on a worker even when the async caller
/// is cancelled between jobs. Its pin also funds the deferred destructor.
pub(crate) struct Value<T: Send + 'static, P: Send + 'static> {
    value: Option<(T, P)>,
    workers: Workers,
}
impl<T: Send + 'static, P: Send + 'static> Value<T, P> {
    pub(crate) fn new(value: T, pin: P, workers: Workers) -> Self {
        Self {
            value: Some((value, pin)),
            workers,
        }
    }
    /// Call only inside an I/O job; no raw file-owning state leaves the workers.
    pub(crate) fn take(mut self) -> (T, P) {
        self.value.take().expect("owned file value")
    }
    /// Borrow only inside an I/O job. Sharing this owner does not move its
    /// filesystem destructor onto an async runtime thread.
    pub(crate) fn get(&self) -> &T {
        &self.value.as_ref().expect("owned file value").0
    }
}
impl<T: Send + 'static, P: Send + 'static> Drop for Value<T, P> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            // Reserved capacity makes this send nonblocking while the pool is
            // healthy. The pinned value outlives its filesystem cleanup.
            let _ = self.workers.sender.send(Box::new(move || drop(value)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    struct FileDrop(Arc<AtomicBool>);
    impl Drop for FileDrop {
        fn drop(&mut self) {
            assert!(
                std::thread::current()
                    .name()
                    .unwrap()
                    .starts_with("pipestream-v2-file-")
            );
            self.0.store(true, Ordering::SeqCst);
        }
    }
    struct PinDrop {
        file: Arc<AtomicBool>,
        done: Option<oneshot::Sender<bool>>,
    }
    impl Drop for PinDrop {
        fn drop(&mut self) {
            let _ = self
                .done
                .take()
                .unwrap()
                .send(self.file.load(Ordering::SeqCst));
        }
    }
    #[tokio::test]
    async fn async_value_drop_runs_cleanup_before_releasing_its_pin_on_a_file_worker() {
        let workers = Workers::new(1, 1).unwrap();
        let destination = workers.clone();
        let file = Arc::new(AtomicBool::new(false));
        let (done, finished) = oneshot::channel();
        let value = workers
            .run(move || {
                Ok(Value::new(
                    FileDrop(file.clone()),
                    PinDrop {
                        file,
                        done: Some(done),
                    },
                    destination,
                ))
            })
            .await
            .unwrap();
        drop(value);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), finished)
                .await
                .unwrap()
                .unwrap()
        );
    }
}
