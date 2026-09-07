//! Durable session client: owned network operations commit their evidence before
//! returning success. Cancellation of an accepted waiter does not cancel that
//! collector or authorize replacement operation identities.
use super::{
    journal::{Intent, Journal, JournalError, ObservedWork, RetainedReference, ScopeObservation},
    transport::{self, Transport},
};
use pipestream_core::v2::*;
use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::JoinSet,
};

mod objects;
mod operations;
pub use objects::{Admission, Output};

type Result<T> = std::result::Result<T, Failure>;
type Ticket = Arc<OwnedSemaphorePermit>;
type Task = Pin<Box<dyn Future<Output = ()> + Send>>;
struct Job {
    task: Task,
    barrier: bool,
}
static CLIENTS: Semaphore = Semaphore::const_new(64);

#[derive(Debug)]
pub enum Failure {
    Protocol(Error),
    Refused(Refusal),
    Journal(JournalError),
    Connection(anyhow::Error),
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Protocol(e) => e.fmt(f),
            Self::Journal(e) => e.fmt(f),
            Self::Connection(e) => e.fmt(f),
            Self::Refused(r) => write!(f, "authority refusal {}: {}", r.code.name(), r.detail.0),
        }
    }
}
impl std::error::Error for Failure {}
impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        Self::Protocol(e)
    }
}
impl From<JournalError> for Failure {
    fn from(e: JournalError) -> Self {
        Self::Journal(e)
    }
}
impl From<anyhow::Error> for Failure {
    fn from(e: anyhow::Error) -> Self {
        Self::Connection(e)
    }
}
fn error(code: ErrorCode, detail: &'static str) -> Failure {
    Error { code, detail }.into()
}
fn closed() -> Failure {
    error(ErrorCode::Cancelled, "durable client is closed")
}

/// Trusted application configuration, never derived from an output locator.
/// The client requires exactly the durable profile combination of its journal;
/// the capability count/size/deadline offers come from `transport`.
pub struct Endpoint {
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub server_name: String,
    pub security: transport::Security,
    pub transport: transport::Options,
}
#[derive(Clone, Copy)]
pub struct Options {
    pub in_flight: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self { in_flight: 16 }
    }
}
struct Context {
    journal: Journal,
    transport: Transport,
    binding: Control,
    identity: SessionIdentity,
    storage: tokio::sync::Mutex<()>,
}
impl Context {
    async fn store<T>(
        &self,
        operation: impl Future<Output = std::result::Result<T, JournalError>>,
    ) -> Result<T> {
        // Only storage operations serialize. Network waits never hold this lock.
        // The accepted-operation ceiling bounds these mutex waiters as well.
        let _storage = self.storage.lock().await;
        Ok(operation.await?)
    }
    async fn control(&self, request: Control) -> Result<Control> {
        match self.transport.exchange(request, None).await? {
            transport::Reply::Control(Control::Refusal(r)) => Err(Failure::Refused(r)),
            transport::Reply::Control(c) => Ok(c),
            transport::Reply::Object(_) => {
                Err(error(ErrorCode::FrameError, "unexpected object response"))
            }
        }
    }
    async fn receipt(&self, response: Control) -> Result<OperationReceipt> {
        let receipt = match response {
            Control::Scope(Scope::Declared { receipt, .. } | Scope::Cancelled { receipt, .. })
            | Control::Work(
                Work::Admitted { receipt, .. }
                | Work::OperationResponse { receipt, .. }
                | Work::Retried { receipt, .. }
                | Work::Cancelled { receipt, .. }
                | Work::Skipped { receipt, .. },
            ) => receipt,
            Control::Refusal(r) => return Err(Failure::Refused(r)),
            _ => {
                return Err(error(
                    ErrorCode::FrameError,
                    "expected immutable operation receipt",
                ));
            }
        };
        self.store(self.journal.record_receipt(receipt.clone()))
            .await?;
        Ok(receipt)
    }
}
struct Acceptance {
    sender: Option<mpsc::Sender<Job>>,
    barrier: bool,
}
struct Inner {
    context: Arc<Context>,
    accept: Mutex<Acceptance>,
    slots: Arc<Semaphore>,
    options: Options,
    done: watch::Receiver<Option<bool>>,
}
struct Packet<T> {
    value: Result<T>,
    _ticket: Ticket,
}

/// Owns the supplied journal and one authenticated connection. Do not mutate the
/// journal through retained raw clones while attached. Close drains accepted
/// collectors, then closes the transport and journal worker; it never cancels
/// remote durable work. Reconnecting reopens the same journal, not new history.
#[derive(Clone)]
pub struct Client(Arc<Inner>);
impl Client {
    pub async fn connect(
        mut endpoint: Endpoint,
        journal: Journal,
        options: Options,
    ) -> Result<Self> {
        if !(1..=32).contains(&options.in_flight) || endpoint.server_name.len() > 253 {
            return Err(error(
                ErrorCode::LimitExceeded,
                "invalid durable client limits or server name",
            ));
        }
        let lease = CLIENTS
            .try_acquire()
            .map_err(|_| error(ErrorCode::LimitExceeded, "durable client owner ceiling"))?;
        let journal_owner = journal.claim_session()?;
        let mut profiles = vec![ProfileId(DURABLE_WORK.into())];
        if journal.creation().results {
            profiles.push(ProfileId(RESULT_DELIVERY.into()));
        }
        endpoint.transport.offer.supported = profiles.clone();
        endpoint.transport.offer.required = profiles;
        let (reply, receive) = oneshot::channel();
        tokio::spawn(async move {
            let prepared = bind(endpoint, &journal).await;
            match prepared {
                Ok((transport, binding, identity)) => {
                    let (sender, incoming) = mpsc::channel(options.in_flight);
                    let (done, finished) = watch::channel(None);
                    let context = Arc::new(Context {
                        journal,
                        transport,
                        binding,
                        identity,
                        storage: tokio::sync::Mutex::new(()),
                    });
                    let client = Self(Arc::new(Inner {
                        context: context.clone(),
                        accept: Mutex::new(Acceptance {
                            sender: Some(sender),
                            barrier: false,
                        }),
                        slots: Arc::new(Semaphore::new(options.in_flight)),
                        options,
                        done: finished,
                    }));
                    // A cancelled connect waiter drops this handle, closing the
                    // queue after the original binding has been persisted.
                    let owner = Arc::downgrade(&client.0);
                    let _ = reply.send(Ok(client));
                    let failed = run(context, incoming, owner).await;
                    drop(journal_owner);
                    drop(lease);
                    let _ = done.send(Some(failed));
                }
                Err(e) => {
                    let _ = journal.shutdown().await;
                    drop(journal_owner);
                    drop(lease);
                    let _ = reply.send(Err(e));
                }
            }
        });
        receive
            .await
            .map_err(|_| error(ErrorCode::InternalError, "durable client startup failed"))?
    }
    pub fn identity(&self) -> &SessionIdentity {
        &self.0.context.identity
    }
    /// Exact authenticated binding, exposed only after journal persistence.
    pub fn binding(&self) -> &Control {
        &self.0.context.binding
    }
    pub fn in_flight(&self) -> usize {
        self.0.options.in_flight - self.0.slots.available_permits()
    }
    pub fn close(&self) {
        self.0
            .accept
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sender
            .take();
    }
    pub async fn closed(&self) -> Result<()> {
        let mut done = self.0.done.clone();
        loop {
            if let Some(failed) = *done.borrow_and_update() {
                return if failed {
                    Err(error(
                        ErrorCode::InternalError,
                        "durable client owner failed",
                    ))
                } else {
                    Ok(())
                };
            }
            done.changed()
                .await
                .map_err(|_| error(ErrorCode::InternalError, "durable client owner disappeared"))?;
        }
    }
    pub async fn shutdown(&self) -> Result<()> {
        self.close();
        self.closed().await
    }
    fn ticket(&self) -> Result<Ticket> {
        self.0
            .slots
            .clone()
            .try_acquire_owned()
            .map(Arc::new)
            .map_err(|_| {
                error(
                    ErrorCode::LimitExceeded,
                    "durable client operation/reply ceiling",
                )
            })
    }
    fn submit(&self, task: Task) -> Result<()> {
        let accept = self
            .0
            .accept
            .lock()
            .map_err(|_| error(ErrorCode::InternalError, "durable client acceptance lock"))?;
        if accept.barrier {
            return Err(error(ErrorCode::NotReady, "durable client drain barrier"));
        }
        accept
            .sender
            .as_ref()
            .ok_or_else(closed)?
            .try_send(Job {
                task,
                barrier: false,
            })
            .map_err(|_| closed())
    }
    async fn operation<T: Send + 'static>(
        &self,
        work: impl FnOnce(Arc<Context>, Ticket) -> Pin<Box<dyn Future<Output = Result<T>> + Send>>
        + Send
        + 'static,
    ) -> Result<T> {
        let ticket = self.ticket()?;
        let context = self.0.context.clone();
        let (reply, receive) = oneshot::channel();
        self.submit(Box::pin(async move {
            let value = work(context, ticket.clone()).await;
            let _ = reply.send(Packet {
                value,
                _ticket: ticket,
            });
        }))?;
        receive
            .await
            .map_err(|_| {
                error(
                    ErrorCode::InternalError,
                    "durable client operation owner failed",
                )
            })?
            .value
    }
}

async fn bind(
    endpoint: Endpoint,
    journal: &Journal,
) -> Result<(Transport, Control, SessionIdentity)> {
    let transport = Transport::connect(
        endpoint.local,
        endpoint.remote,
        &endpoint.server_name,
        endpoint.security,
        endpoint.transport,
    )
    .await?;
    let outcome = async {
        journal
            .creation()
            .validate_selection(transport.selected())?;
        let request = match journal.binding().await? {
            Some(_) => {
                let identity = journal.identity().await?;
                Control::Session(Session::Attach {
                    request: Id(1),
                    authority: identity.authority,
                    owner: identity.owner,
                    generation: identity.generation,
                })
            }
            None => journal.creation().request(Id(1))?,
        };
        let binding = match transport.exchange(request, None).await? {
            transport::Reply::Control(Control::Refusal(r)) => return Err(Failure::Refused(r)),
            transport::Reply::Control(c) => c,
            _ => return Err(error(ErrorCode::FrameError, "expected session binding")),
        };
        journal
            .record_binding(binding.clone(), transport.selected().clone())
            .await?;
        Ok((binding, journal.identity().await?))
    }
    .await;
    match outcome {
        Ok((binding, identity)) => Ok((transport, binding, identity)),
        Err(e) => {
            transport.close();
            transport.closed().await;
            Err(e)
        }
    }
}
fn task_failed(
    result: std::result::Result<(), tokio::task::JoinError>,
    owner: &std::sync::Weak<Inner>,
    context: &Context,
) -> bool {
    if result.is_ok() {
        return false;
    }
    if let Some(owner) = owner.upgrade() {
        owner
            .accept
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sender
            .take();
    }
    context.transport.close();
    true
}
async fn run(
    context: Arc<Context>,
    mut incoming: mpsc::Receiver<Job>,
    owner: std::sync::Weak<Inner>,
) -> bool {
    let mut tasks = JoinSet::new();
    let mut failed = false;
    loop {
        while let Some(result) = tasks.try_join_next() {
            failed |= task_failed(result, &owner, &context);
        }
        tokio::select! {
            job = incoming.recv() => match job {
                Some(job) if job.barrier => {
                    while let Some(result) = tasks.join_next().await { failed |= task_failed(result, &owner, &context); }
                    tasks.spawn(job.task);
                    while let Some(result) = tasks.join_next().await { failed |= task_failed(result, &owner, &context); }
                },
                Some(job) => { tasks.spawn(job.task); }, None => break
            },
            Some(result) = tasks.join_next(), if !tasks.is_empty() => { failed |= task_failed(result, &owner, &context); },
        }
    }
    while let Some(result) = tasks.join_next().await {
        failed |= task_failed(result, &owner, &context);
    }
    context.transport.close();
    context.transport.closed().await;
    failed |= context.journal.shutdown().await.is_err();
    failed
}
