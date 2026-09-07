//! Bounded, authenticated V2 wire transport. This is the network half of a
//! client, not a durable-session facade: callers must commit journal intents
//! before mutations and verify/persist returned commitments before using them.
//! Dropping a request waiter never changes a remote durable outcome.
use crate::{v2_core::framing, v2_flow, v2_tls};
use pipestream_core::v2::*;
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration as Elapsed,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::JoinSet,
    time::Instant,
};

mod objects;
mod requests;
pub use objects::{Input, Output, VerifiedObject};

const CHUNK: usize = 8192;
static CONNECTIONS: Semaphore = Semaphore::const_new(64);
type Result<T> = std::result::Result<T, Error>;
type Ticket = Arc<OwnedSemaphorePermit>;
fn error(code: ErrorCode, detail: &'static str) -> Error {
    Error { code, detail }
}
fn stopped() -> Error {
    error(ErrorCode::Cancelled, "client transport closed")
}
fn code(e: &Error) -> quinn::VarInt {
    e.code.quic_error().try_into().expect("fixed error")
}
fn network(e: anyhow::Error) -> Error {
    e.downcast_ref::<Error>().cloned().unwrap_or_else(stopped)
}

/// TLS trust and optional caller identity. There is no insecure-verifier,
/// redirect, resumption or early-data option on this boundary.
pub struct Security {
    config: quinn::ClientConfig,
    caller_identity: bool,
}
impl Security {
    pub fn new(
        roots: rustls::RootCertStore,
        identity: Option<(
            Vec<rustls::pki_types::CertificateDer<'static>>,
            rustls::pki_types::PrivateKeyDer<'static>,
        )>,
    ) -> anyhow::Result<Self> {
        let caller_identity = identity.is_some();
        Ok(Self {
            config: v2_tls::client_configuration(roots, identity)?,
            caller_identity,
        })
    }
}

#[derive(Clone)]
pub struct Options {
    pub offer: Capabilities,
    pub flow: v2_flow::Limits,
    pub handshake_timeout: Elapsed,
    pub frame_timeout: Elapsed,
    /// Bounds unanswered requests, including abandoned callers. Expiry closes
    /// this connection; it does not discard correlation and accept late replies
    /// as if they belonged to a different request.
    pub response_timeout: Elapsed,
}
impl Options {
    fn validate(&self) -> Result<()> {
        Control::Capabilities(self.offer.clone()).encode(INITIAL_CONTROL_LIMIT)?;
        self.flow.validate()?;
        if self.offer.response.0 != 0
            || self.offer.stream_limit.0 != u64::from(self.flow.data_streams)
            || self.offer.pending_limit.0 > 128
            || self
                .offer
                .supported
                .iter()
                .any(|p| ![u64::from(DURABLE_WORK), u64::from(RESULT_DELIVERY)].contains(&p.0))
            || (2 * self.offer.pending_limit.0 + 4) * (self.offer.control_limit.0 + 5)
                > 2 * 1024 * 1024
            || self.flow.receive_budget()? + self.flow.data_send + self.flow.control_send
                > 2 * 1024 * 1024
        {
            return Err(error(
                ErrorCode::LimitExceeded,
                "invalid client capability or raw-state budget",
            ));
        }
        for duration in [
            self.handshake_timeout,
            self.frame_timeout,
            self.response_timeout,
        ] {
            if duration.is_zero() || duration > Elapsed::from_secs(3600) {
                return Err(error(
                    ErrorCode::LimitExceeded,
                    "client timeout outside 1ns..1h",
                ));
            }
        }
        Ok(())
    }
}
impl Default for Options {
    fn default() -> Self {
        Self {
            offer: Capabilities {
                response: ResponseFlag(0),
                supported: vec![],
                required: vec![],
                control_limit: ControlLimit(8192),
                stream_limit: ConcurrencyLimit(4),
                pending_limit: ConcurrencyLimit(16),
                object_limit: Number(16 * 1024 * 1024),
                stream_idle_ms: IdleMs(5000),
                stream_lifetime_ms: LifetimeMs(30000),
            },
            flow: Default::default(),
            handshake_timeout: Elapsed::from_secs(5),
            frame_timeout: Elapsed::from_secs(10),
            response_timeout: Elapsed::from_secs(60),
        }
    }
}

/// A wire response. Object bytes remain explicitly unverified until validated
/// length, digest and FIN produce `Output::verification()`.
pub enum Reply {
    Control(Control),
    Object(Output),
}
struct Packet {
    reply: Result<Reply>,
    _ticket: Ticket,
}
type ReplySender = oneshot::Sender<Packet>;
struct Pending {
    reply: Option<ReplySender>,
    ticket: Ticket,
    deadline: Option<Instant>,
}
struct Book {
    correlation: Correlation,
    next: u64,
    requests: BTreeMap<(u8, u64), Pending>,
    detached: bool,
}
struct Shared {
    connection: quinn::Connection,
    flow: v2_flow::Connection,
    selected: Capabilities,
    options: Options,
    book: Mutex<Book>,
    slots: Arc<Semaphore>,
    inputs: Arc<Semaphore>,
    outputs: Arc<Semaphore>,
    opening: tokio::sync::Mutex<()>,
    writes: mpsc::Sender<Vec<u8>>,
    uploads: mpsc::Sender<objects::Upload>,
    finished: watch::Receiver<bool>,
}
impl Shared {
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Book>> {
        self.book
            .lock()
            .map_err(|_| error(ErrorCode::InternalError, "client correlation lock poisoned"))
    }
    fn ticket(&self) -> Result<Ticket> {
        if self.connection.close_reason().is_some() {
            return Err(stopped());
        }
        self.slots
            .clone()
            .try_acquire_owned()
            .map(Arc::new)
            .map_err(|_| {
                error(
                    ErrorCode::LimitExceeded,
                    "client pending or unread reply ceiling",
                )
            })
    }
    fn deliver(&self, response: Control) -> Result<()> {
        let key = match &response {
            Control::Work(Work::Admitted {
                request: RequestTag::Input { stream },
                ..
            })
            | Control::Refusal(Refusal {
                request: RequestTag::Input { stream },
                ..
            }) => (1, stream.0),
            _ => (
                0,
                request_id(&response)
                    .ok_or_else(|| error(ErrorCode::FrameError, "uncorrelated response"))?
                    .0,
            ),
        };
        let mut book = self.lock()?;
        book.correlation.accept(&response)?;
        let pending = book
            .requests
            .remove(&key)
            .ok_or_else(|| error(ErrorCode::InternalError, "missing client request"))?;
        if let Some(reply) = pending.reply {
            let _ = reply.send(Packet {
                reply: Ok(Reply::Control(response)),
                _ticket: pending.ticket,
            });
        }
        Ok(())
    }
}

struct Handle(Arc<Shared>);
impl Drop for Handle {
    fn drop(&mut self) {
        self.0
            .connection
            .close(0u32.into(), b"client handle closed");
    }
}
/// Clones share one connection, monotonically allocated request IDs and bounded
/// pending state. The last handle closes network I/O, never remote work.
#[derive(Clone)]
pub struct Transport(Arc<Handle>);
impl Transport {
    pub async fn connect(
        local: SocketAddr,
        remote: SocketAddr,
        server_name: &str,
        mut security: Security,
        options: Options,
    ) -> anyhow::Result<Self> {
        options.validate()?;
        let lease = CONNECTIONS
            .try_acquire()
            .map_err(|_| error(ErrorCode::LimitExceeded, "global client connection ceiling"))?;
        let mut config = quinn::TransportConfig::default();
        options.flow.configure(&mut config, quinn::Side::Client)?;
        // Receive-window geometry is reserved in advance, but no object stream
        // is authorized until its profile has actually been selected.
        config.max_concurrent_uni_streams(0u32.into());
        security.config.transport_config(Arc::new(config));
        let mut endpoint = quinn::Endpoint::client(local)?;
        endpoint.set_default_client_config(security.config);
        // The owned task retains the global slot through handshake cancellation,
        // malformed selection, I/O-task cleanup and Quinn's draining lifetime.
        let connecting = endpoint.connect(remote, server_name)?;
        let (mut reply, receive) = oneshot::channel();
        tokio::spawn(async move {
            let prepared = tokio::select! {
                result = prepare(connecting, security.caller_identity, options) => result,
                _ = reply.closed() => Err(stopped().into()),
            };
            let done = match prepared {
                Ok(prepared) => {
                    let Prepared {
                        shared,
                        send,
                        recv,
                        outgoing,
                        incoming,
                        done,
                    } = prepared;
                    let _ = reply.send(Ok(Self(Arc::new(Handle(shared.clone())))));
                    run(shared, send, recv, outgoing, incoming).await;
                    endpoint.close(0u32.into(), b"client transport ended");
                    Some(done)
                }
                Err(e) => {
                    let failure = e.downcast_ref::<Error>().cloned().unwrap_or_else(stopped);
                    endpoint.close(code(&failure), failure.detail.as_bytes());
                    let _ = reply.send(Err(e));
                    None
                }
            };
            endpoint.wait_idle().await;
            drop(lease);
            if let Some(done) = done {
                let _ = done.send(true);
            }
        });
        receive.await.map_err(|_| stopped())?
    }
    pub fn selected(&self) -> &Capabilities {
        &self.0.0.selected
    }
}
struct Prepared {
    shared: Arc<Shared>,
    send: v2_flow::Writer,
    recv: quinn::RecvStream,
    outgoing: mpsc::Receiver<Vec<u8>>,
    incoming: mpsc::Receiver<objects::Upload>,
    done: watch::Sender<bool>,
}
struct Negotiating(quinn::Connection, bool);
impl Drop for Negotiating {
    fn drop(&mut self) {
        if self.1 {
            self.0
                .close(code(&stopped()), b"client negotiation abandoned");
        }
    }
}
async fn prepare(
    connecting: quinn::Connecting,
    caller_identity: bool,
    options: Options,
) -> anyhow::Result<Prepared> {
    let connection = tokio::time::timeout(options.handshake_timeout, connecting)
        .await
        .map_err(|_| error(ErrorCode::LimitExceeded, "client TLS handshake deadline"))??;
    let mut guard = Negotiating(connection.clone(), true);
    let prepared: anyhow::Result<Prepared> = async move {
        let flow = v2_flow::Connection::new(connection.clone(), options.flow)?;
        let deadline = Instant::now() + options.handshake_timeout;
        let (mut send, mut recv) = tokio::time::timeout_at(deadline, flow.open_control()).await??;
        let bytes = Control::Capabilities(options.offer.clone()).encode(INITIAL_CONTROL_LIMIT)?;
        tokio::time::timeout_at(deadline, send.write_all(&bytes)).await??;
        let selected = match framing::receive(&mut recv, None, deadline).await? {
            framing::Frame::Control(Control::Capabilities(c)) => c,
            _ => {
                return Err(error(
                    ErrorCode::FrameError,
                    "missing initial capability selection",
                )
                .into());
            }
        };
        options.offer.validate_selection(&selected)?;
        if selected.has(DURABLE_WORK) && !caller_identity {
            let failure = error(
                ErrorCode::Unauthorized,
                "durable selection without a client certificate",
            );
            connection.close(code(&failure), failure.detail.as_bytes());
            return Err(failure.into());
        }
        if selected.has(RESULT_DELIVERY) {
            connection.set_max_concurrent_uni_streams((selected.stream_limit.0 as u32).into());
        }
        let count = selected.pending_limit.0 as usize;
        let streams = selected.stream_limit.0 as usize;
        let (writes, outgoing) = mpsc::channel(count);
        let (uploads, incoming) = mpsc::channel(streams);
        let (done, finished) = watch::channel(false);
        let shared = Arc::new(Shared {
            connection,
            flow,
            book: Mutex::new(Book {
                correlation: Correlation::new(selected.clone())?,
                next: 1,
                requests: BTreeMap::new(),
                detached: false,
            }),
            selected,
            options,
            slots: Arc::new(Semaphore::new(count)),
            inputs: Arc::new(Semaphore::new(streams)),
            outputs: Arc::new(Semaphore::new(streams)),
            opening: tokio::sync::Mutex::new(()),
            writes,
            uploads,
            finished,
        });
        Ok(Prepared {
            shared,
            send,
            recv,
            outgoing,
            incoming,
            done,
        })
    }
    .await;
    if let Err(e) = &prepared {
        let failure = e.downcast_ref::<Error>().cloned().unwrap_or_else(stopped);
        guard.0.close(code(&failure), failure.detail.as_bytes());
    }
    guard.1 = false;
    prepared
}
impl Transport {
    /// Queued, unresolved and completed-but-unconsumed replies all retain slots.
    pub fn in_flight(&self) -> usize {
        self.selected().pending_limit.0 as usize - self.0.0.slots.available_permits()
    }
    pub fn close(&self) {
        self.0.0.connection.close(0u32.into(), b"client closed");
    }
    pub async fn closed(&self) {
        let mut done = self.0.0.finished.clone();
        while !*done.borrow_and_update() {
            if done.changed().await.is_err() {
                break;
            }
        }
    }
    /// Assigns the connection-local request ID; the supplied placeholder ID is
    /// not transmitted. Mutation operation IDs and commitments are never changed.
    /// RESULT reads require a previously authenticated, identity-checked manifest.
    /// Cancellation drops only the waiter: correlation stays until a valid reply
    /// or bounded connection failure. The journal remains the recovery authority.
    pub async fn exchange(
        &self,
        mut request: Control,
        manifest: Option<&Manifest>,
    ) -> Result<Reply> {
        let shared = &self.0.0;
        let ticket = shared.ticket()?;
        let (send, recv) = oneshot::channel();
        {
            // A detach cannot overtake an input whose actual stream is being
            // allocated. Ordinary controls never wait for this allocator.
            let _opening = if matches!(request, Control::Drain(Drain::Detach { .. })) {
                Some(shared.opening.lock().await)
            } else {
                None
            };
            let mut book = shared.lock()?;
            if book.detached {
                return Err(error(ErrorCode::NotReady, "client connection draining"));
            }
            requests::number(&mut request, Id(book.next))?;
            let bytes = request.encode(shared.selected.control_limit.0 as usize)?;
            book.correlation.register(&request, manifest)?;
            let id = book.next;
            book.next = book
                .next
                .checked_add(1)
                .ok_or_else(|| error(ErrorCode::LimitExceeded, "request IDs exhausted"))?;
            book.detached = matches!(request, Control::Drain(Drain::Detach { .. }));
            book.requests.insert(
                (0, id),
                Pending {
                    reply: Some(send),
                    ticket,
                    deadline: Some(Instant::now() + shared.options.response_timeout),
                },
            );
            if shared.writes.try_send(bytes).is_err() {
                shared
                    .connection
                    .close(code(&stopped()), b"client writer unavailable");
                return Err(stopped());
            }
        }
        recv.await.map_err(|_| stopped())?.reply
    }
}

async fn reader(shared: Arc<Shared>, mut recv: quinn::RecvStream) -> Result<()> {
    let mut frames = 0;
    loop {
        frames += 1;
        if frames == 32 {
            tokio::task::yield_now().await;
            frames = 0;
        }
        match framing::receive_next(
            &mut recv,
            shared.selected.control_limit.0 as usize,
            shared.options.frame_timeout,
        )
        .await
        .map_err(network)?
        {
            framing::Frame::Ignored => {}
            framing::Frame::Control(response) => shared.deliver(response)?,
            framing::Frame::Fin => {
                return Err(error(
                    ErrorCode::ControlReset,
                    "server control direction ended",
                ));
            }
        }
    }
}
async fn writer(
    shared: Arc<Shared>,
    mut send: v2_flow::Writer,
    mut writes: mpsc::Receiver<Vec<u8>>,
) -> Result<()> {
    while let Some(bytes) = writes.recv().await {
        tokio::time::timeout(shared.options.frame_timeout, send.write_all(&bytes))
            .await
            .map_err(|_| error(ErrorCode::LimitExceeded, "client control send deadline"))??;
    }
    Err(stopped())
}
async fn run(
    shared: Arc<Shared>,
    send: v2_flow::Writer,
    recv: quinn::RecvStream,
    writes: mpsc::Receiver<Vec<u8>>,
    mut uploads: mpsc::Receiver<objects::Upload>,
) {
    let mut tasks = JoinSet::new();
    tasks.spawn(reader(shared.clone(), recv));
    tasks.spawn(writer(shared.clone(), send, writes));
    let mut timer = tokio::time::interval(Elapsed::from_millis(20));
    let failure = 'running: loop {
        // Finished task records are reaped before admitting replacements; their
        // resource slots alone do not bound an unreaped JoinSet's history.
        while let Some(result) = tasks.try_join_next() {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => break 'running e,
                Err(_) => break 'running error(ErrorCode::InternalError, "client I/O task failed"),
            }
        }
        tokio::select! {
            _ = shared.connection.closed() => break stopped(),
            result = tasks.join_next() => match result {
                Some(Ok(Ok(()))) => {},
                Some(Ok(Err(e))) => break e,
                _ => break error(ErrorCode::InternalError, "client I/O task failed"),
            },
            Some(upload) = uploads.recv() => { tasks.spawn(objects::upload(shared.clone(), upload)); },
            stream = shared.connection.accept_uni() => {
                let mut stream = match stream { Ok(s) => s, Err(_) => break stopped() };
                match shared.outputs.clone().try_acquire_owned() {
                    Ok(slot) => { tasks.spawn(objects::download(shared.clone(), stream, slot)); },
                    Err(_) => { let _ = stream.stop(code(&error(ErrorCode::LimitExceeded, "client incoming stream ceiling"))); },
                }
            },
            _ = timer.tick() => {
                match shared.lock() {
                    Ok(book) if book.requests.values().any(|p| p.deadline.is_some_and(|d| Instant::now() >= d)) => break error(ErrorCode::LimitExceeded, "client response deadline"),
                    Err(e) => break e,
                    _ => {},
                }
            }
        }
    };
    shared
        .connection
        .close(code(&failure), failure.detail.as_bytes());
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    uploads.close();
    while uploads.try_recv().is_ok() {}
    if let Ok(mut book) = shared.lock() {
        for (_, pending) in std::mem::take(&mut book.requests) {
            if let Some(reply) = pending.reply {
                let _ = reply.send(Packet {
                    reply: Err(failure.clone()),
                    _ticket: pending.ticket,
                });
            }
        }
    }
}
