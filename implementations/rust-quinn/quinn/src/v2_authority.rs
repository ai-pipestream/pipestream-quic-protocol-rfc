//! Authenticated durable control dispatch and connection-local accounting.
//!
//! This adapter uses the real authority transactions, not a second state store.
//! The durable listener in `server` integrates it; the separate Core listener
//! remains Core-only. Direct adapter use negotiates no profile. The
//! endpoint must decode frames in order, discard ignorable frames, send immediate
//! refusals through its bounded control writer, and keep each `Response` alive
//! until its control write or result transfer ends. Input transport must retain
//! its `InputSlot` through admission and the response write. Payload I/O and
//! application callbacks belong on separately bounded workers, not this pool.

use crate::v2_tls::{Identity, Peer, ServerSecurity};
use pipestream_core::v2::{
    authority::{
        AuthorityStore, Binding, StoreError,
        payload::PayloadStore,
        results::{ResultRead, ResultService},
    },
    *,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration as Elapsed,
};
use tokio::{
    sync::{Notify, Semaphore},
    time::Instant,
};

pub mod input;
pub mod output;
pub mod runtime;
pub mod server;
pub(crate) mod workers;

fn error(code: ErrorCode, detail: &'static str) -> Error {
    Error { code, detail }
}
fn storage(error: StoreError) -> Error {
    match error {
        StoreError::Protocol(error) => error,
        // Do not put paths, SQL or retained records into network diagnostics.
        _ => self::error(
            ErrorCode::InternalError,
            "authority storage operation failed",
        ),
    }
}
fn refusal(request: Id, error: Error) -> Control {
    Control::Refusal(Refusal {
        request: RequestTag::Control { request },
        code: error.code,
        detail: Detail(error.detail.into()),
    })
}

/// Shared metadata concurrency ceiling. A submitted blocking operation retains
/// its permit even if the connection task is cancelled: detached SQLite work
/// cannot silently escape the ceiling or make a draining connection look idle.
#[derive(Clone)]
pub struct Authority {
    store: AuthorityStore,
    payloads: PayloadStore,
    results: ResultService,
    slots: Arc<Semaphore>,
    metadata_jobs: usize,
}
impl Authority {
    /// Call during setup, outside the async control reader. File/database root
    /// pairing is validated by the result service before a connection can use it.
    pub fn new(
        store: AuthorityStore,
        payloads: PayloadStore,
        metadata_jobs: usize,
    ) -> Result<Self, Error> {
        if !(1..=64).contains(&metadata_jobs) {
            return Err(error(
                ErrorCode::LimitExceeded,
                "metadata jobs must be in 1..64",
            ));
        }
        let results = ResultService::new(store.clone(), payloads.clone()).map_err(storage)?;
        Ok(Self {
            store,
            payloads,
            results,
            slots: Arc::new(Semaphore::new(metadata_jobs)),
            metadata_jobs,
        })
    }

    /// The supplied capabilities must be the validated selection for this TLS
    /// peer. This does not negotiate or enable a profile on a listener.
    /// Use the same flow owner for this connection's control and result writers;
    /// configure its receive limits before the handshake on both peers.
    pub fn connection(
        &self,
        peer: Arc<Peer>,
        security: Arc<ServerSecurity>,
        caps: Capabilities,
        flow: crate::v2_flow::Connection,
    ) -> Result<Connection, Error> {
        if !flow.belongs_to(peer.connection()) {
            return Err(error(
                ErrorCode::InternalError,
                "flow owner belongs to another TLS connection",
            ));
        }
        Control::Capabilities(caps.clone()).encode(INITIAL_CONTROL_LIMIT)?;
        if caps.response.0 != 1 || !caps.has(DURABLE_WORK) {
            return Err(error(
                ErrorCode::ExtensionUnsupported,
                "durable work not selected",
            ));
        }
        let identity = security
            .authorize(&peer)?
            .cloned()
            .ok_or_else(|| error(ErrorCode::Unauthorized, "durable caller identity missing"))?;
        if &identity.authority != self.store.authority() {
            return Err(error(
                ErrorCode::Unauthorized,
                "TLS and store authorities differ",
            ));
        }
        Ok(Connection {
            shared: Arc::new(Shared {
                authority: self.clone(),
                peer,
                security,
                identity,
                caps,
                flow,
                state: Mutex::new(State::default()),
                changed: Notify::new(),
            }),
        })
    }
}

#[derive(Default)]
struct State {
    highest: u64,
    pending: usize,
    inputs: usize,
    outputs: usize,
    binding: Option<Binding>,
    binding_pending: bool,
    completing: bool,
    detached: bool,
}
struct Shared {
    authority: Authority,
    peer: Arc<Peer>,
    security: Arc<ServerSecurity>,
    identity: Identity,
    caps: Capabilities,
    flow: crate::v2_flow::Connection,
    state: Mutex<State>,
    changed: Notify,
}
impl Shared {
    fn state(&self) -> Result<std::sync::MutexGuard<'_, State>, Error> {
        self.state
            .lock()
            .map_err(|_| error(ErrorCode::InternalError, "connection state poisoned"))
    }
    fn authorize(&self) -> Result<(), Error> {
        if self.security.authorize(&self.peer)? != Some(&self.identity) {
            return Err(error(
                ErrorCode::Unauthorized,
                "connection identity changed",
            ));
        }
        Ok(())
    }
}

/// One authenticated connection; submission order is the control wire order.
/// It cannot rebind to another session, including while creation is in flight.
#[derive(Clone)]
pub struct Connection {
    shared: Arc<Shared>,
}

pub enum Submission {
    /// No operation started. The endpoint sends this on its bounded writer.
    Refused(Control),
    Pending(Pending),
}

#[derive(Clone, Copy)]
enum Kind {
    Ordinary,
    Binding,
    Complete,
    Detach,
    Input,
    Output,
}
struct Ticket {
    shared: Arc<Shared>,
    kind: Kind,
}
impl Drop for Ticket {
    fn drop(&mut self) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending -= 1;
        match self.kind {
            Kind::Binding => state.binding_pending = false,
            Kind::Complete => state.completing = false,
            Kind::Input => state.inputs -= 1,
            Kind::Output => state.outputs -= 1,
            _ => {}
        }
        drop(state);
        self.shared.changed.notify_waiters();
    }
}

/// Owns an accepted request before, during and after an actual database task.
pub struct Pending {
    ticket: Arc<Ticket>,
    message: Control,
    binding: Option<Binding>,
    accepted: Instant,
}
pub enum ResponseBody {
    Control(Control),
    Result(ResultRead),
}
pub struct Response {
    body: ResponseBody,
    _ticket: Arc<Ticket>,
}
impl Response {
    /// Borrow instead of extracting: pending accounting must outlive the write
    /// or result transfer, including waiting for a QUIC stream/send window.
    pub fn body(&mut self) -> &mut ResponseBody {
        &mut self.body
    }

    /// After scheduling a successful result FIN, finish the read before releasing
    /// its pending slot. For an abort, simply drop this response instead.
    pub fn finish_result(self, now: std::time::Instant) -> Result<(), StoreError> {
        let Self { body, _ticket } = self;
        match body {
            ResponseBody::Result(read) => read.finish(now),
            ResponseBody::Control(_) => {
                Err(error(ErrorCode::Conflict, "not a result response").into())
            }
        }
    }
}

/// Clone into every outstanding input I/O/commit job. The last copy releases
/// connection request/stream credit, never an earlier cancelled async waiter.
#[derive(Clone)]
pub struct InputSlot {
    ticket: Arc<Ticket>,
    binding: Binding,
}
impl InputSlot {
    pub fn binding(&self) -> &Binding {
        &self.binding
    }
    pub fn capabilities(&self) -> &Capabilities {
        &self.ticket.shared.caps
    }
    pub fn authorize(&self) -> Result<(), Error> {
        self.ticket.shared.authorize()
    }
}

impl Connection {
    /// Structural/direction/ID errors are fatal (`Err`). Well-formed refused
    /// requests still consume their ID and return a correlated `Refused` value.
    /// Call synchronously in decode order, then run returned requests concurrently.
    pub fn submit(&self, message: Control) -> Result<Submission, Error> {
        let shared = &self.shared;
        message.encode(shared.caps.control_limit.0 as usize)?;
        let context = match message.validate_context(true, Some(&shared.caps)) {
            Ok(()) => None,
            Err(error) if error.code == ErrorCode::ExtensionUnsupported => Some(error),
            Err(error) => return Err(error),
        };
        let request = request_id(&message)
            .ok_or_else(|| error(ErrorCode::FrameError, "control request missing"))?;
        let mut state = shared.state()?;
        if (state.highest == 0 && request.0 != 1) || request.0 <= state.highest {
            return Err(error(
                ErrorCode::FrameError,
                "request ID not increasing from one",
            ));
        }
        state.highest = request.0;
        let denied = |error| Ok(Submission::Refused(refusal(request, error)));
        if let Err(error) = shared.authorize() {
            return denied(error);
        }
        if state.detached || state.completing {
            return denied(error(ErrorCode::NotReady, "connection is draining"));
        }
        if let Some(error) = context {
            return denied(error);
        }
        let kind = match &message {
            Control::Session(Session::Create { .. } | Session::Attach { .. }) => Kind::Binding,
            Control::Drain(Drain::Complete { .. }) => Kind::Complete,
            Control::Drain(Drain::Detach { .. }) => Kind::Detach,
            Control::Result(ResultMessage::Read { .. }) => Kind::Output,
            _ => Kind::Ordinary,
        };
        if matches!(kind, Kind::Binding) && (state.binding.is_some() || state.binding_pending) {
            return denied(error(
                ErrorCode::Conflict,
                "connection already binding or bound",
            ));
        }
        if !matches!(
            message,
            Control::Session(_) | Control::Drain(Drain::Detach { .. })
        ) && state.binding.is_none()
        {
            return denied(error(ErrorCode::NotReady, "session not attached"));
        }
        if matches!(kind, Kind::Complete) && state.pending != 0 {
            return denied(error(
                ErrorCode::NotReady,
                "connection has pending requests or transfers",
            ));
        }
        if state.pending >= shared.caps.pending_limit.0 as usize {
            return denied(error(ErrorCode::LimitExceeded, "connection pending limit"));
        }
        if matches!(kind, Kind::Output) && state.outputs >= shared.caps.stream_limit.0 as usize {
            return denied(error(
                ErrorCode::LimitExceeded,
                "connection result stream limit",
            ));
        }
        let binding = state.binding.clone();
        state.pending += 1;
        match kind {
            Kind::Binding => state.binding_pending = true,
            Kind::Complete => state.completing = true,
            Kind::Detach => state.detached = true,
            Kind::Output => state.outputs += 1,
            _ => {}
        }
        Ok(Submission::Pending(Pending {
            ticket: Arc::new(Ticket {
                shared: shared.clone(),
                kind,
            }),
            message,
            binding,
            accepted: Instant::now(),
        }))
    }

    /// Reserve before accepting an input header. Its actual QUIC stream ID and
    /// correlated admission response remain the input transport's responsibility.
    pub fn input(&self) -> Result<InputSlot, Error> {
        let shared = &self.shared;
        shared.authorize()?;
        let mut state = shared.state()?;
        if state.detached || state.completing {
            return Err(error(ErrorCode::NotReady, "connection is draining"));
        }
        let binding = state
            .binding
            .clone()
            .ok_or_else(|| error(ErrorCode::NotReady, "session not attached"))?;
        if state.pending >= shared.caps.pending_limit.0 as usize
            || state.inputs >= shared.caps.stream_limit.0 as usize
        {
            return Err(error(
                ErrorCode::LimitExceeded,
                "connection input or pending limit",
            ));
        }
        state.pending += 1;
        state.inputs += 1;
        Ok(InputSlot {
            ticket: Arc::new(Ticket {
                shared: shared.clone(),
                kind: Kind::Input,
            }),
            binding,
        })
    }
}

mod requests;
