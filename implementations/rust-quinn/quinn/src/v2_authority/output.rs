//! Actual retained-result streams. The enclosing endpoint still owns control
//! decoding/writing, profile negotiation and lifecycle maintenance.
use super::*;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Wake, Waker},
};

#[derive(Clone)]
pub struct Options {
    pub active: usize,
    pub active_per_owner: usize,
    pub file_workers: usize,
    pub stream_open_timeout: Elapsed,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            active: 8,
            active_per_owner: 4,
            file_workers: 4,
            stream_open_timeout: Elapsed::from_secs(5),
        }
    }
}
#[derive(Default)]
struct Counts {
    active: usize,
    owners: BTreeMap<String, usize>,
}
struct SharedOutput {
    authority: Authority,
    options: Options,
    workers: workers::Workers,
    counts: Mutex<Counts>,
}
#[derive(Clone)]
pub struct Outputs {
    shared: Arc<SharedOutput>,
}
struct Lease {
    shared: Arc<SharedOutput>,
    owner: String,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut counts = self
            .shared
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        counts.active -= 1;
        if let Some(n) = counts.owners.get_mut(&self.owner) {
            *n -= 1;
            if *n == 0 {
                counts.owners.remove(&self.owner);
            }
        }
    }
}
#[derive(Clone)]
struct Pins {
    _lease: Arc<Lease>,
    ticket: Arc<Ticket>,
}
impl Pins {
    fn authorize(&self) -> Result<(), Error> {
        self.ticket.shared.authorize()
    }
}
type Read = workers::Value<ResultRead, Pins>;

impl Outputs {
    /// One shared output quota and file pool for an authority's connections.
    pub fn new(authority: Authority, options: Options) -> Result<Self, Error> {
        if !(1..=128).contains(&options.active)
            || !(1..=options.active).contains(&options.active_per_owner)
            || options.file_workers > options.active
            || options.stream_open_timeout.is_zero()
            || options.stream_open_timeout > Elapsed::from_secs(30)
        {
            return Err(error(
                ErrorCode::LimitExceeded,
                "invalid result transport limits",
            ));
        }
        let workers = workers::Workers::new(options.file_workers, options.active)?;
        Ok(Self {
            shared: Arc::new(SharedOutput {
                authority,
                options,
                workers,
                counts: Mutex::new(Counts::default()),
            }),
        })
    }
    /// Route an already validated, ordered RESULT Read submission here, instead
    /// of calling `Pending::run`. No result lease or file exists before quotas.
    pub fn request(&self, pending: Pending) -> Result<Request, Error> {
        if !Arc::ptr_eq(
            &self.shared.authority.slots,
            &pending.ticket.shared.authority.slots,
        ) {
            return Err(error(
                ErrorCode::InternalError,
                "output service belongs to another authority instance",
            ));
        }
        if !matches!(pending.message, Control::Result(ResultMessage::Read { .. })) {
            return Err(error(
                ErrorCode::FrameError,
                "output transport needs a result read",
            ));
        }
        let pins = self.reserve(&pending.ticket);
        Ok(Request {
            shared: self.shared.clone(),
            pending,
            pins,
        })
    }
    fn reserve(&self, ticket: &Arc<Ticket>) -> Result<Pins, Error> {
        ticket.shared.authorize()?;
        let owner = ticket.shared.identity.owner.0.clone();
        let mut counts = self
            .shared
            .counts
            .lock()
            .map_err(|_| error(ErrorCode::InternalError, "result quota lock poisoned"))?;
        if counts.active >= self.shared.options.active
            || counts.owners.get(&owner).copied().unwrap_or(0)
                >= self.shared.options.active_per_owner
        {
            return Err(error(
                ErrorCode::LimitExceeded,
                "result transfer capacity exhausted",
            ));
        }
        counts.active += 1;
        *counts.owners.entry(owner.clone()).or_default() += 1;
        Ok(Pins {
            ticket: ticket.clone(),
            _lease: Arc::new(Lease {
                shared: self.shared.clone(),
                owner,
            }),
        })
    }
}

pub struct Request {
    shared: Arc<SharedOutput>,
    pending: Pending,
    pins: Result<Pins, Error>,
}
#[derive(Debug)]
pub enum Status {
    /// Local bytes and FIN scheduled, not proof of client receipt.
    Sent,
    /// The header started its response; only RESET_STREAM reports the error.
    Aborted(ErrorCode),
    /// No header byte was sent. Send this on the bounded control writer.
    Refused(Control),
}
/// Retain until the refusal write finishes, or the sent/aborted status is handled.
pub struct Delivery {
    status: Status,
    _ticket: Arc<Ticket>,
    _pins: Option<Pins>,
}
impl Delivery {
    pub fn status(&self) -> &Status {
        &self.status
    }
}
impl Request {
    pub async fn run(self) -> Delivery {
        let request = request_id(&self.pending.message).expect("validated request");
        let mut started = false;
        let result = match &self.pins {
            Ok(pins) => transfer(&self.shared, &self.pending, pins, &mut started).await,
            Err(error) => Err(error.clone()),
        };
        let status = match result {
            Ok(()) => Status::Sent,
            Err(error) if started => Status::Aborted(error.code),
            Err(error) => Status::Refused(refusal(request, error)),
        };
        Delivery {
            status,
            _ticket: self.pending.ticket,
            _pins: self.pins.ok(),
        }
    }
}

struct Sending {
    stream: quinn::SendStream,
    finished: bool,
}
impl Sending {
    fn abort(&mut self, code: ErrorCode) {
        let _ = self
            .stream
            .reset(quinn::VarInt::from_u64(code.quic_error()).expect("fixed application error"));
        self.finished = true;
    }
}
impl Drop for Sending {
    fn drop(&mut self) {
        if !self.finished {
            self.abort(ErrorCode::Cancelled);
        }
    }
}
struct Writable(Notify);
impl Wake for Writable {
    fn wake(self: Arc<Self>) {
        self.0.notify_one();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.notify_one();
    }
}

fn live(pins: &Pins, deadline: Instant) -> Result<(), Error> {
    pins.authorize()?;
    if Instant::now() >= deadline {
        return Err(error(ErrorCode::LimitExceeded, "result transport deadline"));
    }
    Ok(())
}
async fn bounded<T>(
    deadline: Instant,
    future: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| error(ErrorCode::LimitExceeded, "result transport deadline"))?
}
async fn checked(
    shared: &Arc<SharedOutput>,
    read: Read,
    deadline: Instant,
) -> Result<(Read, Instant), Error> {
    let workers = shared.workers.clone();
    bounded(
        deadline,
        shared.workers.run(move || {
            let (mut read, pins) = read.take();
            pins.authorize()?;
            read.check_deadline(std::time::Instant::now())
                .map_err(storage)?;
            let deadline = Instant::from_std(read.next_deadline().map_err(storage)?).min(deadline);
            Ok((workers::Value::new(read, pins, workers), deadline))
        }),
    )
    .await
}

async fn transfer(
    shared: &Arc<SharedOutput>,
    pending: &Pending,
    pins: &Pins,
    started: &mut bool,
) -> Result<(), Error> {
    let lifetime =
        pending.accepted + Elapsed::from_millis(pins.ticket.shared.caps.stream_lifetime_ms.0);
    let first_pins = pins.clone();
    let service = shared.clone();
    let identity = pending
        .binding
        .as_ref()
        .ok_or_else(|| error(ErrorCode::NotReady, "session not attached"))?
        .identity
        .clone();
    let Control::Result(message) = pending.message.clone() else {
        unreachable!("validated read")
    };
    let (mut read, mut deadline) = bounded(
        lifetime,
        shared.workers.run(move || {
            live(&first_pins, lifetime)?;
            let mut read = service
                .authority
                .results
                .begin_read(
                    &identity,
                    &message,
                    &first_pins.ticket.shared.caps,
                    std::time::Instant::now(),
                )
                .map_err(storage)?;
            read.check_deadline(std::time::Instant::now())
                .map_err(storage)?;
            let deadline = Instant::from_std(read.next_deadline().map_err(storage)?).min(lifetime);
            Ok((
                workers::Value::new(read, first_pins, service.workers.clone()),
                deadline,
            ))
        }),
    )
    .await?;
    let open_deadline = deadline.min(Instant::now() + shared.options.stream_open_timeout);
    let opening = pins.ticket.shared.peer.connection().open_uni();
    tokio::pin!(opening);
    let stream = loop {
        live(pins, open_deadline)?;
        tokio::select! {
            result = &mut opening => break result.map_err(|_| error(ErrorCode::Cancelled, "result connection closed"))?,
            _ = tokio::time::sleep_until(open_deadline.min(Instant::now() + Elapsed::from_millis(20))) => {
                (read, deadline) = checked(shared, read, open_deadline).await?;
            }
        }
    };
    let mut sending = Sending {
        stream,
        finished: false,
    };
    let result = send_object(
        shared,
        pins,
        read,
        deadline.min(lifetime),
        lifetime,
        &mut sending.stream,
        started,
    )
    .await;
    match result {
        Ok(()) => {
            sending.finished = true;
            Ok(())
        }
        Err(error) => {
            sending.abort(error.code);
            Err(error)
        }
    }
}

async fn send_object(
    shared: &Arc<SharedOutput>,
    pins: &Pins,
    read: Read,
    deadline: Instant,
    lifetime: Instant,
    send: &mut quinn::SendStream,
    started: &mut bool,
) -> Result<(), Error> {
    let workers = shared.workers.clone();
    let (mut read, header) = bounded(
        deadline,
        shared.workers.run(move || {
            let (mut read, pins) = read.take();
            pins.authorize()?;
            let header = read
                .start(std::time::Instant::now())
                .map_err(storage)?
                .encode_framed()?;
            Ok((workers::Value::new(read, pins, workers), header))
        }),
    )
    .await?;
    let writable = Arc::new(Writable(Notify::new()));
    let mut deadline = deadline;
    let mut used = 0;
    while used < header.len() {
        let (next, count, next_deadline) = write(
            shared,
            pins,
            read,
            &header[used..],
            deadline.min(lifetime),
            send,
            &writable,
        )
        .await?;
        read = next;
        deadline = next_deadline;
        used += count;
        *started |= count != 0;
    }
    let mut buffer = vec![0; shared.authority.payloads.chunk_limit().min(16384)];
    loop {
        let workers = shared.workers.clone();
        let count;
        (read, buffer, count, deadline) = bounded(
            deadline.min(lifetime),
            shared.workers.run(move || {
                let (mut read, pins) = read.take();
                pins.authorize()?;
                let count = read
                    .read_chunk(&mut buffer, std::time::Instant::now())
                    .map_err(storage)?;
                read.check_deadline(std::time::Instant::now())
                    .map_err(storage)?;
                let deadline = Instant::from_std(read.next_deadline().map_err(storage)?);
                Ok((
                    workers::Value::new(read, pins, workers),
                    buffer,
                    count,
                    deadline,
                ))
            }),
        )
        .await?;
        if count == 0 {
            break;
        }
        let mut used = 0;
        while used < count {
            let (next, sent, _) = write(
                shared,
                pins,
                read,
                &buffer[used..count],
                deadline.min(lifetime),
                send,
                &writable,
            )
            .await?;
            read = next;
            let now = std::time::Instant::now();
            let workers = shared.workers.clone();
            let progress_deadline = lifetime.min(
                Instant::from_std(now)
                    + Elapsed::from_millis(pins.ticket.shared.caps.stream_idle_ms.0),
            );
            (read, deadline) = bounded(
                progress_deadline,
                shared.workers.run(move || {
                    let (mut read, pins) = read.take();
                    pins.authorize()?;
                    read.sent(sent, now).map_err(storage)?;
                    read.check_deadline(std::time::Instant::now())
                        .map_err(storage)?;
                    let deadline = Instant::from_std(read.next_deadline().map_err(storage)?);
                    Ok((workers::Value::new(read, pins, workers), deadline))
                }),
            )
            .await?;
            used += sent;
        }
    }
    (read, deadline) = checked(shared, read, deadline.min(lifetime)).await?;
    live(pins, deadline.min(lifetime))?;
    send.finish()
        .map_err(|_| error(ErrorCode::Cancelled, "result stream closed before FIN"))?;
    let now = std::time::Instant::now();
    bounded(
        deadline.min(lifetime),
        shared.workers.run(move || {
            let (read, pins) = read.take();
            pins.authorize()?;
            read.finish(now).map_err(storage)
        }),
    )
    .await
}

async fn write(
    shared: &Arc<SharedOutput>,
    pins: &Pins,
    mut read: Read,
    bytes: &[u8],
    mut deadline: Instant,
    send: &mut quinn::SendStream,
    writable: &Arc<Writable>,
) -> Result<(Read, usize, Instant), Error> {
    let waker = Waker::from(writable.clone());
    loop {
        (read, deadline) = checked(shared, read, deadline).await?;
        live(pins, deadline)?;
        // Never leave a write future armed across an authorization check. Quinn
        // gets one poll after fresh validation, then wakes our separate notifier.
        let result = Pin::new(&mut *send).poll_write(&mut Context::from_waker(&waker), bytes);
        match result {
            Poll::Ready(Ok(count)) if count != 0 => return Ok((read, count, deadline)),
            Poll::Ready(Ok(_)) => {
                return Err(error(
                    ErrorCode::InternalError,
                    "result writer made no progress",
                ));
            }
            Poll::Ready(Err(_)) => {
                return Err(error(ErrorCode::Cancelled, "result receiver stopped"));
            }
            Poll::Pending => {}
        }
        tokio::select! {
            _ = writable.0.notified() => {},
            _ = tokio::time::sleep_until(deadline.min(Instant::now() + Elapsed::from_millis(20))) => {},
        }
    }
}
