//! Actual QUIC input reception through validated durable admission. This module
//! is not yet connected to the public listener and enables no profile itself.
use super::*;
use pipestream_core::v2::authority::ingress::{
    Applications, InputPreparation, InputReception, ReceivingInput,
};
use std::collections::BTreeMap;

#[derive(Clone)]
pub struct Options {
    pub active: usize,
    pub active_per_owner: usize,
    pub file_workers: usize,
    pub header_timeout: Elapsed,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            active: 8,
            active_per_owner: 4,
            file_workers: 4,
            header_timeout: Elapsed::from_secs(5),
        }
    }
}
#[derive(Default)]
struct Counts {
    active: usize,
    owners: BTreeMap<String, usize>,
}
struct SharedInput {
    authority: Authority,
    applications: Arc<Applications>,
    options: Options,
    workers: workers::Workers,
    counts: Mutex<Counts>,
}
#[derive(Clone)]
pub struct Inputs {
    shared: Arc<SharedInput>,
}
struct Lease {
    shared: Arc<SharedInput>,
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
/// Clone into file jobs: cancellation of the async receiver is not evidence
/// that those jobs stopped using storage or finished their possible commit.
#[derive(Clone)]
struct Pins {
    _lease: Arc<Lease>,
    slot: InputSlot,
}
impl Pins {
    fn authorize(&self) -> Result<(), Error> {
        self.slot.authorize()
    }
}
enum Beginning {
    Replay(OperationReceipt),
    Receiving(workers::Value<Box<ReceivingInput>, Pins>),
}

impl Inputs {
    /// Set up paired roots and a fixed I/O pool before accepting input streams.
    /// Clone this value across connections to share its global/owner ceilings.
    pub fn new(
        authority: Authority,
        applications: Arc<Applications>,
        options: Options,
    ) -> Result<Self, Error> {
        if !(1..=128).contains(&options.active)
            || !(1..=options.active).contains(&options.active_per_owner)
            || options.file_workers > options.active
            || options.header_timeout.is_zero()
            || options.header_timeout > Elapsed::from_secs(30)
        {
            return Err(error(
                ErrorCode::LimitExceeded,
                "invalid input transport limits",
            ));
        }
        let workers = workers::Workers::new(options.file_workers, options.active)?;
        Ok(Self {
            shared: Arc::new(SharedInput {
                authority,
                applications,
                options,
                workers,
                counts: Mutex::new(Counts::default()),
            }),
        })
    }
    fn reserve(&self, connection: &Connection) -> Result<Pins, Error> {
        let slot = connection.input()?;
        let owner = slot.binding.identity.owner.0.clone();
        let mut counts = self
            .shared
            .counts
            .lock()
            .map_err(|_| error(ErrorCode::InternalError, "input quota lock poisoned"))?;
        if counts.active >= self.shared.options.active
            || counts.owners.get(&owner).copied().unwrap_or(0)
                >= self.shared.options.active_per_owner
        {
            return Err(error(
                ErrorCode::LimitExceeded,
                "input transfer capacity exhausted",
            ));
        }
        counts.active += 1;
        *counts.owners.entry(owner.clone()).or_default() += 1;
        Ok(Pins {
            slot,
            _lease: Arc::new(Lease {
                shared: self.shared.clone(),
                owner,
            }),
        })
    }
    /// Accept only a stream from this authenticated connection, never a caller-
    /// supplied receive stream from another TLS peer. Call in a bounded endpoint
    /// select loop; `Request::run` can progress separately from the control reader.
    pub async fn accept(&self, connection: &Connection) -> anyhow::Result<Request> {
        if !Arc::ptr_eq(
            &self.shared.authority.slots,
            &connection.shared.authority.slots,
        ) {
            return Err(error(
                ErrorCode::InternalError,
                "input service belongs to another authority instance",
            )
            .into());
        }
        let recv = connection.shared.peer.connection().accept_uni().await?;
        let accepted = Instant::now();
        let stream = StreamId(u64::from(recv.id()));
        let pins = self.reserve(connection);
        Ok(Request {
            shared: self.shared.clone(),
            recv,
            stream,
            pins,
            accepted,
        })
    }
}

pub struct Request {
    shared: Arc<SharedInput>,
    recv: quinn::RecvStream,
    stream: StreamId,
    pins: Result<Pins, Error>,
    accepted: Instant,
}
/// Hold through the control response write. Dropping a reply before writing
/// means a lost acknowledgment; it never undoes a committed admission.
pub struct Reply {
    control: Control,
    _pins: Option<Pins>,
}
impl Reply {
    pub fn control(&self) -> &Control {
        &self.control
    }
}

impl Request {
    pub async fn run(mut self) -> Reply {
        let tag = RequestTag::Input {
            stream: self.stream,
        };
        let result = match &self.pins {
            Ok(pins) => receive(&self.shared, pins, &mut self.recv, self.accepted).await,
            Err(error) => Err(error.clone()),
        };
        let control = match result {
            Ok((receipt, replay)) => {
                if replay {
                    let _ = self.recv.stop(0u32.into());
                }
                Control::Work(Work::Admitted {
                    request: tag,
                    receipt,
                })
            }
            Err(error) => {
                let _ = self.recv.stop(
                    quinn::VarInt::from_u64(error.code.quic_error())
                        .expect("fixed application code"),
                );
                Control::Refusal(Refusal {
                    request: tag,
                    code: error.code,
                    detail: Detail(error.detail.into()),
                })
            }
        };
        Reply {
            control,
            _pins: self.pins.ok(),
        }
    }
}

async fn read(
    recv: &mut quinn::RecvStream,
    bytes: &mut [u8],
    deadline: Instant,
) -> Result<Option<usize>, Error> {
    if Instant::now() >= deadline {
        return Err(error(ErrorCode::LimitExceeded, "input receive deadline"));
    }
    let result = tokio::time::timeout_at(deadline, recv.read(bytes))
        .await
        .map_err(|_| error(ErrorCode::LimitExceeded, "input receive deadline"))?;
    if Instant::now() >= deadline {
        return Err(error(ErrorCode::LimitExceeded, "input receive deadline"));
    }
    result.map_err(|_| error(ErrorCode::IntegrityError, "input stream interrupted"))
}
async fn fill(
    recv: &mut quinn::RecvStream,
    bytes: &mut [u8],
    deadline: Instant,
) -> Result<(), Error> {
    let mut used = 0;
    while used < bytes.len() {
        used += read(recv, &mut bytes[used..], deadline)
            .await?
            .ok_or_else(|| error(ErrorCode::FrameError, "truncated input header"))?;
    }
    Ok(())
}

async fn receive(
    shared: &Arc<SharedInput>,
    pins: &Pins,
    recv: &mut quinn::RecvStream,
    accepted: Instant,
) -> Result<(OperationReceipt, bool), Error> {
    let header_deadline = accepted + shared.options.header_timeout;
    let mut prefix = [0; 4];
    fill(recv, &mut prefix, header_deadline).await?;
    let length = object_header_length(prefix)?;
    let mut header = vec![0; length];
    fill(recv, &mut header, header_deadline).await?;
    let header = InputHeader::decode(&header)?;
    let start = std::time::Instant::now();
    let lifetime = Instant::from_std(start)
        + Elapsed::from_millis(pins.slot.capabilities().stream_lifetime_ms.0);
    let idle = Elapsed::from_millis(pins.slot.capabilities().stream_idle_ms.0);
    let mut progress = Instant::from_std(start);
    let first_pins = pins.clone();
    let service = shared.clone();
    let reception =
        shared
            .workers
            .run(move || {
                first_pins.authorize()?;
                if std::time::Instant::now() >= lifetime.into_std() {
                    return Err(error(ErrorCode::LimitExceeded, "input preflight deadline"));
                }
                let received = service
                    .authority
                    .store
                    .receive_input(
                        &first_pins.slot.binding.identity,
                        &header,
                        first_pins.slot.capabilities(),
                        &service.authority.payloads,
                        &service.applications,
                        start,
                    )
                    .map_err(storage)?;
                Ok(match received {
                    InputReception::Replay(receipt) => Beginning::Replay(receipt),
                    InputReception::Receiving(receiving) => Beginning::Receiving(
                        workers::Value::new(receiving, first_pins, service.workers.clone()),
                    ),
                })
            })
            .await?;
    let mut receiving = match reception {
        Beginning::Replay(receipt) => return Ok((receipt, true)),
        Beginning::Receiving(receiving) => receiving,
    };
    let mut buffer = vec![0; shared.authority.payloads.chunk_limit().min(16384)];
    loop {
        let count = read(recv, &mut buffer, lifetime.min(progress + idle)).await?;
        let Some(count) = count else {
            break;
        };
        if count == 0 {
            continue;
        }
        let now = std::time::Instant::now();
        progress = Instant::from_std(now);
        let job_pins = pins.clone();
        let workers = shared.workers.clone();
        let deadline = lifetime.min(progress + idle).into_std();
        (receiving, buffer) = shared
            .workers
            .run(move || {
                job_pins.authorize()?;
                if std::time::Instant::now() >= deadline {
                    return Err(error(ErrorCode::LimitExceeded, "input storage deadline"));
                }
                let (mut value, value_pins) = receiving.take();
                value.receive(&buffer[..count], now).map_err(storage)?;
                // A slow file write cannot renew the next receive's idle/lifetime.
                value
                    .check_deadline(std::time::Instant::now())
                    .map_err(storage)?;
                Ok((workers::Value::new(value, value_pins, workers), buffer))
            })
            .await?;
    }
    let now = std::time::Instant::now();
    let job_pins = pins.clone();
    let service = shared.clone();
    // Do not return a timeout refusal once a possible admission commit is
    // running. Connection loss may abandon the reply, but the job owns its pins
    // and the caller resolves uncertainty using this same immutable operation.
    shared
        .workers
        .run(move || {
            job_pins.authorize()?;
            let (receiving, _value_pins) = receiving.take();
            let validated = receiving.finish(now).map_err(storage)?;
            match service
                .authority
                .store
                .prepare_input(
                    validated,
                    job_pins.slot.capabilities(),
                    &service.applications,
                )
                .map_err(storage)?
            {
                InputPreparation::Replay(receipt) => Ok((receipt, true)),
                InputPreparation::Ready(prepared) => service
                    .authority
                    .store
                    .admit_input(
                        *prepared,
                        job_pins.slot.capabilities(),
                        &service.applications,
                    )
                    .map(|receipt| (receipt, false))
                    .map_err(storage),
            }
        })
        .await
}
