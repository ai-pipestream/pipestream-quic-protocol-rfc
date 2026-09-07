//! Retained result delivery, independent of application execution. The endpoint
//! owns connection correlation and must drive bounded maintenance when peers
//! stop sending progress. No result request creates or retries a work attempt.

use super::{
    payload::{ObjectReader, PayloadStore},
    *,
};
use std::{
    collections::BTreeMap,
    sync::{Mutex, MutexGuard, Weak},
    time::Instant,
};

#[derive(Clone)]
pub struct ResultService {
    shared: Arc<Shared>,
}
struct Shared {
    store: AuthorityStore,
    payloads: PayloadStore,
    registry: Mutex<Registry>,
}
#[derive(Default)]
struct Registry {
    last: u64,
    reads: BTreeMap<u64, Arc<Mutex<Lease>>>,
}
struct Lease {
    identity: SessionIdentity,
    header: ResultHeader,
    reader: Option<ObjectReader>,
    created: Instant,
    observed: Instant,
    progress: Instant,
    idle: Elapsed,
    lifetime: Elapsed,
    started: bool,
    pending_bytes: usize,
    eof: bool,
    closed: Option<ErrorCode>,
}

/// An opaque connection-local transfer. Dropping it aborts delivery only.
/// The service can expire/revoke the underlying read even while this is held.
pub struct ResultRead {
    shared: Weak<Shared>,
    id: u64,
    lease: Arc<Mutex<Lease>>,
}
#[derive(Default)]
pub struct ReadCursor {
    after: u64,
    through: Option<u64>,
}
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReadMaintenance {
    pub inspected: usize,
    pub closed: usize,
    pub busy: usize,
}

fn lock<T>(value: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    value
        .lock()
        .map_err(|_| StoreError::Corrupt("result service lock poisoned"))
}
fn selected_results(caps: &Capabilities) -> Result<()> {
    selected(caps)?;
    if !caps.has(RESULT_DELIVERY) {
        return Err(protocol(
            ErrorCode::ExtensionUnsupported,
            "result delivery not selected",
        ));
    }
    Ok(())
}
fn retained_manifest(
    store: &AuthorityStore,
    tx: &Transaction<'_>,
    identity: &SessionIdentity,
    key: &WorkKey,
    attempt: Id,
    caps: &Capabilities,
) -> Result<Manifest> {
    selected_results(caps)?;
    let binding = store.authorize_session(tx, identity, Permission::ReadResult)?;
    sessions::check_connection(tx, &binding, caps)?;
    key.check()?;
    attempt.check()?;
    let (_, view) = scopes::work(tx, identity.generation, key)?;
    view.validate_profiles(binding.results)?;
    if view.attempt.0 != attempt.0 {
        return Err(protocol(ErrorCode::NotFound, "result attempt not retained"));
    }
    let manifest = view
        .manifest
        .ok_or_else(|| protocol(ErrorCode::NotReady, "result not published"))?;
    if manifest.authority != identity.authority
        || manifest.owner != identity.owner
        || manifest.generation != identity.generation
    {
        return Err(StoreError::Corrupt("manifest authority identity changed"));
    }
    Ok(manifest)
}

impl ResultService {
    pub fn new(store: AuthorityStore, payloads: PayloadStore) -> Result<Self> {
        let mut connection = store.connect()?;
        let tx = connection.transaction()?;
        execution::bound_payloads(&tx, &payloads)?;
        drop(tx);
        Ok(Self {
            shared: Arc::new(Shared {
                store,
                payloads,
                registry: Mutex::new(Registry::default()),
            }),
        })
    }

    /// Read retained evidence, including under unsafe time or after external
    /// output expiry. The manifest's availability timestamp grants no read lease.
    pub fn manifest(
        &self,
        identity: &SessionIdentity,
        key: &WorkKey,
        attempt: Id,
        caps: &Capabilities,
    ) -> Result<Manifest> {
        let mut connection = self.shared.store.connect()?;
        let tx = connection.transaction()?;
        let manifest = retained_manifest(&self.shared.store, &tx, identity, key, attempt, caps)?;
        Control::Result(ResultMessage::ManifestResponse {
            request: Id(MAX_NUMBER),
            manifest: manifest.clone(),
        })
        .encode(caps.control_limit.0 as usize)?;
        self.shared
            .store
            .authorize(&identity.owner, Permission::ReadResult)?;
        Ok(manifest)
    }

    /// Pin the exact published object before returning a pending stream. Shared
    /// payload handle ceilings bound pending/active reads globally and per owner,
    /// including across multiple services on the same payload root. No send
    /// buffer is allocated here. The endpoint also enforces connection limits.
    pub fn begin_read(
        &self,
        identity: &SessionIdentity,
        request: &ResultMessage,
        caps: &Capabilities,
        now: Instant,
    ) -> Result<ResultRead> {
        selected_results(caps)?;
        request.check()?;
        let ResultMessage::Read {
            request,
            work,
            attempt,
            index,
            expected_sha256: sha256,
        } = request
        else {
            return Err(protocol(
                ErrorCode::FrameError,
                "expected object-read request",
            ));
        };
        let mut connection = self.shared.store.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let manifest = retained_manifest(&self.shared.store, &tx, identity, work, *attempt, caps)?;
        let output = manifest
            .outputs
            .get(index.0 as usize)
            .ok_or_else(|| protocol(ErrorCode::NotFound, "result output index absent"))?;
        if output.sha256 != *sha256 {
            return Err(protocol(
                ErrorCode::IntegrityError,
                "result commitment changed",
            ));
        }
        let utc = self.shared.store.check_clock(&tx)?;
        if utc >= manifest.available_until {
            return Err(protocol(ErrorCode::Expired, "result availability expired"));
        }
        if output.length > caps.object_limit {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "result exceeds negotiated object limit",
            ));
        }
        execution::bound_payloads(&tx, &self.shared.payloads)?;
        let (_, job, _, _, _) = execution::load(&tx, identity, work)?;
        if !job.outputs_live {
            return Err(protocol(
                ErrorCode::OutputUnavailable,
                "published output is not retained",
            ));
        }
        let header = ResultHeader {
            kind: Literal,
            request: *request,
            generation: identity.generation,
            work: work.clone(),
            attempt: *attempt,
            index: *index,
            length: output.length,
            sha256: output.sha256,
        };
        header.encode()?;
        let reader = self
            .shared
            .payloads
            .open_output(
                &job.reservation_key.0,
                *index,
                &identity.owner,
                &Input {
                    length: output.length,
                    sha256: output.sha256,
                    content_type: output.content_type.clone(),
                },
            )
            .map_err(storage_read_error)?;
        self.shared.store.remember_clock(&tx, utc)?;
        self.shared
            .store
            .authorize(&identity.owner, Permission::ReadResult)?;
        commit(tx, "result-read")?;
        let lease = Arc::new(Mutex::new(Lease {
            identity: identity.clone(),
            header,
            reader: Some(reader),
            created: now,
            observed: now,
            progress: now,
            idle: Elapsed::from_millis(caps.stream_idle_ms.0),
            lifetime: Elapsed::from_millis(caps.stream_lifetime_ms.0),
            started: false,
            pending_bytes: 0,
            eof: false,
            closed: None,
        }));
        let mut registry = lock(&self.shared.registry)?;
        let id = increment(registry.last)?;
        registry.last = id;
        registry.reads.insert(id, lease.clone());
        Ok(ResultRead {
            shared: Arc::downgrade(&self.shared),
            id,
            lease,
        })
    }

    pub fn pending(&self) -> Result<usize> {
        Ok(lock(&self.shared.registry)?.reads.len())
    }

    /// Visit at most `limit` leases (1..256), without holding the registry lock
    /// across disk/authorization work. A busy read stays pinned and is revisited;
    /// closing never refunds a handle while its I/O is still in progress.
    /// Each pass fixes its upper bound so new arrivals cannot starve older reads.
    pub fn maintain(
        &self,
        cursor: &mut ReadCursor,
        limit: usize,
        now: Instant,
    ) -> Result<ReadMaintenance> {
        if !(1..=256).contains(&limit) {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "invalid read maintenance batch",
            ));
        }
        let mut report = ReadMaintenance::default();
        for _ in 0..limit {
            let next = {
                let registry = lock(&self.shared.registry)?;
                let through = *cursor.through.get_or_insert(registry.last);
                registry
                    .reads
                    .range((
                        std::ops::Bound::Excluded(cursor.after),
                        std::ops::Bound::Included(through),
                    ))
                    .next()
                    .map(|(id, entry)| (*id, entry.clone()))
            };
            let Some((id, entry)) = next else {
                cursor.after = 0;
                cursor.through = None;
                break;
            };
            cursor.after = id;
            report.inspected += 1;
            let mut lease = match entry.try_lock() {
                Ok(lease) => lease,
                Err(std::sync::TryLockError::WouldBlock) => {
                    report.busy += 1;
                    continue;
                }
                Err(_) => return Err(StoreError::Corrupt("result lease lock poisoned")),
            };
            // A timer timestamp may predate foreground progress that occurred
            // while this pass was descheduled. It is not a clock rollback and
            // must not replace the foreground observation high-water mark.
            if let Err(error) = lease.check(&self.shared.store, now.max(lease.observed)) {
                lease.close(error_code(&error));
                drop(lease);
                lock(&self.shared.registry)?.reads.remove(&id);
                report.closed += 1;
            }
        }
        Ok(report)
    }
}

fn storage_read_error(error: StoreError) -> StoreError {
    match error {
        StoreError::Io(_) => protocol(ErrorCode::OutputUnavailable, "retained output I/O failed"),
        other => other,
    }
}
fn error_code(error: &StoreError) -> ErrorCode {
    match error {
        StoreError::Protocol(error) => error.code,
        _ => ErrorCode::InternalError,
    }
}
impl Lease {
    fn check(&self, store: &AuthorityStore, now: Instant) -> Result<()> {
        if let Some(code) = self.closed {
            return Err(protocol(code, "result transfer is closed"));
        }
        let mut connection = store.connect()?;
        let tx = connection.transaction()?;
        store.authorize_session(&tx, &self.identity, Permission::ReadResult)?;
        if now < self.observed {
            return Err(protocol(
                ErrorCode::ClockUnsafe,
                "result monotonic clock regressed",
            ));
        }
        if now.duration_since(self.created) >= self.lifetime
            || now.duration_since(self.progress) >= self.idle
        {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "result stream deadline reached",
            ));
        }
        Ok(())
    }
    fn close(&mut self, reason: ErrorCode) {
        self.closed.get_or_insert(reason);
        self.reader.take();
        self.pending_bytes = 0;
    }
}
impl Shared {
    fn remove(&self, id: u64) {
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reads
            .remove(&id);
    }
}
impl Drop for Shared {
    fn drop(&mut self) {
        for entry in self
            .registry
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reads
            .values()
        {
            entry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .close(ErrorCode::Cancelled);
        }
    }
}
impl ResultRead {
    fn with<T>(
        &mut self,
        now: Instant,
        operation: impl FnOnce(&mut Lease) -> Result<T>,
    ) -> Result<T> {
        let shared = self
            .shared
            .upgrade()
            .ok_or_else(|| protocol(ErrorCode::Cancelled, "result service stopped"))?;
        let mut lease = lock(&self.lease)?;
        let result = lease.check(&shared.store, now).and_then(|()| {
            lease.observed = now;
            operation(&mut lease)
        });
        if let Err(error) = &result {
            lease.close(error_code(error));
            shared.remove(self.id);
        }
        result
    }
    /// Start the one response stream. Pending time is part of its lifetime.
    pub fn start(&mut self, now: Instant) -> Result<ResultHeader> {
        self.with(now, |lease| {
            if lease.started {
                return Err(protocol(
                    ErrorCode::Conflict,
                    "result stream already started",
                ));
            }
            lease.started = true;
            Ok(lease.header.clone())
        })
    }
    pub fn buffer_limit(&self) -> Result<usize> {
        Ok(self
            .shared
            .upgrade()
            .ok_or_else(|| protocol(ErrorCode::Cancelled, "result service stopped"))?
            .payloads
            .chunk_limit())
    }
    /// Check immediately before scheduling a transport write or FIN, including
    /// after waiting for flow-control capacity with a previously read chunk.
    /// This grants no idle-time renewal. All `now` values come from the local
    /// monotonic clock, never a peer-supplied timestamp.
    pub fn check_deadline(&mut self, now: Instant) -> Result<()> {
        self.with(now, |_| Ok(()))
    }
    /// At most one borrowed chunk may be outstanding. Bytes remain provisional
    /// until the receiving endpoint verifies the full object and FIN.
    pub fn read_chunk(&mut self, bytes: &mut [u8], now: Instant) -> Result<usize> {
        self.with(now, |lease| {
            if !lease.started || lease.pending_bytes != 0 {
                return Err(protocol(
                    ErrorCode::Conflict,
                    "result stream is not ready for another chunk",
                ));
            }
            let count = lease
                .reader
                .as_mut()
                .ok_or_else(|| protocol(ErrorCode::Conflict, "result reader closed"))?
                .read_chunk(bytes)
                .map_err(storage_read_error)?;
            lease.pending_bytes = count;
            lease.eof = count == 0;
            Ok(count)
        })
    }
    /// Report bytes actually accepted by the bounded transport writer, not a
    /// disk read or application enqueue. Call `check_deadline` before that
    /// transport write. Empty progress cannot renew idle time.
    pub fn sent(&mut self, count: usize, now: Instant) -> Result<()> {
        self.with(now, |lease| {
            if !lease.started || count > lease.pending_bytes {
                return Err(protocol(
                    ErrorCode::IntegrityError,
                    "result send progress exceeds read bytes",
                ));
            }
            lease.pending_bytes -= count;
            if count != 0 {
                lease.progress = now;
            }
            Ok(())
        })
    }
    /// Check authorization/deadline before FIN, then call this after scheduling
    /// a successful transport FIN, never after a reset.
    /// This releases delivery resources and makes no claim of client receipt.
    pub fn finish(mut self, now: Instant) -> Result<()> {
        self.with(now, |lease| {
            if !lease.started || !lease.eof || lease.pending_bytes != 0 {
                return Err(protocol(
                    ErrorCode::IntegrityError,
                    "result stream lacks complete verified output",
                ));
            }
            lease.close(ErrorCode::AlreadyTerminal);
            Ok(())
        })
    }
}
impl Drop for ResultRead {
    fn drop(&mut self) {
        self.lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .close(ErrorCode::Cancelled);
        if let Some(shared) = self.shared.upgrade() {
            shared.remove(self.id);
        }
    }
}
