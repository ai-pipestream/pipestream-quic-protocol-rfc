use super::*;

struct Cancel(watch::Sender<bool>);
impl Drop for Cancel {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}
struct WriteGuard(Option<watch::Sender<bool>>);
impl Drop for WriteGuard {
    fn drop(&mut self) {
        if let Some(cancel) = &self.0 {
            let _ = cancel.send(true);
        }
    }
}
struct Sender(v2_flow::Writer, bool);
impl Drop for Sender {
    fn drop(&mut self) {
        if !self.1 {
            let _ = self.0.reset(code(&stopped()));
        }
    }
}
struct Receiver(quinn::RecvStream, bool);
impl Drop for Receiver {
    fn drop(&mut self) {
        if !self.1 {
            let _ = self.0.stop(code(&stopped()));
        }
    }
}
enum Write {
    Bytes(Vec<u8>, oneshot::Sender<Result<()>>),
    Finish(oneshot::Sender<Result<()>>),
}
pub(super) struct Upload {
    sender: Sender,
    header: InputHeader,
    writes: mpsc::Receiver<Write>,
    cancel: watch::Receiver<bool>,
    _slot: OwnedSemaphorePermit,
}

/// Incremental input sender. An accepted write whose future is cancelled aborts
/// this stream; retrying that buffer on the same stream is never permitted.
/// Local FIN is not admission. Only `response` returns the correlated authority
/// receipt/refusal, which still needs durable journal validation and recording.
pub struct Input {
    stream: StreamId,
    writes: mpsc::Sender<Write>,
    cancel: Cancel,
    response: Option<oneshot::Receiver<Packet>>,
    finished: bool,
}
/// Independently owned upload half. Dropping it aborts unfinished transmission,
/// but does not cancel its separately owned admission-response collector.
pub struct InputWriter(Input);
impl InputWriter {
    pub fn stream_id(&self) -> StreamId {
        self.0.stream_id()
    }
    pub async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.0.write(bytes).await
    }
    pub async fn finish(&mut self) -> Result<()> {
        self.0.finish().await
    }
}
pub struct InputResponse(oneshot::Receiver<Packet>);
impl InputResponse {
    pub async fn receive(self) -> Result<Control> {
        match self.0.await.map_err(|_| stopped())?.reply? {
            Reply::Control(control) => Ok(control),
            Reply::Object(_) => Err(error(ErrorCode::InternalError, "input got an object reply")),
        }
    }
}
impl Input {
    pub fn split(mut self) -> (InputWriter, InputResponse) {
        let response = self.response.take().expect("unsplit input response");
        (InputWriter(self), InputResponse(response))
    }
    pub fn stream_id(&self) -> StreamId {
        self.stream
    }
    pub async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        if self.finished || *self.cancel.0.borrow() {
            return Err(stopped());
        }
        let mut guard = WriteGuard(Some(self.cancel.0.clone()));
        for chunk in bytes.chunks(CHUNK) {
            let permit = self.writes.reserve().await.map_err(|_| stopped())?;
            let (send, recv) = oneshot::channel();
            permit.send(Write::Bytes(chunk.to_vec(), send));
            recv.await.map_err(|_| stopped())??;
        }
        guard.0 = None;
        Ok(())
    }
    /// Commit the local length/digest check and send FIN, not an admission claim.
    pub async fn finish(&mut self) -> Result<()> {
        if self.finished || *self.cancel.0.borrow() {
            return Err(stopped());
        }
        let permit = self.writes.reserve().await.map_err(|_| stopped())?;
        let (send, recv) = oneshot::channel();
        let mut guard = WriteGuard(Some(self.cancel.0.clone()));
        permit.send(Write::Finish(send));
        recv.await.map_err(|_| stopped())??;
        self.finished = true;
        guard.0 = None;
        Ok(())
    }
    /// Also usable after an upload was stopped: an identical header replay can
    /// legitimately return its original admission without sending the body again.
    pub async fn response(mut self) -> Result<Control> {
        InputResponse(self.response.take().ok_or_else(stopped)?)
            .receive()
            .await
    }
}

impl Transport {
    /// The caller must already have durably retained this admission intent and
    /// a covering declaration receipt. This low-level transport does not invent
    /// either commitment from a scope page or successful stream transmission.
    pub async fn input(&self, header: InputHeader) -> Result<Input> {
        let shared = &self.0.0;
        header.encode_framed()?;
        PayloadReceiver::new(
            header.parameters.input.length,
            header.parameters.input.sha256,
            &shared.selected,
            std::time::Instant::now(),
        )?;
        let ticket = shared.ticket()?;
        let slot = shared
            .inputs
            .clone()
            .try_acquire_owned()
            .map_err(|_| error(ErrorCode::LimitExceeded, "client outgoing stream ceiling"))?;
        // Only stream allocation is serialized, never control I/O or payload
        // writes. Register actual QUIC IDs in allocation order, not poll order.
        let _opening = shared.opening.lock().await;
        {
            let book = shared.lock()?;
            if book.detached {
                return Err(error(ErrorCode::NotReady, "client connection draining"));
            }
            if !shared.selected.has(DURABLE_WORK) {
                return Err(error(
                    ErrorCode::ExtensionUnsupported,
                    "input requires durable profile",
                ));
            }
            if book.requests.keys().filter(|key| key.0 == 1).count()
                >= shared.selected.stream_limit.0 as usize
            {
                return Err(error(
                    ErrorCode::LimitExceeded,
                    "client unresolved admission ceiling",
                ));
            }
        }
        let writer = tokio::time::timeout(shared.options.frame_timeout, shared.flow.open_data())
            .await
            .map_err(|_| error(ErrorCode::LimitExceeded, "client input open deadline"))??;
        let sender = Sender(writer, false);
        let stream = StreamId(u64::from(sender.0.id()));
        let (reply, response) = oneshot::channel();
        let (writes, incoming) = mpsc::channel(1);
        let (cancel, cancelled) = watch::channel(false);
        {
            let mut book = shared.lock()?;
            if book.detached {
                return Err(error(ErrorCode::NotReady, "client connection draining"));
            }
            book.correlation.register_input(stream, &header)?;
            book.requests.insert(
                (1, stream.0),
                Pending {
                    reply: Some(reply),
                    ticket,
                    deadline: Some(
                        Instant::now()
                            + shared.options.response_timeout
                            + Elapsed::from_millis(shared.selected.stream_lifetime_ms.0),
                    ),
                },
            );
            if shared
                .uploads
                .try_send(Upload {
                    sender,
                    header,
                    writes: incoming,
                    cancel: cancelled,
                    _slot: slot,
                })
                .is_err()
            {
                shared
                    .connection
                    .close(code(&stopped()), b"client upload queue unavailable");
                return Err(stopped());
            }
        }
        Ok(Input {
            stream,
            writes,
            cancel: Cancel(cancel),
            response: Some(response),
            finished: false,
        })
    }
}

struct Deadlines {
    end: Instant,
    idle: Elapsed,
    last: Instant,
}
impl Deadlines {
    fn new(selected: &Capabilities) -> Self {
        let now = Instant::now();
        Self {
            end: now + Elapsed::from_millis(selected.stream_lifetime_ms.0),
            idle: Elapsed::from_millis(selected.stream_idle_ms.0),
            last: now,
        }
    }
    fn next(&self) -> Instant {
        self.end.min(self.last + self.idle)
    }
    fn progress(&mut self, bytes: usize) {
        if bytes != 0 {
            self.last = Instant::now();
        }
    }
}
async fn interrupted(cancel: &mut watch::Receiver<bool>) {
    if !*cancel.borrow_and_update() {
        let _ = cancel.changed().await;
    }
}
async fn bounded<T>(
    deadline: Instant,
    cancel: &mut watch::Receiver<bool>,
    operation: impl Future<Output = Result<T>>,
) -> Result<T> {
    if Instant::now() >= deadline {
        return Err(error(ErrorCode::LimitExceeded, "client object deadline"));
    }
    let value = tokio::select! {
        biased;
        _ = interrupted(cancel) => Err(stopped()),
        result = tokio::time::timeout_at(deadline, operation) => result.map_err(|_| error(ErrorCode::LimitExceeded, "client object deadline"))?,
    }?;
    if Instant::now() >= deadline {
        return Err(error(ErrorCode::LimitExceeded, "client object deadline"));
    }
    Ok(value)
}
pub(super) async fn upload(shared: Arc<Shared>, mut job: Upload) -> Result<()> {
    let result = upload_body(&shared, &mut job).await;
    if let Err(e) = result {
        let _ = job.sender.0.reset(code(&e));
    }
    // A local abort is not an admission reply. Keep its correlation until the
    // server's mandatory receipt/refusal, including a legitimate late receipt.
    Ok(())
}
async fn upload_body(shared: &Shared, job: &mut Upload) -> Result<()> {
    let mut deadlines = Deadlines::new(&shared.selected);
    let mut check = PayloadReceiver::new(
        job.header.parameters.input.length,
        job.header.parameters.input.sha256,
        &shared.selected,
        std::time::Instant::now(),
    )?;
    let header = job.header.encode_framed()?;
    bounded(
        deadlines
            .next()
            .min(Instant::now() + shared.options.frame_timeout),
        &mut job.cancel,
        job.sender.0.write_all(&header),
    )
    .await?;
    loop {
        let message = bounded(deadlines.next(), &mut job.cancel, async {
            job.writes.recv().await.ok_or_else(stopped)
        })
        .await?;
        match message {
            Write::Bytes(bytes, reply) => {
                let mut offset = 0;
                let result = async {
                    while offset < bytes.len() {
                        let n = bounded(
                            deadlines.next(),
                            &mut job.cancel,
                            job.sender.0.write(&bytes[offset..]),
                        )
                        .await?;
                        if n == 0 {
                            return Err(error(
                                ErrorCode::InternalError,
                                "input writer made no progress",
                            ));
                        }
                        check.receive(&bytes[offset..offset + n], std::time::Instant::now())?;
                        offset += n;
                        deadlines.progress(n);
                    }
                    Ok(())
                }
                .await;
                let _ = reply.send(result.clone());
                result?;
            }
            Write::Finish(reply) => {
                let result = check
                    .finish(std::time::Instant::now())
                    .map(|_| ())
                    .and_then(|_| job.sender.0.finish().map_err(|_| stopped()));
                let _ = reply.send(result.clone());
                result?;
                job.sender.1 = true;
                return Ok(());
            }
        }
    }
}

/// Constructed only by successful incremental length/SHA-256/FIN validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedObject {
    header: ResultHeader,
}
impl VerifiedObject {
    pub fn header(&self) -> &ResultHeader {
        &self.header
    }
}

pub struct Output {
    header: ResultHeader,
    bytes: mpsc::Receiver<Vec<u8>>,
    status: watch::Receiver<Option<Result<VerifiedObject>>>,
    verified: Option<VerifiedObject>,
    _cancel: Cancel,
    _ticket: Ticket,
}
impl Output {
    pub fn header(&self) -> &ResultHeader {
        &self.header
    }
    /// Bounded chunks for reversible staging only. An earlier chunk can still
    /// belong to a later truncated/corrupt transfer. None means verified FIN.
    pub async fn read_unverified(&mut self) -> Result<Option<Vec<u8>>> {
        if let Some(Err(e)) = &*self.status.borrow() {
            return Err(e.clone());
        }
        if let Some(bytes) = self.bytes.recv().await {
            return Ok(Some(bytes));
        }
        let proof = self.status.borrow().clone().ok_or_else(stopped)??;
        self.verified = Some(proof);
        Ok(None)
    }
    /// Available only after `read_unverified` has returned `Ok(None)`.
    pub fn verification(&self) -> Option<&VerifiedObject> {
        self.verified.as_ref()
    }
}

async fn header(shared: &Shared, recv: &mut quinn::RecvStream) -> Result<ResultHeader> {
    let deadline = Instant::now() + shared.options.frame_timeout;
    let read = async {
        let mut prefix = [0; 4];
        recv.read_exact(&mut prefix)
            .await
            .map_err(|_| error(ErrorCode::FrameError, "truncated result header prefix"))?;
        let length = object_header_length(prefix)?;
        let mut bytes = vec![0; length];
        recv.read_exact(&mut bytes)
            .await
            .map_err(|_| error(ErrorCode::FrameError, "truncated result header"))?;
        ResultHeader::decode(&bytes)
    };
    tokio::time::timeout_at(deadline, read)
        .await
        .map_err(|_| error(ErrorCode::LimitExceeded, "client result header deadline"))?
}
pub(super) async fn download(
    shared: Arc<Shared>,
    recv: quinn::RecvStream,
    _slot: OwnedSemaphorePermit,
) -> Result<()> {
    let mut recv = Receiver(recv, false);
    let header = match header(&shared, &mut recv.0).await {
        Ok(h) => h,
        Err(e) => {
            let _ = recv.0.stop(code(&e));
            return Ok(());
        }
    };
    let id = header.request;
    let (send, bytes) = mpsc::channel(1);
    let (status, finished) = watch::channel(None);
    let (cancel, mut cancelled) = watch::channel(false);
    let mut transfer = {
        let mut book = shared.lock()?;
        let transfer = match book
            .correlation
            .start_result(&header, std::time::Instant::now())
        {
            Ok(transfer) => transfer,
            Err(e) if matches!(e.code, ErrorCode::IntegrityError | ErrorCode::LimitExceeded) => {
                // A recognizable read with an invalid object commitment fails
                // only this delivery. Never turn it into a second control reply.
                book.correlation
                    .abort(&RequestTag::Control { request: id })?;
                let pending = book.requests.remove(&(0, id.0)).ok_or_else(|| {
                    error(ErrorCode::InternalError, "missing refused result request")
                })?;
                if let Some(reply) = pending.reply {
                    let _ = reply.send(Packet {
                        reply: Err(e.clone()),
                        _ticket: pending.ticket,
                    });
                }
                let _ = recv.0.stop(code(&e));
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let pending = book
            .requests
            .get_mut(&(0, id.0))
            .ok_or_else(|| error(ErrorCode::InternalError, "missing result request"))?;
        pending.deadline = None;
        let reply = pending
            .reply
            .take()
            .ok_or_else(|| error(ErrorCode::FrameError, "second result response"))?;
        let output = Output {
            header: header.clone(),
            bytes,
            status: finished,
            verified: None,
            _cancel: Cancel(cancel),
            _ticket: pending.ticket.clone(),
        };
        let _ = reply.send(Packet {
            reply: Ok(Reply::Object(output)),
            _ticket: pending.ticket.clone(),
        });
        transfer
    };
    let mut deadlines = Deadlines::new(&shared.selected);
    let result = async {
        loop {
            let mut bytes = vec![0; CHUNK];
            let n = bounded(deadlines.next(), &mut cancelled, async {
                recv.0.read(&mut bytes).await.map_err(|_| stopped())
            })
            .await?;
            match n {
                Some(n) => {
                    transfer.receive(&bytes[..n], std::time::Instant::now())?;
                    deadlines.progress(n);
                    bytes.truncate(n);
                    bounded(deadlines.next(), &mut cancelled, async {
                        send.send(bytes).await.map_err(|_| stopped())
                    })
                    .await?;
                }
                None => {
                    recv.1 = true;
                    let proof = transfer.finish(std::time::Instant::now())?;
                    shared.lock()?.correlation.finish_result(proof)?;
                    return Ok(VerifiedObject { header });
                }
            }
        }
    }
    .await;
    {
        let mut book = shared.lock()?;
        if let Err(e) = &result {
            let _ = recv.0.stop(code(e));
            book.correlation
                .abort(&RequestTag::Control { request: id })?;
        }
        book.requests.remove(&(0, id.0));
    }
    let _ = status.send(Some(result));
    Ok(())
}
