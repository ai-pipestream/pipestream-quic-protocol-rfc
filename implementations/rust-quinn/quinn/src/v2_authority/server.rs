//! Authenticated V2 listener over the durable authority and real object adapters.
//! Binding performs blocking storage audits. Run owns every connection task;
//! shutdown reports live background work instead of abandoning its resource pins.
use super::*;
use crate::{v2_core::framing, v2_flow};
use anyhow::Result as NetResult;
use pipestream_core::v2::authority::{execution::ResultEndpoint, ingress::Applications};
use std::{
    collections::BTreeMap,
    future::Future,
    net::SocketAddr,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};

#[derive(Clone)]
pub struct Options {
    pub offer: Capabilities,
    pub connections: usize,
    pub connections_per_principal: usize,
    pub anonymous_connections: usize,
    pub response_queue: usize,
    pub handshake_timeout: Elapsed,
    pub control_frame_timeout: Elapsed,
    pub shutdown_grace: Elapsed,
    pub flow: v2_flow::Limits,
    pub inputs: input::Options,
    pub outputs: output::Options,
    pub runtime: runtime::Options,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            offer: Capabilities {
                response: ResponseFlag(0),
                supported: vec![
                    ProfileId(DURABLE_WORK.into()),
                    ProfileId(RESULT_DELIVERY.into()),
                ],
                required: vec![],
                control_limit: ControlLimit(65536),
                stream_limit: ConcurrencyLimit(4),
                pending_limit: ConcurrencyLimit(16),
                object_limit: Number(16 * 1024 * 1024),
                stream_idle_ms: IdleMs(5000),
                stream_lifetime_ms: LifetimeMs(30000),
            },
            connections: 16,
            connections_per_principal: 4,
            anonymous_connections: 4,
            response_queue: 2,
            handshake_timeout: Elapsed::from_secs(5),
            control_frame_timeout: Elapsed::from_secs(10),
            shutdown_grace: Elapsed::from_secs(5),
            flow: Default::default(),
            inputs: Default::default(),
            outputs: Default::default(),
            runtime: Default::default(),
        }
    }
}
impl Options {
    fn validate(&self) -> NetResult<()> {
        Control::Capabilities(self.offer.clone()).encode(INITIAL_CONTROL_LIMIT)?;
        self.flow.validate()?;
        if self.offer.response.0 != 0
            || !self.offer.has(DURABLE_WORK)
            || self
                .offer
                .supported
                .iter()
                .any(|id| id.0 != u64::from(DURABLE_WORK) && id.0 != u64::from(RESULT_DELIVERY))
            || u64::from(self.flow.data_streams) != self.offer.stream_limit.0
        {
            return Err(error(
                ErrorCode::LimitExceeded,
                "invalid durable listener capabilities or stream geometry",
            )
            .into());
        }
        if !(1..=1024).contains(&self.connections)
            || !(1..=self.connections).contains(&self.connections_per_principal)
            || !(1..=self.connections).contains(&self.anonymous_connections)
            || !(1..=64).contains(&self.response_queue)
        {
            return Err(error(ErrorCode::LimitExceeded, "invalid listener count limits").into());
        }
        // Conservative encoded-state ceilings include pending request/response
        // pairs, input refusals, the bounded writer queue and in-flight framing.
        // These are not claims about measured heap/RSS or allocator overhead.
        let frames = 2 * self.offer.pending_limit.0
            + self.offer.stream_limit.0
            + self.response_queue as u64
            + 3;
        let raw = self.connections as u64 * frames * (self.offer.control_limit.0 + 5);
        let transport = self.connections as u64
            * (self.flow.receive_budget()? + self.flow.data_send + self.flow.control_send + 65536);
        if raw > 128 * 1024 * 1024 || transport > 128 * 1024 * 1024 {
            return Err(error(
                ErrorCode::LimitExceeded,
                "listener raw control or transport budget exceeds 128 MiB",
            )
            .into());
        }
        for timeout in [
            self.handshake_timeout,
            self.control_frame_timeout,
            self.shutdown_grace,
        ] {
            if timeout.is_zero() || timeout > Elapsed::from_secs(30) {
                return Err(error(
                    ErrorCode::LimitExceeded,
                    "listener timeout must be positive and at most 30 seconds",
                )
                .into());
            }
        }
        Ok(())
    }
}

type Principal = Option<(String, String)>;
#[derive(Default)]
struct Principals(Mutex<BTreeMap<Principal, usize>>);
struct PrincipalSlot(Arc<Principals>, Principal);
impl Principals {
    fn acquire(
        self: &Arc<Self>,
        identity: Option<&Identity>,
        options: &Options,
    ) -> NetResult<PrincipalSlot> {
        let principal = identity.map(|i| (i.authority.0.clone(), i.owner.0.clone()));
        let max = if principal.is_some() {
            options.connections_per_principal
        } else {
            options.anonymous_connections
        };
        let mut counts = self
            .0
            .lock()
            .map_err(|_| error(ErrorCode::InternalError, "principal quota lock"))?;
        if counts.get(&principal).copied().unwrap_or(0) >= max {
            return Err(error(ErrorCode::LimitExceeded, "principal connection ceiling").into());
        }
        *counts.entry(principal.clone()).or_default() += 1;
        Ok(PrincipalSlot(self.clone(), principal))
    }
}
impl Drop for PrincipalSlot {
    fn drop(&mut self) {
        let mut counts = self
            .0
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = counts.get_mut(&self.1) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&self.1);
            }
        }
    }
}

/// Reports observations after admissions stop. False means a live task or a
/// busy accounting lock, never permission to delete roots or restart an owner.
#[derive(Debug)]
pub struct Shutdown {
    pub runtime: runtime::Snapshot,
    pub connection_tasks_idle: bool,
    pub metadata_idle: bool,
    pub inputs_idle: bool,
    pub outputs_idle: bool,
    pub transport_idle: bool,
    pub fault: Option<ErrorCode>,
}
impl Shutdown {
    pub fn drained(&self) -> bool {
        self.runtime.finished
            && self.connection_tasks_idle
            && self.metadata_idle
            && self.inputs_idle
            && self.outputs_idle
            && self.transport_idle
    }
}

pub struct Server {
    endpoint: quinn::Endpoint,
    security: Arc<ServerSecurity>,
    authority: Authority,
    inputs: input::Inputs,
    outputs: output::Outputs,
    runtime: Option<runtime::Runtime>,
    connection_tasks: Arc<AtomicUsize>,
    options: Options,
}
impl Server {
    /// Blocking setup/audits, called outside a connection reader. The application
    /// registry and result endpoint are explicit deployment configuration.
    pub fn bind(
        address: SocketAddr,
        mut security: ServerSecurity,
        authority: Authority,
        applications: Arc<Applications>,
        result_endpoint: ResultEndpoint,
        options: Options,
    ) -> NetResult<Self> {
        options.validate()?;
        let mut transport = quinn::TransportConfig::default();
        options
            .flow
            .configure(&mut transport, quinn::Side::Server)?;
        transport
            .crypto_buffer_size(65536)
            .datagram_receive_buffer_size(None)
            // A permitted 30-second WORK wait must not race our own idle timer.
            // PING keeps normal peers alive without inventing application progress.
            .keep_alive_interval(Some(Elapsed::from_secs(5)))
            .max_idle_timeout(Some(Elapsed::from_secs(60).try_into()?));
        security.set_transport_config(Arc::new(transport));
        security.bound_incoming(
            options.connections,
            65536,
            options.connections as u64 * 65536,
        );
        let mut endpoint_config = quinn::EndpointConfig::default();
        endpoint_config.supported_versions(vec![1]);
        let endpoint = quinn::Endpoint::new(
            endpoint_config,
            Some(security.configuration()),
            std::net::UdpSocket::bind(address)?,
            Arc::new(quinn::TokioRuntime),
        )?;
        let inputs = input::Inputs::new(
            authority.clone(),
            applications.clone(),
            options.inputs.clone(),
        )?;
        let outputs = output::Outputs::new(authority.clone(), options.outputs.clone())?;
        let mut supported = options.offer.clone();
        supported.response = ResponseFlag(1);
        let runtime = authority.start_runtime(
            applications,
            result_endpoint,
            supported,
            options.runtime.clone(),
        )?;
        Ok(Self {
            endpoint,
            security: Arc::new(security),
            authority,
            inputs,
            outputs,
            runtime: Some(runtime),
            connection_tasks: Arc::new(AtomicUsize::new(0)),
            options,
        })
    }
    pub fn local_addr(&self) -> NetResult<SocketAddr> {
        Ok(self.endpoint.local_addr()?)
    }

    /// Stop transport admissions, cancel owned connection waiters, and wait up
    /// to shutdown_grace for actual execution/maintenance and file cleanup. An
    /// expired grace returns a non-drained report; accepted callbacks retain pins.
    pub async fn run(mut self, shutdown: impl Future<Output = ()>) -> NetResult<Shutdown> {
        tokio::pin!(shutdown);
        let mut tasks = JoinSet::new();
        let principals = Arc::new(Principals::default());
        let mut health = tokio::time::interval(Elapsed::from_millis(20));
        health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut fault = None;
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = health.tick() => {
                    let snapshot = self.runtime.as_ref().expect("owned runtime").snapshot();
                    fault = runtime_fault(&snapshot);
                    if fault.is_some() || snapshot.finished {
                        fault.get_or_insert(ErrorCode::InternalError);
                        break;
                    }
                },
                ended = tasks.join_next(), if !tasks.is_empty() => {
                    if ended.is_some_and(|ended| ended.is_err()) {
                        fault = Some(ErrorCode::InternalError);
                        break;
                    }
                },
                incoming = self.endpoint.accept() => {
                    let Some(incoming) = incoming else { break };
                    if tasks.len() >= self.options.connections || self.endpoint.open_connections() >= self.options.connections {
                        incoming.refuse();
                        continue;
                    }
                    let security = self.security.clone();
                    let authority = self.authority.clone();
                    let inputs = self.inputs.clone();
                    let outputs = self.outputs.clone();
                    let options = self.options.clone();
                    let principals = principals.clone();
                    let connection_tasks = self.connection_tasks.clone();
                    tasks.spawn(async move {
                        if let Ok(peer) = security.accept(incoming, options.handshake_timeout).await {
                            let peer = Arc::new(peer);
                            let _close = Close(peer.connection().clone());
                            let result = async {
                                let _slot = principals.acquire(security.authorize(&peer)?, &options)?;
                                connection(peer.clone(), security, authority, inputs, outputs, options, connection_tasks).await
                            }.await;
                            if let Err(failure) = result {
                                let code = failure.downcast_ref::<Error>().map_or(ErrorCode::InternalError, |e| e.code);
                                close(peer.connection(), code);
                            }
                        }
                    });
                }
            }
        }
        let deadline = Instant::now() + self.options.shutdown_grace;
        self.endpoint.close(
            fault.map_or(0, ErrorCode::quic_error).try_into()?,
            b"listener stopped, no work completion assertion",
        );
        self.runtime.as_ref().expect("owned runtime").request_stop();
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        loop {
            if self.runtime.as_ref().expect("owned runtime").is_finished()
                && self.connection_tasks.load(Ordering::Acquire) == 0
                && self.authority.slots.available_permits() == self.authority.metadata_jobs
                && self.inputs.is_idle()
                && self.outputs.is_idle()
            {
                break;
            }
            if Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep_until(deadline.min(Instant::now() + Elapsed::from_millis(10))).await;
        }
        let runtime = self.runtime.take().expect("owned runtime");
        let snapshot = if runtime.is_finished() {
            runtime.shutdown()?
        } else {
            runtime.snapshot()
        };
        let transport_idle = tokio::time::timeout_at(deadline, self.endpoint.wait_idle())
            .await
            .is_ok();
        Ok(Shutdown {
            fault: fault.or_else(|| runtime_fault(&snapshot)),
            runtime: snapshot,
            connection_tasks_idle: self.connection_tasks.load(Ordering::Acquire) == 0,
            metadata_idle: self.authority.slots.available_permits() == self.authority.metadata_jobs,
            inputs_idle: self.inputs.is_idle(),
            outputs_idle: self.outputs.is_idle(),
            transport_idle,
        })
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.endpoint
            .close(0u32.into(), b"listener dropped, no completion assertion");
        if let Some(runtime) = &self.runtime {
            runtime.request_stop();
        }
    }
}
fn runtime_fault(snapshot: &runtime::Snapshot) -> Option<ErrorCode> {
    snapshot
        .maintenance
        .as_ref()
        .and_then(|s| s.fault.map(|(_, code)| code))
        .or_else(|| {
            snapshot
                .execution
                .as_ref()
                .filter(|s| s.faulted)
                .map(|_| ErrorCode::InternalError)
        })
}
fn close(connection: &quinn::Connection, code: ErrorCode) {
    if connection.close_reason().is_none() {
        connection.close(
            code.quic_error().try_into().expect("V2 code"),
            code.name().as_bytes(),
        );
    }
}
struct Close(quinn::Connection);
impl Drop for Close {
    fn drop(&mut self) {
        close(&self.0, ErrorCode::Cancelled);
    }
}

struct TaskPin(Arc<AtomicUsize>);
impl Drop for TaskPin {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
// Field order matters: drop the actual future and all its held responses before
// reporting the child gone. Aborting its parent only schedules child cancellation.
struct Tracked<F> {
    future: std::pin::Pin<Box<F>>,
    _pin: TaskPin,
}
impl<F: Future> Future for Tracked<F> {
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}
struct TaskSet {
    tasks: JoinSet<NetResult<()>>,
    live: Arc<AtomicUsize>,
}
impl TaskSet {
    fn new(live: Arc<AtomicUsize>) -> Self {
        Self {
            tasks: JoinSet::new(),
            live,
        }
    }
    fn spawn(&mut self, future: impl Future<Output = NetResult<()>> + Send + 'static) {
        self.live.fetch_add(1, Ordering::AcqRel);
        self.tasks.spawn(Tracked {
            future: Box::pin(future),
            _pin: TaskPin(self.live.clone()),
        });
    }
    fn len(&self) -> usize {
        self.tasks.len()
    }
    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
    async fn join_next(&mut self) -> Option<Result<NetResult<()>, tokio::task::JoinError>> {
        self.tasks.join_next().await
    }
}

enum Outbound {
    Immediate(Control),
    Response(Response),
    Input(input::Reply),
    Output(output::Delivery),
}
impl Outbound {
    fn control(&mut self) -> NetResult<Option<&Control>> {
        Ok(match self {
            Self::Immediate(control) => Some(control),
            Self::Response(response) => match response.body() {
                ResponseBody::Control(control) => Some(control),
                ResponseBody::Result(_) => {
                    return Err(error(
                        ErrorCode::InternalError,
                        "result bypassed object transport",
                    )
                    .into());
                }
            },
            Self::Input(reply) => Some(reply.control()),
            Self::Output(delivery) => match delivery.status() {
                output::Status::Refused(control) => Some(control),
                _ => None,
            },
        })
    }
}
async fn send_control(
    send: &mut v2_flow::Writer,
    control: &Control,
    limit: usize,
    deadline: Instant,
) -> NetResult<()> {
    let bytes = control.encode(limit)?;
    if Instant::now() >= deadline {
        return Err(error(ErrorCode::LimitExceeded, "control write deadline").into());
    }
    tokio::time::timeout_at(deadline, send.write_all(&bytes))
        .await
        .map_err(|_| error(ErrorCode::LimitExceeded, "control write deadline"))??;
    if Instant::now() >= deadline {
        return Err(error(ErrorCode::LimitExceeded, "control write deadline").into());
    }
    Ok(())
}
#[derive(Clone)]
struct Outbox {
    sender: mpsc::Sender<Queued>,
    pending: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}
struct WritePin(Arc<AtomicUsize>, Arc<Notify>);
impl Drop for WritePin {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
        self.1.notify_one();
    }
}
struct Queued {
    value: Outbound,
    deadline: Instant,
    _pin: WritePin,
}
async fn queue(
    outbox: &Outbox,
    value: Outbound,
    timeout: Elapsed,
    drain: Option<Instant>,
) -> NetResult<()> {
    let deadline = (Instant::now() + timeout).min(drain.unwrap_or(Instant::now() + timeout));
    outbox.pending.fetch_add(1, Ordering::AcqRel);
    let queued = Queued {
        value,
        deadline,
        _pin: WritePin(outbox.pending.clone(), outbox.changed.clone()),
    };
    tokio::time::timeout_at(deadline, outbox.sender.send(queued))
        .await
        .map_err(|_| error(ErrorCode::LimitExceeded, "control queue deadline"))?
        .map_err(|_| error(ErrorCode::ControlReset, "control writer ended"))?;
    Ok(())
}

async fn connection(
    peer: Arc<Peer>,
    security: Arc<ServerSecurity>,
    authority: Authority,
    inputs: input::Inputs,
    outputs: output::Outputs,
    options: Options,
    connection_tasks: Arc<AtomicUsize>,
) -> NetResult<()> {
    let raw = peer.connection();
    let flow = v2_flow::Connection::new(raw.clone(), options.flow)?;
    let (mut send, mut recv) =
        tokio::time::timeout(options.control_frame_timeout, flow.accept_control())
            .await
            .map_err(|_| error(ErrorCode::LimitExceeded, "control stream open deadline"))??;
    let stopped = send.stopped();
    tokio::pin!(stopped);
    let first = tokio::select! {
        biased;
        _ = &mut stopped => return Err(error(ErrorCode::ControlReset, "control response direction stopped").into()),
        frame = framing::receive(&mut recv, None, Instant::now() + options.control_frame_timeout) => frame?,
    };
    let framing::Frame::Control(first) = first else {
        return Err(error(ErrorCode::FrameError, "missing capabilities").into());
    };
    first.validate_context(true, None)?;
    let Control::Capabilities(offer) = first else {
        unreachable!("first context validated")
    };
    let selected =
        Capabilities::select(&offer, &options.offer, security.authorize(&peer)?.is_some())?;
    let durable = selected
        .has(DURABLE_WORK)
        .then(|| authority.connection(peer.clone(), security.clone(), selected.clone(), flow))
        .transpose()?;
    send_control(
        &mut send,
        &Control::Capabilities(selected.clone()),
        INITIAL_CONTROL_LIMIT,
        Instant::now() + options.control_frame_timeout,
    )
    .await?;
    let limit = selected.control_limit.0 as usize;
    let timeout = options.control_frame_timeout;
    let mut tasks = TaskSet::new(connection_tasks.clone());
    let mut jobs = TaskSet::new(connection_tasks.clone());
    let mut input_jobs = TaskSet::new(connection_tasks);
    let (frames, mut incoming) = mpsc::channel(1);
    tasks.spawn(async move {
        let mut ready = 0;
        loop {
            // This receive future is never cancelled by another stream or job.
            let frame = match framing::receive_next(&mut recv, limit, timeout).await {
                Ok(frame) => frame,
                Err(error) => {
                    // Preserve the named framing error even if channel closure
                    // is observed before the reader's task-completion event.
                    let _ = frames.send(Err(error)).await;
                    return Ok(());
                }
            };
            if matches!(frame, framing::Frame::Ignored) {
                ready += 1;
                if ready == 32 {
                    tokio::task::yield_now().await;
                    ready = 0;
                }
                continue;
            }
            let fin = matches!(frame, framing::Frame::Fin);
            if frames.send(Ok(frame)).await.is_err() {
                return Ok(());
            }
            if fin {
                return Ok(());
            }
        }
    });
    let (sender, mut outgoing) = mpsc::channel::<Queued>(options.response_queue);
    let completed = Arc::new(AtomicBool::new(false));
    let finished = completed.clone();
    let changed = Arc::new(Notify::new());
    let responses = Outbox {
        sender,
        pending: Arc::new(AtomicUsize::new(0)),
        changed: changed.clone(),
    };
    let (finish_send, mut finish_receive) = oneshot::channel();
    let mut finish_send = Some(finish_send);
    let writer_peer = raw.clone();
    tasks.spawn(async move {
        loop {
            let response = tokio::select! {
                finish = &mut finish_receive => {
                    finish.map_err(|_| error(ErrorCode::ControlReset, "control owner ended"))?;
                    send.finish()?;
                    return Ok(());
                },
                response = outgoing.recv() => response,
            };
            let Some(mut response) = response else {
                return Ok(());
            };
            if let Some(control) = response.value.control().inspect_err(|failure| {
                close(
                    &writer_peer,
                    failure
                        .downcast_ref::<Error>()
                        .map_or(ErrorCode::InternalError, |e| e.code),
                );
            })? {
                let drain = matches!(
                    control,
                    Control::Drain(Drain::Completed { .. } | Drain::Detached { .. })
                );
                if let Err(failure) =
                    send_control(&mut send, control, limit, response.deadline).await
                {
                    // Publish the real timeout/reset before dropping the reply
                    // receiver. Its producer might currently await queue space;
                    // channel closure must not replace LIMIT_EXCEEDED with RESET.
                    close(
                        &writer_peer,
                        failure
                            .downcast_ref::<Error>()
                            .map_or(ErrorCode::InternalError, |e| e.code),
                    );
                    return Err(failure);
                }
                if drain {
                    finished.store(true, Ordering::Release);
                }
            }
            // All response/input/output quota pins outlive the actual write.
            drop(response);
        }
    });
    let mut highest = 0;
    let mut drain = None;
    let mut fin = false;
    let mut finishing = false;
    loop {
        if fin
            && !finishing
            && completed.load(Ordering::Acquire)
            && jobs.is_empty()
            && input_jobs.is_empty()
            && responses.pending.load(Ordering::Acquire) == 0
        {
            // write_all only schedules bytes. Closing QUIC here can discard
            // them. Finish control and await its FIN acknowledgment instead.
            finish_send
                .take()
                .expect("finish once")
                .send(())
                .map_err(|_| error(ErrorCode::ControlReset, "control writer ended"))?;
            finishing = true;
            drain = Some((Instant::now() + timeout).min(drain.unwrap_or(Instant::now() + timeout)));
        }
        tokio::select! {
            biased;
            stopped = &mut stopped => {
                if finishing && matches!(stopped, Ok(None)) {
                    raw.close(0u32.into(), b"control FIN acknowledged");
                    return Ok(());
                }
                return Err(error(ErrorCode::ControlReset, "control response direction stopped").into());
            },
            _ = raw.closed() => return Ok(()),
            _ = async { if let Some(deadline) = drain { tokio::time::sleep_until(deadline).await } else { std::future::pending().await } } => {
                return Err(error(ErrorCode::LimitExceeded, "detach lifetime ended").into());
            },
            _ = changed.notified() => {},
            ended = tasks.join_next(), if !tasks.is_empty() => {
                ended.expect("nonempty tasks").map_err(|_| error(ErrorCode::InternalError, "connection task panicked"))??;
            },
            ended = jobs.join_next(), if !jobs.is_empty() => {
                ended.expect("nonempty jobs").map_err(|_| error(ErrorCode::InternalError, "request task panicked"))??;
            },
            ended = input_jobs.join_next(), if !input_jobs.is_empty() => {
                ended.expect("nonempty input jobs").map_err(|_| error(ErrorCode::InternalError, "input task panicked"))??;
            },
            frame = incoming.recv(), if !fin => {
                match frame.transpose()? {
                    Some(framing::Frame::Fin) => {
                        if drain.is_none() && !completed.load(Ordering::Acquire) {
                            return Err(error(ErrorCode::FrameError, "control ended before drain").into());
                        }
                        fin = true;
                    },
                    Some(framing::Frame::Control(message)) => {
                        let detach = matches!(message, Control::Drain(Drain::Detach { .. }));
                        if let Some(connection) = &durable {
                            let output = matches!(message, Control::Result(ResultMessage::Read { .. }));
                            match connection.submit(message)? {
                                Submission::Refused(control) => queue(&responses, Outbound::Immediate(control), timeout, drain).await?,
                                Submission::Pending(pending) => {
                                    if detach { drain = Some(pending.accepted + Elapsed::from_millis(selected.stream_lifetime_ms.0)); }
                                    let responses = responses.clone();
                                    let outputs = outputs.clone();
                                    jobs.spawn(async move {
                                        let response = if output { Outbound::Output(outputs.request(pending)?.run().await) }
                                            else { Outbound::Response(pending.run().await?) };
                                        queue(&responses, response, timeout, drain).await
                                    });
                                }
                            }
                        } else {
                            let control = core(&peer, &security, &selected, message, &mut highest, &mut drain)?;
                            queue(&responses, Outbound::Immediate(control), timeout, drain).await?;
                        }
                    },
                    Some(framing::Frame::Ignored) => unreachable!("reader discards extensions"),
                    None => return Err(error(ErrorCode::ControlReset, "control reader ended").into()),
                }
            },
            input = async { match &durable {
                Some(connection) => inputs.accept(connection).await,
                None => std::future::pending().await,
            } },
                if !finishing && durable.is_some() && input_jobs.len() < selected.stream_limit.0 as usize => {
                let input = input?;
                let responses = responses.clone();
                input_jobs.spawn(async move { queue(&responses, Outbound::Input(input.run().await), timeout, drain).await });
            },
            input = raw.accept_uni(), if !finishing && durable.is_none() => {
                let mut input = input?;
                let stream = StreamId(u64::from(input.id()));
                let _ = input.stop(ErrorCode::ExtensionUnsupported.quic_error().try_into()?);
                queue(&responses, Outbound::Immediate(Control::Refusal(Refusal {
                    request: RequestTag::Input { stream }, code: ErrorCode::ExtensionUnsupported,
                    detail: Detail("Core has no object format".into()),
                })), timeout, drain).await?;
            },
        }
    }
}

fn core(
    peer: &Peer,
    security: &ServerSecurity,
    selected: &Capabilities,
    message: Control,
    highest: &mut u64,
    drain: &mut Option<Instant>,
) -> NetResult<Control> {
    let context = match message.validate_context(true, Some(selected)) {
        Ok(()) => None,
        Err(error) if error.code == ErrorCode::ExtensionUnsupported => Some(error),
        Err(error) => return Err(error.into()),
    };
    let request =
        request_id(&message).ok_or_else(|| error(ErrorCode::FrameError, "request missing"))?;
    if (*highest == 0 && request.0 != 1) || request.0 <= *highest {
        return Err(error(ErrorCode::FrameError, "request ID not increasing from one").into());
    }
    *highest = request.0;
    let refusal = security
        .authorize(peer)
        .err()
        .or_else(|| drain.map(|_| error(ErrorCode::NotReady, "connection detached")))
        .or(context);
    if let Some(error) = refusal {
        return Ok(super::refusal(request, error));
    }
    if matches!(message, Control::Drain(Drain::Detach { .. })) {
        *drain = Some(Instant::now() + Elapsed::from_millis(selected.stream_lifetime_ms.0));
        return Ok(Control::Drain(Drain::Detached { request }));
    }
    Err(error(ErrorCode::InternalError, "unhandled Core request").into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn child_task_accounting_waits_for_future_cleanup_after_parent_abort() {
        struct OnDrop(Arc<AtomicUsize>, Arc<AtomicBool>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                assert_eq!(self.0.load(Ordering::Acquire), 1);
                self.1.store(true, Ordering::Release);
            }
        }
        for poll_first in [false, true] {
            let live = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::new(AtomicBool::new(false));
            let mut tasks = TaskSet::new(live.clone());
            let guard = OnDrop(live.clone(), dropped.clone());
            let (started, running) = oneshot::channel();
            tasks.spawn(async move {
                let _guard = guard;
                let _ = started.send(());
                std::future::pending::<()>().await;
                Ok(())
            });
            if poll_first {
                running.await.unwrap();
            }
            drop(tasks);
            tokio::time::timeout(Elapsed::from_secs(5), async {
                while live.load(Ordering::Acquire) != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(dropped.load(Ordering::Acquire));
        }
    }

    #[test]
    fn listener_rejects_unfunded_geometry_queues_and_configuration() {
        Options::default().validate().unwrap();
        let cases: [fn(&mut Options); 8] = [
            |o| o.response_queue = 0,
            |o| o.connections_per_principal = o.connections + 1,
            |o| o.flow.data_streams = 0,
            |o| o.handshake_timeout = Elapsed::ZERO,
            |o| o.control_frame_timeout = Elapsed::from_secs(31),
            |o| o.shutdown_grace = Elapsed::ZERO,
            |o| {
                o.connections = 1024;
                o.offer.control_limit = ControlLimit(1 << 20);
            },
            |o| {
                o.flow.receive_stream = 1 << 20;
                o.flow.data_streams = 128;
                o.offer.stream_limit = ConcurrencyLimit(128);
            },
        ];
        for change in cases {
            let mut options = Options::default();
            change(&mut options);
            let failure = options.validate().unwrap_err();
            assert_eq!(
                failure.downcast_ref::<Error>().unwrap().code,
                ErrorCode::LimitExceeded
            );
        }
    }
}
