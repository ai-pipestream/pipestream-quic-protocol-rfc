//! Version-2 mandatory Core endpoint. Durable profiles are never advertised here.
//! This is the bounded transport foundation for authority integration, not an
//! implementation of the complete durable-work/result-delivery combination.

use crate::v2_tls::{Identity, Peer, ServerSecurity};
use anyhow::{Result, bail};
use pipestream_core::v2::*;
use std::{
    collections::BTreeMap,
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};
use tokio::{task::JoinSet, time::Instant};

pub(crate) mod framing;
use framing::Frame;

fn failure(code: ErrorCode, detail: &'static str) -> Error {
    Error { code, detail }
}

/// Local deployment ceilings, separate from negotiated per-connection limits.
#[derive(Clone)]
pub struct Options {
    pub offer: Capabilities,
    pub connections: usize,
    pub connections_per_principal: usize,
    pub anonymous_connections: usize,
    pub handshake_timeout: StdDuration,
    pub control_frame_timeout: StdDuration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            offer: Capabilities {
                response: ResponseFlag(0),
                supported: vec![],
                required: vec![],
                control_limit: ControlLimit(65536),
                stream_limit: ConcurrencyLimit(1),
                pending_limit: ConcurrencyLimit(16),
                object_limit: Number(0),
                stream_idle_ms: IdleMs(5000),
                stream_lifetime_ms: LifetimeMs(30000),
            },
            connections: 64,
            connections_per_principal: 8,
            anonymous_connections: 8,
            handshake_timeout: StdDuration::from_secs(5),
            control_frame_timeout: StdDuration::from_secs(10),
        }
    }
}

impl Options {
    fn validate(&self) -> Result<()> {
        Control::Capabilities(self.offer.clone()).encode(INITIAL_CONTROL_LIMIT)?;
        if self.offer.response.0 != 0
            || !self.offer.supported.is_empty()
            || !self.offer.required.is_empty()
        {
            bail!("Core server cannot advertise an unimplemented durable profile");
        }
        if !(1..=1024).contains(&self.connections)
            || !(1..=self.connections).contains(&self.connections_per_principal)
            || !(1..=self.connections).contains(&self.anonymous_connections)
            || self.connections as u64 * self.offer.control_limit.0 > 64 * 1024 * 1024
        {
            bail!("connection ceilings exceed count or 64 MiB raw-control-buffer budget");
        }
        for timeout in [self.handshake_timeout, self.control_frame_timeout] {
            if timeout.is_zero() || timeout > StdDuration::from_secs(30) {
                bail!("local handshake/control timeout must be positive and at most 30 seconds");
            }
        }
        Ok(())
    }
}

type Principal = Option<(String, String)>;
#[derive(Default)]
struct Principals(Mutex<BTreeMap<Principal, usize>>);
struct PrincipalSlot {
    table: Arc<Principals>,
    principal: Principal,
}

impl Principals {
    fn acquire(
        self: &Arc<Self>,
        identity: Option<&Identity>,
        options: &Options,
    ) -> Result<PrincipalSlot> {
        let principal = identity.map(|i| (i.authority.0.clone(), i.owner.0.clone()));
        let ceiling = if principal.is_some() {
            options.connections_per_principal
        } else {
            options.anonymous_connections
        };
        let mut table = self
            .0
            .lock()
            .map_err(|_| failure(ErrorCode::InternalError, "principal quota lock"))?;
        if table.get(&principal).copied().unwrap_or(0) >= ceiling {
            return Err(failure(ErrorCode::LimitExceeded, "principal connection ceiling").into());
        }
        *table.entry(principal.clone()).or_default() += 1;
        Ok(PrincipalSlot {
            table: self.clone(),
            principal,
        })
    }
}
impl Drop for PrincipalSlot {
    fn drop(&mut self) {
        let mut table = self
            .table
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = table.get_mut(&self.principal) {
            *count -= 1;
            if *count == 0 {
                table.remove(&self.principal);
            }
        }
    }
}

/// Owns a QUIC-v1 listener and bounded Core connections. The supplied security
/// policy is immutable for this server instance; replace it by restarting this
/// listener. Credential validity is rechecked before every control request.
pub struct Server {
    endpoint: quinn::Endpoint,
    security: Arc<ServerSecurity>,
    options: Options,
}

impl Server {
    pub fn bind(
        address: SocketAddr,
        mut security: ServerSecurity,
        options: Options,
    ) -> Result<Self> {
        options.validate()?;
        let mut transport = quinn::TransportConfig::default();
        transport
            .max_concurrent_bidi_streams(1u32.into())
            .max_concurrent_uni_streams(0u32.into())
            .stream_receive_window(65536u32.into())
            .receive_window(65536u32.into())
            .send_window(65536)
            .crypto_buffer_size(65536)
            .datagram_receive_buffer_size(None)
            .max_idle_timeout(Some(StdDuration::from_secs(30).try_into()?));
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
        Ok(Self {
            endpoint,
            security: Arc::new(security),
            options,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.endpoint.local_addr()?)
    }

    /// Shutdown ends connections without claiming durable-work completion.
    /// Each connection task is owned by this run and is aborted on cancellation.
    pub async fn run(self, shutdown: impl Future<Output = ()>) -> Result<()> {
        tokio::pin!(shutdown);
        let mut tasks = JoinSet::new();
        let principals = Arc::new(Principals::default());
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = tasks.join_next(), if !tasks.is_empty() => {},
                incoming = self.endpoint.accept() => {
                    let Some(incoming) = incoming else { break };
                    // Quinn's open count also retains closed/draining transport
                    // state, so rapid close/reconnect cannot bypass the ceiling.
                    if tasks.len() >= self.options.connections || self.endpoint.open_connections() >= self.options.connections {
                        incoming.refuse();
                        continue;
                    }
                    let security = self.security.clone();
                    let options = self.options.clone();
                    let principals = principals.clone();
                    tasks.spawn(async move {
                        if let Ok(peer) = security.accept(incoming, options.handshake_timeout).await {
                            let result = async {
                                let _slot = principals.acquire(security.authorize(&peer)?, &options)?;
                                connection(&peer, &security, &options).await
                            }.await;
                            if let Err(error) = result
                                && peer.connection().close_reason().is_none() {
                                    let code = error.downcast_ref::<Error>().map_or(ErrorCode::InternalError, |e| e.code);
                                    peer.connection().close(code.quic_error().try_into().expect("V2 error"), code.name().as_bytes());
                            }
                        }
                    });
                }
            }
        }
        self.endpoint
            .close(0u32.into(), b"server shutdown, no completion assertion");
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        tokio::time::timeout(StdDuration::from_secs(5), self.endpoint.wait_idle()).await?;
        Ok(())
    }
}

async fn connection(peer: &Peer, security: &ServerSecurity, options: &Options) -> Result<()> {
    let connection = peer.connection();
    let (mut send, mut recv) =
        tokio::time::timeout(options.control_frame_timeout, connection.accept_bi())
            .await
            .map_err(|_| failure(ErrorCode::LimitExceeded, "control stream open deadline"))??;
    if u64::from(recv.id()) != 0 {
        return Err(failure(ErrorCode::FrameError, "control must use Stream 0").into());
    }
    let Frame::Control(first) = receive_control(
        &send,
        &mut recv,
        None,
        Instant::now() + options.control_frame_timeout,
    )
    .await?
    else {
        return Err(failure(ErrorCode::FrameError, "missing capabilities").into());
    };
    first.validate_context(true, None)?;
    let Control::Capabilities(offer) = first else {
        unreachable!("validated first message")
    };
    let selected =
        Capabilities::select(&offer, &options.offer, security.authorize(peer)?.is_some())?;
    framing::send(
        &mut send,
        &Control::Capabilities(selected.clone()),
        INITIAL_CONTROL_LIMIT,
        Instant::now() + options.control_frame_timeout,
    )
    .await?;
    let limit = selected.control_limit.0 as usize;
    let mut highest = 0;
    let mut detach_deadline = None;
    let mut ready_frames = 0u8;
    loop {
        // Bound a continuously ready connection's work between scheduler yields,
        // including a stream of empty ignorable frames with no response writes.
        if ready_frames == 32 {
            tokio::task::yield_now().await;
            ready_frames = 0;
        }
        ready_frames += 1;
        let deadline = (Instant::now() + options.control_frame_timeout)
            .min(detach_deadline.unwrap_or(Instant::now() + options.control_frame_timeout));
        let frame = receive_control(&send, &mut recv, Some(limit), deadline).await?;
        let message = match frame {
            Frame::Ignored => continue,
            Frame::Fin if detach_deadline.is_some() => {
                // A queued response is not delivery. Immediate QUIC close can
                // discard it, so finish this direction and await its FIN ACK.
                send.finish()
                    .map_err(|_| failure(ErrorCode::ControlReset, "control closed before FIN"))?;
                let stopped = tokio::time::timeout_at(deadline, send.stopped())
                    .await
                    .map_err(|_| {
                        failure(
                            ErrorCode::LimitExceeded,
                            "control FIN acknowledgment deadline",
                        )
                    })??;
                if stopped.is_some() {
                    return Err(failure(
                        ErrorCode::ControlReset,
                        "control response direction stopped",
                    )
                    .into());
                }
                connection.close(0u32.into(), b"detached");
                return Ok(());
            }
            Frame::Fin => {
                return Err(failure(ErrorCode::FrameError, "control ended before detach").into());
            }
            Frame::Control(message) => message,
        };
        let context_refusal = match message.validate_context(true, Some(&selected)) {
            Ok(()) => None,
            Err(error) if error.code == ErrorCode::ExtensionUnsupported => Some(error),
            Err(error) => return Err(error.into()),
        };
        let request = request_id(&message)
            .ok_or_else(|| failure(ErrorCode::FrameError, "missing request ID"))?;
        if (highest == 0 && request.0 != 1) || request.0 <= highest {
            return Err(
                failure(ErrorCode::FrameError, "request ID not increasing from one").into(),
            );
        }
        highest = request.0;
        let authorization = security.authorize(peer);
        let refusal = if let Err(error) = authorization {
            Some(error)
        } else if detach_deadline.is_some() {
            Some(failure(ErrorCode::NotReady, "connection detached"))
        } else {
            context_refusal
        };
        let response = if let Some(error) = refusal {
            Control::Refusal(Refusal {
                request: RequestTag::Control { request },
                code: error.code,
                detail: Detail(error.detail.into()),
            })
        } else if matches!(message, Control::Drain(Drain::Detach { .. })) {
            detach_deadline =
                Some(Instant::now() + StdDuration::from_millis(selected.stream_lifetime_ms.0));
            Control::Drain(Drain::Detached { request })
        } else {
            return Err(failure(ErrorCode::InternalError, "unhandled Core request").into());
        };
        framing::send(
            &mut send,
            &response,
            limit,
            (Instant::now() + options.control_frame_timeout)
                .min(detach_deadline.unwrap_or(Instant::now() + options.control_frame_timeout)),
        )
        .await?;
    }
}

async fn receive_control(
    send: &quinn::SendStream,
    recv: &mut quinn::RecvStream,
    limit: Option<usize>,
    deadline: Instant,
) -> Result<Frame> {
    tokio::select! {
        biased;
        stopped = send.stopped() => {
            stopped?;
            Err(failure(ErrorCode::ControlReset, "control response direction stopped").into())
        },
        frame = framing::receive(recv, limit, deadline) => frame,
    }
}
