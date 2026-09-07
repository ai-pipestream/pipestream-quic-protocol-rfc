//! Black-box Core checks over actual QUIC. The PKI fixture is shared, not
//! the server's control reader: these clients construct/read raw wire frames.
use super::*;
use crate::v2_core::{Options, Server};
use tokio::{sync::oneshot, task::JoinHandle};

struct Running {
    fixture: Fixture,
    address: std::net::SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<()>>>,
}
impl Running {
    fn start(options: Options) -> Self {
        Self::with_fixture(Fixture::new(), options)
    }
    fn with_fixture(fixture: Fixture, options: Options) -> Self {
        let security = ServerSecurity::new(
            vec![fixture.certificate.der.clone()],
            fixture.certificate.key.clone_key(),
            Some(fixture.policy.clone()),
        )
        .unwrap();
        let server = Server::bind("127.0.0.1:0".parse().unwrap(), security, options).unwrap();
        let address = server.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(server.run(async {
            let _ = stopped.await;
        }));
        Self {
            fixture,
            address,
            stop: Some(stop),
            task: Some(task),
        }
    }
    async fn raw(
        &self,
        principal: Option<usize>,
    ) -> (
        quinn::Endpoint,
        Result<quinn::Connection, quinn::ConnectionError>,
    ) {
        self.raw_config(self.fixture.config(principal)).await
    }
    async fn raw_config(
        &self,
        config: quinn::ClientConfig,
    ) -> (
        quinn::Endpoint,
        Result<quinn::Connection, quinn::ConnectionError>,
    ) {
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(config);
        let connection = tokio::time::timeout(
            HANDSHAKE,
            endpoint.connect(self.address, "localhost").unwrap(),
        )
        .await
        .unwrap();
        (endpoint, connection)
    }
    async fn session(&self, principal: Option<usize>) -> Session {
        self.session_config(self.fixture.config(principal)).await
    }
    async fn session_config(&self, config: quinn::ClientConfig) -> Session {
        let (endpoint, connection) = self.raw_config(config).await;
        let connection = connection.unwrap();
        let (send, recv) = connection.open_bi().await.unwrap();
        Session {
            endpoint,
            connection,
            send,
            recv,
            limit: INITIAL_CONTROL_LIMIT,
        }
    }
    async fn finish(mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        self.stop.take();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
struct Session {
    endpoint: quinn::Endpoint,
    connection: quinn::Connection,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    limit: usize,
}
impl Drop for Session {
    fn drop(&mut self) {
        self.endpoint.close(0u32.into(), b"test done");
    }
}
impl Session {
    async fn write(&mut self, bytes: &[u8]) {
        tokio::time::timeout(HANDSHAKE, self.send.write_all(bytes))
            .await
            .unwrap()
            .unwrap();
    }
    async fn send(&mut self, message: Control) {
        self.write(&message.encode(self.limit).unwrap()).await;
    }
    async fn receive(&mut self) -> Control {
        let mut prefix = [0; 5];
        tokio::time::timeout(HANDSHAKE, self.recv.read_exact(&mut prefix))
            .await
            .unwrap()
            .unwrap();
        let n = u32::from_be_bytes(prefix[1..].try_into().unwrap()) as usize;
        assert!(n <= self.limit);
        let mut bytes = vec![0; n + 5];
        bytes[..5].copy_from_slice(&prefix);
        tokio::time::timeout(HANDSHAKE, self.recv.read_exact(&mut bytes[5..]))
            .await
            .unwrap()
            .unwrap();
        Control::decode(&bytes, self.limit).unwrap()
    }
    async fn negotiate(&mut self, offered: Capabilities) -> Capabilities {
        self.send(Control::Capabilities(offered.clone())).await;
        let Control::Capabilities(selected) = self.receive().await else {
            panic!("not capabilities")
        };
        offered.validate_selection(&selected).unwrap();
        self.limit = selected.control_limit.0 as usize;
        selected
    }
    async fn detach(&mut self, id: u64) {
        self.send(Control::Drain(Drain::Detach { request: Id(id) }))
            .await;
        assert_eq!(
            self.receive().await,
            Control::Drain(Drain::Detached { request: Id(id) })
        );
    }
    async fn closed(&self, code: ErrorCode) {
        let error = tokio::time::timeout(HANDSHAKE, self.connection.closed())
            .await
            .unwrap();
        assert!(
            matches!(&error, quinn::ConnectionError::ApplicationClosed(close) if close.error_code.into_inner() == code.quic_error()),
            "{error:?}"
        );
    }
}
fn core_offer() -> Capabilities {
    Options::default().offer
}

#[tokio::test]
async fn core_half_close_preserves_detach_and_pipelined_refusal_bytes() {
    let running = Running::start(Options::default());
    let mut client = running.session(None).await;
    client.negotiate(core_offer()).await;
    client
        .send(Control::Drain(Drain::Detach { request: Id(1) }))
        .await;
    client
        .send(Control::Session(
            pipestream_core::v2::Session::NextSequence { request: Id(2) },
        ))
        .await;
    client.send.finish().unwrap();
    assert_eq!(
        client.receive().await,
        Control::Drain(Drain::Detached { request: Id(1) })
    );
    assert!(matches!(
        client.receive().await,
        Control::Refusal(Refusal {
            request: RequestTag::Control { request: Id(2) },
            code: ErrorCode::NotReady,
            ..
        })
    ));
    let closed = tokio::time::timeout(HANDSHAKE, client.connection.closed())
        .await
        .unwrap();
    assert!(
        matches!(closed, quinn::ConnectionError::ApplicationClosed(c) if c.error_code.into_inner() == 0)
    );
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn core_negotiates_minima_streams_large_ignored_frames_and_detaches() {
    let mut options = Options::default();
    options.offer.control_limit = ControlLimit(1_048_576);
    let running = Running::start(options);
    let mut client = running.session(Some(0)).await;
    let mut offered = core_offer();
    offered.control_limit = ControlLimit(524288);
    offered.pending_limit = ConcurrencyLimit(3);
    let selected = client.negotiate(offered).await;
    assert!(selected.supported.is_empty());
    assert_eq!(selected.control_limit.0, 524288);
    assert_eq!(selected.pending_limit.0, 3);
    // Greater than the 64 KiB QUIC receive window, so a whole-frame wait
    // without incremental credit replenishment would deadlock here.
    let mut ignored = vec![0x80];
    ignored.extend_from_slice(&131072u32.to_be_bytes());
    for b in ignored {
        client.write(&[b]).await;
    }
    for _ in 0..128 {
        client.write(&[0xff; 1024]).await;
    }
    let mut book = Correlation::new(selected).unwrap();
    let request = Control::Drain(Drain::Detach { request: Id(1) });
    book.register(&request, None).unwrap();
    client.send(request).await;
    book.accept(&client.receive().await).unwrap();
    assert_eq!(book.pending(), 0);
    client.send.finish().unwrap();
    let closed = tokio::time::timeout(HANDSHAKE, client.connection.closed())
        .await
        .unwrap();
    assert!(
        matches!(closed, quinn::ConnectionError::ApplicationClosed(close) if close.error_code.into_inner() == 0)
    );
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn core_refuses_profile_requests_without_closing_and_refuses_new_work_after_detach() {
    let running = Running::start(Options::default());
    let mut client = running.session(None).await;
    client.negotiate(core_offer()).await;
    client
        .send(Control::Session(
            pipestream_core::v2::Session::NextSequence { request: Id(1) },
        ))
        .await;
    assert!(matches!(
        client.receive().await,
        Control::Refusal(Refusal {
            request: RequestTag::Control { request: Id(1) },
            code: ErrorCode::ExtensionUnsupported,
            ..
        })
    ));
    client.detach(3).await; // Strict increase is required, not consecutive numbering.
    client
        .send(Control::Drain(Drain::Detach { request: Id(4) }))
        .await;
    assert!(matches!(
        client.receive().await,
        Control::Refusal(Refusal {
            request: RequestTag::Control { request: Id(4) },
            code: ErrorCode::NotReady,
            ..
        })
    ));
    client
        .send(Control::Drain(Drain::Detach { request: Id(4) }))
        .await;
    client.closed(ErrorCode::FrameError).await;
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn core_required_profiles_fail_negotiation_without_a_capabilities_ack() {
    let running = Running::start(Options::default());
    for (principal, expected) in [
        (None, ErrorCode::Unauthorized),
        (Some(0), ErrorCode::ExtensionUnsupported),
    ] {
        let mut client = running.session(principal).await;
        client.send(Control::Capabilities(offer(true))).await;
        client.closed(expected).await;
    }
    running.finish().await;
}

#[tokio::test]
async fn core_unknown_types_oversize_noncanonical_ids_and_wrong_direction_have_named_closes() {
    let running = Running::start(Options::default());
    let cases = [
        (vec![0x00, 0, 0, 0, 0], ErrorCode::FrameError),
        (vec![0xc0, 0, 0, 0, 0], ErrorCode::ExtensionUnsupported),
        (vec![0x06, 0xff, 0xff, 0xff, 0xff], ErrorCode::FrameError),
        (
            vec![0x06, 0, 0, 0, 4, 0x82, 0x02, 0x18, 0x01],
            ErrorCode::FrameError,
        ),
        (
            Control::Drain(Drain::Detached { request: Id(1) })
                .encode(4096)
                .unwrap(),
            ErrorCode::FrameError,
        ),
        (
            Control::Drain(Drain::Detach { request: Id(2) })
                .encode(4096)
                .unwrap(),
            ErrorCode::FrameError,
        ),
        (
            Control::Capabilities(core_offer()).encode(4096).unwrap(),
            ErrorCode::FrameError,
        ),
    ];
    for (bytes, expected) in cases {
        let mut client = running.session(None).await;
        client.negotiate(core_offer()).await;
        client.write(&bytes).await;
        client.closed(expected).await;
    }
    let mut valid = running.session(None).await;
    valid.negotiate(core_offer()).await;
    valid.detach(1).await;
    drop(valid);
    running.finish().await;
}

#[tokio::test]
async fn core_truncated_and_unnegotiated_control_frames_fail_closed() {
    let running = Running::start(Options::default());
    for bytes in [
        vec![],
        vec![1, 0],
        vec![1, 0, 0, 0, 10, 0x80],
        vec![0x80, 0, 0, 0, 0],
        vec![1, 0xff, 0xff, 0xff, 0xff],
    ] {
        let mut client = running.session(None).await;
        client.write(&bytes).await;
        client.send.finish().unwrap();
        client.closed(ErrorCode::FrameError).await;
    }
    running.finish().await;
}

#[tokio::test]
async fn core_control_reset_in_either_direction_terminates_the_connection() {
    let running = Running::start(Options::default());
    for receiver_stops in [false, true] {
        let mut client = running.session(None).await;
        client.negotiate(core_offer()).await;
        if receiver_stops {
            client.recv.stop(777u32.into()).unwrap();
        } else {
            client.send.reset(777u32.into()).unwrap();
        }
        client.closed(ErrorCode::ControlReset).await;
    }
    running.finish().await;
}

#[tokio::test]
async fn core_partial_headers_have_a_bounded_whole_frame_deadline() {
    let options = Options {
        control_frame_timeout: Duration::from_millis(150),
        ..Options::default()
    };
    let running = Running::start(options);
    let mut client = running.session(None).await;
    client.write(&[1]).await;
    client.closed(ErrorCode::LimitExceeded).await;
    drop(client);
    running.finish().await;
}

async fn quota_close(running: &Running, principal: Option<usize>, before_tls: bool) {
    let (endpoint, connected) = running.raw(principal).await;
    let closed = match connected {
        Ok(connection) => tokio::time::timeout(HANDSHAKE, connection.closed())
            .await
            .unwrap(),
        Err(error) => error,
    };
    if before_tls {
        assert!(
            matches!(&closed, quinn::ConnectionError::ConnectionClosed(close) if close.error_code == quinn::TransportErrorCode::CONNECTION_REFUSED),
            "{closed:?}"
        );
    } else {
        assert!(
            matches!(&closed, quinn::ConnectionError::ApplicationClosed(close) if close.error_code.into_inner() == ErrorCode::LimitExceeded.quic_error()),
            "{closed:?}"
        );
    }
    endpoint.close(0u32.into(), b"done");
}

#[tokio::test]
async fn core_principal_rotation_and_anonymous_quotas_do_not_block_another_owner() {
    let mut fixture = Fixture::new();
    let mut principals = fixture.policy.principals.clone();
    principals.insert(
        fixture.clients[2].fingerprint(),
        IdentityLabel("bob".into()),
    );
    fixture.policy = Arc::new(
        ClientAuthentication::new(
            IdentityLabel("issuer-a".into()),
            roots(&fixture.issuer),
            principals,
            fixture.clock.clone(),
        )
        .unwrap(),
    );
    let options = Options {
        connections_per_principal: 1,
        anonymous_connections: 1,
        ..Options::default()
    };
    let running = Running::with_fixture(fixture, options);
    let mut alice = running.session(Some(0)).await;
    alice.negotiate(core_offer()).await;
    quota_close(&running, Some(1), false).await;
    let mut anonymous = running.session(None).await;
    anonymous.negotiate(core_offer()).await;
    quota_close(&running, None, false).await;
    let mut bob = running.session(Some(2)).await;
    bob.negotiate(core_offer()).await;
    bob.detach(1).await;
    alice.detach(1).await;
    anonymous.detach(1).await;
    drop((bob, alice, anonymous));
    running.finish().await;
}

#[tokio::test]
async fn core_global_ceiling_refuses_new_tls_without_disturbing_existing_connections() {
    let options = Options {
        connections: 2,
        connections_per_principal: 1,
        anonymous_connections: 1,
        ..Options::default()
    };
    let running = Running::start(options);
    let mut alice = running.session(Some(0)).await;
    alice.negotiate(core_offer()).await;
    let mut anonymous = running.session(None).await;
    anonymous.negotiate(core_offer()).await;
    quota_close(&running, Some(1), true).await;
    alice.detach(1).await;
    anonymous.detach(1).await;
    drop((alice, anonymous));
    running.finish().await;
}

#[tokio::test]
async fn core_revalidates_credentials_on_each_request_without_restoring_expired_peers() {
    let running = Running::start(Options::default());
    let mut client = running.session(Some(0)).await;
    client.negotiate(core_offer()).await;
    for (id, year) in [(1, 2032), (2, 2030)] {
        running
            .fixture
            .clock
            .0
            .store(at(year, 6, 1), Ordering::SeqCst);
        client
            .send(Control::Drain(Drain::Detach { request: Id(id) }))
            .await;
        assert!(matches!(
            client.receive().await,
            Control::Refusal(Refusal {
                code: ErrorCode::Unauthorized,
                ..
            })
        ));
    }
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn core_refuses_invalid_local_limits_and_unimplemented_advertisements() {
    let fixture = Fixture::new();
    for fault in [
        "global",
        "principal",
        "anonymous",
        "memory",
        "timeout",
        "profile",
    ] {
        let mut options = Options::default();
        match fault {
            "global" => options.connections = 0,
            "principal" => options.connections_per_principal = 65,
            "anonymous" => options.anonymous_connections = 0,
            "memory" => {
                options.connections = 65;
                options.offer.control_limit = ControlLimit(1_048_576);
            }
            "timeout" => options.handshake_timeout = Duration::ZERO,
            _ => options.offer.supported = vec![ProfileId(DURABLE_WORK.into())],
        }
        let security = ServerSecurity::new(
            vec![fixture.certificate.der.clone()],
            fixture.certificate.key.clone_key(),
            Some(fixture.policy.clone()),
        )
        .unwrap();
        assert!(
            Server::bind("127.0.0.1:0".parse().unwrap(), security, options).is_err(),
            "{fault}"
        );
    }
}

#[tokio::test]
async fn core_prohibits_object_and_second_control_streams_without_blocking_control() {
    let running = Running::start(Options::default());
    let mut client = running.session(None).await;
    client.negotiate(core_offer()).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), client.connection.open_uni())
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), client.connection.open_bi())
            .await
            .is_err()
    );
    client.detach(1).await;
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn core_peer_not_reading_responses_is_bounded_and_does_not_block_another_peer() {
    let options = Options {
        control_frame_timeout: Duration::from_millis(300),
        ..Options::default()
    };
    let running = Running::start(options);
    let mut config = running.fixture.config(None);
    let mut transport = quinn::TransportConfig::default();
    transport
        .receive_window(4096u32.into())
        .stream_receive_window(4096u32.into());
    config.transport_config(Arc::new(transport));
    let mut blocked = running.session_config(config).await;
    blocked.negotiate(core_offer()).await;
    let mut healthy = running.session(Some(0)).await;
    healthy.negotiate(core_offer()).await;
    // Receive credit was capped before TLS: already-advertised credit cannot
    // be revoked by lowering a window after the handshake. Violate the client's
    // pending ceiling deliberately; response backpressure must not grow an
    // unbounded queue or stall unrelated connections.
    let flood = async {
        for request in 1..=20000 {
            let bytes = Control::Session(pipestream_core::v2::Session::NextSequence {
                request: Id(request),
            })
            .encode(blocked.limit)
            .unwrap();
            if blocked.send.write_all(&bytes).await.is_err() {
                break;
            }
        }
    };
    tokio::time::timeout(HANDSHAKE, async {
        tokio::join!(flood, healthy.detach(1));
    })
    .await
    .unwrap();
    blocked.closed(ErrorCode::LimitExceeded).await;
    drop((blocked, healthy));
    running.finish().await;
}

#[tokio::test]
async fn core_shutdown_closes_active_connections_without_a_completion_response() {
    let running = Running::start(Options::default());
    let mut client = running.session(None).await;
    client.negotiate(core_offer()).await;
    running.finish().await;
    let close = tokio::time::timeout(HANDSHAKE, client.connection.closed())
        .await
        .unwrap();
    assert!(
        matches!(close, quinn::ConnectionError::ApplicationClosed(close) if close.error_code.into_inner() == 0)
    );
    assert!(client.recv.read_exact(&mut [0; 5]).await.is_err());
}
