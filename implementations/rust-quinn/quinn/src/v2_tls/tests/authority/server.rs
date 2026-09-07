//! Black-box requests to the public durable listener. Only TLS/store fixtures
//! and typed wire codecs are shared; no request calls the local dispatcher.
use super::*;
use crate::{
    v2_authority::server::{Options, Server, Shutdown},
    v2_flow,
};
use tokio::{sync::oneshot, task::JoinHandle};
mod journal;

struct Running {
    db: Database,
    tls: Fixture,
    address: std::net::SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<anyhow::Result<Shutdown>>>,
}
fn options() -> Options {
    Options {
        offer: Capabilities {
            response: ResponseFlag(0),
            required: vec![],
            ..caps()
        },
        flow: v2_flow::Limits {
            data_send: 8192,
            control_send: 4096,
            receive_stream: 8192,
            data_streams: 4,
        },
        ..Default::default()
    }
}
impl Running {
    fn new(options: Options) -> Self {
        let db = Database::new();
        let tls = Fixture::new();
        Self::with(db, tls, options)
    }
    fn with(db: Database, tls: Fixture, options: Options) -> Self {
        let security = ServerSecurity::new(
            vec![tls.certificate.der.clone()],
            tls.certificate.key.clone_key(),
            Some(tls.policy.clone()),
        )
        .unwrap();
        let server = Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            security,
            db.authority.clone(),
            inputs::applications(),
            ResultEndpoint::new("localhost:7443".into()).unwrap(),
            options,
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(server.run(async {
            let _ = stopped.await;
        }));
        Self {
            db,
            tls,
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
        let limits = options().flow;
        let mut transport = quinn::TransportConfig::default();
        limits
            .configure(&mut transport, quinn::Side::Client)
            .unwrap();
        let mut config = self.tls.config(principal);
        config.transport_config(Arc::new(transport));
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
    async fn client(&self, principal: Option<usize>) -> Client {
        let (endpoint, connection) = self.raw(principal).await;
        let connection = connection.unwrap();
        let flow = v2_flow::Connection::new(connection.clone(), options().flow).unwrap();
        let (send, recv) = flow.open_control().await.unwrap();
        Client {
            endpoint,
            connection,
            flow,
            send,
            recv,
            next: 1,
            limit: INITIAL_CONTROL_LIMIT,
        }
    }
    async fn shutdown(&mut self) -> Shutdown {
        self.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    }
    async fn finish(mut self) {
        let report = self.shutdown().await;
        assert!(report.drained(), "{report:?}");
        assert!(report.fault.is_none(), "{report:?}");
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
struct Client {
    endpoint: quinn::Endpoint,
    connection: quinn::Connection,
    flow: v2_flow::Connection,
    send: v2_flow::Writer,
    recv: quinn::RecvStream,
    next: u64,
    limit: usize,
}
impl Drop for Client {
    fn drop(&mut self) {
        self.endpoint.close(0u32.into(), b"test finished");
    }
}
impl Client {
    async fn write(&mut self, bytes: &[u8]) {
        tokio::time::timeout(HANDSHAKE, self.send.write_all(bytes))
            .await
            .unwrap()
            .unwrap();
    }
    async fn receive(&mut self) -> Control {
        self.receive_within(HANDSHAKE).await
    }
    async fn receive_within(&mut self, timeout: Duration) -> Control {
        let mut prefix = [0; 5];
        tokio::time::timeout(timeout, self.recv.read_exact(&mut prefix))
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
    async fn negotiate(&mut self, offer: Capabilities) -> Capabilities {
        self.write(
            &Control::Capabilities(offer.clone())
                .encode(INITIAL_CONTROL_LIMIT)
                .unwrap(),
        )
        .await;
        let Control::Capabilities(selected) = self.receive().await else {
            panic!("missing selection")
        };
        offer.validate_selection(&selected).unwrap();
        self.limit = selected.control_limit.0 as usize;
        selected
    }
    async fn request(&mut self, make: impl FnOnce(u64) -> Control) -> Id {
        let id = self.next;
        self.next += 1;
        self.write(&make(id).encode(self.limit).unwrap()).await;
        Id(id)
    }
    async fn call(&mut self, make: impl FnOnce(u64) -> Control) -> Control {
        self.request(make).await;
        self.receive().await
    }
    async fn input(&self, bytes: &[u8]) -> u64 {
        let mut send = self.flow.open_data().await.unwrap();
        let stream = u64::from(send.id());
        let mut data = inputs::header(bytes).encode_framed().unwrap();
        data.extend_from_slice(bytes);
        tokio::time::timeout(HANDSHAKE, send.write_all(&data))
            .await
            .unwrap()
            .unwrap();
        send.finish().unwrap();
        stream
    }
    async fn success(&mut self) -> (Id, WorkView) {
        tokio::time::timeout(HANDSHAKE, async {
            loop {
                let response = self
                    .call(|id| {
                        Control::Work(Work::Watch {
                            request: Id(id),
                            work: key(),
                            after_revision: Number(0),
                            wait_ms: WaitMs(0),
                        })
                    })
                    .await;
                let Control::Work(Work::View { revision, work, .. }) = response else {
                    panic!("{response:?}")
                };
                if work.state == State::SUCCEEDED {
                    return (revision, *work);
                }
                assert!(!work.state.is_terminal(), "{work:?}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
    async fn result(&mut self, manifest: &Manifest) -> Vec<u8> {
        let request = self
            .request(|id| {
                Control::Result(ResultMessage::Read {
                    request: Id(id),
                    work: manifest.work.clone(),
                    attempt: manifest.attempt,
                    index: OutputIndex(0),
                    expected_sha256: manifest.outputs[0].sha256,
                })
            })
            .await;
        self.receive_result(request, manifest, OutputIndex(0)).await
    }
    async fn receive_result(
        &mut self,
        request: Id,
        manifest: &Manifest,
        index: OutputIndex,
    ) -> Vec<u8> {
        let mut recv = tokio::time::timeout(HANDSHAKE, self.connection.accept_uni())
            .await
            .unwrap()
            .unwrap();
        let mut length = [0; 4];
        tokio::time::timeout(HANDSHAKE, recv.read_exact(&mut length))
            .await
            .unwrap()
            .unwrap();
        let length = u32::from_be_bytes(length) as usize;
        assert!((1..=4096).contains(&length));
        let mut encoded = vec![0; length];
        tokio::time::timeout(HANDSHAKE, recv.read_exact(&mut encoded))
            .await
            .unwrap()
            .unwrap();
        let header = ResultHeader::decode(&encoded).unwrap();
        assert_eq!(header.request, request);
        assert_eq!(header.generation, manifest.generation);
        assert_eq!(header.work, manifest.work);
        assert_eq!(header.attempt, manifest.attempt);
        assert_eq!(header.index, index);
        assert_eq!(header.length, manifest.outputs[index.0 as usize].length);
        assert_eq!(header.sha256, manifest.outputs[index.0 as usize].sha256);
        let bytes = tokio::time::timeout(HANDSHAKE, recv.read_to_end(1 << 20))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(header.length.0, bytes.len() as u64);
        assert_eq!(header.sha256.0, <[u8; 32]>::from(Sha256::digest(&bytes)));
        bytes
    }
    async fn closed(&self, code: ErrorCode) {
        let result = tokio::time::timeout(HANDSHAKE, self.connection.closed())
            .await
            .unwrap();
        assert!(
            matches!(&result, quinn::ConnectionError::ApplicationClosed(c) if c.error_code.into_inner() == code.quic_error()),
            "{result:?}"
        );
    }
}
fn offered() -> Capabilities {
    Capabilities {
        required: vec![
            ProfileId(DURABLE_WORK.into()),
            ProfileId(RESULT_DELIVERY.into()),
        ],
        ..options().offer
    }
}
async fn declare_one(client: &mut Client) -> Digest {
    assert!(matches!(
        client.call(create).await,
        Control::Session(Session::Binding {
            generation: Id(1),
            ..
        })
    ));
    seal(client.call(|id| declare(id, vec![Id(1)], true)).await)
}
fn admitted(control: Control, stream: u64) {
    assert!(
        matches!(&control, Control::Work(Work::Admitted { request: RequestTag::Input { stream: actual }, .. }) if actual.0 == stream),
        "{control:?}"
    );
}

#[tokio::test]
async fn listener_copies_real_objects_repeats_reads_and_completes_an_exact_root_cut() {
    for bytes in [Vec::new(), vec![0x5a; 65536]] {
        let running = Running::new(options());
        let mut client = running.client(Some(0)).await;
        client.negotiate(offered()).await;
        let seal = declare_one(&mut client).await;
        let stream = client.input(&bytes).await;
        admitted(client.receive().await, stream);
        let (revision, view) = client.success().await;
        let manifest = view.manifest.as_ref().unwrap();
        assert_eq!(client.result(manifest).await, bytes);
        assert_eq!(client.result(manifest).await, bytes);
        assert_eq!(client.success().await, (revision, view.clone()));
        assert!(
            matches!(client.call(|id| Control::Result(ResultMessage::GetManifest {
            request: Id(id), work: key(), attempt: manifest.attempt,
        })).await, Control::Result(ResultMessage::ManifestResponse { manifest: actual, .. }) if actual == *manifest)
        );
        let response = client
            .call(|id| {
                Control::Scope(Scope::Checkpoint {
                    request: Id(id),
                    scope: Number(0),
                    seal,
                    wait_ms: WaitMs(1000),
                })
            })
            .await;
        let Control::Scope(Scope::CheckpointResponse { summary, .. }) = response else {
            panic!("{response:?}")
        };
        assert_eq!(summary.counts.success, Number(1));
        let response = client
            .call(|id| {
                Control::Drain(Drain::Complete {
                    request: Id(id),
                    generation: Id(1),
                    root_summary: summary.clone(),
                })
            })
            .await;
        assert!(
            matches!(response, Control::Drain(Drain::Completed { root_summary, .. }) if root_summary == summary)
        );
        client.send.finish().unwrap();
        drop(client);
        running.finish().await;
    }
}

#[tokio::test]
async fn listener_keeps_a_partial_control_frame_while_input_and_execution_progress() {
    let running = Running::new(options());
    let mut client = running.client(Some(0)).await;
    client.negotiate(offered()).await;
    declare_one(&mut client).await;
    let id = client.next;
    client.next += 1;
    let frame = next(id).encode(client.limit).unwrap();
    client.write(&frame[..3]).await;
    let stream = client.input(&vec![0x31; 65536]).await;
    admitted(client.receive().await, stream);
    client.write(&frame[3..]).await;
    assert_eq!(
        client.receive().await,
        Control::Session(Session::Sequence {
            request: Id(id),
            next_creation_sequence: Id(2)
        })
    );
    assert_eq!(client.success().await.1.state, State::SUCCEEDED);
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn listener_frame_deadline_starts_at_first_byte_and_bounds_partial_headers() {
    let mut limits = options();
    limits.control_frame_timeout = Duration::from_millis(200);
    let running = Running::new(limits);
    let mut client = running.client(Some(0)).await;
    client.negotiate(offered()).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        client.call(next).await,
        Control::Session(Session::Sequence {
            request: Id(1),
            next_creation_sequence: Id(1)
        })
    );
    client.write(&[2]).await;
    client.closed(ErrorCode::LimitExceeded).await;
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn listener_core_fallback_requires_no_identity_but_required_durable_does() {
    let running = Running::new(options());
    for principal in [None, Some(2)] {
        let mut client = running.client(principal).await;
        let selected = client.negotiate(options().offer).await;
        assert!(selected.supported.is_empty());
        refused(&client.call(next).await, 1, ErrorCode::ExtensionUnsupported);
        let detach = client
            .request(|id| Control::Drain(Drain::Detach { request: Id(id) }))
            .await;
        let late = client.request(next).await;
        client.send.finish().unwrap();
        assert_eq!(
            client.receive().await,
            Control::Drain(Drain::Detached { request: detach })
        );
        refused(&client.receive().await, late.0, ErrorCode::NotReady);
        let mut required = running.client(principal).await;
        required
            .write(
                &Control::Capabilities(offered())
                    .encode(INITIAL_CONTROL_LIMIT)
                    .unwrap(),
            )
            .await;
        required.closed(ErrorCode::Unauthorized).await;
    }
    running.finish().await;
}

#[tokio::test]
async fn listener_control_stop_and_duplicate_ids_have_named_connection_errors() {
    let running = Running::new(options());
    let mut client = running.client(Some(0)).await;
    client.negotiate(offered()).await;
    client.recv.stop(0u32.into()).unwrap();
    client.closed(ErrorCode::ControlReset).await;
    drop(client);
    let mut client = running.client(Some(0)).await;
    client.negotiate(offered()).await;
    client.call(next).await;
    client.write(&next(1).encode(client.limit).unwrap()).await;
    client.closed(ErrorCode::FrameError).await;
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn listener_reconnects_with_rotated_credentials_without_readmitting_work() {
    let running = Running::new(options());
    let mut client = running.client(Some(0)).await;
    client.negotiate(offered()).await;
    declare_one(&mut client).await;
    let bytes = vec![0x19; 32768];
    let stream = client.input(&bytes).await;
    admitted(client.receive().await, stream);
    drop(client);
    let mut client = running.client(Some(1)).await;
    client.negotiate(offered()).await;
    assert!(matches!(
        client.call(|id| attach(id, "alice", 1)).await,
        Control::Session(Session::Binding {
            generation: Id(1),
            ..
        })
    ));
    let (_, view) = client.success().await;
    assert_eq!(view.attempt, Number(1));
    assert_eq!(client.result(view.manifest.as_ref().unwrap()).await, bytes);
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn listener_shutdown_reports_a_metadata_commit_still_running_after_grace() {
    let mut limits = options();
    limits.shutdown_grace = Duration::from_millis(50);
    let mut running = Running::new(limits);
    let _release = ReleaseOnDrop(running.db.access.clone());
    running.db.access.pause.store(true, Ordering::SeqCst);
    let mut client = running.client(Some(0)).await;
    client.negotiate(offered()).await;
    client.request(create).await;
    tokio::time::timeout(HANDSHAKE, running.db.access.entered.notified())
        .await
        .unwrap();
    // The one metadata slot remains occupied by creation, not by its cancelled
    // waiter. A second request is refused without blocking the control reader.
    refused(&client.call(next).await, 2, ErrorCode::LimitExceeded);
    let report = running.shutdown().await;
    assert!(!report.drained());
    assert!(!report.metadata_idle);
    running.db.access.release();
    tokio::time::timeout(HANDSHAKE, async {
        while running
            .db
            .store
            .next_creation(&IdentityLabel("alice".into()))
            .unwrap()
            != Id(2)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    drop(client);
}

#[tokio::test]
async fn listener_long_watch_does_not_block_control_and_detach_drains_queued_refusals() {
    let mut limits = options();
    limits.control_frame_timeout = Duration::from_millis(200);
    let mut db = Database::new();
    // Both initial metadata reads may overlap. Their advertised pending ceiling
    // is not a reservation of the deployment's separate metadata capacity.
    db.authority = Authority::new(db.store.clone(), db.payloads.clone(), 2).unwrap();
    let running = Running::with(db, Fixture::new(), limits);
    let mut client = running.client(Some(0)).await;
    client.negotiate(offered()).await;
    declare_one(&mut client).await;
    let watch = client
        .request(|id| {
            Control::Work(Work::Watch {
                request: Id(id),
                work: key(),
                after_revision: Number(1),
                wait_ms: WaitMs(600),
            })
        })
        .await;
    let independent = client.request(next).await;
    assert_eq!(
        client.receive().await,
        Control::Session(Session::Sequence {
            request: independent,
            next_creation_sequence: Id(2),
        })
    );
    let detach = client
        .request(|id| Control::Drain(Drain::Detach { request: Id(id) }))
        .await;
    let mut late = Vec::new();
    for _ in 0..8 {
        late.push(client.request(next).await);
    }
    client.send.finish().unwrap();
    let mut saw_watch = false;
    let mut saw_detach = false;
    for _ in 0..10 {
        match client.receive().await {
            Control::Refusal(Refusal {
                request: RequestTag::Control { request },
                code: ErrorCode::NotReady,
                ..
            }) => {
                let index = late
                    .iter()
                    .position(|id| *id == request)
                    .expect("one correlated late refusal");
                late.remove(index);
            }
            Control::Work(Work::View {
                request,
                revision: Id(1),
                work,
            }) => {
                assert_eq!(request, watch);
                assert_eq!(work.state, State::DECLARED);
                assert!(!saw_watch);
                saw_watch = true;
            }
            Control::Drain(Drain::Detached { request }) => {
                assert_eq!(request, detach);
                assert!(saw_watch);
                assert!(!saw_detach);
                saw_detach = true;
            }
            response => panic!("{response:?}"),
        }
    }
    assert!(saw_watch && saw_detach && late.is_empty());
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
async fn listener_stalled_result_keeps_control_live_and_refuses_a_premature_completed_cut() {
    let running = Running::new(options());
    let mut client = running.client(Some(0)).await;
    client.negotiate(offered()).await;
    let seal = declare_one(&mut client).await;
    let bytes = vec![0x51; 65536];
    let stream = client.input(&bytes).await;
    admitted(client.receive().await, stream);
    let (_, view) = client.success().await;
    let manifest = view.manifest.unwrap();
    let Control::Scope(Scope::CheckpointResponse { summary, .. }) = client
        .call(|id| {
            Control::Scope(Scope::Checkpoint {
                request: Id(id),
                scope: Number(0),
                seal,
                wait_ms: WaitMs(1000),
            })
        })
        .await
    else {
        panic!("checkpoint expected")
    };
    let request = client
        .request(|id| {
            Control::Result(ResultMessage::Read {
                request: Id(id),
                work: key(),
                attempt: manifest.attempt,
                index: OutputIndex(0),
                expected_sha256: manifest.outputs[0].sha256,
            })
        })
        .await;
    let mut result = tokio::time::timeout(HANDSHAKE, client.connection.accept_uni())
        .await
        .unwrap()
        .unwrap();
    // Consuming no bytes keeps this 64 KiB result beyond the granted 8 KiB
    // window; an opened stream is not a completed output transfer.
    let completion = client.next;
    refused(
        &client
            .call(|id| {
                Control::Drain(Drain::Complete {
                    request: Id(id),
                    generation: Id(1),
                    root_summary: summary,
                })
            })
            .await,
        completion,
        ErrorCode::NotReady,
    );
    assert!(matches!(
        client.call(next).await,
        Control::Session(Session::Sequence {
            next_creation_sequence: Id(2),
            ..
        })
    ));
    let encoded = tokio::time::timeout(HANDSHAKE, result.read_to_end(1 << 20))
        .await
        .unwrap()
        .unwrap();
    let length = u32::from_be_bytes(encoded[..4].try_into().unwrap()) as usize;
    let header = ResultHeader::decode(&encoded[4..4 + length]).unwrap();
    assert_eq!(header.request, request);
    assert_eq!(header.sha256, manifest.outputs[0].sha256);
    assert_eq!(&encoded[4 + length..], bytes);
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn listener_bad_input_refusal_preserves_the_declared_obligation_and_control_stream() {
    let running = Running::new(options());
    let mut client = running.client(Some(0)).await;
    client.negotiate(offered()).await;
    declare_one(&mut client).await;
    for (encoded, expected) in [
        (vec![0, 0, 0, 0], ErrorCode::FrameError),
        (
            {
                let mut frame = inputs::header(b"abc").encode_framed().unwrap();
                frame.extend_from_slice(b"bad");
                frame
            },
            ErrorCode::IntegrityError,
        ),
    ] {
        let mut input = client.flow.open_data().await.unwrap();
        let stream = StreamId(u64::from(input.id()));
        let _ = input.write_all(&encoded).await;
        let _ = input.finish();
        let response = client.receive().await;
        assert!(
            matches!(&response, Control::Refusal(Refusal { request: RequestTag::Input { stream: actual }, code, .. }) if *actual == stream && *code == expected),
            "{response:?}"
        );
        assert!(matches!(client.call(|id| Control::Work(Work::Watch {
            request: Id(id), work: key(), after_revision: Number(0), wait_ms: WaitMs(0),
        })).await, Control::Work(Work::View { work, .. }) if work.state == State::DECLARED));
    }
    let stream = client.input(b"abc").await;
    admitted(client.receive().await, stream);
    assert_eq!(client.success().await.1.attempt, Number(1));
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn listener_malformed_controls_close_by_name_without_poisoning_other_connections() {
    let running = Running::new(options());
    let cases = [
        (vec![0, 0, 0, 0, 0], ErrorCode::FrameError),
        (vec![0xc0, 0, 0, 0, 0], ErrorCode::ExtensionUnsupported),
        (vec![2, 0xff, 0xff, 0xff, 0xff], ErrorCode::FrameError),
        (vec![6, 0, 0, 0, 4, 0x82, 2, 0x18, 1], ErrorCode::FrameError),
        (
            Control::Drain(Drain::Detached { request: Id(1) })
                .encode(4096)
                .unwrap(),
            ErrorCode::FrameError,
        ),
        (next(2).encode(4096).unwrap(), ErrorCode::FrameError),
    ];
    for (frame, code) in cases {
        let mut client = running.client(Some(0)).await;
        client.negotiate(offered()).await;
        client.write(&frame).await;
        client.closed(code).await;
    }
    let mut client = running.client(Some(0)).await;
    client.negotiate(offered()).await;
    assert_eq!(
        client.call(next).await,
        Control::Session(Session::Sequence {
            request: Id(1),
            next_creation_sequence: Id(1)
        })
    );
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn listener_reopens_real_roots_and_returns_the_same_creation_work_and_output() {
    let mut running = Running::new(options());
    let mut client = running.client(Some(0)).await;
    client.negotiate(offered()).await;
    declare_one(&mut client).await;
    let stream = client.input(b"retained output").await;
    admitted(client.receive().await, stream);
    let before = client.success().await;
    drop(client);
    let report = running.shutdown().await;
    assert!(report.drained(), "{report:?}");
    let db = std::mem::replace(&mut running.db, Database::new());
    drop(running);
    let Database {
        _directory,
        store,
        payloads,
        access,
        authority,
        clock,
    } = db;
    drop(authority);
    drop(store);
    drop(payloads);
    // No old authority/payload handle survives this exclusive on-disk reopen.
    // This is listener/storage restart, not the still-required process-kill driver.
    let store = AuthorityStore::open(
        &_directory.path().join("authority.sqlite"),
        IdentityLabel("issuer-a".into()),
        storage_policy(),
        PhysicalLimits::default(),
        clock.clone(),
        access.clone(),
    )
    .unwrap();
    let payloads = PayloadStore::open(
        &_directory.path().join("objects"),
        store.payload_identity().unwrap(),
        object_policy(),
    )
    .unwrap();
    let authority = Authority::new(store.clone(), payloads.clone(), 1).unwrap();
    let db = Database {
        _directory,
        store,
        payloads,
        access,
        authority,
        clock,
    };
    let running = Running::with(db, Fixture::new(), options());
    let mut client = running.client(Some(0)).await;
    client.negotiate(offered()).await;
    assert!(matches!(
        client.call(create).await,
        Control::Session(Session::Binding {
            generation: Id(1),
            creation_sequence: Id(1),
            ..
        })
    ));
    assert_eq!(client.success().await, before);
    assert_eq!(
        client.result(before.1.manifest.as_ref().unwrap()).await,
        b"retained output"
    );
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn listener_maximum_work_wait_outlasts_default_peer_idle_without_faking_progress() {
    let running = Running::new(options());
    let mut client = running.client(Some(0)).await;
    client.negotiate(offered()).await;
    declare_one(&mut client).await;
    let request = client
        .request(|id| {
            Control::Work(Work::Watch {
                request: Id(id),
                work: key(),
                after_revision: Number(1),
                wait_ms: WaitMs(30000),
            })
        })
        .await;
    let started = std::time::Instant::now();
    let response = client.receive_within(Duration::from_secs(35)).await;
    assert!(started.elapsed() >= Duration::from_secs(29));
    assert!(
        matches!(response, Control::Work(Work::View { request: actual, revision: Id(1), work })
        if actual == request && work.state == State::DECLARED)
    );
    // Transport keep-alives did not create a new application revision/outcome.
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn listener_rotated_certificates_share_owner_quota_without_exhausting_anonymous_core() {
    let mut limits = options();
    limits.connections_per_principal = 1;
    let running = Running::new(limits);
    let mut first = running.client(Some(0)).await;
    first.negotiate(offered()).await;
    let (_endpoint, second) = running.raw(Some(1)).await;
    let refusal = match second {
        Ok(connection) => tokio::time::timeout(HANDSHAKE, connection.closed())
            .await
            .unwrap(),
        Err(error) => error,
    };
    assert!(
        matches!(refusal, quinn::ConnectionError::ApplicationClosed(c)
        if c.error_code.into_inner() == ErrorCode::LimitExceeded.quic_error())
    );
    let mut anonymous = running.client(None).await;
    assert!(
        anonymous
            .negotiate(options().offer)
            .await
            .supported
            .is_empty()
    );
    assert!(matches!(
        anonymous
            .call(|id| Control::Drain(Drain::Detach { request: Id(id) }))
            .await,
        Control::Drain(Drain::Detached { .. })
    ));
    drop(anonymous);
    drop(first);
    running.finish().await;
}

#[tokio::test]
async fn listener_nonreading_control_peer_is_bounded_and_other_connections_progress() {
    let mut limits = options();
    limits.control_frame_timeout = Duration::from_millis(200);
    let running = Running::new(limits);
    let mut blocked = running.client(Some(0)).await;
    blocked.negotiate(offered()).await;
    let mut bytes = Vec::new();
    for id in 1..=10000 {
        bytes.extend_from_slice(&next(id).encode(blocked.limit).unwrap());
    }
    let flood = tokio::spawn(async move {
        // Intentionally never consume a control response. The sender may also
        // block once the server's bounded reader/writer queues stop consuming.
        let _ = tokio::time::timeout(HANDSHAKE, blocked.send.write_all(&bytes)).await;
        blocked.closed(ErrorCode::LimitExceeded).await;
    });
    let mut healthy = running.client(None).await;
    healthy.negotiate(options().offer).await;
    assert!(matches!(
        healthy
            .call(|id| Control::Drain(Drain::Detach { request: Id(id) }))
            .await,
        Control::Drain(Drain::Detached { .. })
    ));
    tokio::time::timeout(HANDSHAKE, flood)
        .await
        .unwrap()
        .unwrap();
    drop(healthy);
    running.finish().await;
}
