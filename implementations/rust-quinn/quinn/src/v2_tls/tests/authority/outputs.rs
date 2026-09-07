//! Real result objects over authenticated QUIC; control requests remain local
//! adapter calls, not a claim of public durable-profile interoperability.
use super::*;
use crate::v2_authority::output::{Delivery, Options, Outputs, Status};

fn fixture() -> Fixture {
    let mut tls = Fixture::new();
    let mut transport = quinn::TransportConfig::default();
    transport.send_window(8192);
    tls.security.set_transport_config(Arc::new(transport));
    tls
}
async fn connect(
    db: &Database,
    tls: &Fixture,
    caps: Capabilities,
    principal: usize,
    streams: u32,
) -> (Connection, Exchange) {
    let mut transport = quinn::TransportConfig::default();
    transport
        .stream_receive_window(4096u32.into())
        .receive_window(16384u32.into())
        .max_concurrent_uni_streams(streams.into());
    let mut config = tls.config(Some(principal));
    config.transport_config(Arc::new(transport));
    let mut wire = tls.connect(config, "localhost").await;
    let connection = db.adapter(tls, &mut wire, caps).unwrap();
    (connection, wire)
}
async fn publish(
    db: &Database,
    connection: &Connection,
    wire: &Exchange,
    bytes: &[u8],
) -> (SessionIdentity, Manifest) {
    let (identity, executor) = admit(db, connection, wire, bytes).await;
    let manifest = executor.run(&identity, &key()).unwrap().manifest.unwrap();
    (identity, manifest)
}
async fn admit(
    db: &Database,
    connection: &Connection,
    wire: &Exchange,
    bytes: &[u8],
) -> (SessionIdentity, Executor) {
    control(connection, create(1)).await;
    control(connection, declare(2, vec![Id(1)], true)).await;
    let apps = inputs::applications();
    let input = crate::v2_authority::input::Inputs::new(
        db.authority.clone(),
        apps.clone(),
        Default::default(),
    )
    .unwrap();
    let identity = connection.input().unwrap().binding().identity.clone();
    let mut header = inputs::header(bytes);
    header.generation = identity.generation;
    let mut object = header.encode_framed().unwrap();
    object.extend_from_slice(bytes);
    let (reply, _) =
        inputs::transfer(&input, connection, wire.client.as_ref().unwrap(), object).await;
    assert!(matches!(
        reply.control(),
        Control::Work(Work::Admitted { .. })
    ));
    drop(reply);
    let executor = Executor::new(
        db.store.clone(),
        db.payloads.clone(),
        apps,
        ResultEndpoint::new("localhost:7443".into()).unwrap(),
        caps(),
        pipestream_core::v2::Duration(100),
    )
    .unwrap();
    (identity, executor)
}
fn options() -> Options {
    Options {
        active: 2,
        active_per_owner: 2,
        file_workers: 1,
        stream_open_timeout: Duration::from_millis(200),
    }
}
fn read(request: u64, manifest: &Manifest) -> Control {
    Control::Result(ResultMessage::Read {
        request: Id(request),
        work: manifest.work.clone(),
        attempt: manifest.attempt,
        index: OutputIndex(0),
        expected_sha256: manifest.outputs[0].sha256,
    })
}
async fn header(recv: &mut quinn::RecvStream) -> ResultHeader {
    let mut prefix = [0; 4];
    recv.read_exact(&mut prefix).await.unwrap();
    let size = u32::from_be_bytes(prefix) as usize;
    assert!((1..=4096).contains(&size));
    let mut bytes = vec![0; size];
    recv.read_exact(&mut bytes).await.unwrap();
    ResultHeader::decode(&bytes).unwrap()
}
async fn received(client: &quinn::Connection) -> (ResultHeader, Vec<u8>) {
    let mut recv = client.accept_uni().await.unwrap();
    assert_eq!(u64::from(recv.id()) % 4, 3);
    let header = header(&mut recv).await;
    let bytes = recv.read_to_end(1 << 20).await.unwrap();
    assert_eq!(header.length.0, bytes.len() as u64);
    assert_eq!(header.sha256.0, <[u8; 32]>::from(Sha256::digest(&bytes)));
    (header, bytes)
}
async fn result(task: tokio::task::JoinHandle<Delivery>) -> Delivery {
    tokio::time::timeout(HANDSHAKE, task)
        .await
        .unwrap()
        .unwrap()
}
async fn detach(connection: &Connection, request: u64) {
    assert!(matches!(
        control(
            connection,
            Control::Drain(Drain::Detach {
                request: Id(request)
            })
        )
        .await,
        Control::Drain(Drain::Detached { .. })
    ));
}

#[tokio::test]
async fn results_cross_small_quic_windows_and_reopen_without_new_execution() {
    for bytes in [Vec::new(), vec![0x5a; 65536]] {
        let db = Database::new();
        let tls = fixture();
        let (connection, wire) = connect(&db, &tls, caps(), 0, 2).await;
        let (identity, manifest) = publish(&db, &connection, &wire, &bytes).await;
        let before = db.store.work_view(&identity, &key(), Number(0)).unwrap();
        let outputs = Outputs::new(db.authority.clone(), options()).unwrap();
        for request in [3, 4] {
            let task = tokio::spawn(
                outputs
                    .request(pending(&connection, read(request, &manifest)))
                    .unwrap()
                    .run(),
            );
            let (header, actual) =
                tokio::time::timeout(HANDSHAKE, received(wire.client.as_ref().unwrap()))
                    .await
                    .unwrap();
            assert_eq!(header.request, Id(request));
            assert_eq!(header.generation, identity.generation);
            assert_eq!(header.work, key());
            assert_eq!(header.attempt, manifest.attempt);
            assert_eq!(header.index, OutputIndex(0));
            assert_eq!(actual, bytes);
            assert!(matches!(result(task).await.status(), Status::Sent));
            assert_eq!(
                db.store.work_view(&identity, &key(), Number(0)).unwrap(),
                before
            );
        }
        detach(&connection, 5).await;
    }
}

#[tokio::test]
async fn stored_result_and_control_share_one_reserved_flow_owner() {
    use crate::v2_flow::{Connection as Flow, Limits};
    let limits = Limits {
        data_send: 8192,
        control_send: 4096,
        receive_stream: 4096,
        data_streams: 1,
    };
    let db = Database::new();
    let mut tls = Fixture::new();
    let mut transport = quinn::TransportConfig::default();
    limits
        .configure(&mut transport, quinn::Side::Server)
        .unwrap();
    tls.security.set_transport_config(Arc::new(transport));
    let mut transport = quinn::TransportConfig::default();
    limits
        .configure(&mut transport, quinn::Side::Client)
        .unwrap();
    let mut config = tls.config(Some(0));
    config.transport_config(Arc::new(transport));
    let mut wire = tls.connect(config, "localhost").await;
    let flow = Flow::new(wire.server.as_ref().unwrap().connection().clone(), limits).unwrap();
    let client = wire.client.as_ref().unwrap();
    let (mut client_send, mut client_recv) = client.open_bi().await.unwrap();
    client_send.write_all(b"x").await.unwrap();
    let (mut server_send, mut server_recv) = flow.accept_control().await.unwrap();
    server_recv.read_exact(&mut [0; 1]).await.unwrap();
    let connection = db
        .adapter_with_flow(&tls, &mut wire, caps(), flow.clone())
        .unwrap();
    let bytes = vec![0x5a; 65536];
    let (identity, manifest) = publish(&db, &connection, &wire, &bytes).await;
    let before = db.store.work_view(&identity, &key(), Number(0)).unwrap();
    let outputs = Outputs::new(db.authority.clone(), options()).unwrap();
    let task = tokio::spawn(
        outputs
            .request(pending(&connection, read(3, &manifest)))
            .unwrap()
            .run(),
    );
    let mut recv = wire.client.as_ref().unwrap().accept_uni().await.unwrap();
    // Do not consume even the object header. Wait for an actual Pending data
    // write in the shared owner, rather than assuming a sleep proves blockage.
    tokio::time::timeout(HANDSHAKE, async {
        while flow.blocked_data_polls() == 0 {
            assert!(!task.is_finished(), "result must remain blocked");
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut response = response(&connection, next(4)).await;
    let ResponseBody::Control(message) = response.body() else {
        panic!("sequence must have a control response");
    };
    let expected = message.clone();
    let frame = message.encode(8192).unwrap();
    tokio::time::timeout(HANDSHAKE, server_send.write_all(&frame))
        .await
        .unwrap()
        .unwrap();
    let mut actual = vec![0; frame.len()];
    client_recv.read_exact(&mut actual).await.unwrap();
    assert_eq!(Control::decode(&actual, 8192).unwrap(), expected);
    assert!(
        !task.is_finished(),
        "control must not await the result body"
    );
    drop(response);
    let header = header(&mut recv).await;
    assert_eq!(header.request, Id(3));
    assert_eq!(header.sha256, manifest.outputs[0].sha256);
    assert_eq!(recv.read_to_end(65536).await.unwrap(), bytes);
    assert!(matches!(result(task).await.status(), Status::Sent));
    assert_eq!(
        db.store.work_view(&identity, &key(), Number(0)).unwrap(),
        before
    );
    detach(&connection, 5).await;
}

#[tokio::test]
async fn authority_refuses_a_flow_owner_from_another_tls_connection() {
    let db = Database::new();
    let tls = Fixture::new();
    let first = tls.connect(tls.config(Some(0)), "localhost").await;
    let mut second = tls.connect(tls.config(Some(0)), "localhost").await;
    let wrong = crate::v2_flow::Connection::new(
        first.server.as_ref().unwrap().connection().clone(),
        Default::default(),
    )
    .unwrap();
    assert!(matches!(
        db.adapter_with_flow(&tls, &mut second, caps(), wrong),
        Err(Error {
            code: ErrorCode::InternalError,
            ..
        })
    ));
}

#[tokio::test]
async fn stalled_result_resets_after_header_without_a_second_control_response() {
    let db = Database::new();
    let tls = fixture();
    let (connection, wire) = connect(&db, &tls, caps(), 0, 1).await;
    let (identity, manifest) = publish(&db, &connection, &wire, &vec![1; 65536]).await;
    let before = db.store.work_view(&identity, &key(), Number(0)).unwrap();
    let outputs = Outputs::new(db.authority.clone(), options()).unwrap();
    let task = tokio::spawn(
        outputs
            .request(pending(&connection, read(3, &manifest)))
            .unwrap()
            .run(),
    );
    let mut recv = wire.client.as_ref().unwrap().accept_uni().await.unwrap();
    assert_eq!(header(&mut recv).await.request, Id(3));
    assert!(matches!(
        control(&connection, next(4)).await,
        Control::Session(Session::Sequence { .. })
    ));
    let delivery = result(task).await;
    assert!(
        matches!(delivery.status(), Status::Aborted(ErrorCode::LimitExceeded)),
        "{:?}",
        delivery.status()
    );
    assert!(matches!(recv.read_to_end(1 << 20).await,
        Err(quinn::ReadToEndError::Read(quinn::ReadError::Reset(code))) if u64::from(code) == ErrorCode::LimitExceeded.quic_error()));
    drop(delivery);
    detach(&connection, 5).await;
    assert_eq!(
        db.store.work_view(&identity, &key(), Number(0)).unwrap(),
        before
    );
}

#[tokio::test]
async fn stopped_result_only_aborts_delivery_and_the_same_object_can_be_read_again() {
    let db = Database::new();
    let tls = fixture();
    let (connection, wire) = connect(&db, &tls, caps(), 0, 2).await;
    let bytes = vec![2; 65536];
    let (identity, manifest) = publish(&db, &connection, &wire, &bytes).await;
    let before = db.store.work_view(&identity, &key(), Number(0)).unwrap();
    let outputs = Outputs::new(db.authority.clone(), options()).unwrap();
    let task = tokio::spawn(
        outputs
            .request(pending(&connection, read(3, &manifest)))
            .unwrap()
            .run(),
    );
    let mut recv = wire.client.as_ref().unwrap().accept_uni().await.unwrap();
    header(&mut recv).await;
    recv.stop(quinn::VarInt::from_u64(ErrorCode::Cancelled.quic_error()).unwrap())
        .unwrap();
    assert!(matches!(
        result(task).await.status(),
        Status::Aborted(ErrorCode::Cancelled)
    ));
    let task = tokio::spawn(
        outputs
            .request(pending(&connection, read(4, &manifest)))
            .unwrap()
            .run(),
    );
    assert_eq!(received(wire.client.as_ref().unwrap()).await.1, bytes);
    assert!(matches!(result(task).await.status(), Status::Sent));
    detach(&connection, 5).await;
    assert_eq!(
        db.store.work_view(&identity, &key(), Number(0)).unwrap(),
        before
    );
}

#[tokio::test]
async fn pending_stream_creation_refuses_by_control_and_releases_the_read_pin() {
    let db = Database::new();
    let tls = fixture();
    let (connection, wire) = connect(&db, &tls, caps(), 0, 0).await;
    let (identity, manifest) = publish(&db, &connection, &wire, b"unopened").await;
    let before = db.store.work_view(&identity, &key(), Number(0)).unwrap();
    let outputs = Outputs::new(db.authority.clone(), options()).unwrap();
    let delivery = outputs
        .request(pending(&connection, read(3, &manifest)))
        .unwrap()
        .run()
        .await;
    let Status::Refused(control) = delivery.status() else {
        panic!("{:?}", delivery.status());
    };
    refused(control, 3, ErrorCode::LimitExceeded);
    // The response's connection slot stays held until the refusal is written.
    drop(delivery);
    detach(&connection, 4).await;
    assert_eq!(
        db.store.work_view(&identity, &key(), Number(0)).unwrap(),
        before
    );
}

#[tokio::test]
async fn expired_credential_stops_a_blocked_result_before_more_transport_scheduling() {
    let db = Database::new();
    let tls = fixture();
    let (connection, wire) = connect(&db, &tls, caps(), 0, 1).await;
    let (identity, manifest) = publish(&db, &connection, &wire, &vec![3; 65536]).await;
    let before = db.store.work_view(&identity, &key(), Number(0)).unwrap();
    let outputs = Outputs::new(db.authority.clone(), options()).unwrap();
    let task = tokio::spawn(
        outputs
            .request(pending(&connection, read(3, &manifest)))
            .unwrap()
            .run(),
    );
    let mut recv = wire.client.as_ref().unwrap().accept_uni().await.unwrap();
    header(&mut recv).await;
    tls.clock.0.store(at(2032, 1, 1), Ordering::SeqCst);
    let delivery = result(task).await;
    assert!(
        matches!(delivery.status(), Status::Aborted(ErrorCode::Unauthorized)),
        "{:?}",
        delivery.status()
    );
    assert!(matches!(recv.read_to_end(1 << 20).await,
        Err(quinn::ReadToEndError::Read(quinn::ReadError::Reset(code))) if u64::from(code) == ErrorCode::Unauthorized.quic_error()));
    drop(delivery);
    immediate(&connection, read(4, &manifest), 4, ErrorCode::Unauthorized);
    assert_eq!(
        db.store.work_view(&identity, &key(), Number(0)).unwrap(),
        before
    );
}

#[tokio::test]
async fn cancelled_result_preflight_keeps_owner_quota_and_detach_pending_until_cleanup() {
    let db = Database::new();
    let tls = fixture();
    let (connection, wire) = connect(&db, &tls, caps(), 0, 1).await;
    let (identity, manifest) = publish(&db, &connection, &wire, b"retained").await;
    let (rotated, rotated_wire) = connect(&db, &tls, caps(), 1, 1).await;
    control(&rotated, attach(1, "alice", 1)).await;
    let before = db.store.work_view(&identity, &key(), Number(0)).unwrap();
    let mut limits = options();
    limits.active_per_owner = 1;
    let outputs = Outputs::new(db.authority.clone(), limits).unwrap();
    db.access.pause_read.store(true, Ordering::SeqCst);
    let _release_on_failure = ReleaseOnDrop(db.access.clone());
    let running = tokio::spawn(
        outputs
            .request(pending(&connection, read(3, &manifest)))
            .unwrap()
            .run(),
    );
    tokio::time::timeout(HANDSHAKE, db.access.entered.notified())
        .await
        .unwrap();
    running.abort();
    assert!(matches!(running.await, Err(error) if error.is_cancelled()));
    assert!(matches!(
        control(&rotated, next(2)).await,
        Control::Session(Session::Sequence { .. })
    ));
    let delivery = outputs
        .request(pending(&rotated, read(3, &manifest)))
        .unwrap()
        .run()
        .await;
    let Status::Refused(control) = delivery.status() else {
        panic!("{:?}", delivery.status());
    };
    refused(control, 3, ErrorCode::LimitExceeded);
    drop(delivery);
    let mut draining = tokio::spawn(
        pending(
            &connection,
            Control::Drain(Drain::Detach { request: Id(4) }),
        )
        .run(),
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(40), &mut draining)
            .await
            .is_err()
    );
    db.access.release();
    assert!(matches!(
        tokio::time::timeout(HANDSHAKE, draining)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .body(),
        ResponseBody::Control(Control::Drain(Drain::Detached { .. }))
    ));
    let task = tokio::spawn(
        outputs
            .request(pending(&rotated, read(4, &manifest)))
            .unwrap()
            .run(),
    );
    assert_eq!(
        received(rotated_wire.client.as_ref().unwrap()).await.1,
        b"retained"
    );
    assert!(matches!(result(task).await.status(), Status::Sent));
    detach(&rotated, 5).await;
    assert_eq!(
        db.store.work_view(&identity, &key(), Number(0)).unwrap(),
        before
    );
}

#[tokio::test]
async fn result_stream_ceiling_counts_pending_creation_and_unsent_refusal() {
    let db = Database::new();
    let tls = fixture();
    let mut selected = caps();
    selected.stream_limit = ConcurrencyLimit(1);
    let (connection, wire) = connect(&db, &tls, selected, 0, 0).await;
    let (_, manifest) = publish(&db, &connection, &wire, b"queued").await;
    let outputs = Outputs::new(db.authority.clone(), options()).unwrap();
    let first = outputs
        .request(pending(&connection, read(3, &manifest)))
        .unwrap();
    immediate(&connection, read(4, &manifest), 4, ErrorCode::LimitExceeded);
    let delivery = first.run().await;
    assert!(matches!(delivery.status(), Status::Refused(_)));
    immediate(&connection, read(5, &manifest), 5, ErrorCode::LimitExceeded);
    drop(delivery);
    // Deferred read cleanup still owns a ticket after the refusal is dropped.
    // Capacity returns only after that cleanup, not synchronously with delivery.
    let request = tokio::time::timeout(HANDSHAKE, async {
        let mut request = 6;
        loop {
            match connection.submit(read(request, &manifest)).unwrap() {
                Submission::Pending(next) => {
                    drop(next);
                    break request;
                }
                Submission::Refused(control) => {
                    refused(&control, request, ErrorCode::LimitExceeded)
                }
            }
            request += 1;
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    detach(&connection, request + 1).await;
}

#[tokio::test]
async fn wrong_result_commitment_and_invalid_output_configuration_never_start_a_stream() {
    let db = Database::new();
    for limits in [
        Options {
            active: 0,
            ..options()
        },
        Options {
            active: 129,
            ..options()
        },
        Options {
            active_per_owner: 0,
            ..options()
        },
        Options {
            active_per_owner: 3,
            ..options()
        },
        Options {
            file_workers: 0,
            ..options()
        },
        Options {
            file_workers: 3,
            ..options()
        },
        Options {
            active: 128,
            file_workers: 33,
            ..options()
        },
        Options {
            stream_open_timeout: Duration::ZERO,
            ..options()
        },
        Options {
            stream_open_timeout: Duration::from_secs(31),
            ..options()
        },
    ] {
        assert!(matches!(
            Outputs::new(db.authority.clone(), limits),
            Err(Error {
                code: ErrorCode::LimitExceeded,
                ..
            })
        ));
    }
    let tls = fixture();
    let (connection, wire) = connect(&db, &tls, caps(), 0, 1).await;
    let (_, manifest) = publish(&db, &connection, &wire, b"committed").await;
    let outputs = Outputs::new(db.authority.clone(), options()).unwrap();
    assert!(matches!(
        outputs.request(pending(&connection, next(3))),
        Err(Error {
            code: ErrorCode::FrameError,
            ..
        })
    ));
    let other = Outputs::new(
        Authority::new(db.store.clone(), db.payloads.clone(), 1).unwrap(),
        options(),
    )
    .unwrap();
    assert!(matches!(
        other.request(pending(&connection, read(4, &manifest))),
        Err(Error {
            code: ErrorCode::InternalError,
            ..
        })
    ));
    let mut wrong = manifest.clone();
    wrong.outputs[0].sha256 = Digest([9; 32]);
    let delivery = outputs
        .request(pending(&connection, read(5, &wrong)))
        .unwrap()
        .run()
        .await;
    let Status::Refused(control) = delivery.status() else {
        panic!("{:?}", delivery.status());
    };
    refused(control, 5, ErrorCode::IntegrityError);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(40),
            wire.client.as_ref().unwrap().accept_uni()
        )
        .await
        .is_err()
    );
    drop(delivery);
    detach(&connection, 6).await;
}

#[tokio::test]
async fn corrupted_retained_output_aborts_without_fin_or_replacing_the_manifest() {
    use std::io::{Seek, SeekFrom, Write};
    let db = Database::new();
    let tls = fixture();
    let (connection, wire) = connect(&db, &tls, caps(), 0, 1).await;
    let (identity, executor) = admit(&db, &connection, &wire, &vec![4; 65536]).await;
    let objects = || {
        std::fs::read_dir(db._directory.path().join("objects"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("object-")
            })
            .collect::<std::collections::BTreeSet<_>>()
    };
    let inputs = objects();
    let manifest = executor.run(&identity, &key()).unwrap().manifest.unwrap();
    let paths: Vec<_> = objects().difference(&inputs).cloned().collect();
    assert_eq!(
        paths.len(),
        1,
        "the actual copy callback produced exactly one output"
    );
    let before = db.store.work_view(&identity, &key(), Number(0)).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&paths[0])
        .unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(&[5]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let outputs = Outputs::new(db.authority.clone(), options()).unwrap();
    let task = tokio::spawn(
        outputs
            .request(pending(&connection, read(3, &manifest)))
            .unwrap()
            .run(),
    );
    let mut recv = wire.client.as_ref().unwrap().accept_uni().await.unwrap();
    assert_eq!(header(&mut recv).await.sha256, manifest.outputs[0].sha256);
    assert!(matches!(recv.read_to_end(1 << 20).await,
        Err(quinn::ReadToEndError::Read(quinn::ReadError::Reset(code))) if u64::from(code) == ErrorCode::OutputUnavailable.quic_error()));
    assert!(matches!(
        result(task).await.status(),
        Status::Aborted(ErrorCode::OutputUnavailable)
    ));
    detach(&connection, 4).await;
    assert_eq!(
        db.store.work_view(&identity, &key(), Number(0)).unwrap(),
        before
    );
}

#[tokio::test]
async fn global_result_quota_spans_distinct_authenticated_owners() {
    let db = Database::new();
    let mut tls = fixture();
    let mut principals = tls.policy.principals.clone();
    principals.insert(tls.clients[2].fingerprint(), IdentityLabel("bob".into()));
    tls.policy = Arc::new(
        ClientAuthentication::new(
            IdentityLabel("issuer-a".into()),
            roots(&tls.issuer),
            principals,
            tls.clock.clone(),
        )
        .unwrap(),
    );
    tls.security = ServerSecurity::new(
        vec![tls.certificate.der.clone()],
        tls.certificate.key.clone_key(),
        Some(tls.policy.clone()),
    )
    .unwrap();
    let (alice, alice_wire) = connect(&db, &tls, caps(), 0, 1).await;
    let (_, alice_manifest) = publish(&db, &alice, &alice_wire, b"alice").await;
    let (bob, bob_wire) = connect(&db, &tls, caps(), 2, 1).await;
    let (_, bob_manifest) = publish(&db, &bob, &bob_wire, b"bob").await;
    let outputs = Outputs::new(
        db.authority.clone(),
        Options {
            active: 1,
            active_per_owner: 1,
            ..options()
        },
    )
    .unwrap();
    let held = outputs
        .request(pending(&alice, read(3, &alice_manifest)))
        .unwrap();
    let denied = outputs
        .request(pending(&bob, read(3, &bob_manifest)))
        .unwrap()
        .run()
        .await;
    let Status::Refused(control) = denied.status() else {
        panic!("{:?}", denied.status());
    };
    refused(control, 3, ErrorCode::LimitExceeded);
    drop(denied);
    drop(held);
    let task = tokio::spawn(
        outputs
            .request(pending(&bob, read(4, &bob_manifest)))
            .unwrap()
            .run(),
    );
    assert_eq!(received(bob_wire.client.as_ref().unwrap()).await.1, b"bob");
    assert!(matches!(result(task).await.status(), Status::Sent));
    detach(&alice, 4).await;
    detach(&bob, 5).await;
}
