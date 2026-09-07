//! Real QUIC input objects, with local dispatcher control calls. No partially
//! implemented profile is advertised by this test harness or public listener.
use super::*;
use crate::v2_authority::input::{Inputs, Options, Reply};
use std::sync::atomic::AtomicUsize;

pub(super) fn applications() -> Arc<Applications> {
    let mut apps = Applications::default();
    apps.register(
        ApplicationLabel("copy/v1".into()),
        vec![Mode(0)],
        RestartSafety::Pure,
        Arc::new(CopyApplication),
    )
    .unwrap();
    Arc::new(apps)
}
pub(super) fn header(bytes: &[u8]) -> InputHeader {
    InputHeader {
        kind: Literal,
        generation: Id(1),
        operation: OperationId([2; 16]),
        parameters: AdmitParameters {
            work: key(),
            input: Input {
                length: Number(bytes.len() as u64),
                sha256: Digest(Sha256::digest(bytes).into()),
                content_type: ApplicationLabel("application/octet-stream".into()),
            },
            application: ApplicationLabel("copy/v1".into()),
            mode: Mode(0),
            execution_ms: pipestream_core::v2::Duration(10000),
            outputs: OutputBudget {
                count: BatchCount(1),
                total_bytes: Number(bytes.len() as u64),
            },
        },
    }
}
fn options() -> Options {
    Options {
        active: 2,
        active_per_owner: 2,
        file_workers: 1,
        header_timeout: Duration::from_millis(200),
    }
}
fn refused_input(reply: &Reply, stream: u64, code: ErrorCode) {
    assert!(
        matches!(reply.control(), Control::Refusal(Refusal { request: RequestTag::Input { stream: actual }, code: actual_code, .. }) if actual.0 == stream && *actual_code == code),
        "{:?}",
        reply.control()
    );
}
pub(super) async fn transfer(
    inputs: &Inputs,
    connection: &Connection,
    client: &quinn::Connection,
    bytes: Vec<u8>,
) -> (Reply, u64) {
    let client = client.clone();
    let sender = tokio::spawn(async move {
        let mut send = client.open_uni().await.unwrap();
        let stream = u64::from(send.id());
        // A valid receiver may stop a malformed stream before all supplied
        // bytes are written. Its correlated response determines this test.
        let _ = send.write_all(&bytes).await;
        let _ = send.finish();
        stream
    });
    let request = tokio::time::timeout(HANDSHAKE, inputs.accept(connection))
        .await
        .unwrap()
        .unwrap();
    let reply = tokio::time::timeout(HANDSHAKE, request.run())
        .await
        .unwrap();
    let stream = tokio::time::timeout(HANDSHAKE, sender)
        .await
        .unwrap()
        .unwrap();
    (reply, stream)
}

#[tokio::test]
async fn input_crosses_small_quic_windows_commits_once_and_replays_without_payload_fin() {
    let db = Database::new();
    let mut tls = Fixture::new();
    let mut transport = quinn::TransportConfig::default();
    transport
        .stream_receive_window(4096u32.into())
        .receive_window(16384u32.into())
        .max_concurrent_uni_streams(3u32.into());
    tls.security.set_transport_config(Arc::new(transport));
    let (connection, wire) = db.connect(&tls, caps(), Some(0)).await;
    control(&connection, create(1)).await;
    control(&connection, declare(2, vec![Id(1)], true)).await;
    let inputs = Inputs::new(db.authority.clone(), applications(), options()).unwrap();
    let bytes = vec![0x5a; 65536];
    let header = header(&bytes);
    let mut framed = header.encode_framed().unwrap();
    framed.extend_from_slice(&bytes);
    let client = wire.client.as_ref().unwrap();
    let (reply, stream) = transfer(&inputs, &connection, client, framed).await;
    let Control::Work(Work::Admitted {
        request: RequestTag::Input { stream: actual },
        receipt,
    }) = reply.control()
    else {
        panic!("{:?}", reply.control())
    };
    assert_eq!(actual.0, stream);
    assert_eq!(stream, 2);
    let receipt = receipt.clone();
    drop(reply);
    let identity = connection.input().unwrap().binding().identity.clone();
    let (revision, view) = db.store.work_view(&identity, &key(), Number(0)).unwrap();
    assert_eq!(view.state, State::ACTIVE);
    assert_eq!(view.attempt, Number(1));
    assert_eq!(view.input.as_ref().unwrap(), &header.parameters.input);
    let mut repeated = client.open_uni().await.unwrap();
    let replay_stream = u64::from(repeated.id());
    repeated
        .write_all(&header.encode_framed().unwrap())
        .await
        .unwrap();
    let reply = tokio::time::timeout(HANDSHAKE, async {
        inputs.accept(&connection).await.unwrap().run().await
    })
    .await
    .unwrap();
    assert!(
        matches!(reply.control(), Control::Work(Work::Admitted { request: RequestTag::Input { stream }, receipt: found }) if stream.0 == replay_stream && *found == receipt)
    );
    assert_eq!(
        tokio::time::timeout(HANDSHAKE, repeated.stopped())
            .await
            .unwrap()
            .unwrap(),
        Some(0u32.into())
    );
    assert_eq!(
        db.store.work_view(&identity, &key(), Number(0)).unwrap(),
        (revision, view)
    );
    let executor = Executor::new(
        db.store.clone(),
        db.payloads.clone(),
        applications(),
        ResultEndpoint::new("localhost:7443".into()).unwrap(),
        caps(),
        pipestream_core::v2::Duration(100),
    )
    .unwrap();
    let complete = executor.run(&identity, &key()).unwrap();
    assert_eq!(
        complete.manifest.unwrap().outputs[0].sha256,
        header.parameters.input.sha256
    );
}

#[tokio::test]
async fn input_bad_headers_geometry_digest_and_identity_leave_only_the_declaration() {
    let db = Database::new();
    let tls = Fixture::new();
    let (connection, wire) = db.connect(&tls, caps(), Some(0)).await;
    control(&connection, create(1)).await;
    control(&connection, declare(2, vec![Id(1)], true)).await;
    let identity = connection.input().unwrap().binding().identity.clone();
    let inputs = Inputs::new(db.authority.clone(), applications(), options()).unwrap();
    let mut cases = vec![
        (0u32.to_be_bytes().to_vec(), ErrorCode::FrameError),
        (4097u32.to_be_bytes().to_vec(), ErrorCode::FrameError),
        (vec![0, 0, 0], ErrorCode::FrameError),
        (vec![0, 0, 0, 1, 0xa0], ErrorCode::FrameError),
    ];
    for (body, expected) in [
        (b"".as_slice(), ErrorCode::IntegrityError),
        (b"xyz".as_slice(), ErrorCode::IntegrityError),
        (b"abcd".as_slice(), ErrorCode::IntegrityError),
    ] {
        let mut frame = header(b"abc").encode_framed().unwrap();
        frame.extend_from_slice(body);
        cases.push((frame, expected));
    }
    let mut stale = header(b"abc");
    stale.generation = Id(2);
    cases.push((stale.encode_framed().unwrap(), ErrorCode::Conflict));
    let mut foreign = header(b"abc");
    foreign.parameters.work.producer = Producer(1);
    cases.push((foreign.encode_framed().unwrap(), ErrorCode::Unauthorized));
    let mut unknown = header(b"abc");
    unknown.parameters.application = ApplicationLabel("unknown/v1".into());
    cases.push((
        unknown.encode_framed().unwrap(),
        ErrorCode::ApplicationUnsupported,
    ));
    for (bytes, code) in cases {
        let (reply, stream) =
            transfer(&inputs, &connection, wire.client.as_ref().unwrap(), bytes).await;
        refused_input(&reply, stream, code);
        drop(reply);
        let (_, view) = db.store.work_view(&identity, &key(), Number(0)).unwrap();
        assert_eq!(view.state, State::DECLARED);
        assert!(matches!(
            db.store.operation(&identity, OperationId([2; 16])),
            Err(store::StoreError::Protocol(Error {
                code: ErrorCode::NotFound,
                ..
            }))
        ));
    }
    assert!(matches!(
        control(&connection, next(3)).await,
        Control::Session(Session::Sequence { .. })
    ));
    assert!(matches!(
        control(
            &connection,
            Control::Drain(Drain::Detach { request: Id(4) })
        )
        .await,
        Control::Drain(Drain::Detached { .. })
    ));
}

#[tokio::test]
async fn input_header_deadline_and_owner_quota_do_not_block_control_dispatch() {
    let db = Database::new();
    let tls = Fixture::new();
    let (connection, wire) = db.connect(&tls, caps(), Some(0)).await;
    let (rotated, rotated_wire) = db.connect(&tls, caps(), Some(1)).await;
    control(&connection, create(1)).await;
    control(&rotated, attach(1, "alice", 1)).await;
    let mut limits = options();
    limits.active_per_owner = 1;
    let inputs = Inputs::new(db.authority.clone(), applications(), limits).unwrap();
    let mut stalled = wire.client.as_ref().unwrap().open_uni().await.unwrap();
    let first_id = u64::from(stalled.id());
    stalled.write_all(&[0]).await.unwrap();
    let request = inputs.accept(&connection).await.unwrap();
    let task = tokio::spawn(request.run());
    let mut other = rotated_wire
        .client
        .as_ref()
        .unwrap()
        .open_uni()
        .await
        .unwrap();
    let other_id = u64::from(other.id());
    other.write_all(&[0]).await.unwrap();
    let reply = inputs.accept(&rotated).await.unwrap().run().await;
    refused_input(&reply, other_id, ErrorCode::LimitExceeded);
    assert!(matches!(
        control(&rotated, next(2)).await,
        Control::Session(Session::Sequence { .. })
    ));
    let reply = tokio::time::timeout(HANDSHAKE, task)
        .await
        .unwrap()
        .unwrap();
    refused_input(&reply, first_id, ErrorCode::LimitExceeded);
    assert_eq!(
        tokio::time::timeout(HANDSHAKE, stalled.stopped())
            .await
            .unwrap()
            .unwrap(),
        Some(quinn::VarInt::from_u64(ErrorCode::LimitExceeded.quic_error()).unwrap())
    );
    drop(reply);
    let (fresh, stream) = transfer(
        &inputs,
        &rotated,
        rotated_wire.client.as_ref().unwrap(),
        vec![0, 0, 0, 0],
    )
    .await;
    refused_input(&fresh, stream, ErrorCode::FrameError);
}

#[tokio::test]
async fn cancelled_input_keeps_quota_and_pending_until_file_work_and_cleanup_end() {
    let db = Database::new();
    let tls = Fixture::new();
    let (connection, wire) = db.connect(&tls, caps(), Some(0)).await;
    let (other, other_wire) = db.connect(&tls, caps(), Some(1)).await;
    control(&connection, create(1)).await;
    control(&connection, declare(2, vec![Id(1)], true)).await;
    control(&other, attach(1, "alice", 1)).await;
    let identity = connection.input().unwrap().binding().identity.clone();
    let mut limits = options();
    limits.active_per_owner = 1;
    let inputs = Inputs::new(db.authority.clone(), applications(), limits).unwrap();
    db.access.pause_admit.store(true, Ordering::SeqCst);
    let _release_on_failure = ReleaseOnDrop(db.access.clone());
    let mut sending = wire.client.as_ref().unwrap().open_uni().await.unwrap();
    sending
        .write_all(&header(b"body").encode_framed().unwrap())
        .await
        .unwrap();
    let running = tokio::spawn(inputs.accept(&connection).await.unwrap().run());
    tokio::time::timeout(HANDSHAKE, db.access.entered.notified())
        .await
        .unwrap();
    running.abort();
    assert!(matches!(running.await, Err(error) if error.is_cancelled()));
    // The file worker is deliberately held. Control metadata uses another pool.
    assert!(matches!(
        control(&other, next(2)).await,
        Control::Session(Session::Sequence { .. })
    ));
    let (refused, stream) = transfer(
        &inputs,
        &other,
        other_wire.client.as_ref().unwrap(),
        vec![0, 0, 0, 0],
    )
    .await;
    refused_input(&refused, stream, ErrorCode::LimitExceeded);
    drop(refused);
    let mut draining = tokio::spawn(
        pending(
            &connection,
            Control::Drain(Drain::Detach { request: Id(3) }),
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
    assert_eq!(db.payloads.usage(None).unwrap().incomplete_objects, 0);
    assert_eq!(
        db.store
            .work_view(&identity, &key(), Number(0))
            .unwrap()
            .1
            .state,
        State::DECLARED
    );
    assert!(matches!(
        db.store.operation(&identity, OperationId([2; 16])),
        Err(store::StoreError::Protocol(Error {
            code: ErrorCode::NotFound,
            ..
        }))
    ));
    let (reply, stream) = transfer(
        &inputs,
        &other,
        other_wire.client.as_ref().unwrap(),
        vec![0, 0, 0, 0],
    )
    .await;
    refused_input(&reply, stream, ErrorCode::FrameError);
}

#[tokio::test]
async fn input_payload_idle_and_lifetime_expiry_never_admit_work() {
    let db = Database::new();
    let tls = Fixture::new();
    let mut limits = caps();
    limits.stream_idle_ms = IdleMs(1000);
    limits.stream_lifetime_ms = LifetimeMs(1000);
    let (connection, wire) = db.connect(&tls, limits, Some(0)).await;
    control(&connection, create(1)).await;
    control(&connection, declare(2, vec![Id(1)], true)).await;
    let identity = connection.input().unwrap().binding().identity.clone();
    let inputs = Inputs::new(db.authority.clone(), applications(), options()).unwrap();
    let mut stalled = wire.client.as_ref().unwrap().open_uni().await.unwrap();
    let stream = u64::from(stalled.id());
    stalled
        .write_all(&header(b"body").encode_framed().unwrap())
        .await
        .unwrap();
    let reply = tokio::time::timeout(HANDSHAKE, async {
        inputs.accept(&connection).await.unwrap().run().await
    })
    .await
    .unwrap();
    refused_input(&reply, stream, ErrorCode::LimitExceeded);
    drop(reply);
    let view = db.store.work_view(&identity, &key(), Number(0)).unwrap().1;
    assert_eq!(view.state, State::DECLARED);
    assert_eq!(view.attempt, Number(0));
    assert!(matches!(
        control(
            &connection,
            Control::Drain(Drain::Detach { request: Id(3) })
        )
        .await,
        Control::Drain(Drain::Detached { .. })
    ));
}

#[tokio::test]
async fn empty_input_requires_a_bound_session_and_actual_fin_before_admission() {
    let db = Database::new();
    let tls = Fixture::new();
    let (connection, wire) = db.connect(&tls, caps(), Some(0)).await;
    let inputs = Inputs::new(db.authority.clone(), applications(), options()).unwrap();
    let client = wire.client.as_ref().unwrap();
    let (reply, stream) = transfer(
        &inputs,
        &connection,
        client,
        header(b"").encode_framed().unwrap(),
    )
    .await;
    refused_input(&reply, stream, ErrorCode::NotReady);
    drop(reply);
    control(&connection, create(1)).await;
    control(&connection, declare(2, vec![Id(1)], true)).await;
    let identity = connection.input().unwrap().binding().identity.clone();
    let mut send = client.open_uni().await.unwrap();
    let stream = u64::from(send.id());
    send.write_all(&header(b"").encode_framed().unwrap())
        .await
        .unwrap();
    let running = tokio::spawn(inputs.accept(&connection).await.unwrap().run());
    tokio::time::timeout(HANDSHAKE, async {
        while db.payloads.usage(None).unwrap().incomplete_objects == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!running.is_finished(), "a zero-byte declaration is not FIN");
    assert_eq!(
        db.store
            .work_view(&identity, &key(), Number(0))
            .unwrap()
            .1
            .state,
        State::DECLARED
    );
    send.finish().unwrap();
    let reply = tokio::time::timeout(HANDSHAKE, running)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        reply.control(),
        Control::Work(Work::Admitted {
            request: RequestTag::Input { stream: actual }, ..
        }) if actual.0 == stream
    ));
    let view = db.store.work_view(&identity, &key(), Number(0)).unwrap().1;
    assert_eq!(view.state, State::ACTIVE);
    assert_eq!(view.input.unwrap(), header(b"").parameters.input);
}

#[tokio::test]
async fn input_configuration_and_connection_authority_mismatch_fail_before_accept() {
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
            header_timeout: Duration::ZERO,
            ..options()
        },
        Options {
            header_timeout: Duration::from_secs(31),
            ..options()
        },
    ] {
        assert!(matches!(
            Inputs::new(db.authority.clone(), applications(), limits),
            Err(Error {
                code: ErrorCode::LimitExceeded,
                ..
            })
        ));
    }
    let tls = Fixture::new();
    let (connection, _wire) = db.connect(&tls, caps(), Some(0)).await;
    let independent = Authority::new(db.store.clone(), db.payloads.clone(), 1).unwrap();
    let inputs = Inputs::new(independent, applications(), options()).unwrap();
    // No stream has been opened. The mismatch must be rejected before waiting
    // for any bytes and before combining independently owned quota domains.
    let failure = tokio::time::timeout(HANDSHAKE, inputs.accept(&connection))
        .await
        .unwrap()
        .err()
        .unwrap();
    assert_eq!(
        failure.downcast_ref::<Error>().unwrap().code,
        ErrorCode::InternalError
    );
}

#[tokio::test]
async fn continuous_payload_progress_cannot_extend_the_input_lifetime() {
    let db = Database::new();
    let tls = Fixture::new();
    let mut limits = caps();
    limits.stream_idle_ms = IdleMs(1000);
    limits.stream_lifetime_ms = LifetimeMs(1000);
    let (connection, wire) = db.connect(&tls, limits, Some(0)).await;
    control(&connection, create(1)).await;
    control(&connection, declare(2, vec![Id(1)], true)).await;
    let identity = connection.input().unwrap().binding().identity.clone();
    let inputs = Inputs::new(db.authority.clone(), applications(), options()).unwrap();
    let mut send = wire.client.as_ref().unwrap().open_uni().await.unwrap();
    let stream = u64::from(send.id());
    send.write_all(&header(&[b'x'; 100]).encode_framed().unwrap())
        .await
        .unwrap();
    let request = inputs.accept(&connection).await.unwrap();
    let progress = Arc::new(AtomicUsize::new(0));
    let sent = progress.clone();
    let sender = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            match send.write_all(b"x").await {
                Ok(()) => {
                    sent.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => break,
            }
        }
    });
    // Payload keeps arriving within the idle bound. The absolute lifetime
    // still ends this transfer long before all 100 declared bytes are sent.
    let reply = tokio::time::timeout(Duration::from_millis(2500), request.run())
        .await
        .unwrap();
    refused_input(&reply, stream, ErrorCode::LimitExceeded);
    assert!(progress.load(Ordering::SeqCst) >= 2);
    drop(reply);
    tokio::time::timeout(HANDSHAKE, sender)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        control(
            &connection,
            Control::Drain(Drain::Detach { request: Id(3) })
        )
        .await,
        Control::Drain(Drain::Detached { .. })
    ));
    assert_eq!(db.payloads.usage(None).unwrap().incomplete_objects, 0);
    assert_eq!(
        db.store
            .work_view(&identity, &key(), Number(0))
            .unwrap()
            .1
            .state,
        State::DECLARED
    );
}

#[tokio::test]
async fn input_idle_deadline_aborts_transport_while_file_preflight_remains_held() {
    let db = Database::new();
    let tls = Fixture::new();
    let (connection, wire) = db.connect(&tls, caps(), Some(0)).await;
    control(&connection, create(1)).await;
    control(&connection, declare(2, vec![Id(1)], true)).await;
    let identity = connection.input().unwrap().binding().identity.clone();
    let inputs = Inputs::new(db.authority.clone(), applications(), options()).unwrap();
    db.access.pause_admit.store(true, Ordering::SeqCst);
    let _release_on_failure = ReleaseOnDrop(db.access.clone());
    let mut sending = wire.client.as_ref().unwrap().open_uni().await.unwrap();
    let stream = u64::from(sending.id());
    sending
        .write_all(&header(b"body").encode_framed().unwrap())
        .await
        .unwrap();
    let running = tokio::spawn(inputs.accept(&connection).await.unwrap().run());
    tokio::time::timeout(HANDSHAKE, db.access.entered.notified())
        .await
        .unwrap();
    let reply = tokio::time::timeout(Duration::from_millis(2000), running)
        .await
        .expect("stream idle deadline must not await blocked filesystem work")
        .unwrap();
    refused_input(&reply, stream, ErrorCode::LimitExceeded);
    drop(reply);
    let mut draining = tokio::spawn(
        pending(
            &connection,
            Control::Drain(Drain::Detach { request: Id(3) }),
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
    assert_eq!(db.payloads.usage(None).unwrap().incomplete_objects, 0);
    assert_eq!(
        db.store
            .work_view(&identity, &key(), Number(0))
            .unwrap()
            .1
            .state,
        State::DECLARED
    );
}
