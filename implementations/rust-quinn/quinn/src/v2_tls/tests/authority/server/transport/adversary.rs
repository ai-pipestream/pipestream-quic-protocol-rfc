//! An authenticated wire peer deliberately sends invalid/slow responses. These
//! tests do not claim its test script is a second durable implementation.
use super::*;
use crate::v2_core::framing;

struct PeerWire {
    peer: Peer,
    send: v2_flow::Writer,
    recv: quinn::RecvStream,
    flow: v2_flow::Connection,
}
impl PeerWire {
    async fn receive(&mut self) -> Control {
        match framing::receive(
            &mut self.recv,
            Some(8192),
            tokio::time::Instant::now() + HANDSHAKE,
        )
        .await
        .unwrap()
        {
            framing::Frame::Control(c) => c,
            _ => panic!("expected control"),
        }
    }
    async fn send(&mut self, control: Control) {
        self.send
            .write_all(&control.encode(8192).unwrap())
            .await
            .unwrap();
    }
}
async fn pair(tls: &Fixture, options: wire::Options) -> (Transport, PeerWire) {
    Control::Capabilities(options.offer.clone())
        .encode(INITIAL_CONTROL_LIMIT)
        .unwrap();
    let client = Transport::connect(
        "127.0.0.1:0".parse().unwrap(),
        tls.endpoint.local_addr().unwrap(),
        "localhost",
        wire::Security::new(roots(&tls.issuer), Some(tls.clients[0].identity())).unwrap(),
        options,
    );
    let server = async {
        let peer = tls
            .security
            .accept(tls.endpoint.accept().await.unwrap(), HANDSHAKE)
            .await
            .unwrap();
        let flow =
            v2_flow::Connection::new(peer.connection().clone(), super::super::options().flow)
                .unwrap();
        let (send, recv) = flow.accept_control().await.unwrap();
        let mut wire = PeerWire {
            peer,
            send,
            recv,
            flow,
        };
        let Control::Capabilities(offer) = wire.receive().await else {
            panic!()
        };
        let selected = Capabilities::select(&offer, &client_options().offer, true).unwrap();
        wire.send(Control::Capabilities(selected)).await;
        wire
    };
    let (client, peer) = tokio::time::timeout(HANDSHAKE, async { tokio::join!(client, server) })
        .await
        .expect("bounded test peer handshake");
    (client.unwrap(), peer)
}
fn manifest(bytes: &[u8]) -> Manifest {
    Manifest {
        version: Literal, authority: IdentityLabel("issuer-a".into()), owner: IdentityLabel("alice".into()),
        generation: Id(1), work: key(), attempt: Id(1), input_sha256: Digest([0;32]), committed_at: Number(1000), available_until: Number(20000),
        outputs: vec![Output { index: OutputIndex(0), length: Number(bytes.len() as u64), sha256: Digest(Sha256::digest(bytes).into()), content_type: ApplicationLabel("application/octet-stream".into()),
            locator: ResultLocator("pipestream://localhost:7443/v2/sessions/1/scopes/0/producers/0/entities/1/attempts/1/outputs/0".into()) }],
    }
}
fn header(manifest: &Manifest, request: Id) -> ResultHeader {
    ResultHeader {
        kind: Literal,
        request,
        generation: manifest.generation,
        work: manifest.work.clone(),
        attempt: manifest.attempt,
        index: OutputIndex(0),
        length: manifest.outputs[0].length,
        sha256: manifest.outputs[0].sha256,
    }
}
async fn still_usable(client: &Transport, peer: &mut PeerWire) {
    let response = client.exchange(next(1), None);
    let server = async {
        let c = peer.receive().await;
        peer.send(Control::Session(Session::Sequence {
            request: request_id(&c).unwrap(),
            next_creation_sequence: Id(7),
        }))
        .await;
    };
    let (reply, ()) = tokio::join!(response, server);
    assert!(matches!(
        reply.unwrap(),
        Reply::Control(Control::Session(Session::Sequence {
            next_creation_sequence: Id(7),
            ..
        }))
    ));
}

#[tokio::test]
async fn file_save_never_installs_corrupt_truncated_or_extra_result_bytes() {
    for bytes in [b"abd".as_slice(), b"ab", b"abcd"] {
        let tls = Fixture::new();
        let (client, mut peer) = pair(&tls, client_options()).await;
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("result");
        let manifest = manifest(b"abc");
        let request = client.exchange(read(&manifest), Some(&manifest));
        let sending = async {
            let request = request_id(&peer.receive().await).unwrap();
            let mut send = peer.flow.open_data().await.unwrap();
            send.write_all(&header(&manifest, request).encode_framed().unwrap())
                .await
                .unwrap();
            let _ = send.write_all(bytes).await;
            let _ = send.finish();
        };
        let (response, ()) = tokio::join!(request, sending);
        let Reply::Object(output) = response.unwrap() else {
            panic!("expected object")
        };
        assert!(output.save_to(target.clone(), 1024).await.is_err());
        assert!(!target.exists());
        // Deferred unlink runs on the file pool, not the network reader.
        tokio::time::timeout(HANDSHAKE, async {
            loop {
                if std::fs::read_dir(directory.path())
                    .unwrap()
                    .next()
                    .is_none()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        still_usable(&client, &mut peer).await;
        client.close();
        client.closed().await;
    }
}

#[tokio::test]
async fn cancelled_file_save_waiter_still_requires_verified_fin_before_installation() {
    let tls = Fixture::new();
    let (client, mut peer) = pair(&tls, client_options()).await;
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("result");
    let bytes = vec![0x8f; 32768];
    let manifest = manifest(&bytes);
    let request = client.exchange(read(&manifest), Some(&manifest));
    let sending = async {
        let request = request_id(&peer.receive().await).unwrap();
        let mut send = peer.flow.open_data().await.unwrap();
        send.write_all(&header(&manifest, request).encode_framed().unwrap())
            .await
            .unwrap();
        send.write_all(&bytes[..1]).await.unwrap();
        send
    };
    let (response, mut sending) = tokio::join!(request, sending);
    let Reply::Object(output) = response.unwrap() else {
        panic!("expected object")
    };
    let saving = tokio::spawn(output.save_to(target.clone(), bytes.len() as u64));
    tokio::time::timeout(HANDSHAKE, async {
        loop {
            if std::fs::read_dir(directory.path())
                .unwrap()
                .next()
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!target.exists());
    saving.abort();
    let _ = saving.await;
    sending.write_all(&bytes[1..]).await.unwrap();
    assert!(!target.exists());
    sending.finish().unwrap();
    tokio::time::timeout(HANDSHAKE, async {
        while !target.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read(target).unwrap(), bytes);
    still_usable(&client, &mut peer).await;
    client.close();
    client.closed().await;
}

#[tokio::test]
async fn mismatched_result_header_only_aborts_that_delivery() {
    let tls = Fixture::new();
    let (client, mut peer) = pair(&tls, client_options()).await;
    let manifest = manifest(b"abc");
    let response = client.exchange(read(&manifest), Some(&manifest));
    let server = async {
        let request = request_id(&peer.receive().await).unwrap();
        let mut changed = header(&manifest, request);
        changed.length.0 += 1;
        let mut out = peer.flow.open_data().await.unwrap();
        out.write_all(&changed.encode_framed().unwrap())
            .await
            .unwrap();
        out.finish().unwrap();
    };
    let (reply, ()) = tokio::join!(response, server);
    assert_eq!(reply.err().unwrap().code, ErrorCode::IntegrityError);
    still_usable(&client, &mut peer).await;
    client.close();
    client.closed().await;
}

#[tokio::test]
async fn corrupt_truncated_and_extra_result_bytes_never_become_verified() {
    for bytes in [b"abd".as_slice(), b"ab", b"abcd"] {
        let tls = Fixture::new();
        let (client, mut peer) = pair(&tls, client_options()).await;
        let manifest = manifest(b"abc");
        let response = client.exchange(read(&manifest), Some(&manifest));
        let server = async {
            let request = request_id(&peer.receive().await).unwrap();
            let mut out = peer.flow.open_data().await.unwrap();
            out.write_all(&header(&manifest, request).encode_framed().unwrap())
                .await
                .unwrap();
            out.write_all(bytes).await.unwrap();
            out.finish().unwrap();
        };
        let (reply, ()) = tokio::join!(response, server);
        let Reply::Object(mut output) = reply.unwrap() else {
            panic!()
        };
        let failure = loop {
            match output.read_unverified().await {
                Ok(Some(_)) => {}
                Ok(None) => panic!("invalid bytes became verified"),
                Err(e) => break e,
            }
        };
        assert_eq!(failure.code, ErrorCode::IntegrityError);
        assert!(output.verification().is_none());
        drop(output);
        still_usable(&client, &mut peer).await;
        client.close();
        client.closed().await;
    }
}

#[tokio::test]
async fn unread_output_deadline_is_driven_without_polling_the_consumer() {
    let tls = Fixture::new();
    let mut options = client_options();
    options.offer.stream_idle_ms = IdleMs(1000);
    let (client, mut peer) = pair(&tls, options).await;
    let manifest = manifest(&[3; 65536]);
    let response = client.exchange(read(&manifest), Some(&manifest));
    let server = async {
        let request = request_id(&peer.receive().await).unwrap();
        let mut out = peer.flow.open_data().await.unwrap();
        out.write_all(&header(&manifest, request).encode_framed().unwrap())
            .await
            .unwrap();
        let stopped = out.stopped();
        let writer = tokio::spawn(async move { out.write_all(&[3; 65536]).await });
        (writer, stopped)
    };
    let (reply, (writer, stopped)) = tokio::join!(response, server);
    let Reply::Object(mut output) = reply.unwrap() else {
        panic!()
    };
    still_usable(&client, &mut peer).await;
    let stopped = tokio::time::timeout(HANDSHAKE, stopped)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(u64::from(stopped), ErrorCode::LimitExceeded.quic_error());
    tokio::time::timeout(HANDSHAKE, writer)
        .await
        .expect("stopped writer must wake")
        .unwrap()
        .unwrap_err();
    let failure = loop {
        match output.read_unverified().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("stalled result completed"),
            Err(e) => break e,
        }
    };
    assert_eq!(failure.code, ErrorCode::LimitExceeded);
    assert!(output.verification().is_none());
    drop(output);
    still_usable(&client, &mut peer).await;
    client.close();
    tokio::time::timeout(HANDSHAKE, client.closed())
        .await
        .expect("client tasks and endpoint must drain");
}

#[tokio::test]
async fn unanswered_request_has_a_bounded_connection_lifetime() {
    let tls = Fixture::new();
    let mut options = client_options();
    options.response_timeout = Duration::from_millis(100);
    let (client, mut peer) = pair(&tls, options).await;
    let response = client.exchange(next(1), None);
    let server = async {
        peer.receive().await;
        let closed = peer.peer.connection().closed().await;
        assert!(
            matches!(closed, quinn::ConnectionError::ApplicationClosed(ref c) if u64::from(c.error_code) == ErrorCode::LimitExceeded.quic_error())
        );
    };
    let (reply, ()) = tokio::join!(response, server);
    assert_eq!(reply.err().unwrap().code, ErrorCode::LimitExceeded);
    client.closed().await;
}

#[tokio::test]
async fn wrong_direction_and_unsolicited_control_are_fatal() {
    for wrong_direction in [false, true] {
        let tls = Fixture::new();
        let (client, mut peer) = pair(&tls, client_options()).await;
        let response = client.exchange(next(1), None);
        let server = async {
            let id = request_id(&peer.receive().await).unwrap();
            let invalid = if wrong_direction {
                next(id.0)
            } else {
                Control::Session(Session::Sequence {
                    request: Id(id.0 + 1),
                    next_creation_sequence: Id(7),
                })
            };
            peer.send(invalid).await;
        };
        let (reply, ()) = tokio::join!(response, server);
        assert_eq!(reply.err().unwrap().code, ErrorCode::FrameError);
        client.closed().await;
    }
}

#[tokio::test]
async fn pending_ceiling_refuses_without_spending_an_id_and_replies_can_reverse() {
    let tls = Fixture::new();
    let (client, mut peer) = pair(&tls, client_options()).await;
    let mut waiting = Vec::new();
    for _ in 0..4 {
        let client = client.clone();
        waiting.push(tokio::spawn(
            async move { control(&client, next(999)).await },
        ));
    }
    let mut requests = Vec::new();
    for _ in 0..4 {
        requests.push(request_id(&peer.receive().await).unwrap());
    }
    assert_eq!(requests, [Id(1), Id(2), Id(3), Id(4)]);
    assert_eq!(
        client.exchange(next(1), None).await.err().unwrap().code,
        ErrorCode::LimitExceeded
    );
    for request in requests.into_iter().rev() {
        peer.send(Control::Session(Session::Sequence {
            request,
            next_creation_sequence: Id(7),
        }))
        .await;
    }
    for waiter in waiting {
        waiter.await.unwrap();
    }
    let request = client.exchange(next(999), None);
    let response = async {
        let id = request_id(&peer.receive().await).unwrap();
        assert_eq!(id, Id(5));
        peer.send(Control::Session(Session::Sequence {
            request: id,
            next_creation_sequence: Id(7),
        }))
        .await;
    };
    let (reply, ()) = tokio::join!(request, response);
    assert!(reply.is_ok());
    client.close();
    client.closed().await;
}

#[tokio::test]
async fn cancelled_connect_closes_the_owned_negotiation() {
    let tls = Fixture::new();
    let remote = tls.endpoint.local_addr().unwrap();
    let security =
        wire::Security::new(roots(&tls.issuer), Some(tls.clients[0].identity())).unwrap();
    let connecting = tokio::spawn(async move {
        Transport::connect(
            "127.0.0.1:0".parse().unwrap(),
            remote,
            "localhost",
            security,
            client_options(),
        )
        .await
    });
    let incoming = tokio::time::timeout(HANDSHAKE, tls.endpoint.accept())
        .await
        .unwrap()
        .unwrap();
    let peer = tls.security.accept(incoming, HANDSHAKE).await.unwrap();
    let flow =
        v2_flow::Connection::new(peer.connection().clone(), super::super::options().flow).unwrap();
    let (_send, mut recv) = flow.accept_control().await.unwrap();
    assert!(matches!(
        framing::receive(&mut recv, None, tokio::time::Instant::now() + HANDSHAKE)
            .await
            .unwrap(),
        framing::Frame::Control(Control::Capabilities(_))
    ));
    connecting.abort();
    let _ = connecting.await;
    let closed = tokio::time::timeout(HANDSHAKE, peer.connection().closed())
        .await
        .unwrap();
    assert!(
        matches!(closed, quinn::ConnectionError::ApplicationClosed(c) if u64::from(c.error_code) == ErrorCode::Cancelled.quic_error())
    );
}

#[tokio::test]
async fn increased_negotiated_limit_closes_with_frame_error() {
    let tls = Fixture::new();
    let remote = tls.endpoint.local_addr().unwrap();
    let security =
        wire::Security::new(roots(&tls.issuer), Some(tls.clients[0].identity())).unwrap();
    let connecting = Transport::connect(
        "127.0.0.1:0".parse().unwrap(),
        remote,
        "localhost",
        security,
        client_options(),
    );
    let server = async {
        let incoming = tokio::time::timeout(HANDSHAKE, tls.endpoint.accept())
            .await
            .unwrap()
            .unwrap();
        let peer = tls.security.accept(incoming, HANDSHAKE).await.unwrap();
        let flow =
            v2_flow::Connection::new(peer.connection().clone(), super::super::options().flow)
                .unwrap();
        let (mut send, mut recv) = flow.accept_control().await.unwrap();
        let framing::Frame::Control(Control::Capabilities(offer)) =
            framing::receive(&mut recv, None, tokio::time::Instant::now() + HANDSHAKE)
                .await
                .unwrap()
        else {
            panic!()
        };
        let mut selected = Capabilities::select(&offer, &client_options().offer, true).unwrap();
        selected.object_limit.0 += 1;
        send.write_all(
            &Control::Capabilities(selected)
                .encode(INITIAL_CONTROL_LIMIT)
                .unwrap(),
        )
        .await
        .unwrap();
        let closed = tokio::time::timeout(HANDSHAKE, peer.connection().closed())
            .await
            .unwrap();
        assert!(
            matches!(closed, quinn::ConnectionError::ApplicationClosed(c) if u64::from(c.error_code) == ErrorCode::FrameError.quic_error())
        );
    };
    let (client, ()) = tokio::join!(connecting, server);
    assert_eq!(
        client.err().unwrap().downcast_ref::<Error>().unwrap().code,
        ErrorCode::FrameError
    );
}

#[tokio::test]
async fn unresolved_admission_limit_refuses_before_allocating_another_stream() {
    let tls = Fixture::new();
    let mut options = client_options();
    options.offer.stream_limit = ConcurrencyLimit(1);
    options.flow.data_streams = 1;
    let (client, mut peer) = pair(&tls, options).await;
    let mut input = client.input(inputs::header(&[])).await.unwrap();
    input.finish().await.unwrap();
    let mut received = peer.peer.connection().accept_uni().await.unwrap();
    received.read_to_end(4096).await.unwrap();
    assert_eq!(
        client.input(inputs::header(&[])).await.err().unwrap().code,
        ErrorCode::LimitExceeded
    );
    peer.send(Control::Refusal(Refusal {
        request: RequestTag::Input {
            stream: input.stream_id(),
        },
        code: ErrorCode::Cancelled,
        detail: Detail("test refusal".into()),
    }))
    .await;
    assert!(matches!(
        input.response().await.unwrap(),
        Control::Refusal(_)
    ));
    // The refused local call must not create stream 6 and then reset it without
    // correlation. Its first successful replacement still has stream ID 6.
    let mut replacement = client.input(inputs::header(&[])).await.unwrap();
    assert_eq!(replacement.stream_id(), StreamId(6));
    replacement.finish().await.unwrap();
    let mut received = peer.peer.connection().accept_uni().await.unwrap();
    received.read_to_end(4096).await.unwrap();
    peer.send(Control::Refusal(Refusal {
        request: RequestTag::Input {
            stream: replacement.stream_id(),
        },
        code: ErrorCode::Cancelled,
        detail: Detail("test refusal".into()),
    }))
    .await;
    replacement.response().await.unwrap();
    still_usable(&client, &mut peer).await;
    client.close();
    client.closed().await;
}
