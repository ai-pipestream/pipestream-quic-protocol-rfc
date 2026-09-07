//! Actual QUIC send admission and receive geometry, not durable RPC conformance.
use super::*;
use crate::v2_flow::{Connection as Flow, Limits, Writer};
use std::task::{Context, Poll, Waker};

fn limits() -> Limits {
    Limits {
        data_send: 8192,
        control_send: 4096,
        receive_stream: 65536,
        data_streams: 3,
    }
}
struct Pair {
    server: Flow,
    client: Flow,
    server_control: Writer,
    client_control: Writer,
    client_recv: quinn::RecvStream,
    _server_recv: quinn::RecvStream,
    _wire: Exchange,
    _fixture: Fixture,
}
impl Pair {
    async fn new(limits: Limits, reserve_receive: bool) -> Self {
        let mut fixture = Fixture::new();
        let mut transport = quinn::TransportConfig::default();
        limits
            .configure(&mut transport, quinn::Side::Server)
            .unwrap();
        fixture.security.set_transport_config(Arc::new(transport));
        let mut transport = quinn::TransportConfig::default();
        limits
            .configure(&mut transport, quinn::Side::Client)
            .unwrap();
        if !reserve_receive {
            transport.receive_window(
                (u64::from(limits.receive_stream) * u64::from(limits.data_streams))
                    .try_into()
                    .unwrap(),
            );
        }
        let mut config = fixture.config(Some(0));
        config.transport_config(Arc::new(transport));
        let wire = fixture.connect(config, "localhost").await;
        let server = Flow::new(wire.server.as_ref().unwrap().connection().clone(), limits).unwrap();
        let client = Flow::new(wire.client.as_ref().unwrap().clone(), limits).unwrap();
        let (mut client_control, client_recv) = client.open_control().await.unwrap();
        client_control.write_all(b"x").await.unwrap();
        let (server_control, mut server_recv) = server.accept_control().await.unwrap();
        server_recv.read_exact(&mut [0; 1]).await.unwrap();
        Self {
            server,
            client,
            server_control,
            client_control,
            client_recv,
            _server_recv: server_recv,
            _wire: wire,
            _fixture: fixture,
        }
    }
}
fn once(writer: &mut Writer, bytes: &[u8]) -> Poll<Result<usize, Error>> {
    writer.poll_write(&mut Context::from_waker(Waker::noop()), bytes)
}

#[tokio::test]
async fn control_uses_reserved_local_send_space_that_data_cannot_consume() {
    let mut pair = Pair::new(limits(), true).await;
    let mut data = pair.server.open_data().await.unwrap();
    let bytes = vec![0x5a; 16384];
    // No await between polls: this current-thread runtime cannot process an
    // intervening network acknowledgment and return send credit during the test.
    assert!(matches!(once(&mut data, &bytes), Poll::Ready(Ok(8192))));
    assert!(matches!(once(&mut data, &bytes), Poll::Pending));
    assert!(matches!(
        once(&mut pair.server_control, &bytes),
        Poll::Ready(Ok(4096))
    ));
    assert!(matches!(once(&mut data, &bytes), Poll::Pending));
    assert!(matches!(
        once(&mut pair.server_control, &bytes),
        Poll::Pending
    ));
    data.reset(0u32.into()).unwrap();
    pair.server_control.reset(0u32.into()).unwrap();
}

#[tokio::test]
async fn failed_control_poll_restores_the_lower_data_send_window() {
    let mut pair = Pair::new(limits(), true).await;
    let mut data = pair.server.open_data().await.unwrap();
    pair.server_control.reset(0u32.into()).unwrap();
    let bytes = vec![0x5a; 16384];
    assert!(matches!(
        once(&mut pair.server_control, &bytes),
        Poll::Ready(Err(Error {
            code: ErrorCode::ControlReset,
            ..
        }))
    ));
    assert!(matches!(once(&mut data, &bytes), Poll::Ready(Ok(8192))));
    assert!(matches!(once(&mut data, &bytes), Poll::Pending));
    data.reset(0u32.into()).unwrap();
}

async fn filled_data(pair: &Pair, bytes: usize) -> Vec<Writer> {
    let mut streams = Vec::new();
    for _ in 0..3 {
        let mut data = pair.server.open_data().await.unwrap();
        tokio::time::timeout(HANDSHAKE, data.write_all(&vec![0x5a; bytes]))
            .await
            .unwrap()
            .unwrap();
        streams.push(data);
    }
    for stream in &mut streams {
        assert!(
            tokio::time::timeout(Duration::from_millis(20), stream.write(b"x"))
                .await
                .is_err()
        );
    }
    streams
}

#[tokio::test]
async fn control_frame_crosses_while_every_data_receive_window_is_stalled() {
    let limits = Limits {
        receive_stream: 4096,
        ..limits()
    };
    assert_eq!(limits.receive_budget().unwrap(), 18725);
    let mut pair = Pair::new(limits, true).await;
    let mut streams = filled_data(&pair, 4096).await;
    let message = Control::Session(Session::Sequence {
        request: Id(1),
        next_creation_sequence: Id(2),
    });
    let bytes = message.encode(4096).unwrap();
    tokio::time::timeout(HANDSHAKE, pair.server_control.write_all(&bytes))
        .await
        .unwrap()
        .unwrap();
    let mut received = vec![0; bytes.len()];
    tokio::time::timeout(HANDSHAKE, pair.client_recv.read_exact(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Control::decode(&received, 4096).unwrap(), message);
    for stream in &mut streams {
        stream.reset(0u32.into()).unwrap();
    }
}

#[tokio::test]
async fn missing_peer_receive_reservation_blocks_control_despite_local_send_space() {
    let limits = Limits {
        receive_stream: 4096,
        ..limits()
    };
    let mut pair = Pair::new(limits, false).await;
    let mut streams = filled_data(&pair, 4096).await;
    // The deliberately unsafe peer advertised only 3 * 4096 connection bytes.
    // Local send reservation and packet priority cannot manufacture remote credit.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), pair.server_control.write(b"c"))
            .await
            .is_err()
    );
    let mut input = pair
        ._wire
        .client
        .as_ref()
        .unwrap()
        .accept_uni()
        .await
        .unwrap();
    input.read_exact(&mut [0; 4096]).await.unwrap();
    input.stop(0u32.into()).unwrap();
    tokio::time::timeout(HANDSHAKE, pair.server_control.write_all(b"c"))
        .await
        .unwrap()
        .unwrap();
    let mut received = [0; 1];
    pair.client_recv.read_exact(&mut received).await.unwrap();
    assert_eq!(received, *b"c");
    for stream in &mut streams {
        let _ = stream.reset(0u32.into());
    }
}

#[tokio::test]
async fn batched_connection_credit_updates_cannot_spend_the_control_reservation() {
    let limits = Limits {
        receive_stream: 1024,
        data_streams: 16,
        ..limits()
    };
    let mut pair = Pair::new(limits, true).await;
    let mut streams = Vec::new();
    for _ in 0..16 {
        let mut data = pair.server.open_data().await.unwrap();
        tokio::time::timeout(HANDSHAKE, data.write_all(&[0x5a; 1024]))
            .await
            .unwrap()
            .unwrap();
        streams.push(data);
    }
    // Quinn updates stream credit at W/8, but connection credit at R/8.
    // These small reads advertise more stream credit before MAX_DATA is due.
    // With only (N+1)*W initial connection credit, replacement data spends the
    // entire control window without triggering a connection credit update.
    let client = pair._wire.client.as_ref().unwrap();
    let mut readers = Vec::new();
    for _ in 0..2 {
        let mut recv = client.accept_uni().await.unwrap();
        let index = ((u64::from(recv.id()) - 3) / 4) as usize;
        recv.read_exact(&mut [0; 512]).await.unwrap();
        tokio::time::timeout(HANDSHAKE, streams[index].write_all(&[0x5a; 512]))
            .await
            .unwrap()
            .unwrap();
        readers.push(recv);
    }
    tokio::time::timeout(
        Duration::from_millis(250),
        pair.server_control.write_all(&[0x63; 1024]),
    )
    .await
    .expect("control credit must survive delayed MAX_DATA")
    .unwrap();
    let mut actual = [0; 1024];
    pair.client_recv.read_exact(&mut actual).await.unwrap();
    assert_eq!(actual, [0x63; 1024]);
    for stream in &mut streams {
        stream.reset(0u32.into()).unwrap();
    }
}

#[tokio::test]
async fn blocked_control_registers_retry_independent_of_data_credit() {
    struct Signal(tokio::sync::Notify);
    impl std::task::Wake for Signal {
        fn wake(self: Arc<Self>) {
            self.0.notify_one();
        }
    }
    let limits = Limits {
        receive_stream: 4096,
        ..limits()
    };
    let mut pair = Pair::new(limits, false).await;
    let mut streams = filled_data(&pair, 4096).await;
    let signal = Arc::new(Signal(tokio::sync::Notify::new()));
    let waker = Waker::from(signal.clone());
    assert!(
        pair.server_control
            .poll_write(&mut Context::from_waker(&waker), b"c")
            .is_pending()
    );
    // With no remote connection credit, Quinn cannot emit a Writable event.
    // The wrapper must still arrange its own bounded retry wake, since Quinn's
    // normal wake condition uses B, not the temporarily available B+C window.
    tokio::time::timeout(Duration::from_millis(100), signal.0.notified())
        .await
        .expect("control poll must register its independent retry wake");
    for stream in &mut streams {
        stream.reset(0u32.into()).unwrap();
    }
}

#[tokio::test]
async fn control_role_and_stream_zero_checks_do_not_allow_arbitrary_priority_writers() {
    let mut pair = Pair::new(limits(), true).await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            pair._wire.server.as_ref().unwrap().connection().open_bi(),
        )
        .await
        .is_err(),
        "the client must not grant incoming bidi credit to the server"
    );
    assert!(matches!(
        pair.server.open_control().await,
        Err(Error {
            code: ErrorCode::FrameError,
            ..
        })
    ));
    assert!(matches!(
        pair.client.accept_control().await,
        Err(Error {
            code: ErrorCode::FrameError,
            ..
        })
    ));
    // The peer grants only one bidi stream until it is closed in both directions.
    pair.client_control.finish().unwrap();
    pair.server_control.finish().unwrap();
    let mut byte = [0; 1];
    assert_eq!(pair._server_recv.read(&mut byte).await.unwrap(), None);
    assert_eq!(pair.client_recv.read(&mut byte).await.unwrap(), None);
    assert!(matches!(
        tokio::time::timeout(HANDSHAKE, pair.client.open_control())
            .await
            .unwrap(),
        Err(Error {
            code: ErrorCode::FrameError,
            ..
        })
    ));
}

#[tokio::test]
async fn consumed_and_reset_data_streams_preserve_control_credit_on_replacement() {
    let limits = Limits {
        receive_stream: 4096,
        data_streams: 1,
        ..limits()
    };
    let mut pair = Pair::new(limits, true).await;
    for reset in [false, true, false, true] {
        let mut data = tokio::time::timeout(HANDSHAKE, pair.server.open_data())
            .await
            .unwrap()
            .unwrap();
        data.write_all(&[0x5a; 4096]).await.unwrap();
        let mut recv = pair
            ._wire
            .client
            .as_ref()
            .unwrap()
            .accept_uni()
            .await
            .unwrap();
        if reset {
            data.reset(0u32.into()).unwrap();
            // Consume until RESET is observed, including any already buffered data.
            assert!(matches!(
                recv.read_to_end(4096).await,
                Err(quinn::ReadToEndError::Read(quinn::ReadError::Reset(_)))
            ));
        } else {
            data.finish().unwrap();
            assert_eq!(recv.read_to_end(4096).await.unwrap(), [0x5a; 4096]);
        }
        tokio::time::timeout(HANDSHAKE, pair.server_control.write_all(b"c"))
            .await
            .unwrap()
            .unwrap();
        let mut byte = [0; 1];
        pair.client_recv.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, *b"c");
    }
}

#[test]
fn flow_configuration_has_checked_count_and_byte_ceilings() {
    for candidate in [
        Limits {
            data_send: 0,
            ..limits()
        },
        Limits {
            data_send: u64::MAX,
            ..limits()
        },
        Limits {
            control_send: 0,
            ..limits()
        },
        Limits {
            control_send: u64::MAX,
            ..limits()
        },
        Limits {
            receive_stream: 1023,
            ..limits()
        },
        Limits {
            receive_stream: u32::MAX,
            ..limits()
        },
        Limits {
            data_streams: 129,
            ..limits()
        },
    ] {
        assert!(matches!(
            candidate.receive_budget(),
            Err(Error {
                code: ErrorCode::LimitExceeded,
                ..
            })
        ));
    }
    assert_eq!(
        Limits {
            data_streams: 0,
            ..limits()
        }
        .receive_budget()
        .unwrap(),
        74899
    );
    for streams in 0..=128 {
        for window in [1024, 4096, 65536, 1048576] {
            let candidate = Limits {
                data_streams: streams,
                receive_stream: window,
                ..limits()
            };
            let budget = candidate.receive_budget().unwrap();
            let data_and_control = (u64::from(streams) + 1) * u64::from(window);
            assert!(budget - budget / 8 >= data_and_control);
            assert!(budget <= 154590062);
        }
    }
}
