use super::*;
use pipestream_core::work_set::{self, WorkSetFrame};

fn status(state: u8, wire_bytes: usize) -> StatusFrame {
    assert!(wire_bytes == 21 || wire_bytes >= 26);
    StatusFrame {
        status: Status {
            state,
            entity_id: 1,
            scope_id: 0,
            cursor: None,
            depth: 0,
        },
        extension: (wire_bytes > 21).then(|| StatusExtension::Opaque(vec![0xa5; wire_bytes - 25])),
    }
}

// A fault-injecting QUIC peer, not a reference server or admission-state oracle.
async fn exchange(
    frames: Vec<StatusFrame>,
    sealed: bool,
    chunked: bool,
    refused: bool,
) -> Result<()> {
    let dir = tempfile::tempdir()?;
    let options = options(dir.path());
    let certs =
        CertificateDer::pem_file_iter(&options.certificate)?.collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(&options.private_key)?;
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    let endpoint = quinn::Endpoint::server(
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls)?)),
        "127.0.0.1:0".parse()?,
    )?;
    let address = endpoint.local_addr()?;
    let expected = frames.clone();
    let server = tokio::spawn(async move {
        let connection = endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let (kind, bytes) = read(&mut recv).await.unwrap();
        assert_eq!(kind, FRAME_CAPABILITIES);
        let caps = decode_capabilities(&bytes).unwrap();
        let layers = if sealed {
            LayerSupport::LAYER1
        } else {
            LayerSupport::LAYER2
        };
        send.write_all(&encode_capabilities(&caps).unwrap())
            .await
            .unwrap();
        if sealed {
            let (kind, bytes) = read(&mut recv).await.unwrap();
            assert_eq!(kind, work_set::FRAME_WORK_SET);
            let mut declaration = work_set::decode(&bytes).unwrap();
            declaration.flags |= work_set::ACK;
            send.write_all(&work_set::encode(&declaration).unwrap())
                .await
                .unwrap();
        }
        let (kind, bytes) = read(&mut recv).await.unwrap();
        assert_eq!(kind, FRAME_STATUS);
        assert_eq!(
            decode_status_frame(&bytes, layers).unwrap().status.state,
            STATUS_PENDING
        );
        for _ in 0..if chunked { 2 } else { 1 } {
            let mut stream = connection.accept_uni().await.unwrap();
            let bytes = stream.read_to_end(4096).await.unwrap();
            let (header, payload) = decode_entity_for(&bytes, layers).unwrap();
            assert_eq!(header.entity_id, 1);
            assert_eq!(payload, b"x");
        }
        for frame in frames {
            let bytes = encode_status_frame(&frame, layers).unwrap();
            assert!(bytes.len() <= MAX_CONTROL_FRAME + 5);
            if let Err(error) = send.write_all(&bytes).await {
                assert!(refused, "unexpected output failure: {error}");
                break;
            }
        }
        tokio::time::timeout(Duration::from_secs(3), connection.closed()).await
    });
    let client_options = RecursiveClientOptions {
        remote: address,
        ca_certificate: options.certificate,
        server_name: "localhost".into(),
        identity: None,
    };
    let mut client = if sealed {
        RecursiveClient::connect_sealed(&client_options).await?
    } else {
        RecursiveClient::connect(&client_options).await?
    };
    if sealed {
        let frame = WorkSetFrame {
            session_id: "status-history".into(),
            producer_id: [7; 16],
            scope_id: 0,
            parent: None,
            sequence: 0,
            entity_ids: vec![1],
            flags: 0,
            seal_digest: None,
        };
        client.declare_work(&frame).await?;
    }
    let header = EntityHeader {
        entity_id: 1,
        parent_id: None,
        scope_id: None,
        parent_scope_id: None,
        layer: 0,
        content_type: None,
        payload_length: Some(1),
        checksum: None,
        metadata: BTreeMap::from([(SESSION_METADATA_KEY.into(), "status-history".into())]),
        chunk_info: None,
        completion_policy: None,
    };
    let result = if chunked {
        let chunks = (0..2)
            .map(|index| {
                let mut header = header.clone();
                header.chunk_info = Some(ChunkInfo {
                    total_chunks: 2,
                    chunk_index: index,
                    chunk_offset: index,
                });
                EntityChunk {
                    header,
                    payload: b"x".to_vec(),
                }
            })
            .collect::<Vec<_>>();
        tokio::time::timeout(
            Duration::from_secs(3),
            client.send_chunked_entity(&chunks, 0),
        )
        .await?
    } else {
        tokio::time::timeout(Duration::from_secs(3), client.send_entity(&header, b"x", 0)).await?
    };
    if refused {
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("PIPESTREAM_LIMIT_EXCEEDED"),
            "{error}"
        );
        let closed = server.await??;
        assert!(
            matches!(closed, quinn::ConnectionError::ApplicationClosed(error) if error.error_code == ERROR_LIMIT_EXCEEDED.into())
        );
    } else {
        assert_eq!(
            result?, expected,
            "accepted history must not be clipped or rewritten"
        );
        client.disconnect_gracefully().await;
        assert!(
            matches!(server.await??, quinn::ConnectionError::ApplicationClosed(error) if error.error_code == ERROR_NO_ERROR.into())
        );
        return Ok(());
    }
    client.disconnect_gracefully().await;
    Ok(())
}

#[tokio::test]
async fn count_limit_keeps_the_terminal_boundary_and_refuses_longer_history() -> Result<()> {
    for sealed in [false, true] {
        for chunked in [false, true] {
            for total in [128, 130] {
                let frames = (0..total)
                    .map(|index| {
                        status(
                            if index == total - 1 {
                                STATUS_COMPLETE
                            } else if index % 2 == 0 {
                                STATUS_PROCESSING
                            } else {
                                STATUS_CHECKPOINT
                            },
                            21,
                        )
                    })
                    .collect();
                exchange(frames, sealed, chunked, total > 128).await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn exhausted_nonterminal_history_refuses_without_another_frame() -> Result<()> {
    for sealed in [false, true] {
        let frames = (0..128)
            .map(|index| {
                status(
                    if index % 2 == 0 {
                        STATUS_PROCESSING
                    } else {
                        STATUS_CHECKPOINT
                    },
                    21,
                )
            })
            .collect();
        exchange(frames, sealed, false, true).await?;
    }
    Ok(())
}

#[tokio::test]
async fn byte_limit_counts_extensions_and_terminal_frame_without_clipping() -> Result<()> {
    for sealed in [false, true] {
        for chunked in [false, true] {
            for extra in [0, 1] {
                let sizes = [1 << 20, 1 << 20, 1 << 20, (1 << 20) - 42 + extra, 21, 21];
                assert_eq!(sizes.iter().sum::<usize>(), (4 << 20) + extra);
                let frames = sizes
                    .into_iter()
                    .enumerate()
                    .map(|(index, bytes)| {
                        status(
                            if index == 5 {
                                STATUS_COMPLETE
                            } else if index % 2 == 0 {
                                STATUS_PROCESSING
                            } else {
                                STATUS_CHECKPOINT
                            },
                            bytes,
                        )
                    })
                    .collect();
                exchange(frames, sealed, chunked, extra != 0).await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn large_yield_token_and_claim_check_remain_exact() -> Result<()> {
    let mut yielded = status(STATUS_YIELDED, 21);
    yielded.extension = Some(StatusExtension::Yield {
        reason: 1,
        token: vec![0x5a; MAX_CONTROL_FRAME - 24],
    });
    let mut deferred = status(STATUS_DEFERRED, 21);
    deferred.extension = Some(StatusExtension::ClaimCheck {
        claim_id: 7,
        expiry_timestamp_micros: 99,
    });
    exchange(
        vec![status(STATUS_PROCESSING, 21), yielded, deferred],
        false,
        false,
        false,
    )
    .await
}
