use super::*;
use pipestream_core::session::EntityKey;

const PARENT: EntityKey = EntityKey {
    scope_id: 7,
    entity_id: 11,
};
const DEPTH: u8 = 2;

fn parent_status(state: u8) -> StatusFrame {
    StatusFrame {
        status: Status {
            state,
            entity_id: PARENT.entity_id,
            scope_id: PARENT.scope_id,
            cursor: None,
            depth: DEPTH,
        },
        extension: None,
    }
}

// This peer injects closure replies only. Real-service tests cover admission
// and descendant resolution; an echoed digest here proves neither.
async fn exchange(frames: Vec<StatusFrame>, sealed: bool, refused: bool) -> Result<()> {
    exchange_with_cut(frames, sealed, refused, None).await
}

async fn exchange_with_cut(
    frames: Vec<StatusFrame>,
    sealed: bool,
    refused: bool,
    cut: Option<(usize, usize)>,
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
    let (partial_sent, partial_received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let connection = endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let (kind, bytes) = read(&mut recv).await.unwrap();
        assert_eq!(kind, FRAME_CAPABILITIES);
        let caps = decode_capabilities(&bytes).unwrap();
        send.write_all(&encode_capabilities(&caps).unwrap())
            .await
            .unwrap();
        let (kind, bytes) = read(&mut recv).await.unwrap();
        assert_eq!(kind, FRAME_SCOPE_DIGEST);
        let mut replies = vec![encode_scope_digest(&decode_scope_digest(&bytes).unwrap()).unwrap()];
        for frame in frames {
            replies.push(
                encode_status_frame(
                    &frame,
                    if sealed {
                        LayerSupport::LAYER1
                    } else {
                        LayerSupport::LAYER2
                    },
                )
                .unwrap(),
            );
        }
        for (index, bytes) in replies.into_iter().enumerate() {
            if let Some((frame, prefix)) = cut
                && frame == index
            {
                assert!(prefix < bytes.len());
                send.write_all(&bytes[..prefix]).await.unwrap();
                partial_sent.send(()).unwrap();
                return tokio::time::timeout(Duration::from_secs(3), connection.closed()).await;
            }
            if let Err(error) = send.write_all(&bytes).await {
                assert!(refused, "unexpected write failure: {error}");
                break;
            }
        }
        tokio::time::timeout(Duration::from_secs(3), connection.closed()).await
    });
    let options = RecursiveClientOptions {
        remote: address,
        ca_certificate: options.certificate,
        server_name: "localhost".into(),
        identity: None,
    };
    let mut client = if sealed {
        RecursiveClient::connect_sealed(&options).await?
    } else {
        RecursiveClient::connect(&options).await?
    };
    let digest = ScopeDigest {
        scope_id: 19,
        entities_processed: 1,
        entities_succeeded: 1,
        entities_failed: 0,
        entities_deferred: 0,
        merkle_root: [7; 32],
    };
    // All local refusals must leave the peer waiting for the first valid digest.
    for (scope_id, parent, depth) in [
        (0, PARENT, DEPTH),
        (PARENT.scope_id, PARENT, DEPTH),
        (
            19,
            EntityKey {
                scope_id: 7,
                entity_id: 0,
            },
            DEPTH,
        ),
        (19, PARENT, 7),
        (19, PARENT, 0),
    ] {
        let mut invalid = digest.clone();
        invalid.scope_id = scope_id;
        assert!(client.close_scope(&invalid, parent, depth).await.is_err());
    }
    if cut.is_some() {
        {
            let operation = client.close_scope(&digest, PARENT, DEPTH);
            tokio::pin!(operation);
            tokio::select! {
                result = &mut operation => panic!("incomplete closure returned: {result:?}"),
                result = partial_received => result?,
                _ = tokio::time::sleep(Duration::from_secs(3)) => panic!("peer did not send partial frame"),
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut operation)
                    .await
                    .is_err()
            );
        }
        assert!(
            matches!(server.await??, quinn::ConnectionError::ApplicationClosed(error) if error.error_code == ERROR_NO_ERROR.into())
        );
        assert!(
            client
                .close_scope(&digest, PARENT, DEPTH)
                .await
                .unwrap_err()
                .to_string()
                .contains("connection is closed")
        );
        client.disconnect_gracefully().await;
        return Ok(());
    }
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        client.close_scope(&digest, PARENT, DEPTH),
    )
    .await?;
    if refused {
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("PIPESTREAM_ENTITY_INVALID"),
            "{error}"
        );
        assert!(
            matches!(server.await??, quinn::ConnectionError::ApplicationClosed(error) if error.error_code == ERROR_ENTITY_INVALID.into())
        );
    } else {
        assert_eq!(result?, (digest, expected));
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
async fn scope_parent_mismatch_is_refused_on_each_status() -> Result<()> {
    for sealed in [false, true] {
        for index in 0..2 {
            for field in ["entity", "scope", "depth"] {
                let mut frames = vec![
                    parent_status(STATUS_REHYDRATING),
                    parent_status(STATUS_COMPLETE),
                ];
                let status = &mut frames[index].status;
                match field {
                    "entity" => status.entity_id += 1,
                    "scope" => status.scope_id += 1,
                    "depth" => status.depth += 1,
                    _ => unreachable!(),
                }
                exchange(frames, sealed, true).await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn matching_scope_parent_completion_is_preserved() -> Result<()> {
    for sealed in [false, true] {
        exchange(
            vec![
                parent_status(STATUS_REHYDRATING),
                parent_status(STATUS_COMPLETE),
            ],
            sealed,
            false,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn scope_parent_failure_is_preserved_without_waiting_for_completion() -> Result<()> {
    for sealed in [false, true] {
        exchange(vec![parent_status(STATUS_FAILED)], sealed, false).await?;
        exchange(
            vec![
                parent_status(STATUS_REHYDRATING),
                parent_status(STATUS_FAILED),
            ],
            sealed,
            false,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn sealed_scope_cursor_and_invalid_lifecycle_are_named_refusals() -> Result<()> {
    for index in 0..2 {
        let mut frames = vec![
            parent_status(STATUS_REHYDRATING),
            parent_status(STATUS_COMPLETE),
        ];
        frames[index].status = Status {
            state: STATUS_UNSPECIFIED,
            entity_id: CONNECTION_LEVEL,
            scope_id: 0,
            cursor: Some(1),
            depth: 0,
        };
        exchange(frames, true, true).await?;
    }
    for sealed in [false, true] {
        exchange(vec![parent_status(STATUS_COMPLETE)], sealed, true).await?;
        exchange(
            vec![
                parent_status(STATUS_REHYDRATING),
                parent_status(STATUS_PROCESSING),
            ],
            sealed,
            true,
        )
        .await?;
        let mut failed = parent_status(STATUS_FAILED);
        failed.status.entity_id += 1;
        exchange(vec![failed], sealed, true).await?;
    }
    Ok(())
}

#[tokio::test]
async fn cancelling_each_partial_scope_reply_closes_without_completion() -> Result<()> {
    for frame in 0..3 {
        for prefix in [0, 2, 7] {
            exchange_with_cut(
                vec![
                    parent_status(STATUS_REHYDRATING),
                    parent_status(STATUS_COMPLETE),
                ],
                true,
                false,
                Some((frame, prefix)),
            )
            .await?;
        }
    }
    Ok(())
}
