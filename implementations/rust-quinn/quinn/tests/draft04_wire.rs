use anyhow::{Result, bail};
use pipestream_core::persistence::SqliteSessionStore;
use pipestream_core::*;
use pipestream_quic::recursive::*;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::{collections::BTreeMap, fs, sync::Arc, time::Duration};

// Raw QUIC peers exercise ordering and refusal behavior independently of RecursiveClient.
struct Fixture {
    dir: tempfile::TempDir,
    options: RecursiveServerOptions,
    endpoint: quinn::Endpoint,
    connection: quinn::Connection,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    server: tokio::task::JoinHandle<Result<()>>,
}

fn options(dir: &std::path::Path) -> RecursiveServerOptions {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(["localhost".to_owned()]).unwrap();
    let certificate = dir.join("server.crt");
    let private_key = dir.join("server.key");
    fs::write(&certificate, cert.pem()).unwrap();
    fs::write(&private_key, signing_key.serialize_pem()).unwrap();
    RecursiveServerOptions {
        bind: "127.0.0.1:0".parse().unwrap(),
        certificate,
        private_key,
        state_database: dir.join("sessions.sqlite3"),
        entity_directory: dir.join("entities"),
        ready_file: None,
        once: true,
        max_scope_depth: 7,
        max_entities_per_scope: 100,
        max_entity_bytes: 1024,
        max_chunks_per_entity: 16,
        max_concurrent_connections: 4,
    }
}

async fn read(recv: &mut quinn::RecvStream) -> Result<(u8, Vec<u8>)> {
    let mut header = [0; 5];
    recv.read_exact(&mut header).await?;
    let size = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
    if size > 65536 {
        bail!("diagnostic frame too large");
    }
    let mut body = vec![0; size];
    recv.read_exact(&mut body).await?;
    Ok((header[0], body))
}

fn offer(layers: LayerSupport, depth: u8) -> Capabilities {
    Capabilities {
        layer1_recursive: layers.layer1_recursive,
        layer2_resilience: layers.layer2_resilience,
        max_scope_depth: Some(depth),
        max_entities_per_scope: Some(100),
        ..Capabilities::default()
    }
}

impl Fixture {
    async fn new(layers: LayerSupport, depth: u8) -> Result<Self> {
        Self::with_processor(layers, depth, Arc::new(ExemplarProcessor::default())).await
    }

    async fn with_processor<P: EntityProcessor>(
        layers: LayerSupport,
        depth: u8,
        processor: Arc<P>,
    ) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let options = options(dir.path());
        let service = RecursiveService::with_limits(
            Arc::new(SqliteSessionStore::open(&options.state_database)?),
            Arc::new(FileEntityStore::open(&options.entity_directory)?),
            processor,
            RecursiveLimits {
                max_scope_depth: 7,
                max_entities_per_scope: 100,
                max_entity_bytes: options.max_entity_bytes,
                max_chunks_per_entity: options.max_chunks_per_entity,
            },
        )?;
        let server = RecursiveServer::bind(&options, service)?;
        let address = server.local_addr()?;
        let server = tokio::spawn(server.run(true));
        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_file_iter(&options.certificate)? {
            roots.add(cert?)?;
        }
        let mut tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        tls.alpn_protocols = vec![ALPN.to_vec()];
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse()?)?;
        endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(tls)?,
        )));
        let connection = endpoint.connect(address, "localhost")?.await?;
        let (mut send, mut recv) = connection.open_bi().await?;
        send.write_all(&encode_capabilities(&offer(layers, depth))?)
            .await?;
        let (kind, body) = read(&mut recv).await?;
        assert_eq!(kind, FRAME_CAPABILITIES);
        assert_eq!(
            decode_capabilities(&body)?.max_scope_depth,
            layers.layer1_recursive.then_some(depth)
        );
        assert_eq!(read(&mut recv).await?.0, FRAME_STATUS);
        Ok(Self {
            dir,
            options,
            endpoint,
            connection,
            send,
            recv,
            server,
        })
    }

    async fn announce(
        &mut self,
        id: u32,
        scope: u32,
        depth: u8,
        layers: LayerSupport,
    ) -> Result<()> {
        self.send
            .write_all(&encode_status_frame(
                &StatusFrame {
                    status: Status {
                        state: STATUS_PENDING,
                        entity_id: id,
                        scope_id: scope,
                        depth,
                        cursor: None,
                    },
                    extension: None,
                },
                layers,
            )?)
            .await?;
        Ok(())
    }

    async fn entity(&self, id: u32, child: bool, action: &str, layers: LayerSupport) -> Result<()> {
        let header = EntityHeader {
            entity_id: id,
            parent_id: child.then_some(1),
            scope_id: child.then_some(1),
            parent_scope_id: child.then_some(0),
            layer: 0,
            content_type: None,
            payload_length: Some(1),
            checksum: None,
            metadata: BTreeMap::from([
                (SESSION_METADATA_KEY.to_owned(), "review-session".to_owned()),
                (ACTION_METADATA_KEY.to_owned(), action.to_owned()),
            ]),
            chunk_info: None,
            completion_policy: None,
        };
        let mut stream = self.connection.open_uni().await?;
        stream
            .write_all(&encode_entity_for(&header, b"x", layers)?)
            .await?;
        stream.finish()?;
        Ok(())
    }

    async fn statuses(&mut self, count: usize) -> Result<Vec<u8>> {
        let mut states = Vec::new();
        for _ in 0..count {
            let (kind, body) =
                tokio::time::timeout(Duration::from_secs(2), read(&mut self.recv)).await??;
            assert_eq!(kind, FRAME_STATUS);
            states.push(
                decode_status_frame(&body, LayerSupport::LAYER2)?
                    .status
                    .state,
            );
        }
        Ok(states)
    }

    async fn refused(&self) -> Result<String> {
        let reason = tokio::time::timeout(Duration::from_secs(2), self.connection.closed()).await?;
        Ok(reason.to_string())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.connection.close(0u32.into(), b"review complete");
        self.endpoint.close(0u32.into(), b"review complete");
        self.server.abort();
        let _ = (&self.dir, &self.options);
    }
}

#[tokio::test]
async fn r3_r4_unknown_frame_and_entity_without_pending() -> Result<()> {
    let mut peer = Fixture::new(LayerSupport::LAYER2, 7).await?;
    peer.send.write_all(&encode_ucf(0xe0, &[1, 2, 3])?).await?;
    peer.entity(1, false, "complete", LayerSupport::LAYER2)
        .await?;
    assert_eq!(
        peer.statuses(2).await?,
        [STATUS_PROCESSING, STATUS_COMPLETE]
    );
    Ok(())
}

#[tokio::test]
async fn r5_announcements_do_not_order_entity_streams() -> Result<()> {
    let mut peer = Fixture::new(LayerSupport::LAYER2, 7).await?;
    peer.entity(1, false, "dehydrate", LayerSupport::LAYER2)
        .await?;
    peer.statuses(2).await?;
    peer.announce(1, 1, 1, LayerSupport::LAYER2).await?;
    peer.announce(2, 1, 1, LayerSupport::LAYER2).await?;
    peer.entity(2, true, "complete", LayerSupport::LAYER2)
        .await?;
    assert_eq!(
        peer.statuses(2).await?,
        [STATUS_PROCESSING, STATUS_COMPLETE]
    );
    peer.entity(1, true, "complete", LayerSupport::LAYER2)
        .await?;
    assert_eq!(
        peer.statuses(2).await?,
        [STATUS_PROCESSING, STATUS_COMPLETE]
    );
    Ok(())
}

#[tokio::test]
async fn stalled_stream_does_not_block_another_entity() -> Result<()> {
    let mut peer = Fixture::new(LayerSupport::LAYER2, 7).await?;
    peer.entity(1, false, "dehydrate", LayerSupport::LAYER2)
        .await?;
    peer.statuses(2).await?;
    let mut stalled = peer.connection.open_uni().await?;
    stalled.write_all(&[0]).await?;
    peer.entity(2, true, "complete", LayerSupport::LAYER2)
        .await?;
    assert_eq!(
        peer.statuses(2).await?,
        [STATUS_PROCESSING, STATUS_COMPLETE]
    );
    Ok(())
}

fn checkpoint(timeout_ms: u64) -> Checkpoint {
    Checkpoint {
        checkpoint_id: "root-cut".into(),
        sequence_number: 1,
        checkpoint_entity_id: 2,
        scope_id: None,
        flags: 0,
        timeout_ms: Some(timeout_ms),
    }
}

#[tokio::test]
async fn r6_pending_checkpoint_allows_descendants_then_acknowledges() -> Result<()> {
    let mut peer = Fixture::new(LayerSupport::LAYER2, 7).await?;
    peer.entity(1, false, "dehydrate", LayerSupport::LAYER2)
        .await?;
    peer.statuses(2).await?;
    let request = checkpoint(2000);
    peer.send
        .write_all(&encode_checkpoint_for(&request, LayerSupport::LAYER2)?)
        .await?;
    peer.entity(1, true, "complete", LayerSupport::LAYER2)
        .await?;
    assert_eq!(
        peer.statuses(2).await?,
        [STATUS_PROCESSING, STATUS_COMPLETE]
    );
    let digest = ScopeDigest {
        scope_id: 1,
        entities_processed: 1,
        entities_succeeded: 1,
        entities_failed: 0,
        entities_deferred: 0,
        merkle_root: pipestream_core::session::merkle_root(&[(
            1,
            pipestream_core::session::EntityState::Complete,
        )])?,
    };
    peer.send.write_all(&encode_scope_digest(&digest)?).await?;
    assert_eq!(read(&mut peer.recv).await?.0, FRAME_SCOPE_DIGEST);
    assert_eq!(
        peer.statuses(2).await?,
        [STATUS_REHYDRATING, STATUS_COMPLETE]
    );
    let (kind, payload) =
        tokio::time::timeout(Duration::from_secs(2), read(&mut peer.recv)).await??;
    assert_eq!(kind, FRAME_CHECKPOINT);
    let mut expected = request;
    expected.flags = CHECKPOINT_ACK;
    assert_eq!(decode_checkpoint(&payload)?, expected);
    Ok(())
}

#[tokio::test]
async fn checkpoint_timeout_is_named_and_does_not_acknowledge() -> Result<()> {
    let mut peer = Fixture::new(LayerSupport::LAYER2, 7).await?;
    peer.entity(1, false, "dehydrate", LayerSupport::LAYER2)
        .await?;
    peer.statuses(2).await?;
    peer.send
        .write_all(&encode_checkpoint(&checkpoint(20))?)
        .await?;
    assert!(
        peer.refused()
            .await?
            .contains("PIPESTREAM_CHECKPOINT_TIMEOUT")
    );
    Ok(())
}

#[tokio::test]
async fn r7_negotiated_depth_is_enforced_before_payload_storage() -> Result<()> {
    let mut peer = Fixture::new(LayerSupport::LAYER2, 0).await?;
    peer.entity(1, false, "dehydrate", LayerSupport::LAYER2)
        .await?;
    peer.statuses(2).await?;
    peer.entity(1, true, "complete", LayerSupport::LAYER2)
        .await?;
    assert!(peer.refused().await?.contains("PIPESTREAM_DEPTH_EXCEEDED"));
    use pipestream_core::persistence::SessionStore;
    let store = SqliteSessionStore::open(&peer.options.state_database)?;
    let session = store.load("review-session")?.unwrap().session;
    assert_eq!(session.entities.len(), 1);
    assert!(
        !peer
            .options
            .entity_directory
            .join("review-session/scope-1/entity-1.bin")
            .exists()
    );
    Ok(())
}

#[derive(Default)]
struct CountingProcessor {
    processed: std::sync::atomic::AtomicUsize,
    rehydrated: std::sync::atomic::AtomicUsize,
}

impl EntityProcessor for CountingProcessor {
    fn process(&self, context: ProcessContext<'_>) -> ProcessingDisposition {
        self.processed
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ExemplarProcessor::default().process(context)
    }
    fn rehydrate(&self, context: RehydrateContext<'_>) -> [u8; 32] {
        self.rehydrated
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ExemplarProcessor::default().rehydrate(context)
    }
    fn resume(&self, _: ResumeContext<'_>) -> [u8; 32] {
        unreachable!()
    }
}

#[tokio::test]
async fn invalid_parent_is_refused_before_application_callback() -> Result<()> {
    let processor = Arc::new(CountingProcessor::default());
    let peer = Fixture::with_processor(LayerSupport::LAYER2, 7, processor.clone()).await?;
    peer.entity(1, true, "complete", LayerSupport::LAYER2)
        .await?;
    assert!(peer.refused().await?.contains("PIPESTREAM_ENTITY_INVALID"));
    assert_eq!(
        processor
            .processed
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    Ok(())
}

#[tokio::test]
async fn invalid_scope_digest_is_refused_before_rehydration_callback() -> Result<()> {
    let processor = Arc::new(CountingProcessor::default());
    let mut peer = Fixture::with_processor(LayerSupport::LAYER2, 7, processor.clone()).await?;
    peer.entity(1, false, "dehydrate", LayerSupport::LAYER2)
        .await?;
    peer.statuses(2).await?;
    peer.entity(1, true, "complete", LayerSupport::LAYER2)
        .await?;
    peer.statuses(2).await?;
    let digest = ScopeDigest {
        scope_id: 1,
        entities_processed: 1,
        entities_succeeded: 1,
        entities_failed: 0,
        entities_deferred: 0,
        merkle_root: [0xa5; 32],
    };
    peer.send.write_all(&encode_scope_digest(&digest)?).await?;
    assert!(peer.refused().await?.contains("PIPESTREAM_INTEGRITY_ERROR"));
    assert_eq!(
        processor
            .rehydrated
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    Ok(())
}

#[tokio::test]
async fn chunks_are_charged_to_an_aggregate_payload_limit() -> Result<()> {
    let peer = Fixture::new(LayerSupport::LAYER2, 7).await?;
    let payload = vec![42; 600];
    let encoded = pipestream_core::entity(1, &payload, "application/octet-stream")?;
    let (mut header, _) = decode_entity(&encoded)?;
    header
        .metadata
        .insert(SESSION_METADATA_KEY.into(), "review-session".into());
    for index in 0..2 {
        header.chunk_info = Some(ChunkInfo {
            total_chunks: 2,
            chunk_index: index,
            chunk_offset: index * 600,
        });
        let mut stream = peer.connection.open_uni().await?;
        stream
            .write_all(&encode_entity_for(&header, &payload, LayerSupport::LAYER2)?)
            .await?;
        stream.finish()?;
    }
    assert!(peer.refused().await?.contains("PIPESTREAM_LIMIT_EXCEEDED"));
    Ok(())
}

#[tokio::test]
async fn r8_layer0_never_receives_layer2_statuses() -> Result<()> {
    let peer = Fixture::new(LayerSupport::LAYER0, 0).await?;
    peer.entity(1, false, "yield", LayerSupport::LAYER0).await?;
    assert!(
        peer.refused()
            .await?
            .contains("PIPESTREAM_LAYER_UNSUPPORTED")
    );
    Ok(())
}
#[tokio::test]
async fn r9_mismatched_checkpoint_ack_is_refused() -> Result<()> {
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
    let server = tokio::spawn(async move {
        let connection = endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        read(&mut recv).await.unwrap();
        send.write_all(&encode_capabilities(&offer(LayerSupport::LAYER2, 7)).unwrap())
            .await
            .unwrap();
        send.write_all(
            &encode_status(Status {
                state: STATUS_UNSPECIFIED,
                entity_id: CONNECTION_LEVEL,
                scope_id: 0,
                depth: 0,
                cursor: None,
            })
            .unwrap(),
        )
        .await
        .unwrap();
        let (_, bytes) = read(&mut recv).await.unwrap();
        let mut ack = decode_checkpoint_for(&bytes, LayerSupport::LAYER2).unwrap();
        ack.checkpoint_id = "WRONG-CHECKPOINT".to_owned();
        ack.sequence_number += 100;
        ack.flags = CHECKPOINT_ACK;
        send.write_all(&encode_checkpoint_for(&ack, LayerSupport::LAYER2).unwrap())
            .await
            .unwrap();
        connection.closed().await;
    });
    let mut client = RecursiveClient::connect(&RecursiveClientOptions {
        remote: address,
        ca_certificate: options.certificate,
        server_name: "localhost".to_owned(),
    })
    .await?;
    let error = client
        .checkpoint(&Checkpoint {
            checkpoint_id: "correct".to_owned(),
            sequence_number: 1,
            checkpoint_entity_id: 2,
            scope_id: None,
            flags: 0,
            timeout_ms: None,
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("PIPESTREAM_ENTITY_INVALID"));
    client.disconnect();
    server.abort();
    Ok(())
}
