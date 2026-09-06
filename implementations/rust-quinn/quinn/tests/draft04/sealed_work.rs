use super::*;
use pipestream_core::{
    persistence::SessionStore,
    session::{EntityKey, EntityState},
    work_set::{self, WorkSetFrame},
};

pub(super) fn declaration(
    scope: u32,
    seq: u64,
    ids: &[u32],
    sealed: Option<&[u32]>,
) -> WorkSetFrame {
    let parent = (scope != 0).then_some(EntityKey {
        scope_id: 0,
        entity_id: 1,
    });
    WorkSetFrame {
        session_id: "review-session".into(),
        producer_id: [1; 16],
        scope_id: scope,
        parent,
        sequence: seq,
        entity_ids: ids.to_vec(),
        flags: if sealed.is_some() { work_set::SEAL } else { 0 },
        seal_digest: sealed.map(|all| {
            work_set::seal_digest(
                "review-session",
                [1; 16],
                scope,
                parent,
                &all.iter().copied().collect(),
            )
        }),
    }
}

async fn fixture() -> Result<Fixture> {
    let mut caps = offer(LayerSupport::LAYER1, 7);
    caps.extensions.supported = vec![work_set::EXTENSION_SEALED_WORK_SETS];
    caps.extensions.required = caps.extensions.supported.clone();
    Fixture::with_capabilities(caps, Arc::new(ExemplarProcessor::default())).await
}

pub(super) async fn declare(peer: &mut Fixture, frame: &WorkSetFrame) -> Result<()> {
    peer.send.write_all(&work_set::encode(frame)?).await?;
    let (kind, body) = tokio::time::timeout(Duration::from_secs(2), read(&mut peer.recv)).await??;
    assert_eq!(kind, work_set::FRAME_WORK_SET);
    let mut expected = frame.clone();
    expected.flags |= work_set::ACK;
    assert_eq!(work_set::decode(&body)?, expected);
    Ok(())
}

#[tokio::test]
async fn sealed_checkpoint_waits_for_missing_entity_and_seal_over_quic() -> Result<()> {
    let mut peer = fixture().await?;
    declare(&mut peer, &declaration(0, 0, &[1, 2], None)).await?;
    peer.entity(1, false, "complete", LayerSupport::LAYER1)
        .await?;
    peer.statuses(2).await?;
    let request = checkpoint(2000);
    peer.send.write_all(&encode_checkpoint(&request)?).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), read(&mut peer.recv))
            .await
            .is_err()
    );
    peer.entity(2, false, "complete", LayerSupport::LAYER1)
        .await?;
    peer.statuses(2).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), read(&mut peer.recv))
            .await
            .is_err()
    );
    declare(&mut peer, &declaration(0, 1, &[], Some(&[1, 2]))).await?;
    let (kind, body) = tokio::time::timeout(Duration::from_secs(2), read(&mut peer.recv)).await??;
    assert_eq!(kind, FRAME_CHECKPOINT);
    let mut ack = request;
    ack.flags = CHECKPOINT_ACK;
    assert_eq!(decode_checkpoint(&body)?, ack);
    peer.send.write_all(&encode_goaway(2)?).await?;
    assert_eq!(read(&mut peer.recv).await?.0, FRAME_GOAWAY);
    let store = SqliteSessionStore::open(&peer.options.state_database)?;
    let s = store.load("review-session")?.unwrap().session;
    assert!(s.work_scope_ready(0));
    assert!(s.checkpoints[&(0, 1)].acknowledged);
    Ok(())
}

#[tokio::test]
async fn sealed_descendants_rehydrate_and_root_checkpoint_accounts_for_them() -> Result<()> {
    let mut peer = fixture().await?;
    declare(&mut peer, &declaration(0, 0, &[1], Some(&[1]))).await?;
    peer.entity(1, false, "dehydrate", LayerSupport::LAYER1)
        .await?;
    peer.statuses(2).await?;
    declare(&mut peer, &declaration(1, 0, &[1, 2], Some(&[1, 2]))).await?;
    let mut cp = checkpoint(2000);
    cp.checkpoint_entity_id = 1;
    peer.send.write_all(&encode_checkpoint(&cp)?).await?;
    // Payload arrival order is independent of declaration order.
    for id in [2, 1] {
        peer.entity(id, true, "complete", LayerSupport::LAYER1)
            .await?;
        peer.statuses(2).await?;
    }
    let store = SqliteSessionStore::open(&peer.options.state_database)?;
    let s = store.load("review-session")?.unwrap().session;
    assert!(!s.checkpoint_satisfied(0, 1)?);
    peer.send
        .write_all(&encode_scope_digest(&s.scope_digest(1)?)?)
        .await?;
    assert_eq!(read(&mut peer.recv).await?.0, FRAME_SCOPE_DIGEST);
    assert_eq!(
        peer.statuses(2).await?,
        [STATUS_REHYDRATING, STATUS_COMPLETE]
    );
    assert_eq!(read(&mut peer.recv).await?.0, FRAME_CHECKPOINT);
    let s = store.load("review-session")?.unwrap().session;
    assert!(s.checkpoints[&(0, 1)].acknowledged);
    assert_eq!(
        s.entities[&EntityKey {
            scope_id: 0,
            entity_id: 1
        }]
            .state,
        EntityState::Complete
    );
    Ok(())
}

#[tokio::test]
async fn unsealed_digest_and_undeclared_entities_are_named_refusals() -> Result<()> {
    let mut peer = fixture().await?;
    declare(&mut peer, &declaration(0, 0, &[1], Some(&[1]))).await?;
    peer.entity(2, false, "complete", LayerSupport::LAYER1)
        .await?;
    assert!(peer.refused().await?.contains("PIPESTREAM_ENTITY_INVALID"));
    assert!(
        !peer
            .options
            .entity_directory
            .join("review-session/scope-0/entity-2.bin")
            .exists()
    );
    let store = SqliteSessionStore::open(&peer.options.state_database)?;
    assert!(
        store
            .load("review-session")?
            .unwrap()
            .session
            .entities
            .is_empty()
    );

    let mut peer = fixture().await?;
    declare(&mut peer, &declaration(0, 0, &[1], Some(&[1]))).await?;
    peer.entity(1, false, "dehydrate", LayerSupport::LAYER1)
        .await?;
    peer.statuses(2).await?;
    declare(&mut peer, &declaration(1, 0, &[1], None)).await?;
    peer.entity(1, true, "complete", LayerSupport::LAYER1)
        .await?;
    peer.statuses(2).await?;
    let digest = ScopeDigest {
        scope_id: 1,
        entities_processed: 1,
        entities_succeeded: 1,
        entities_failed: 0,
        entities_deferred: 0,
        merkle_root: pipestream_core::session::merkle_root(&[(1, EntityState::Complete)])?,
    };
    peer.send.write_all(&encode_scope_digest(&digest)?).await?;
    assert!(peer.refused().await?.contains("PIPESTREAM_SCOPE_INVALID"));
    Ok(())
}

#[tokio::test]
async fn work_set_requires_negotiation_and_refuses_late_ids_bad_seals_and_early_goaway()
-> Result<()> {
    let mut peer = Fixture::new(LayerSupport::LAYER1, 7).await?;
    peer.send
        .write_all(&work_set::encode(&declaration(0, 0, &[1], Some(&[1])))?)
        .await?;
    assert!(
        peer.refused()
            .await?
            .contains("PIPESTREAM_EXTENSION_UNSUPPORTED")
    );
    for case in 0..3 {
        let mut peer = fixture().await?;
        declare(&mut peer, &declaration(0, 0, &[1], Some(&[1]))).await?;
        if case == 0 {
            peer.send
                .write_all(&work_set::encode(&declaration(0, 1, &[2], None))?)
                .await?;
        } else if case == 1 {
            peer.send.write_all(&encode_goaway(1)?).await?;
        } else {
            let mut changed = declaration(0, 0, &[1], Some(&[1]));
            changed.producer_id = [2; 16];
            peer.send.write_all(&work_set::encode(&changed)?).await?;
        }
        assert!(peer.refused().await?.contains("PIPESTREAM_ENTITY_INVALID"));
    }
    let mut peer = fixture().await?;
    let mut bad = declaration(0, 0, &[1], Some(&[1]));
    bad.seal_digest = Some([0; 32]);
    peer.send.write_all(&work_set::encode(&bad)?).await?;
    assert!(peer.refused().await?.contains("PIPESTREAM_INTEGRITY_ERROR"));
    Ok(())
}

#[tokio::test]
async fn pending_requires_a_declared_identity_and_depth() -> Result<()> {
    for case in 0..3 {
        let mut peer = fixture().await?;
        if case != 0 {
            declare(&mut peer, &declaration(0, 0, &[1], Some(&[1]))).await?;
        }
        if case == 2 {
            peer.entity(1, false, "dehydrate", LayerSupport::LAYER1)
                .await?;
            peer.statuses(2).await?;
            declare(&mut peer, &declaration(1, 0, &[1], Some(&[1]))).await?;
        }
        peer.announce(
            if case == 1 { 2 } else { 1 },
            u32::from(case == 2),
            if case == 2 { 2 } else { 0 },
            LayerSupport::LAYER1,
        )
        .await?;
        assert!(peer.refused().await?.contains("PIPESTREAM_ENTITY_INVALID"));
        let store = SqliteSessionStore::open(&peer.options.state_database)?;
        assert!(
            store
                .load("review-session")?
                .is_none_or(|s| s.session.entities.len() == usize::from(case == 2))
        );
    }
    Ok(())
}

#[tokio::test]
async fn sealed_announcements_are_bounded_and_cannot_recycle_ids() -> Result<()> {
    let mut caps = offer(LayerSupport::LAYER1, 7);
    caps.max_window_size = 1;
    caps.extensions.supported = vec![work_set::EXTENSION_SEALED_WORK_SETS];
    caps.extensions.required = caps.extensions.supported.clone();
    let mut peer = Fixture::with_capabilities(caps, Arc::new(ExemplarProcessor::default())).await?;
    declare(&mut peer, &declaration(0, 0, &[1, 2], Some(&[1, 2]))).await?;
    peer.announce(1, 0, 0, LayerSupport::LAYER1).await?;
    peer.announce(2, 0, 0, LayerSupport::LAYER1).await?;
    assert!(peer.refused().await?.contains("PIPESTREAM_LIMIT_EXCEEDED"));

    let mut peer = fixture().await?;
    declare(&mut peer, &declaration(0, 0, &[1], Some(&[1]))).await?;
    peer.send
        .write_all(&encode_status(Status {
            state: STATUS_UNSPECIFIED,
            entity_id: CONNECTION_LEVEL,
            scope_id: 0,
            depth: 0,
            cursor: Some(1),
        })?)
        .await?;
    assert!(peer.refused().await?.contains("PIPESTREAM_ENTITY_INVALID"));
    Ok(())
}

#[tokio::test]
async fn public_client_replays_an_unobserved_seal_ack_after_server_restart() -> Result<()> {
    let mut peer = fixture().await?;
    let request = declaration(0, 0, &[1], Some(&[1]));
    // Do not read the ACK. Restart after the declaration's durable commit.
    peer.send.write_all(&work_set::encode(&request)?).await?;
    let store = Arc::new(SqliteSessionStore::open(&peer.options.state_database)?);
    let before = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(session) = store.load("review-session")? {
                break Result::<_>::Ok(session.session.work_sets.unwrap());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    peer.server.abort();
    let _ = (&mut peer.server).await;
    let service = RecursiveService::with_limits(
        store.clone(),
        Arc::new(FileEntityStore::open(&peer.options.entity_directory)?),
        Arc::new(ExemplarProcessor::default()),
        RecursiveLimits {
            max_scope_depth: 7,
            max_entities_per_scope: 100,
            max_entity_bytes: 1024,
            max_chunks_per_entity: 16,
        },
    )?;
    let server = RecursiveServer::bind(&peer.options, service)?;
    let client_options = RecursiveClientOptions {
        identity: None,
        remote: server.local_addr()?,
        ca_certificate: peer.options.certificate.clone(),
        server_name: "localhost".into(),
    };
    // The fixture owns and aborts this replacement server even if an assertion fails.
    peer.server = tokio::spawn(server.run(true));
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut client = RecursiveClient::connect_sealed(&client_options).await?;
        client.declare_work(&request).await?;
        let header = EntityHeader {
            entity_id: 1,
            parent_id: None,
            scope_id: None,
            parent_scope_id: None,
            layer: 0,
            content_type: None,
            payload_length: Some(1),
            checksum: None,
            metadata: BTreeMap::from([
                (SESSION_METADATA_KEY.to_owned(), "review-session".to_owned()),
                (ACTION_METADATA_KEY.to_owned(), "complete".to_owned()),
            ]),
            chunk_info: None,
            completion_policy: None,
        };
        let statuses = client.send_entity(&header, b"x", 0).await?;
        assert_eq!(statuses.last().unwrap().status.state, STATUS_COMPLETE);
        let mut cp = checkpoint(2000);
        cp.checkpoint_entity_id = 1;
        assert_eq!(client.checkpoint(&cp).await?.flags, CHECKPOINT_ACK);
        client.goaway(1).await?;
        Result::<()>::Ok(())
    })
    .await??;
    let after = store.load("review-session")?.unwrap().session;
    assert_eq!(after.work_sets.as_ref(), Some(&before));
    assert!(after.checkpoints[&(0, 1)].acknowledged);
    assert!(after.work_scope_ready(0));
    assert_eq!(
        fs::read(
            peer.options
                .entity_directory
                .join("review-session/scope-0/entity-1.bin")
        )?,
        b"x"
    );
    Ok(())
}

#[tokio::test]
async fn public_client_refuses_changed_or_malformed_work_set_ack() -> Result<()> {
    for malformed in [false, true] {
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
            let (_, offer) = read(&mut recv).await.unwrap();
            send.write_all(&encode_capabilities(&decode_capabilities(&offer).unwrap()).unwrap())
                .await
                .unwrap();
            let (_, body) = read(&mut recv).await.unwrap();
            let mut ack = work_set::decode(&body).unwrap();
            ack.flags |= work_set::ACK;
            ack.producer_id = [2; 16];
            let bytes = if malformed {
                encode_ucf(work_set::FRAME_WORK_SET, &[0xa0]).unwrap()
            } else {
                work_set::encode(&ack).unwrap()
            };
            send.write_all(&bytes).await.unwrap();
            tokio::time::timeout(Duration::from_secs(2), connection.closed())
                .await
                .unwrap()
        });
        let mut client = RecursiveClient::connect_sealed(&RecursiveClientOptions {
            identity: None,
            remote: address,
            ca_certificate: options.certificate,
            server_name: "localhost".into(),
        })
        .await?;
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            client.declare_work(&declaration(0, 0, &[1], Some(&[1]))),
        )
        .await?
        .unwrap_err();
        let expected = if malformed {
            ERROR_FRAME
        } else {
            ERROR_ENTITY_INVALID
        };
        assert!(error.to_string().contains(if malformed {
            "PIPESTREAM_FRAME_ERROR"
        } else {
            "PIPESTREAM_ENTITY_INVALID"
        }));
        let close = server.await?;
        assert!(
            matches!(close, quinn::ConnectionError::ApplicationClosed(error) if error.error_code == expected.into())
        );
        client.disconnect();
    }
    Ok(())
}
