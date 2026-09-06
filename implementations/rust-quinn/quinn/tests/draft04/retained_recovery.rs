use super::*;
use pipestream_core::authorization::EXTENSION_AUTHENTICATED_SESSIONS;
use pipestream_core::recovery::{
    self, EXTENSION_AUTHENTICATED_RECOVERY, RecoveryFrame, RecoveryOutcome, RecoveryRequest,
};

#[tokio::test]
async fn public_client_rejects_changed_and_malformed_recovery_responses() -> Result<()> {
    for (terminal, malformed) in [(false, false), (false, true), (true, false), (true, true)] {
        let fixture = AuthFixture::new()?;
        let certs = CertificateDer::pem_file_iter(&fixture.options.certificate)?
            .collect::<Result<Vec<_>, _>>()?;
        let key = PrivateKeyDer::from_pem_file(&fixture.options.private_key)?;
        let verifier =
            rustls::server::WebPkiClientVerifier::builder(Arc::new(fixture.roots.clone()))
                .build()?;
        let mut tls = rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
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
            let (_, bytes) = read(&mut recv).await.unwrap();
            send.write_all(&encode_capabilities(&decode_capabilities(&bytes).unwrap()).unwrap())
                .await
                .unwrap();
            let (_, bytes) = read(&mut recv).await.unwrap();
            let RecoveryFrame::Request(request) = recovery::decode(&bytes).unwrap() else {
                panic!("expected request");
            };
            let mut receipt = recovery::RecoveryReceipt {
                request,
                acceptance: recovery::RecoveryAcceptance {
                    entity: pipestream_core::session::EntityKey {
                        scope_id: 0,
                        entity_id: 1,
                    },
                    accepted_at_micros: 20,
                    retain_until_micros: 20 + recovery::RECEIPT_RETENTION_MICROS,
                },
            };
            if terminal {
                send.write_all(
                    &recovery::encode(&RecoveryFrame::Receipt(receipt.clone())).unwrap(),
                )
                .await
                .unwrap();
                // Preserve the request but change valid receipt fields in the outcome.
                receipt.acceptance.accepted_at_micros += 1;
                receipt.acceptance.retain_until_micros += 1;
            } else {
                receipt.request.request_id = [9; 16];
            }
            let response = if malformed {
                encode_ucf(recovery::FRAME_RECOVERY, &[0xa0]).unwrap()
            } else if terminal {
                recovery::encode(&RecoveryFrame::Outcome {
                    receipt,
                    outcome: RecoveryOutcome::Complete,
                })
                .unwrap()
            } else {
                recovery::encode(&RecoveryFrame::Receipt(receipt)).unwrap()
            };
            send.write_all(&response).await.unwrap();
            tokio::time::timeout(Duration::from_secs(2), connection.closed())
                .await
                .unwrap()
        });
        let mut client = RecursiveClient::connect_recovery(&RecursiveClientOptions {
            identity: Some(fixture.identities[0].clone()),
            remote: address,
            ca_certificate: fixture.options.certificate.clone(),
            server_name: "localhost".into(),
        })
        .await?;
        let request = RecoveryRequest {
            authority: "issuer-a".into(),
            session_id: "mismatched".into(),
            request_id: [7; 16],
            claim_id: 99,
            state_checksum: [2; 32],
        };
        let error = if terminal {
            let receipt = client.accept_recovery(&request).await?;
            client.wait_recovery(&receipt).await.unwrap_err()
        } else {
            client.accept_recovery(&request).await.unwrap_err()
        };
        let (code, name) = if malformed {
            (ERROR_FRAME, "PIPESTREAM_FRAME_ERROR")
        } else {
            (ERROR_ENTITY_INVALID, "PIPESTREAM_ENTITY_INVALID")
        };
        assert!(error.to_string().contains(name), "{error}");
        assert!(
            matches!(server.await?, quinn::ConnectionError::ApplicationClosed(error) if error.error_code == code.into())
        );
        client.disconnect();
    }
    Ok(())
}

fn request(claim: &ClaimRedemption) -> RecoveryRequest {
    RecoveryRequest {
        authority: "issuer-a".into(),
        session_id: claim.session_id.clone(),
        request_id: [7; 16],
        claim_id: claim.claim_id,
        state_checksum: claim.state_checksum,
    }
}

async fn raw(
    options: &RecursiveClientOptions,
    recovery: bool,
) -> Result<(
    quinn::Endpoint,
    quinn::Connection,
    quinn::SendStream,
    quinn::RecvStream,
)> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(&options.ca_certificate)? {
        roots.add(cert?)?;
    }
    let identity = options.identity.as_ref().unwrap();
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(
            CertificateDer::pem_file_iter(&identity.certificate)?.collect::<Result<Vec<_>, _>>()?,
            PrivateKeyDer::from_pem_file(&identity.private_key)?,
        )?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse()?)?;
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(tls)?,
    )));
    let connection = endpoint.connect(options.remote, "localhost")?.await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    let mut capabilities = offer(LayerSupport::LAYER2, 7);
    let mut ids = vec![EXTENSION_AUTHENTICATED_SESSIONS];
    if recovery {
        ids.push(EXTENSION_AUTHENTICATED_RECOVERY);
    }
    capabilities.extensions.supported = ids.clone();
    capabilities.extensions.required = ids;
    send.write_all(&encode_capabilities(&capabilities)?).await?;
    let (kind, bytes) = read(&mut recv).await?;
    assert_eq!(kind, FRAME_CAPABILITIES);
    capabilities.validate_response(&decode_capabilities(&bytes)?)?;
    assert_eq!(read(&mut recv).await?.0, FRAME_STATUS);
    Ok((endpoint, connection, send, recv))
}

#[tokio::test]
async fn unobserved_recovery_receipt_replays_after_restart_without_a_second_resume() -> Result<()> {
    let mut fixture = AuthFixture::new()?;
    let options = fixture.listen(Some("issuer-a"), Some(0))?;
    let storage_pair = fixture.store.payload_binding()?;
    assert!(storage_pair.payloads().is_some());
    let claim = begin_durable_yield(&options, "lost-recovery-ack").await?;
    let request = request(&claim);
    let (endpoint, connection, mut send, _unread) = raw(&options, true).await?;
    send.write_all(&recovery::encode(&RecoveryFrame::Request(request.clone()))?)
        .await?;
    let receipt = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state = fixture
                .store
                .load(&claim.session_id)
                .unwrap()
                .unwrap()
                .session;
            if let Some(receipt) = state.recovery_receipts.get(&request.request_id)
                && fixture.store.unfinished_job_count().unwrap() == 0
            {
                break receipt.clone();
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    // Never read the response stream. Stop every test listener and reopen durable state.
    connection.close(0u32.into(), b"lost acknowledgement");
    endpoint.close(0u32.into(), b"test reconnect");
    while let Some(server) = fixture.servers.pop() {
        server.abort();
        let _ = server.await;
    }
    fixture.store = Arc::new(SqliteSessionStore::open(&fixture.options.state_database)?);
    assert_eq!(fixture.store.payload_binding()?, storage_pair);
    let rotated = fixture.listen(Some("issuer-a"), Some(1))?;
    assert_eq!(fixture.store.payload_binding()?, storage_pair);
    let mut client = RecursiveClient::connect_recovery(&rotated).await?;
    let replayed = client.accept_recovery(&request).await?;
    assert_eq!(replayed, receipt);
    assert_eq!(
        client.wait_recovery(&replayed).await?,
        RecoveryOutcome::Complete
    );
    let state = fixture.store.load(&claim.session_id)?.unwrap().session;
    assert_eq!(state.recovery_receipts.len(), 1);
    assert_eq!(fixture.processor.resumed.load(Ordering::SeqCst), 1);
    assert_eq!(state.executions[&receipt.execution_key()].epoch, 1);
    fixture.store.integrity_check()?;
    client.disconnect();
    Ok(())
}

#[tokio::test]
async fn recovery_refuses_wrong_owner_authority_request_identity_and_revocation() -> Result<()> {
    let mut fixture = AuthFixture::new()?;
    let alice = fixture.listen(Some("issuer-a"), Some(0))?;
    let claim = begin_durable_yield(&alice, "retained-access").await?;
    let request = request(&claim);
    let mut client = RecursiveClient::connect_recovery(&alice).await?;
    let receipt = client.accept_recovery(&request).await?;
    assert_eq!(
        client.wait_recovery(&receipt).await?,
        RecoveryOutcome::Complete
    );
    client.disconnect();
    let before = fixture.store.load(&claim.session_id)?.unwrap().session;
    for (authority, identity) in [("issuer-a", 2), ("issuer-b", 0)] {
        let options = fixture.listen(Some(authority), Some(identity))?;
        let mut client = RecursiveClient::connect_recovery(&options).await?;
        let error = client.accept_recovery(&request).await.unwrap_err();
        assert!(format!("{error:#}").contains("PIPESTREAM_UNAUTHORIZED"));
        assert_eq!(
            fixture.store.load(&claim.session_id)?.unwrap().session,
            before
        );
    }
    for changed_field in 0..4 {
        let mut changed = request.clone();
        let expected = match changed_field {
            0 => {
                changed.state_checksum = [9; 32];
                "PIPESTREAM_ENTITY_INVALID"
            }
            1 => {
                changed.request_id = [8; 16];
                "PIPESTREAM_CLAIM_NOT_FOUND"
            }
            2 => {
                changed.authority = "issuer-b".into();
                "PIPESTREAM_UNAUTHORIZED"
            }
            _ => {
                changed.session_id = "absent-session".into();
                "PIPESTREAM_UNAUTHORIZED"
            }
        };
        let mut client = RecursiveClient::connect_recovery(&alice).await?;
        assert!(
            format!("{:#}", client.accept_recovery(&changed).await.unwrap_err()).contains(expected)
        );
        assert_eq!(
            fixture.store.load(&claim.session_id)?.unwrap().session,
            before
        );
    }
    fixture
        .store
        .transact(&claim.session_id, |s| s.revoke_claim(claim.claim_id))?;
    let revoked = fixture.store.load(&claim.session_id)?.unwrap().session;
    let mut client = RecursiveClient::connect_recovery(&alice).await?;
    assert!(
        format!("{:#}", client.accept_recovery(&request).await.unwrap_err())
            .contains("PIPESTREAM_UNAUTHORIZED")
    );
    assert_eq!(
        fixture.store.load(&claim.session_id)?.unwrap().session,
        revoked
    );
    assert_eq!(fixture.processor.resumed.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn application_refusal_replays_after_restart_without_retrying_resume() -> Result<()> {
    let mut fixture = AuthFixture::new()?;
    fixture.processor = Arc::new(Processor {
        panic_resume: true,
        ..Processor::default()
    });
    let options = fixture.listen(Some("issuer-a"), Some(0))?;
    let claim = begin_durable_yield(&options, "refused-recovery").await?;
    let request = request(&claim);
    let mut client = RecursiveClient::connect_recovery(&options).await?;
    let receipt = client.accept_recovery(&request).await?;
    let outcome = client.wait_recovery(&receipt).await?;
    assert!(
        matches!(&outcome, RecoveryOutcome::Refused(_)),
        "{outcome:?}"
    );
    client.disconnect();
    while let Some(server) = fixture.servers.pop() {
        server.abort();
        let _ = server.await;
    }
    fixture.store = Arc::new(SqliteSessionStore::open(&fixture.options.state_database)?);
    let options = fixture.listen(Some("issuer-a"), Some(1))?;
    let mut client = RecursiveClient::connect_recovery(&options).await?;
    let replayed = client.accept_recovery(&request).await?;
    assert_eq!(replayed, receipt);
    assert_eq!(client.wait_recovery(&replayed).await?, outcome);
    let retained = fixture.store.load(&claim.session_id)?.unwrap().session;
    assert_eq!(retained.recovery_receipts.len(), 1);
    assert_eq!(retained.executions[&receipt.execution_key()].epoch, 1);
    assert_ne!(
        retained.entities[&receipt.acceptance.entity].state,
        pipestream_core::session::EntityState::Complete
    );
    assert_eq!(fixture.processor.resumed.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.store.unfinished_job_count()?, 0);
    fixture.store.integrity_check()?;
    client.disconnect();
    Ok(())
}

#[tokio::test]
async fn recovery_and_legacy_frames_cannot_cross_negotiated_profiles() -> Result<()> {
    let mut fixture = AuthFixture::new()?;
    let options = fixture.listen(Some("issuer-a"), Some(0))?;
    let claim = begin_durable_yield(&options, "recovery-negotiation").await?;
    for recovery_enabled in [false, true] {
        let (endpoint, connection, mut send, _recv) = raw(&options, recovery_enabled).await?;
        let bytes = if recovery_enabled {
            encode_claim_redemption(&claim)?
        } else {
            recovery::encode(&RecoveryFrame::Request(request(&claim)))?
        };
        send.write_all(&bytes).await?;
        let error = tokio::time::timeout(Duration::from_secs(2), connection.closed()).await?;
        assert!(
            error
                .to_string()
                .contains("PIPESTREAM_EXTENSION_UNSUPPORTED"),
            "{error}"
        );
        endpoint.close(0u32.into(), b"finished");
    }
    let state = fixture.store.load(&claim.session_id)?.unwrap().session;
    assert!(state.recovery_receipts.is_empty());
    assert!(state.claims[&claim.claim_id].redeemed_at_micros.is_none());
    let anonymous = fixture.listen(None, None)?;
    assert!(
        format!(
            "{:#}",
            RecursiveClient::connect_recovery(&anonymous)
                .await
                .err()
                .unwrap()
        )
        .contains("PIPESTREAM_UNAUTHORIZED")
    );
    Ok(())
}
