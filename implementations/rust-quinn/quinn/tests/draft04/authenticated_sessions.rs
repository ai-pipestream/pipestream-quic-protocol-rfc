use super::*;
use pipestream_core::{
    authorization::PrincipalBinding,
    persistence::SessionStore,
    work_set::{self, WorkSetFrame},
};
use pipestream_quic::authentication::{AuthenticationPolicy, ClientIdentity};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering};

#[path = "retained_recovery.rs"]
mod retained_recovery;

#[path = "storage_quotas.rs"]
mod storage_quotas;

#[derive(Default)]
struct Processor {
    processed: AtomicUsize,
    resumed: AtomicUsize,
    revoke_during_process: Option<Arc<SqliteSessionStore>>,
    panic_resume: bool,
}

impl EntityProcessor for Processor {
    fn process(&self, context: ProcessContext<'_>) -> Result<ProcessingDisposition, ProtocolError> {
        self.processed.fetch_add(1, Ordering::SeqCst);
        if let Some(store) = &self.revoke_during_process {
            store
                .transact(
                    context.session_id,
                    pipestream_core::session::Session::revoke_access,
                )
                .unwrap();
        }
        ExemplarProcessor::default().process(context)
    }
    fn rehydrate(&self, context: RehydrateContext<'_>) -> [u8; 32] {
        ExemplarProcessor::default().rehydrate(context)
    }
    fn resume(&self, context: ResumeContext<'_>) -> [u8; 32] {
        self.resumed.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic_resume, "injected resume failure");
        ExemplarProcessor::default().resume(context)
    }
}

#[tokio::test]
async fn revocation_during_callback_prevents_result_publication() -> Result<()> {
    let mut fixture = AuthFixture::new()?;
    fixture.processor = Arc::new(Processor {
        revoke_during_process: Some(fixture.store.clone()),
        ..Processor::default()
    });
    let options = fixture.listen(Some("issuer-a"), Some(0))?;
    let error = begin_durable_yield(&options, "revoked-in-flight")
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("PIPESTREAM_UNAUTHORIZED"));
    let retained = fixture.store.load("revoked-in-flight")?.unwrap();
    assert!(retained.session.owner.as_ref().unwrap().revoked);
    assert!(retained.session.claims.is_empty());
    assert_eq!(retained.session.executions.len(), 1);
    assert!(
        retained
            .session
            .executions
            .values()
            .all(|attempt| attempt.completed_at_micros.is_none())
    );
    assert!(retained.session.entities.values().all(|entity| entity.state
        == pipestream_core::session::EntityState::Processing
        && entity.output_digest.is_none()));
    assert_eq!(fixture.processor.processed.load(Ordering::SeqCst), 1);
    Ok(())
}

struct AuthFixture {
    _dir: tempfile::TempDir,
    options: RecursiveServerOptions,
    roots: rustls::RootCertStore,
    mappings: BTreeMap<[u8; 32], String>,
    identities: Vec<ClientIdentity>,
    store: Arc<SqliteSessionStore>,
    processor: Arc<Processor>,
    servers: Vec<tokio::task::JoinHandle<Result<()>>>,
}

impl AuthFixture {
    fn new() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let options = options(dir.path());
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let issuer = CertifiedIssuer::self_signed(params, KeyPair::generate()?)?;
        let mut roots = rustls::RootCertStore::empty();
        roots.add(issuer.der().clone())?;
        let mut identities = Vec::new();
        let mut mappings = BTreeMap::new();
        for (index, principal) in ["alice", "alice", "bob", "unmapped", "untrusted", "expired"]
            .iter()
            .enumerate()
        {
            let key = KeyPair::generate()?;
            let mut params = CertificateParams::new(vec![format!("client-{index}")])?;
            params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            if *principal == "expired" {
                params.not_before = rcgen::date_time_ymd(2000, 1, 1);
                params.not_after = rcgen::date_time_ymd(2001, 1, 1);
            }
            let certificate = if *principal == "untrusted" {
                params.self_signed(&key)?
            } else {
                params.signed_by(&key, &issuer)?
            };
            let hash: [u8; 32] = Sha256::digest(certificate.der().as_ref()).into();
            // Even a pinned leaf must pass certificate-chain verification.
            if *principal != "unmapped" {
                mappings.insert(hash, (*principal).to_owned());
            }
            let identity = ClientIdentity {
                certificate: dir.path().join(format!("client-{index}.crt")),
                private_key: dir.path().join(format!("client-{index}.key")),
            };
            fs::write(&identity.certificate, certificate.pem())?;
            fs::write(&identity.private_key, key.serialize_pem())?;
            identities.push(identity);
        }
        let store = Arc::new(SqliteSessionStore::open(&options.state_database)?);
        Ok(Self {
            _dir: dir,
            options,
            roots,
            mappings,
            identities,
            store,
            processor: Arc::new(Processor::default()),
            servers: Vec::new(),
        })
    }

    fn service(&self, authority: Option<&str>) -> Result<RecursiveService<Processor>> {
        let mut service = RecursiveService::with_limits(
            self.store.clone(),
            Arc::new(FileEntityStore::open(&self.options.entity_directory)?),
            self.processor.clone(),
            RecursiveLimits {
                max_scope_depth: 7,
                max_entities_per_scope: 100,
                max_entity_bytes: 1024,
                max_chunks_per_entity: 16,
            },
        )?;
        if let Some(authority) = authority {
            service = service.with_authentication(AuthenticationPolicy::new(
                authority.into(),
                self.roots.clone(),
                self.mappings.clone(),
            )?);
        }
        Ok(service)
    }

    fn listen(
        &mut self,
        authority: Option<&str>,
        identity: Option<usize>,
    ) -> Result<RecursiveClientOptions> {
        let server = RecursiveServer::bind(&self.options, self.service(authority)?)?;
        let client = RecursiveClientOptions {
            remote: server.local_addr()?,
            ca_certificate: self.options.certificate.clone(),
            server_name: "localhost".into(),
            identity: identity.map(|index| self.identities[index].clone()),
        };
        self.servers.push(tokio::spawn(server.run(false)));
        Ok(client)
    }
}

impl Drop for AuthFixture {
    fn drop(&mut self) {
        for server in &self.servers {
            server.abort();
        }
    }
}

#[tokio::test]
async fn missing_untrusted_and_unmapped_credentials_cannot_admit_work() -> Result<()> {
    let mut fixture = AuthFixture::new()?;
    for identity in [None, Some(3), Some(4), Some(5)] {
        let options = fixture.listen(Some("issuer-a"), identity)?;
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            begin_durable_yield(&options, "must-not-exist"),
        )
        .await?;
        assert!(result.is_err());
        if identity == Some(3) {
            let error = format!("{:#}", result.unwrap_err());
            assert!(error.contains("PIPESTREAM_UNAUTHORIZED"), "{error}");
        }
    }
    assert!(fixture.store.list_session_ids()?.is_empty());
    assert_eq!(fixture.processor.processed.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.processor.resumed.load(Ordering::SeqCst), 0);
    let names = fs::read_dir(&fixture.options.entity_directory)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<std::collections::BTreeSet<_>>>()?;
    assert_eq!(
        names,
        [".retained-lock", ".retained-policy"]
            .map(std::ffi::OsString::from)
            .into()
    );
    let files = FileEntityStore::open(&fixture.options.entity_directory)?;
    assert_eq!(files.retained_usage()?, RetainedUsage::default());
    assert_eq!(files.spool().usage()?.bytes, 0);
    assert_eq!(files.spool().usage()?.files, 0);
    Ok(())
}

#[tokio::test]
async fn configured_client_identity_cannot_silently_fall_back_to_anonymous_work() -> Result<()> {
    let mut fixture = AuthFixture::new()?;
    let options = fixture.listen(None, Some(0))?;
    let error = tokio::time::timeout(
        Duration::from_secs(3),
        begin_durable_yield(&options, "no-downgrade"),
    )
    .await?
    .unwrap_err();
    assert!(format!("{error:#}").contains("PIPESTREAM_EXTENSION_UNSUPPORTED"));
    assert!(fixture.store.list_session_ids()?.is_empty());
    assert_eq!(fixture.processor.processed.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn mutual_tls_alone_does_not_bypass_required_session_negotiation() -> Result<()> {
    let mut fixture = AuthFixture::new()?;
    let options = fixture.listen(Some("issuer-a"), Some(0))?;
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(&options.ca_certificate)? {
        roots.add(cert?)?;
    }
    let identity = options.identity.as_ref().unwrap();
    let certs =
        CertificateDer::pem_file_iter(&identity.certificate)?.collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(&identity.private_key)?;
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse()?)?;
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(tls)?,
    )));
    let connection = endpoint.connect(options.remote, "localhost")?.await?;
    let (mut send, _recv) = connection.open_bi().await?;
    send.write_all(&encode_capabilities(&offer(LayerSupport::LAYER2, 7))?)
        .await?;
    let close = tokio::time::timeout(Duration::from_secs(3), connection.closed()).await?;
    assert!(
        matches!(close, quinn::ConnectionError::ApplicationClosed(error) if error.error_code == ERROR_EXTENSION_UNSUPPORTED.into())
    );
    assert!(fixture.store.list_session_ids()?.is_empty());
    assert_eq!(fixture.processor.processed.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn claims_are_principal_and_authority_bound_without_an_anonymous_bypass() -> Result<()> {
    let mut fixture = AuthFixture::new()?;
    let alice = fixture.listen(Some("issuer-a"), Some(0))?;
    let claim = begin_durable_yield(&alice, "owned-claim").await?;
    let before = fixture.store.load("owned-claim")?.unwrap();
    assert_eq!(
        before.session.owner.as_ref().unwrap().binding,
        PrincipalBinding::new("issuer-a", "alice")?
    );
    for (authority, identity) in [
        (Some("issuer-a"), Some(2)),
        (Some("issuer-b"), Some(0)),
        (None, None),
    ] {
        let options = fixture.listen(authority, identity)?;
        let error = tokio::time::timeout(
            Duration::from_secs(3),
            finish_durable_yield(&options, &claim),
        )
        .await?
        .unwrap_err();
        assert!(format!("{error:#}").contains("PIPESTREAM_UNAUTHORIZED"));
        assert_eq!(fixture.store.load("owned-claim")?.unwrap(), before);
    }
    assert_eq!(fixture.processor.resumed.load(Ordering::SeqCst), 0);
    // Certificate rotation can retain a stable principal through explicit operator mapping.
    let rotated = fixture.listen(Some("issuer-a"), Some(1))?;
    finish_durable_yield(&rotated, &claim).await?;
    assert_eq!(fixture.processor.resumed.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.store.load("owned-claim")?.unwrap().session.owner,
        before.session.owner
    );
    Ok(())
}

fn declaration() -> WorkSetFrame {
    WorkSetFrame {
        session_id: "owned-set".into(),
        producer_id: [1; 16],
        scope_id: 0,
        parent: None,
        sequence: 0,
        entity_ids: vec![1],
        flags: work_set::SEAL,
        seal_digest: Some(work_set::seal_digest(
            "owned-set",
            [1; 16],
            0,
            None,
            &std::collections::BTreeSet::from([1]),
        )),
    }
}

#[tokio::test]
async fn sealed_set_ownership_and_revocation_apply_to_live_and_reconnected_clients() -> Result<()> {
    let mut fixture = AuthFixture::new()?;
    let options = fixture.listen(Some("issuer-a"), Some(0))?;
    let mut alice = RecursiveClient::connect_sealed(&options).await?;
    alice.declare_work(&declaration()).await?;
    let before = fixture.store.load("owned-set")?.unwrap();
    let bob_options = fixture.listen(Some("issuer-a"), Some(2))?;
    let mut bob = RecursiveClient::connect_sealed(&bob_options).await?;
    let error = bob.declare_work(&declaration()).await.unwrap_err();
    assert!(format!("{error:#}").contains("PIPESTREAM_UNAUTHORIZED"));
    assert_eq!(fixture.store.load("owned-set")?.unwrap(), before);
    fixture.store.transact("owned-set", |s| s.revoke_access())?;
    let revoked = fixture.store.load("owned-set")?.unwrap();
    assert!(
        format!(
            "{:#}",
            alice.declare_work(&declaration()).await.unwrap_err()
        )
        .contains("PIPESTREAM_UNAUTHORIZED")
    );
    let rotated = fixture.listen(Some("issuer-a"), Some(1))?;
    let mut reconnect = RecursiveClient::connect_sealed(&rotated).await?;
    assert!(
        format!(
            "{:#}",
            reconnect.declare_work(&declaration()).await.unwrap_err()
        )
        .contains("PIPESTREAM_UNAUTHORIZED")
    );
    assert_eq!(fixture.store.load("owned-set")?.unwrap(), revoked);
    Ok(())
}

#[tokio::test]
async fn background_recovery_obeys_durable_owner_and_revocation() -> Result<()> {
    let mut fixture = AuthFixture::new()?;
    let options = fixture.listen(Some("issuer-a"), Some(0))?;
    let mut requests = Vec::new();
    for id in ["recover-allowed", "recover-revoked"] {
        let request = begin_durable_yield(&options, id).await?;
        requests.push((id, request));
    }
    // Exercise the blocking recovery API with no periodic dispatcher racing the assertions.
    for server in fixture.servers.drain(..) {
        server.abort();
        let _ = server.await;
    }
    for (id, request) in requests {
        fixture.store.transact(id, |session| {
            session.redeem_claim(request.claim_id, request.state_checksum, 1)?;
            let key = pipestream_core::execution::ExecutionKey {
                entity: session.claims[&request.claim_id].entity,
                stage: pipestream_core::execution::ExecutionStage::Resume {
                    claim_id: request.claim_id,
                },
            };
            session.enqueue_job(
                key,
                pipestream_core::jobs::JobInput::Resume {
                    claim_id: request.claim_id,
                },
                1,
            )?;
            if id == "recover-revoked" {
                session.revoke_access()?;
            }
            Ok(())
        })?;
    }
    assert_eq!(fixture.service(None)?.recover_interrupted_resumptions()?, 0);
    assert_eq!(
        fixture
            .service(Some("issuer-b"))?
            .recover_interrupted_resumptions()?,
        0
    );
    assert_eq!(fixture.processor.resumed.load(Ordering::SeqCst), 0);
    let reopened = SqliteSessionStore::open(&fixture.options.state_database)?;
    let revoked = reopened.load("recover-revoked")?.unwrap();
    assert_eq!(
        fixture
            .service(Some("issuer-a"))?
            .recover_interrupted_resumptions()?,
        1
    );
    assert_eq!(
        fixture
            .service(Some("issuer-a"))?
            .recover_interrupted_resumptions()?,
        0
    );
    assert_eq!(fixture.processor.resumed.load(Ordering::SeqCst), 1);
    assert_eq!(reopened.load("recover-revoked")?.unwrap(), revoked);
    Ok(())
}
