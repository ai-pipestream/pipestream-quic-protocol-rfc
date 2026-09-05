use super::*;
use pipestream_core::{persistence::SessionStore, session::EntityKey};
use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};

#[derive(Default)]
struct Gate {
    entered: AtomicBool,
    released: Mutex<bool>,
    changed: Condvar,
}

impl Gate {
    fn hold(&self) {
        self.entered.store(true, Ordering::SeqCst);
        let (_released, timed) = self
            .changed
            .wait_timeout_while(
                self.released.lock().unwrap(),
                Duration::from_secs(10),
                |released| !*released,
            )
            .unwrap();
        assert!(!timed.timed_out(), "storage test did not release its gate");
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
    async fn entered(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !self.entered.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        Ok(())
    }
}

struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

async fn lock_database(peer: &Fixture) -> Result<(Release, tokio::task::JoinHandle<Result<()>>)> {
    let store = SqliteSessionStore::open(&peer.options.state_database)?;
    let gate = Arc::new(Gate::default());
    let release = Release(gate.clone());
    let held = gate.clone();
    let task = tokio::task::spawn_blocking(move || {
        store.transact("review-session", |_| {
            held.hold();
            Ok(())
        })?;
        Ok(())
    });
    gate.entered().await?;
    Ok((release, task))
}

#[tokio::test]
async fn sqlite_writer_cannot_postpone_checkpoint_clock_until_transaction_returns() -> Result<()> {
    let mut peer = Fixture::new(LayerSupport::LAYER2, 7).await?;
    peer.entity(1, false, "complete", LayerSupport::LAYER2)
        .await?;
    peer.statuses(2).await?;
    let store = SqliteSessionStore::open(&peer.options.state_database)?;
    let (release, writer) = lock_database(&peer).await?;
    let started = tokio::time::Instant::now();
    peer.send
        .write_all(&encode_checkpoint(&checkpoint(40))?)
        .await?;
    let reason = peer.refused().await?;
    assert!(reason.contains("PIPESTREAM_CHECKPOINT_TIMEOUT"), "{reason}");
    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(
        !writer.is_finished(),
        "writer must still be held at refusal"
    );
    let state = store.load("review-session")?.unwrap();
    assert!(
        state.session.checkpoints.is_empty(),
        "request cannot have committed behind the held writer"
    );
    drop(release);
    writer.await??;
    Ok(())
}

#[tokio::test]
async fn duplicate_capabilities_bypass_a_stalled_checkpoint_transaction() -> Result<()> {
    let mut peer = Fixture::new(LayerSupport::LAYER2, 7).await?;
    peer.entity(1, false, "complete", LayerSupport::LAYER2)
        .await?;
    peer.statuses(2).await?;
    let (release, writer) = lock_database(&peer).await?;
    peer.send
        .write_all(&encode_checkpoint(&checkpoint(5000))?)
        .await?;
    // Allow the request to enter the storage worker, then exercise the same stream.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let started = tokio::time::Instant::now();
    peer.send
        .write_all(&encode_capabilities(&offer(LayerSupport::LAYER2, 7))?)
        .await?;
    let reason = peer.refused().await?;
    assert!(reason.contains("PIPESTREAM_FRAME_ERROR"), "{reason}");
    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(!writer.is_finished());
    drop(release);
    writer.await??;
    Ok(())
}

struct HeldLineage {
    inner: FileEntityStore,
    gate: Arc<Gate>,
}

#[tokio::test]
async fn control_backlog_is_bounded_and_oversized_frames_refuse_during_storage_stall() -> Result<()>
{
    for oversized in [false, true] {
        let mut peer = Fixture::new(LayerSupport::LAYER2, 7).await?;
        peer.entity(1, false, "complete", LayerSupport::LAYER2)
            .await?;
        peer.statuses(2).await?;
        let (release, writer) = lock_database(&peer).await?;
        peer.send
            .write_all(&encode_checkpoint(&checkpoint(5000))?)
            .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        if oversized {
            let mut header = vec![FRAME_STATUS];
            header.extend_from_slice(&((MAX_CONTROL_FRAME + 1) as u32).to_be_bytes());
            peer.send.write_all(&header).await?;
        } else {
            let frame = encode_status(Status {
                state: STATUS_PENDING,
                entity_id: 2,
                scope_id: 0,
                cursor: None,
                depth: 0,
            })?;
            peer.send.write_all(&frame.repeat(64)).await?;
        }
        let reason = peer.refused().await?;
        assert!(reason.contains("PIPESTREAM_LIMIT_EXCEEDED"), "{reason}");
        assert!(!writer.is_finished());
        drop(release);
        writer.await??;
    }
    Ok(())
}
impl EntityStore for HeldLineage {
    fn put(&self, id: &str, key: EntityKey, payload: &[u8]) -> std::io::Result<()> {
        self.inner.put(id, key, payload)
    }
    fn put_payload(
        &self,
        principal: Option<&pipestream_core::authorization::PrincipalBinding>,
        id: &str,
        key: EntityKey,
        payload: &spool::Payload,
    ) -> std::io::Result<()> {
        self.inner.put_payload(principal, id, key, payload)
    }
    fn spool(&self) -> &Arc<spool::SpoolStore> {
        self.inner.spool()
    }
    fn load_payload(
        &self,
        principal: Option<&pipestream_core::authorization::PrincipalBinding>,
        id: &str,
        key: EntityKey,
        length: u64,
        digest: [u8; 32],
    ) -> std::io::Result<spool::Payload> {
        self.inner.load_payload(principal, id, key, length, digest)
    }
    fn put_lineage(
        &self,
        principal: Option<&pipestream_core::authorization::PrincipalBinding>,
        id: &str,
        digest: [u8; 32],
    ) -> std::io::Result<()> {
        if id == "review-session" {
            self.gate.hold();
        }
        self.inner.put_lineage(principal, id, digest)
    }
}

async fn lineage_peer(gate: Arc<Gate>) -> Result<Fixture> {
    let dir = tempfile::tempdir()?;
    let mut options = options(dir.path());
    options.once = false;
    let service = RecursiveService::new(
        Arc::new(SqliteSessionStore::open(&options.state_database)?),
        Arc::new(HeldLineage {
            inner: FileEntityStore::open(&options.entity_directory)?,
            gate,
        }),
        Arc::new(ExemplarProcessor::default()),
        7,
        100,
    )?;
    let server = RecursiveServer::bind(&options, service)?;
    let address = server.local_addr()?;
    let server = tokio::spawn(server.run(false));
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
    send.write_all(&encode_capabilities(&offer(LayerSupport::LAYER2, 7))?)
        .await?;
    assert_eq!(read(&mut recv).await?.0, FRAME_CAPABILITIES);
    assert_eq!(read(&mut recv).await?.0, FRAME_STATUS);
    Ok(Fixture {
        dir,
        options,
        endpoint,
        connection,
        send,
        recv,
        server,
    })
}

#[tokio::test]
async fn stalled_lineage_does_not_ack_after_deadline_or_block_another_connection() -> Result<()> {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let mut peer = lineage_peer(gate.clone()).await?;
    peer.entity(1, false, "complete", LayerSupport::LAYER2)
        .await?;
    peer.statuses(2).await?;
    peer.send
        .write_all(&encode_checkpoint(&checkpoint(300))?)
        .await?;
    gate.entered().await?;

    let other = peer
        .endpoint
        .connect(peer.connection.remote_address(), "localhost")?
        .await?;
    let (mut send, mut recv) = other.open_bi().await?;
    send.write_all(&encode_capabilities(&offer(LayerSupport::LAYER2, 7))?)
        .await?;
    assert_eq!(read(&mut recv).await?.0, FRAME_CAPABILITIES);
    assert_eq!(read(&mut recv).await?.0, FRAME_STATUS);
    let (mut header, _) = decode_entity(&entity(1, b"x", "text/plain")?)?;
    header
        .metadata
        .insert(SESSION_METADATA_KEY.into(), "independent-session".into());
    let header = encode_entity_header_for(&header, LayerSupport::LAYER2)?;
    let mut stream = other.open_uni().await?;
    stream
        .write_all(&(header.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(&header).await?;
    stream.write_all(b"x").await?;
    stream.finish()?;
    for expected in [STATUS_PROCESSING, STATUS_COMPLETE] {
        let (kind, body) =
            tokio::time::timeout(Duration::from_millis(500), read(&mut recv)).await??;
        assert_eq!(kind, FRAME_STATUS);
        assert_eq!(
            decode_status_frame(&body, LayerSupport::LAYER2)?
                .status
                .state,
            expected
        );
    }
    let reason = peer.refused().await?;
    assert!(reason.contains("PIPESTREAM_CHECKPOINT_TIMEOUT"), "{reason}");
    assert!(!*gate.released.lock().unwrap());
    assert!(
        read(&mut peer.recv).await.is_err(),
        "held lineage must not emit a checkpoint ACK"
    );
    gate.release();
    other.close(0u32.into(), b"test done");
    Ok(())
}
