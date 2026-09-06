use super::*;
use pipestream_core::{
    persistence::SessionStore,
    session::{EntityKey, Session},
};
use sha2::{Digest, Sha256};

struct ServerTask(tokio::task::JoinHandle<Result<()>>);
impl Drop for ServerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test]
async fn reconciled_sealed_input_stays_pending_and_only_matching_chunks_restore_completion()
-> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), scenario()).await?
}

async fn scenario() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let options = options(dir.path());
    let store = Arc::new(SqliteSessionStore::open(&options.state_database)?);
    let request = sealed_work::declaration(0, 0, &[1], Some(&[1]));
    let mut session = Session::new_sealed("review-session", [1; 16], 7, 100)?;
    session.declare_work(&request, 1)?;
    let original = store.create(&session)?;
    let files = FileEntityStore::open(&options.entity_directory)?;
    files.bind_session_store(&store)?;
    files.put(
        "review-session",
        EntityKey {
            scope_id: 0,
            entity_id: 1,
        },
        b"abcdef",
    )?;
    drop(files);
    let report = FileEntityStore::reconcile(
        &options.entity_directory,
        spool::SpoolLimits::default(),
        &store,
    )?;
    assert_eq!(report.orphan_bodies_removed, 1);
    assert_eq!(store.load("review-session")?.unwrap(), original);
    let metadata = options
        .entity_directory
        .join("review-session/scope-0/entity-1.bin.commit");
    let commitment = fs::read(&metadata)?;
    let mut cp = checkpoint(1000);
    cp.checkpoint_entity_id = 1;
    for mode in ["missing", "changed", "matching"] {
        let service = RecursiveService::with_limits(
            store.clone(),
            Arc::new(FileEntityStore::open(&options.entity_directory)?),
            Arc::new(ExemplarProcessor::default()),
            RecursiveLimits {
                max_scope_depth: 7,
                max_entities_per_scope: 100,
                max_entity_bytes: 1024,
                max_chunks_per_entity: 16,
            },
        )?;
        let server = RecursiveServer::bind(&options, service)?;
        let client_options = RecursiveClientOptions {
            identity: None,
            remote: server.local_addr()?,
            ca_certificate: options.certificate.clone(),
            server_name: "localhost".into(),
        };
        let mut task = ServerTask(tokio::spawn(server.run(true)));
        let mut client = RecursiveClient::connect_sealed(&client_options).await?;
        client.declare_work(&request).await?;
        if mode == "missing" {
            let error = client.checkpoint(&cp).await.unwrap_err().to_string();
            assert!(error.contains("PIPESTREAM_CHECKPOINT_TIMEOUT"), "{error}");
            client.disconnect_gracefully().await;
        } else {
            let mut chunks = Vec::new();
            for (index, original) in [(1, b"def"), (0, b"abc")] {
                let payload = if mode == "changed" { b"xxx" } else { original };
                chunks.push(EntityChunk {
                    header: EntityHeader {
                        entity_id: 1,
                        parent_id: None,
                        scope_id: None,
                        parent_scope_id: None,
                        layer: 0,
                        content_type: None,
                        payload_length: Some(3),
                        checksum: Some(Sha256::digest(payload).into()),
                        metadata: BTreeMap::from([
                            (SESSION_METADATA_KEY.into(), "review-session".into()),
                            (ACTION_METADATA_KEY.into(), "complete".into()),
                        ]),
                        chunk_info: Some(ChunkInfo {
                            total_chunks: 2,
                            chunk_index: index,
                            chunk_offset: index * 3,
                        }),
                        completion_policy: None,
                    },
                    payload: payload.to_vec(),
                });
            }
            let result = client.send_chunked_entity(&chunks, 0).await;
            if mode == "changed" {
                let error = result.unwrap_err().to_string();
                assert!(error.contains("PIPESTREAM_ENTITY_INVALID"), "{error}");
                client.disconnect_gracefully().await;
            } else {
                assert_eq!(result?.last().unwrap().status.state, STATUS_COMPLETE);
                assert_eq!(client.checkpoint(&cp).await?.flags, CHECKPOINT_ACK);
                client.goaway(1).await?;
            }
        }
        let ended = (&mut task.0).await?;
        assert_eq!(ended.is_ok(), mode == "matching", "{mode}: {ended:?}");
        let retained = store.load("review-session")?.unwrap().session;
        assert_eq!(retained.work_sets, original.session.work_sets);
        if mode != "matching" {
            assert!(retained.entities.is_empty());
            // The control deadline can expire before request persistence under
            // storage load. Neither an absent request nor a retained pending
            // request is evidence that missing declared work completed.
            assert!(!retained.work_scope_ready(0));
            assert!(retained.checkpoints.values().all(|cut| !cut.acknowledged));
            assert_eq!(fs::read(&metadata)?, commitment);
        } else {
            assert!(retained.work_scope_ready(0));
            assert!(retained.checkpoints[&(0, 1)].acknowledged);
            assert!(!metadata.exists());
            assert_eq!(
                fs::read(
                    options
                        .entity_directory
                        .join("review-session/scope-0/entity-1.bin")
                )?,
                b"abcdef"
            );
        }
    }
    Ok(())
}
