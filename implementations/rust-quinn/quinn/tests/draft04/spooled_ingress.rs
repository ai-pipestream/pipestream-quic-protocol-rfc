use super::*;
use pipestream_core::persistence::SessionStore;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn invalid_header_is_refused_without_waiting_for_payload_or_creating_a_spool() -> Result<()> {
    let processor = Arc::new(CountingProcessor::default());
    let peer = Fixture::with_processor(LayerSupport::LAYER0, 0, processor.clone()).await?;
    // Canonical {"layer": 0, "entity-id": 0}; the identity is reserved.
    let header = b"\xa2\x65layer\x00\x69entity-id\x00";
    let mut stream = peer.connection.open_uni().await?;
    stream
        .write_all(&(header.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(header).await?;
    // Deliberately neither send the body nor FIN.
    assert!(peer.refused().await?.contains("PIPESTREAM_ENTITY_INVALID"));
    assert_eq!(processor.processed.load(Ordering::SeqCst), 0);
    assert!(!peer.options.entity_directory.join(".spool").exists());
    assert!(
        SqliteSessionStore::open(&peer.options.state_database)?
            .list_session_ids()?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn fin_length_and_checksum_validation_precede_admission_and_clean_spool_files() -> Result<()>
{
    for wrong_length in [true, false] {
        let processor = Arc::new(CountingProcessor::default());
        let peer = Fixture::with_processor(LayerSupport::LAYER0, 0, processor.clone()).await?;
        let encoded = entity(1, b"abc", "application/octet-stream")?;
        let (mut header, _) = decode_entity(&encoded)?;
        header
            .metadata
            .insert(SESSION_METADATA_KEY.into(), "invalid-spool".into());
        if wrong_length {
            header.payload_length = Some(4);
        } else {
            header.checksum = Some([0; 32]);
        }
        let header = encode_entity_header_for(&header, LayerSupport::LAYER0)?;
        let mut stream = peer.connection.open_uni().await?;
        stream
            .write_all(&(header.len() as u32).to_be_bytes())
            .await?;
        stream.write_all(&header).await?;
        stream.write_all(b"abc").await?;
        stream.finish()?;
        let reason = peer.refused().await?;
        assert!(
            reason.contains(if wrong_length {
                "PIPESTREAM_ENTITY_INVALID"
            } else {
                "PIPESTREAM_INTEGRITY_ERROR"
            }),
            "{reason}"
        );
        assert_eq!(processor.processed.load(Ordering::SeqCst), 0);
        assert!(
            SqliteSessionStore::open(&peer.options.state_database)?
                .load("invalid-spool")?
                .is_none()
        );
        let entities = FileEntityStore::open(&peer.options.entity_directory)?;
        assert_eq!(entities.spool().usage()?.bytes, 0);
        assert_eq!(entities.spool().usage()?.files, 0);
        assert_eq!(
            fs::read_dir(peer.options.entity_directory.join(".spool"))?.count(),
            0
        );
    }
    Ok(())
}

#[tokio::test]
async fn zero_byte_partial_entities_hit_file_budget_over_quic() -> Result<()> {
    let processor = Arc::new(CountingProcessor::default());
    let limits = spool::SpoolLimits {
        max_files: 2,
        principal_files: 2,
        connection_files: 2,
        ..spool::SpoolLimits::default()
    };
    let peer =
        Fixture::with_spool_limits(offer(LayerSupport::LAYER2, 7), processor.clone(), limits)
            .await?;
    for id in 1..=3 {
        let header = EntityHeader {
            entity_id: id,
            parent_id: None,
            scope_id: None,
            parent_scope_id: None,
            layer: 0,
            payload_length: Some(0),
            content_type: None,
            checksum: None,
            metadata: BTreeMap::from([(SESSION_METADATA_KEY.into(), "empty-chunks".into())]),
            chunk_info: Some(ChunkInfo {
                total_chunks: 2,
                chunk_index: 0,
                chunk_offset: 0,
            }),
            completion_policy: None,
        };
        let mut stream = peer.connection.open_uni().await?;
        stream
            .write_all(&encode_entity_for(&header, &[], LayerSupport::LAYER2)?)
            .await?;
        stream.finish()?;
    }
    assert!(peer.refused().await?.contains("PIPESTREAM_LIMIT_EXCEEDED"));
    assert_eq!(processor.processed.load(Ordering::SeqCst), 0);
    assert!(
        SqliteSessionStore::open(&peer.options.state_database)?
            .list_session_ids()?
            .is_empty()
    );
    Ok(())
}
