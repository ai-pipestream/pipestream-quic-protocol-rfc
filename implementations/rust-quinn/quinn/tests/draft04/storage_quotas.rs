use super::*;
use pipestream_core::persistence::{JobQueueLimits, StorageLimits};

fn fixture(limits: StorageLimits) -> Result<AuthFixture> {
    let mut fixture = AuthFixture::new()?;
    fixture.options.state_database = fixture._dir.path().join("quota.sqlite3");
    fixture.store = Arc::new(SqliteSessionStore::open_with_limits(
        &fixture.options.state_database,
        JobQueueLimits::default(),
        limits,
    )?);
    Ok(fixture)
}

fn work(id: &str, sequence: u64, ids: Vec<u32>, all: Option<&[u32]>) -> WorkSetFrame {
    WorkSetFrame {
        session_id: id.into(),
        producer_id: [1; 16],
        scope_id: 0,
        parent: None,
        sequence,
        entity_ids: ids,
        flags: if all.is_some() { work_set::SEAL } else { 0 },
        seal_digest: all
            .map(|all| work_set::seal_digest(id, [1; 16], 0, None, &all.iter().copied().collect())),
    }
}

#[tokio::test]
async fn full_store_refuses_new_sessions_but_replays_retained_declarations() -> Result<()> {
    let mut fixture = fixture(StorageLimits {
        sessions: 2,
        principal_sessions: 1,
        ..StorageLimits::default()
    })?;
    let alice = fixture.listen(Some("issuer-a"), Some(0))?;
    let bob = fixture.listen(Some("issuer-a"), Some(2))?;
    let request = work("alice-set", 0, vec![1], Some(&[1]));
    let mut client = RecursiveClient::connect_sealed(&alice).await?;
    client.declare_work(&request).await?;
    client.disconnect();
    let before = fixture.store.load("alice-set")?.unwrap();
    let mut denied = RecursiveClient::connect_sealed(&alice).await?;
    assert!(
        format!(
            "{:#}",
            denied
                .declare_work(&work("alice-over", 0, vec![1], Some(&[1])))
                .await
                .unwrap_err()
        )
        .contains("PIPESTREAM_LIMIT_EXCEEDED")
    );
    assert!(fixture.store.load("alice-over")?.is_none());
    let mut client = RecursiveClient::connect_sealed(&bob).await?;
    client
        .declare_work(&work("bob-set", 0, vec![1], Some(&[1])))
        .await?;
    client.disconnect();
    assert_eq!(fixture.store.storage_usage()?.sessions, 2);
    let other_authority = fixture.listen(Some("issuer-b"), Some(0))?;
    let mut global_denied = RecursiveClient::connect_sealed(&other_authority).await?;
    assert!(
        format!(
            "{:#}",
            global_denied
                .declare_work(&work("global-over", 0, vec![1], Some(&[1])))
                .await
                .unwrap_err()
        )
        .contains("PIPESTREAM_LIMIT_EXCEEDED")
    );
    assert!(fixture.store.load("global-over")?.is_none());
    // Reopen under the same durable policy and rotate Alice's client certificate.
    while let Some(server) = fixture.servers.pop() {
        server.abort();
        let _ = server.await;
    }
    fixture.store = Arc::new(SqliteSessionStore::open(&fixture.options.state_database)?);
    let rotated = fixture.listen(Some("issuer-a"), Some(1))?;
    let mut replay = RecursiveClient::connect_sealed(&rotated).await?;
    replay.declare_work(&request).await?;
    assert_eq!(
        fixture.store.load("alice-set")?.unwrap().session,
        before.session
    );
    assert_eq!(fixture.store.storage_usage()?.sessions, 2);
    assert_eq!(fixture.processor.processed.load(Ordering::SeqCst), 0);
    fixture.store.integrity_check()?;
    replay.disconnect();
    Ok(())
}

#[tokio::test]
async fn record_exhaustion_cannot_extend_or_seal_an_acknowledged_work_set() -> Result<()> {
    let mut fixture = fixture(StorageLimits {
        record_bytes: 256,
        ..StorageLimits::default()
    })?;
    let options = fixture.listen(Some("issuer-a"), Some(0))?;
    let request = work("growing-set", 0, vec![1], None);
    let mut client = RecursiveClient::connect_sealed(&options).await?;
    client.declare_work(&request).await?;
    let before = fixture.store.load("growing-set")?.unwrap();
    let usage = fixture.store.storage_usage()?;
    let all: Vec<_> = (1..=100).collect();
    let more = work("growing-set", 1, (2..=100).collect(), Some(&all));
    let error = client.declare_work(&more).await.unwrap_err();
    assert!(
        format!("{error:#}").contains("PIPESTREAM_LIMIT_EXCEEDED"),
        "{error:#}"
    );
    assert_eq!(fixture.store.load("growing-set")?.unwrap(), before);
    assert_eq!(fixture.store.storage_usage()?, usage);
    let mut replay = RecursiveClient::connect_sealed(&options).await?;
    replay.declare_work(&request).await?;
    let state = fixture.store.load("growing-set")?.unwrap().session;
    let scope = &state.work_sets.as_ref().unwrap().scopes[&0];
    assert_eq!(scope.ids, std::collections::BTreeSet::from([1]));
    assert!(scope.seal_digest.is_none());
    assert!(!state.work_scope_ready(0));
    fixture.store.integrity_check()?;
    replay.disconnect();
    Ok(())
}
