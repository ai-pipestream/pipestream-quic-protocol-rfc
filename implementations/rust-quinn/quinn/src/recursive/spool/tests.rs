use super::*;

fn limits() -> SpoolLimits {
    SpoolLimits {
        max_bytes: 16,
        max_files: 8,
        principal_bytes: 8,
        principal_files: 4,
        connection_files: 2,
        max_principals: 2,
    }
}

#[tokio::test]
async fn connections_share_principal_and_global_limits_without_reopen_bypass() {
    let dir = tempfile::tempdir().unwrap();
    let store = SpoolStore::new(dir.path().join("spool"), limits()).unwrap();
    let alias = SpoolStore::new(dir.path().join("spool"), limits()).unwrap();
    assert!(Arc::ptr_eq(&store, &alias));
    assert!(SpoolStore::new(dir.path().join("spool"), SpoolLimits::default()).is_err());
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    let bob = PrincipalBinding::new("issuer", "bob").unwrap();
    let first = store.connection(Some(&alice), 8).unwrap();
    let second = alias.connection(Some(&alice), 8).unwrap();
    let third = store.connection(Some(&bob), 8).unwrap();
    let held = first
        .create()
        .await
        .unwrap()
        .append(b"12345678")
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    let error = second
        .create()
        .await
        .unwrap()
        .append(b"x")
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, pipestream_core::ERROR_LIMIT_EXCEEDED);
    let other = third
        .create()
        .await
        .unwrap()
        .append(b"abcdefgh")
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    assert_eq!(store.usage().unwrap().bytes, 16);
    assert_eq!(store.usage().unwrap().files, 2);
    assert!(first.create().await.unwrap().append(b"x").await.is_err());
    drop(held);
    let replaced = second
        .create()
        .await
        .unwrap()
        .append(b"12345678")
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    let reader = replaced.reader();
    drop(replaced);
    assert_eq!(store.usage().unwrap().bytes, 16);
    drop(reader);
    drop(other);
    let usage = store.usage().unwrap();
    assert_eq!((usage.bytes, usage.files, usage.peak_bytes), (0, 0, 16));
    assert_eq!(
        std::fs::read_dir(dir.path().join("spool")).unwrap().count(),
        0
    );
}

#[tokio::test]
async fn empty_payloads_and_idle_principals_cannot_bypass_file_or_identity_limits() {
    let dir = tempfile::tempdir().unwrap();
    let store = SpoolStore::new(dir.path().join("spool"), limits()).unwrap();
    let connection = store.connection(None, 8).unwrap();
    let a = connection.create().await.unwrap().finish().await.unwrap();
    let b = connection.create().await.unwrap().finish().await.unwrap();
    assert!(a.is_empty());
    assert!(connection.create().await.is_err());
    assert_eq!(store.usage().unwrap().files, 2);
    drop(a);
    drop(b);
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    let bob = PrincipalBinding::new("issuer", "bob").unwrap();
    let owned = store.connection(Some(&alice), 8).unwrap();
    assert!(store.connection(Some(&bob), 8).is_err());
    drop(owned);
    assert!(store.connection(Some(&bob), 8).is_ok());
}

#[tokio::test]
async fn concatenation_keeps_original_files_and_reader_keeps_disk_credit() {
    let dir = tempfile::tempdir().unwrap();
    let store = SpoolStore::new(dir.path().join("spool"), limits()).unwrap();
    let connection = store.connection(None, 8).unwrap();
    let a = connection
        .create()
        .await
        .unwrap()
        .append(b"abc")
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    let b = connection
        .create()
        .await
        .unwrap()
        .append(b"defgh")
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    let result = Payload::concatenate(vec![a, b]).await.unwrap();
    assert_eq!(result.len(), 8);
    assert_eq!(
        result.digest(),
        <[u8; 32]>::from(Sha256::digest(b"abcdefgh"))
    );
    assert_eq!(
        (
            store.usage().unwrap().files,
            store.usage().unwrap().peak_files
        ),
        (2, 2)
    );
    let mut reader = result.reader();
    drop(result);
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"abcdefgh");
    assert_eq!(store.usage().unwrap().bytes, 8);
    drop(reader);
    assert_eq!(store.usage().unwrap().bytes, 0);
}

#[tokio::test]
async fn changed_spool_chunk_is_not_rehashed_into_a_successful_entity() {
    let dir = tempfile::tempdir().unwrap();
    let store = SpoolStore::new(dir.path().join("spool"), limits()).unwrap();
    let connection = store.connection(None, 8).unwrap();
    for replacement in [b"xyz".as_slice(), b"a".as_slice()] {
        let payload = connection
            .create()
            .await
            .unwrap()
            .append(b"abc")
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        std::fs::write(payload.0.segments[0].path(), replacement).unwrap();
        let error = Payload::concatenate(vec![payload]).await.unwrap_err();
        assert_eq!(error.code, pipestream_core::ERROR_INTEGRITY);
        assert_eq!(
            (store.usage().unwrap().bytes, store.usage().unwrap().files),
            (0, 0)
        );
    }
}

#[test]
fn abandoned_files_are_counted_on_restart_and_never_deleted_implicitly() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spool");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("abandoned"), b"12345678").unwrap();
    let store = SpoolStore::new(path.clone(), limits()).unwrap();
    assert_eq!(
        (store.usage().unwrap().bytes, store.usage().unwrap().files),
        (8, 1)
    );
    drop(store);
    assert!(
        SpoolStore::new(
            path.clone(),
            SpoolLimits {
                max_bytes: 7,
                ..limits()
            }
        )
        .is_err()
    );
    assert_eq!(std::fs::read(path.join("abandoned")).unwrap(), b"12345678");
}

#[test]
fn cancelling_queued_io_does_not_release_credit_before_the_owned_writer_finishes() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let store = SpoolStore::new(dir.path().join("spool"), limits()).unwrap();
        let connection = store.connection(None, 8).unwrap();
        let writer = connection.create().await.unwrap();
        let (release, wait) = std::sync::mpsc::channel();
        let (started, active) = tokio::sync::oneshot::channel();
        let occupier = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            wait.recv().unwrap();
        });
        active.await.unwrap();
        let append = tokio::spawn(async move { writer.append(b"abc").await });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while store.usage().unwrap().bytes != 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        append.abort();
        let _ = append.await;
        assert_eq!(
            (store.usage().unwrap().bytes, store.usage().unwrap().files),
            (3, 1)
        );
        release.send(()).unwrap();
        occupier.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while store.usage().unwrap().files != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(store.usage().unwrap().bytes, 0);
        assert_eq!(
            std::fs::read_dir(dir.path().join("spool")).unwrap().count(),
            0
        );
    });
}

#[tokio::test]
async fn payload_copy_is_immutable_and_never_requires_whole_entity_allocation() {
    use crate::recursive::{EntityStore, FileEntityStore};
    use pipestream_core::session::EntityKey;
    let dir = tempfile::tempdir().unwrap();
    let entities = FileEntityStore::open(dir.path().join("entities")).unwrap();
    let connection = entities.spool().connection(None, 1024).unwrap();
    let payload = connection
        .create()
        .await
        .unwrap()
        .append(b"payload")
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    let key = EntityKey {
        scope_id: 0,
        entity_id: 1,
    };
    entities
        .put_payload(None, "session", key, &payload)
        .unwrap();
    entities
        .put_payload(None, "session", key, &payload)
        .unwrap();
    let changed = connection
        .create()
        .await
        .unwrap()
        .append(b"changed")
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    assert_eq!(
        entities
            .put_payload(None, "session", key, &changed)
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(
        std::fs::read(dir.path().join("entities/session/scope-0/entity-1.bin")).unwrap(),
        b"payload"
    );
}
