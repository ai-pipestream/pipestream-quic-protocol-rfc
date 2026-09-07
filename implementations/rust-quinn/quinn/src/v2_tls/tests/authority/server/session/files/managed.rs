use super::*;
use durable::files::managed::{ManagedResults, ResultPolicy};

fn result_policy() -> ResultPolicy {
    ResultPolicy {
        objects: Id(1),
        bytes: Number(262144 + 524),
        owner_objects: Id(1),
        owner_bytes: Number(262144 + 524),
        chunk_bytes: Id(4096),
        handles: Id(4),
        owner_handles: Id(4),
    }
}
async fn open(path: &std::path::Path, fresh: bool) -> ManagedResults {
    if fresh {
        ManagedResults::initialize(
            path.into(),
            creation().authority,
            creation().owner,
            result_policy(),
        )
        .await
        .unwrap()
    } else {
        ManagedResults::open(
            path.into(),
            creation().authority,
            creation().owner,
            result_policy(),
        )
        .await
        .unwrap()
    }
}

#[tokio::test]
async fn managed_download_reopens_exact_bytes_without_remote_reexecution_and_enforces_shared_quota()
{
    let running = Running::new(options());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let client = connect(&running, journal(&path, true).await, 0, 4).await;
    client.mutate(declaration()).await.unwrap();
    let bytes = vec![0xd3; 262144];
    let source = directory.path().join("source");
    std::fs::write(&source, &bytes).unwrap();
    FileInput::open(source, 262144)
        .await
        .unwrap()
        .send(client.clone(), admission(&bytes), declaration().operation)
        .await
        .unwrap();
    let terminal = success(&client).await;
    let reference = client
        .select_output(key(), Id(1), OutputIndex(0))
        .await
        .unwrap();
    let root = directory.path().join("copies");
    let store = open(&root, true).await;
    let output = client
        .read_output(key(), Id(1), OutputIndex(0))
        .await
        .unwrap();
    let saved = store.save(output).await.unwrap();
    assert_eq!(
        saved.verification.header().length,
        Number(bytes.len() as u64)
    );
    assert_eq!(store.usage().await.unwrap().charged_bytes, 262144 + 524);
    let extra = client
        .read_output(key(), Id(1), OutputIndex(0))
        .await
        .unwrap();
    assert!(matches!(
        store.save(extra).await,
        Err(Failure::Protocol(Error {
            code: ErrorCode::LimitExceeded,
            ..
        }))
    ));
    assert_eq!(success(&client).await, terminal);
    store.close().await.unwrap();
    client.shutdown().await.unwrap();
    running.finish().await;

    // The server is stopped: this is deliberately local-only possession, not
    // fresh authorization or an assertion that remote output is still available.
    let journal = journal(&path, false).await;
    let retained = journal
        .retained_reference(key(), Id(1), OutputIndex(0))
        .await
        .unwrap();
    assert_eq!(retained, reference);
    let store = open(&root, false).await;
    assert_eq!(store.recovered_stages().await.unwrap(), 0);
    let mut local = store.find(retained.clone()).await.unwrap().unwrap();
    assert_eq!(local.key(), saved.key);
    assert!(matches!(
        store.remove(saved.key.clone()).await,
        Err(Failure::Protocol(Error {
            code: ErrorCode::Conflict,
            ..
        }))
    ));
    let mut actual = vec![];
    while let Some(chunk) = local.read_unverified().await.unwrap() {
        actual.extend(chunk);
    }
    assert!(local.verified());
    assert_eq!(actual, bytes);
    local.close().await.unwrap();
    let exports = durable::files::managed::exports::ManagedExports::initialize(
        directory.path().join("exports"),
        creation().authority,
        creation().owner,
        durable::files::managed::exports::ExportPolicy {
            objects: 1,
            bytes: 262144 + 112,
        },
    )
    .await
    .unwrap();
    let exports = durable::files::managed::exports::tests::cancel_after_enqueue(
        exports,
        OperationId([1; 16]),
        retained.clone(),
        store.find(retained.clone()).await.unwrap().unwrap(),
    )
    .await;
    assert_eq!(exports.usage().await.unwrap().charged_bytes, 262144 + 112);
    let replayed = exports
        .export(
            OperationId([1; 16]),
            retained.clone(),
            store.find(retained.clone()).await.unwrap().unwrap(),
        )
        .await
        .unwrap();
    assert!(replayed.replayed);
    assert_eq!(std::fs::read(replayed.path).unwrap(), bytes);
    let clone = exports.clone();
    assert!(exports.close().await.is_err());
    clone.close().await.unwrap();
    assert!(store.remove(saved.key).await.unwrap());
    assert!(store.find(retained).await.unwrap().is_none());
    assert_eq!(store.usage().await.unwrap().objects, 0);
    store.close().await.unwrap();
    journal.shutdown().await.unwrap();
}

#[tokio::test]
async fn managed_root_requires_matching_owner_and_all_cloned_owners_to_release() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("copies");
    let store = open(&root, true).await;
    let clone = store.clone();
    assert!(matches!(
        store.close().await,
        Err(Failure::Protocol(Error {
            code: ErrorCode::Conflict,
            ..
        }))
    ));
    assert!(matches!(
        ManagedResults::open(
            root.clone(),
            creation().authority,
            creation().owner,
            result_policy()
        )
        .await,
        Err(Failure::Protocol(Error {
            code: ErrorCode::LimitExceeded,
            ..
        }))
    ));
    clone.close().await.unwrap();
    assert!(matches!(
        ManagedResults::open(
            root.clone(),
            creation().authority,
            IdentityLabel("bob".into()),
            result_policy()
        )
        .await,
        Err(Failure::Protocol(Error {
            code: ErrorCode::IntegrityError,
            ..
        }))
    ));
    open(&root, false).await.close().await.unwrap();
}
