//! Public durable facade against actual authenticated QUIC and on-disk stores.
use super::*;
use crate::v2_client::{
    journal::{self as local, Creation, Intent, Journal},
    session::{self as durable, Client as DurableClient, Failure},
    transport as wire,
};
mod files;

fn creation() -> Creation {
    Creation {
        authority: IdentityLabel("issuer-a".into()),
        owner: IdentityLabel("alice".into()),
        creation_sequence: Id(1),
        policy: policy(),
        results: true,
    }
}
async fn journal(path: &std::path::Path, fresh: bool) -> Journal {
    let options = local::Options { in_flight: 1 };
    if fresh {
        Journal::initialize(
            path.into(),
            creation(),
            local::JournalLimits::default(),
            PhysicalLimits::default(),
            options,
        )
        .await
        .unwrap()
    } else {
        Journal::open(
            path.into(),
            creation(),
            local::JournalLimits::default(),
            PhysicalLimits::default(),
            options,
        )
        .await
        .unwrap()
    }
}
fn endpoint(running: &Running, principal: usize) -> durable::Endpoint {
    durable::Endpoint {
        local: "127.0.0.1:0".parse().unwrap(),
        remote: running.address,
        server_name: "localhost".into(),
        security: wire::Security::new(
            roots(&running.tls.issuer),
            Some(running.tls.clients[principal].identity()),
        )
        .unwrap(),
        transport: wire::Options {
            offer: Capabilities {
                response: ResponseFlag(0),
                ..caps()
            },
            flow: options().flow,
            ..Default::default()
        },
    }
}
async fn connect(
    running: &Running,
    journal: Journal,
    principal: usize,
    slots: usize,
) -> DurableClient {
    DurableClient::connect(
        endpoint(running, principal),
        journal,
        durable::Options { in_flight: slots },
    )
    .await
    .unwrap()
}
fn declaration() -> Intent {
    Intent {
        operation: OperationId([1; 16]),
        mutation: Mutation::Declare {
            scope: Number(0),
            entity_ids: vec![Id(1)],
            seal: true,
        },
    }
}
fn admission(bytes: &[u8]) -> Intent {
    let header = inputs::header(bytes);
    Intent {
        operation: header.operation,
        mutation: Mutation::Admit(header.parameters),
    }
}
async fn idle(client: &DurableClient) {
    tokio::time::timeout(HANDSHAKE, async {
        while client.in_flight() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn success(client: &DurableClient) -> local::ObservedWork {
    tokio::time::timeout(HANDSHAKE, async {
        loop {
            let view = client.watch(key(), Number(0), WaitMs(0)).await.unwrap();
            if view.view.state == State::SUCCEEDED {
                return view;
            }
            assert!(!view.view.state.is_terminal());
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn facade_persists_full_streaming_round_trip_and_exact_coverage_across_reopen() {
    let running = Running::new(options());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let client = connect(&running, journal(&path, true).await, 0, 4).await;
    assert_eq!(client.identity().generation, Id(1));
    client.mutate(declaration()).await.unwrap();
    let bytes = vec![0x6d; 262144];
    let intent = admission(&bytes);
    let mut input = client
        .input(intent.clone(), declaration().operation)
        .await
        .unwrap();
    for bytes in bytes.chunks(7001) {
        input.write(bytes).await.unwrap();
    }
    input.finish().await.unwrap();
    let receipt = input.receipt().await.unwrap();
    assert_eq!(
        client.receipt(intent.operation).await.unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(client.intent(intent.operation).await.unwrap(), intent);
    let view = success(&client).await;
    let reference = client
        .select_output(key(), Id(1), OutputIndex(0))
        .await
        .unwrap();
    assert_eq!(
        &client.manifest(key(), Id(1)).await.unwrap(),
        reference.manifest()
    );
    client.shutdown().await.unwrap();
    let client = connect(&running, journal(&path, false).await, 1, 4).await;
    assert_eq!(client.observed_work(key()).await.unwrap(), Some(view));
    assert_eq!(
        client
            .retained_reference(key(), Id(1), OutputIndex(0))
            .await
            .unwrap(),
        reference
    );
    assert_eq!(
        client.recover_operation(intent.operation).await.unwrap(),
        receipt
    );
    let mut output = client
        .read_output(key(), Id(1), OutputIndex(0))
        .await
        .unwrap();
    assert!(output.verification().is_none());
    assert!(client.receipt(intent.operation).await.unwrap().is_some());
    let mut actual = Vec::new();
    while let Some(bytes) = output.read_unverified().await.unwrap() {
        actual.extend(bytes);
    }
    assert_eq!(actual, bytes);
    assert!(output.verification().is_some());
    drop(output);
    let scope = client
        .scope_page(Number(0), Number(0), PageLimit(256))
        .await
        .unwrap();
    assert!(scope.membership_verified);
    let summary = client
        .checkpoint(Number(0), scope.seal.unwrap(), WaitMs(30000))
        .await
        .unwrap();
    assert_eq!(
        client.covered_scope(Number(0)).await.unwrap(),
        Some(summary.clone())
    );
    assert_eq!(client.complete().await.unwrap(), summary);
    client.closed().await.unwrap();
    let saved = journal(&path, false).await;
    assert!(
        saved
            .unresolved(Number(0), PageLimit(256))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(saved.covered_scope(Number(0)).await.unwrap(), Some(summary));
    saved.shutdown().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn cancelled_mutation_waiter_keeps_its_slot_and_still_commits_the_receipt() {
    let running = Running::new(options());
    let _release = ReleaseOnDrop(running.db.access.clone());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let store = journal(&path, true).await;
    let client = connect(&running, store.clone(), 0, 1).await;
    client.mutate(declaration()).await.unwrap();
    let intent = Intent {
        operation: OperationId([3; 16]),
        mutation: Mutation::Cancel { work: key() },
    };
    running.db.access.pause_cancel.store(true, Ordering::SeqCst);
    let task = {
        let client = client.clone();
        let intent = intent.clone();
        tokio::spawn(async move { client.mutate(intent).await })
    };
    tokio::time::timeout(HANDSHAKE, running.db.access.entered.notified())
        .await
        .unwrap();
    assert_eq!(store.intent(intent.operation).await.unwrap(), intent);
    task.abort();
    let _ = task.await;
    assert_eq!(client.in_flight(), 1);
    assert!(matches!(
        client.receipt(intent.operation).await,
        Err(Failure::Protocol(Error {
            code: ErrorCode::LimitExceeded,
            ..
        }))
    ));
    running.db.access.release();
    idle(&client).await;
    assert!(client.receipt(intent.operation).await.unwrap().is_some());
    client.shutdown().await.unwrap();
    let saved = journal(&path, false).await;
    assert!(saved.receipt(intent.operation).await.unwrap().is_some());
    saved.shutdown().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn dropped_upload_waiter_is_saved_before_client_shutdown_finishes() {
    let running = Running::new(options());
    let _release = ReleaseOnDrop(running.db.access.clone());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let client = connect(&running, journal(&path, true).await, 0, 4).await;
    client.mutate(declaration()).await.unwrap();
    running.db.access.pause_admit.store(true, Ordering::SeqCst);
    let bytes = [0x65; 4096];
    let intent = admission(&bytes);
    let mut input = client
        .input(intent.clone(), declaration().operation)
        .await
        .unwrap();
    input.write(&bytes).await.unwrap();
    input.finish().await.unwrap();
    tokio::time::timeout(HANDSHAKE, running.db.access.entered.notified())
        .await
        .unwrap();
    drop(input);
    client.close();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), client.closed())
            .await
            .is_err()
    );
    running.db.access.release();
    tokio::time::timeout(HANDSHAKE, client.closed())
        .await
        .unwrap()
        .unwrap();
    let saved = journal(&path, false).await;
    assert!(saved.receipt(intent.operation).await.unwrap().is_some());
    saved.shutdown().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn missing_covering_receipt_and_changed_intent_never_transmit_new_admission() {
    let running = Running::new(options());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let store = journal(&path, true).await;
    let client = connect(&running, store.clone(), 0, 4).await;
    store.prepare(declaration()).await.unwrap();
    let intent = admission(b"abc");
    assert!(matches!(
        client.input(intent.clone(), declaration().operation).await,
        Err(Failure::Protocol(Error {
            code: ErrorCode::NotReady,
            ..
        }))
    ));
    assert!(matches!(
        store.intent(intent.operation).await,
        Err(local::JournalError::Protocol(Error {
            code: ErrorCode::NotFound,
            ..
        }))
    ));
    client.mutate(declaration()).await.unwrap();
    let mut input = client
        .input(intent.clone(), declaration().operation)
        .await
        .unwrap();
    input.write(b"abc").await.unwrap();
    input.finish().await.unwrap();
    let receipt = input.receipt().await.unwrap();
    let changed = admission(b"abd");
    assert!(matches!(
        client.input(changed, declaration().operation).await,
        Err(Failure::Journal(local::JournalError::Protocol(Error {
            code: ErrorCode::Conflict,
            ..
        })))
    ));
    assert_eq!(
        client.receipt(intent.operation).await.unwrap(),
        Some(receipt)
    );
    client.shutdown().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn second_facade_cannot_take_the_same_journal_or_close_the_first() {
    let running = Running::new(options());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let store = journal(&path, true).await;
    let client = connect(&running, store.clone(), 0, 4).await;
    let second =
        DurableClient::connect(endpoint(&running, 1), store, durable::Options::default()).await;
    assert!(matches!(
        second,
        Err(Failure::Journal(local::JournalError::Protocol(Error {
            code: ErrorCode::Conflict,
            ..
        })))
    ));
    client.mutate(declaration()).await.unwrap();
    client.detach().await.unwrap();
    client.closed().await.unwrap();
    let saved = journal(&path, false).await;
    assert!(saved.covered_scope(Number(0)).await.unwrap().is_none());
    saved.shutdown().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn completion_barrier_waits_for_accepted_operations_and_reopens_after_refusal() {
    let running = Running::new(options());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let client = connect(&running, journal(&path, true).await, 0, 4).await;
    client.mutate(declaration()).await.unwrap();
    let current = client.watch(key(), Number(0), WaitMs(0)).await.unwrap();
    let watching = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .watch(key(), Number(current.revision.0), WaitMs(400))
                .await
        })
    };
    tokio::time::timeout(HANDSHAKE, async {
        while client.in_flight() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let completing = {
        let client = client.clone();
        tokio::spawn(async move { client.complete().await })
    };
    tokio::time::timeout(HANDSHAKE, async {
        while client.in_flight() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!completing.is_finished());
    assert!(matches!(
        client.covered_scope(Number(0)).await,
        Err(Failure::Protocol(Error {
            code: ErrorCode::NotReady,
            ..
        }))
    ));
    // WORK waits return the unchanged view at timeout. Only a checkpoint wait
    // without closure returns WAIT_TIMEOUT; do not conflate these contracts.
    assert_eq!(watching.await.unwrap().unwrap(), current);
    assert!(matches!(
        completing.await.unwrap(),
        Err(Failure::Journal(local::JournalError::Protocol(Error {
            code: ErrorCode::NotReady,
            ..
        })))
    ));
    let skip = Intent {
        operation: OperationId([3; 16]),
        mutation: Mutation::Skip { work: key() },
    };
    client.mutate(skip).await.unwrap();
    assert_eq!(
        client
            .watch(key(), Number(0), WaitMs(0))
            .await
            .unwrap()
            .view
            .state,
        State::SKIPPED
    );
    let scope = client
        .scope_page(Number(0), Number(0), PageLimit(256))
        .await
        .unwrap();
    let summary = client
        .checkpoint(Number(0), scope.seal.unwrap(), WaitMs(30000))
        .await
        .unwrap();
    assert_eq!(client.complete().await.unwrap(), summary);
    client.closed().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn binding_identity_mismatch_preserves_original_creation_without_a_binding() {
    let running = Running::new(options());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let mut wanted = creation();
    wanted.owner = IdentityLabel("bob".into());
    let store = Journal::initialize(
        path.clone(),
        wanted.clone(),
        local::JournalLimits::default(),
        PhysicalLimits::default(),
        local::Options::default(),
    )
    .await
    .unwrap();
    let result =
        DurableClient::connect(endpoint(&running, 0), store, durable::Options::default()).await;
    assert!(matches!(
        result,
        Err(Failure::Journal(local::JournalError::Protocol(Error {
            code: ErrorCode::IntegrityError,
            ..
        })))
    ));
    let saved = Journal::open(
        path,
        wanted.clone(),
        local::JournalLimits::default(),
        PhysicalLimits::default(),
        local::Options::default(),
    )
    .await
    .unwrap();
    assert_eq!(saved.creation(), &wanted);
    assert!(saved.binding().await.unwrap().is_none());
    saved.shutdown().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn authority_refusal_preserves_original_intent_for_an_explicit_same_operation_retry() {
    let running = Running::new(options());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let client = connect(&running, journal(&path, true).await, 0, 4).await;
    client.mutate(declaration()).await.unwrap();
    let intent = Intent {
        operation: OperationId([4; 16]),
        mutation: Mutation::Cancel { work: key() },
    };
    running.db.access.allowed.store(false, Ordering::SeqCst);
    assert!(matches!(
        client.mutate(intent.clone()).await,
        Err(Failure::Refused(Refusal {
            code: ErrorCode::Unauthorized,
            ..
        }))
    ));
    assert_eq!(client.intent(intent.operation).await.unwrap(), intent);
    assert!(client.receipt(intent.operation).await.unwrap().is_none());
    assert_eq!(
        client
            .unresolved(Number(0), PageLimit(256))
            .await
            .unwrap()
            .len(),
        1
    );
    running.db.access.allowed.store(true, Ordering::SeqCst);
    let receipt = client.mutate(intent.clone()).await.unwrap();
    assert_eq!(
        client.recover_operation(intent.operation).await.unwrap(),
        receipt
    );
    assert!(
        client
            .unresolved(Number(0), PageLimit(256))
            .await
            .unwrap()
            .is_empty()
    );
    client.shutdown().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn cancelled_connect_waiter_still_persists_the_original_binding_before_shutdown() {
    let running = Running::new(options());
    let _release = ReleaseOnDrop(running.db.access.clone());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let store = journal(&path, true).await;
    running.db.access.pause.store(true, Ordering::SeqCst);
    let endpoint = endpoint(&running, 0);
    let owned = store.clone();
    let connecting = tokio::spawn(async move {
        DurableClient::connect(endpoint, owned, durable::Options::default()).await
    });
    tokio::time::timeout(HANDSHAKE, running.db.access.entered.notified())
        .await
        .unwrap();
    assert!(store.binding().await.unwrap().is_none());
    connecting.abort();
    let _ = connecting.await;
    running.db.access.release();
    tokio::time::timeout(HANDSHAKE, store.closed())
        .await
        .unwrap()
        .unwrap();
    let saved = journal(&path, false).await;
    assert_eq!(saved.creation().creation_sequence, Id(1));
    assert_eq!(saved.identity().await.unwrap().generation, Id(1));
    saved.shutdown().await.unwrap();
    running.finish().await;
}
