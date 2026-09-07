use super::*;
use durable::files::FileInput;
mod managed;

#[tokio::test]
async fn early_authority_refusal_is_not_hidden_by_stopped_file_upload() {
    let running = Running::new(options());
    let directory = tempfile::tempdir().unwrap();
    let client = connect(
        &running,
        journal(&directory.path().join("client.sqlite"), true).await,
        0,
        4,
    )
    .await;
    client.mutate(declaration()).await.unwrap();
    let bytes = vec![0x75; 262144];
    let source = directory.path().join("source");
    std::fs::write(&source, &bytes).unwrap();
    let input = FileInput::open(source, 262144).await.unwrap();
    running.db.access.allowed.store(false, Ordering::SeqCst);
    let intent = admission(&bytes);
    let result = input
        .send(client.clone(), intent.clone(), declaration().operation)
        .await;
    assert!(
        matches!(
            &result,
            Err(Failure::Refused(Refusal {
                code: ErrorCode::Unauthorized,
                ..
            }))
        ),
        "{result:?}"
    );
    assert_eq!(client.intent(intent.operation).await.unwrap(), intent);
    assert!(client.receipt(intent.operation).await.unwrap().is_none());
    client.shutdown().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn mismatched_file_commitment_refuses_before_preparing_admission_intent() {
    let running = Running::new(options());
    let directory = tempfile::tempdir().unwrap();
    let client = connect(
        &running,
        journal(&directory.path().join("client.sqlite"), true).await,
        0,
        4,
    )
    .await;
    client.mutate(declaration()).await.unwrap();
    let source = directory.path().join("source");
    std::fs::write(&source, b"abc").unwrap();
    let input = FileInput::open(source, 1024).await.unwrap();
    let intent = admission(b"abd");
    assert!(matches!(
        input
            .send(client.clone(), intent.clone(), declaration().operation)
            .await,
        Err(Failure::Protocol(Error {
            code: ErrorCode::IntegrityError,
            ..
        }))
    ));
    assert!(matches!(
        client.intent(intent.operation).await,
        Err(Failure::Journal(local::JournalError::Protocol(Error {
            code: ErrorCode::NotFound,
            ..
        })))
    ));
    client.shutdown().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn zero_byte_file_still_requires_admission_and_verified_result_fin() {
    let running = Running::new(options());
    let directory = tempfile::tempdir().unwrap();
    let client = connect(
        &running,
        journal(&directory.path().join("client.sqlite"), true).await,
        0,
        4,
    )
    .await;
    client.mutate(declaration()).await.unwrap();
    let source = directory.path().join("source");
    std::fs::write(&source, []).unwrap();
    let receipt = FileInput::open(source, 0)
        .await
        .unwrap()
        .send(client.clone(), admission(&[]), declaration().operation)
        .await
        .unwrap();
    assert_eq!(
        client.receipt(admission(&[]).operation).await.unwrap(),
        Some(receipt)
    );
    success(&client).await;
    client
        .select_output(key(), Id(1), OutputIndex(0))
        .await
        .unwrap();
    let target = directory.path().join("result");
    let output = client
        .read_output(key(), Id(1), OutputIndex(0))
        .await
        .unwrap();
    let saved = output.save_to(target.clone(), 0).await.unwrap();
    assert_eq!(saved.verification.header().length, Number(0));
    assert_eq!(std::fs::metadata(target).unwrap().len(), 0);
    client.shutdown().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn file_round_trip_and_identical_replay_cross_small_windows_without_overwrite() {
    let running = Running::new(options());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let client = connect(&running, journal(&path, true).await, 0, 4).await;
    client.mutate(declaration()).await.unwrap();
    let bytes = vec![0xb7; 262144];
    let source = directory.path().join("source");
    std::fs::write(&source, &bytes).unwrap();
    let intent = admission(&bytes);
    let receipt = FileInput::open(source.clone(), 262144)
        .await
        .unwrap()
        .send(client.clone(), intent.clone(), declaration().operation)
        .await
        .unwrap();
    let replay = FileInput::open(source, 262144)
        .await
        .unwrap()
        .send(client.clone(), intent, declaration().operation)
        .await
        .unwrap();
    assert_eq!(receipt, replay);
    success(&client).await;
    client
        .select_output(key(), Id(1), OutputIndex(0))
        .await
        .unwrap();
    let target = directory.path().join("result");
    let output = client
        .read_output(key(), Id(1), OutputIndex(0))
        .await
        .unwrap();
    let saved = output.save_to(target.clone(), 262144).await.unwrap();
    assert_eq!(saved.path, target);
    assert_eq!(
        saved.verification.header().length,
        Number(bytes.len() as u64)
    );
    assert_eq!(std::fs::read(&target).unwrap(), bytes);
    let output = client
        .read_output(key(), Id(1), OutputIndex(0))
        .await
        .unwrap();
    assert!(matches!(
        output.save_to(target.clone(), 262144).await,
        Err(Failure::Protocol(Error {
            code: ErrorCode::Conflict,
            ..
        }))
    ));
    assert_eq!(std::fs::read(target).unwrap(), bytes);
    let refused = directory.path().join("too-large");
    let output = client
        .read_output(key(), Id(1), OutputIndex(0))
        .await
        .unwrap();
    assert!(matches!(
        output.save_to(refused.clone(), 1).await,
        Err(Failure::Protocol(Error {
            code: ErrorCode::LimitExceeded,
            ..
        }))
    ));
    assert!(!refused.exists());
    client.shutdown().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn changed_prehashed_file_never_receives_new_admission() {
    let running = Running::new(options());
    let directory = tempfile::tempdir().unwrap();
    let client = connect(
        &running,
        journal(&directory.path().join("client.sqlite"), true).await,
        0,
        4,
    )
    .await;
    client.mutate(declaration()).await.unwrap();
    let source = directory.path().join("source");
    std::fs::write(&source, b"original").unwrap();
    let input = FileInput::open(source.clone(), 1024).await.unwrap();
    std::fs::write(source, b"modified").unwrap();
    let intent = admission(b"original");
    assert!(matches!(
        input
            .send(client.clone(), intent.clone(), declaration().operation)
            .await,
        Err(Failure::Protocol(Error {
            code: ErrorCode::IntegrityError,
            ..
        }))
    ));
    assert_eq!(client.intent(intent.operation).await.unwrap(), intent);
    assert!(client.receipt(intent.operation).await.unwrap().is_none());
    assert_eq!(
        client
            .watch(key(), Number(0), WaitMs(0))
            .await
            .unwrap()
            .view
            .state,
        State::DECLARED
    );
    client.shutdown().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn cancelled_file_send_waiter_keeps_upload_and_receipt_collection_owned() {
    let running = Running::new(options());
    let _release = ReleaseOnDrop(running.db.access.clone());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let client = connect(&running, journal(&path, true).await, 0, 4).await;
    client.mutate(declaration()).await.unwrap();
    let bytes = vec![0x65; 65536];
    let source = directory.path().join("source");
    std::fs::write(&source, &bytes).unwrap();
    let input = FileInput::open(source, 65536).await.unwrap();
    let intent = admission(&bytes);
    running.db.access.pause_admit.store(true, Ordering::SeqCst);
    let sender = {
        let client = client.clone();
        let intent = intent.clone();
        tokio::spawn(input.send(client, intent, declaration().operation))
    };
    tokio::time::timeout(HANDSHAKE, running.db.access.entered.notified())
        .await
        .unwrap();
    sender.abort();
    let _ = sender.await;
    running.db.access.release();
    tokio::time::timeout(HANDSHAKE, async {
        loop {
            if client.receipt(intent.operation).await.unwrap().is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.shutdown().await.unwrap();
    let saved = journal(&path, false).await;
    assert!(saved.receipt(intent.operation).await.unwrap().is_some());
    saved.shutdown().await.unwrap();
    running.finish().await;
}
