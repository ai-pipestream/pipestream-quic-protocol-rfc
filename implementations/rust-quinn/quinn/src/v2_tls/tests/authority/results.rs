use super::*;

#[tokio::test]
async fn dispatcher_retry_manifest_and_actual_result_reads_preserve_committed_work() {
    let db = Database::new();
    let tls = Fixture::new();
    let (connection, _wire) = db.connect(&tls, caps(), Some(0)).await;
    control(&connection, create(1)).await;
    let digest = seal(control(&connection, declare(2, vec![Id(1)], true)).await);
    let slot = connection.input().unwrap();
    let identity = slot.binding().identity.clone();
    let mut apps = Applications::default();
    apps.register(
        ApplicationLabel("copy/v1".into()),
        vec![Mode(0)],
        RestartSafety::Pure,
        Arc::new(CopyApplication),
    )
    .unwrap();
    let apps = Arc::new(apps);
    let bytes = vec![0x5a; 12288];
    let header = InputHeader {
        kind: Literal,
        generation: identity.generation,
        operation: OperationId([2; 16]),
        parameters: AdmitParameters {
            work: key(),
            input: Input {
                length: Number(bytes.len() as u64),
                sha256: Digest(Sha256::digest(&bytes).into()),
                content_type: ApplicationLabel("application/octet-stream".into()),
            },
            application: ApplicationLabel("copy/v1".into()),
            mode: Mode(0),
            execution_ms: pipestream_core::v2::Duration(1000),
            outputs: OutputBudget {
                count: BatchCount(1),
                total_bytes: Number(bytes.len() as u64),
            },
        },
    };
    let now = std::time::Instant::now();
    let InputReception::Receiving(mut receiving) = db
        .store
        .receive_input(&identity, &header, &caps(), &db.payloads, &apps, now)
        .unwrap()
    else {
        panic!("new input expected")
    };
    for chunk in bytes.chunks(8192) {
        receiving.receive(chunk, std::time::Instant::now()).unwrap();
    }
    let InputPreparation::Ready(prepared) = db
        .store
        .prepare_input(
            receiving.finish(std::time::Instant::now()).unwrap(),
            &caps(),
            &apps,
        )
        .unwrap()
    else {
        panic!("new preparation expected")
    };
    let receipt = db.store.admit_input(*prepared, &caps(), &apps).unwrap();
    drop(slot);
    assert!(
        matches!(control(&connection, Control::Work(Work::Operation { request: Id(3), operation: header.operation })).await,
        Control::Work(Work::OperationResponse { receipt: found, .. }) if found == receipt)
    );
    let retry = |request| {
        Control::Work(Work::Retry {
            request: Id(request),
            operation: OperationId([3; 16]),
            work: key(),
            expected_attempt: Id(1),
        })
    };
    let Control::Work(Work::Retried {
        receipt: retried, ..
    }) = control(&connection, retry(4)).await
    else {
        panic!("retry expected")
    };
    assert!(
        matches!(control(&connection, retry(5)).await, Control::Work(Work::Retried { receipt: replayed, .. }) if replayed == retried)
    );
    let executor = Executor::new(
        db.store.clone(),
        db.payloads.clone(),
        apps,
        ResultEndpoint::new("localhost:7443".into()).unwrap(),
        caps(),
        pipestream_core::v2::Duration(100),
    )
    .unwrap();
    let completed = executor.run(&identity, &key()).unwrap();
    let manifest = completed.manifest.unwrap();
    assert_eq!(manifest.attempt, Id(2));
    assert!(
        matches!(control(&connection, Control::Result(ResultMessage::GetManifest { request: Id(6), work: key(), attempt: Id(2) })).await,
        Control::Result(ResultMessage::ManifestResponse { manifest: found, .. }) if found == manifest)
    );
    let read = |request, attempt, sha256| {
        Control::Result(ResultMessage::Read {
            request: Id(request),
            work: key(),
            attempt: Id(attempt),
            index: OutputIndex(0),
            expected_sha256: sha256,
        })
    };
    refused(
        &control(&connection, read(7, 2, Digest([9; 32]))).await,
        7,
        ErrorCode::IntegrityError,
    );
    refused(
        &control(&connection, read(8, 1, manifest.outputs[0].sha256)).await,
        8,
        ErrorCode::NotFound,
    );
    let before = db.store.work_view(&identity, &key(), Number(0)).unwrap();
    let mut object = response(&connection, read(9, 2, manifest.outputs[0].sha256)).await;
    assert!(matches!(
        control(&connection, next(10)).await,
        Control::Session(Session::Sequence { .. })
    ));
    let ResponseBody::Result(reader) = object.body() else {
        panic!("result read expected")
    };
    let start = reader.start(std::time::Instant::now()).unwrap();
    assert_eq!(start.request, Id(9));
    assert_eq!(start.sha256, manifest.outputs[0].sha256);
    let mut restored = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        let count = reader
            .read_chunk(&mut buffer, std::time::Instant::now())
            .unwrap();
        if count == 0 {
            break;
        }
        restored.extend_from_slice(&buffer[..count]);
        reader.sent(count, std::time::Instant::now()).unwrap();
    }
    assert_eq!(restored, bytes);
    object.finish_result(std::time::Instant::now()).unwrap();
    let abandoned = response(&connection, read(11, 2, manifest.outputs[0].sha256)).await;
    drop(abandoned);
    assert_eq!(
        db.store.work_view(&identity, &key(), Number(0)).unwrap(),
        before
    );
    assert!(matches!(
        control(
            &connection,
            Control::Work(Work::Cancel {
                request: Id(12),
                operation: OperationId([12; 16]),
                work: key()
            })
        )
        .await,
        Control::Work(Work::Cancelled {
            receipt: OperationReceipt {
                body: Outcome::Cancelled {
                    disposition: Disposition(1),
                    state_at_commit: State::SUCCEEDED,
                    ..
                },
                ..
            },
            ..
        })
    ));
    assert!(matches!(
        control(
            &connection,
            Control::Work(Work::Skip {
                request: Id(13),
                operation: OperationId([13; 16]),
                work: key()
            })
        )
        .await,
        Control::Work(Work::Skipped { .. })
    ));
    assert!(
        matches!(control(&connection, Control::Scope(Scope::Page { request: Id(14), scope: Number(0), after_entity: Number(0), limit: PageLimit(1) })).await, Control::Scope(Scope::PageResponse { more: false, entries, .. }) if entries.len() == 1 && entries[0].state == State::SUCCEEDED)
    );
    let mut cursor = store::ReconcileCursor::default();
    for _ in 0..4 {
        db.store.reconcile(&mut cursor, 8).unwrap();
    }
    let summary = root(&connection, 15, digest).await;
    let held = response(&connection, read(16, 2, manifest.outputs[0].sha256)).await;
    immediate(
        &connection,
        Control::Drain(Drain::Complete {
            request: Id(17),
            generation: Id(1),
            root_summary: summary.clone(),
        }),
        17,
        ErrorCode::NotReady,
    );
    drop(held);
    assert!(matches!(
        control(
            &connection,
            Control::Drain(Drain::Complete {
                request: Id(18),
                generation: Id(1),
                root_summary: summary
            })
        )
        .await,
        Control::Drain(Drain::Completed { .. })
    ));
}
