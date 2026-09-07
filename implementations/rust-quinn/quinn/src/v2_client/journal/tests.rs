use super::*;
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
    time::Duration as Elapsed,
};

fn creation() -> Creation {
    Creation {
        authority: IdentityLabel("issuer-a".into()),
        owner: IdentityLabel("alice".into()),
        creation_sequence: Id(1),
        results: true,
        policy: Policy {
            execution_limit_ms: Duration(60000),
            output_retention_ms: Duration(120000),
            receipt_retention_ms: Duration(180000),
        },
    }
}
fn selection() -> Capabilities {
    Capabilities {
        response: ResponseFlag(1),
        supported: vec![
            ProfileId(DURABLE_WORK.into()),
            ProfileId(RESULT_DELIVERY.into()),
        ],
        required: vec![
            ProfileId(DURABLE_WORK.into()),
            ProfileId(RESULT_DELIVERY.into()),
        ],
        control_limit: ControlLimit(65536),
        stream_limit: ConcurrencyLimit(4),
        pending_limit: ConcurrencyLimit(16),
        object_limit: Number(1 << 20),
        stream_idle_ms: IdleMs(5000),
        stream_lifetime_ms: LifetimeMs(30000),
    }
}
fn binding() -> Control {
    Control::Session(Session::Binding {
        request: Id(1),
        authority: creation().authority,
        owner: creation().owner,
        generation: Id(7),
        creation_sequence: Id(1),
        policy: creation().policy,
        limits: Limits {
            scopes: Id(100),
            entities: Id(10000),
            operations: Id(10000),
            retained_input_bytes: Number(1 << 20),
            retained_output_bytes: Number(1 << 20),
            active_jobs: Id(100),
        },
    })
}
fn declaration(n: u8) -> Intent {
    Intent {
        operation: OperationId([n; 16]),
        mutation: Mutation::Declare {
            scope: Number(0),
            entity_ids: vec![Id(n.into())],
            seal: false,
        },
    }
}
async fn open(path: &std::path::Path, initialize: bool, in_flight: usize) -> Result<Journal> {
    Journal::start(
        path.to_owned(),
        creation(),
        JournalLimits::default(),
        PhysicalLimits::default(),
        Options { in_flight },
        initialize,
    )
    .await
}
async fn bound(path: &std::path::Path, in_flight: usize) -> Journal {
    let journal = open(path, true, in_flight).await.unwrap();
    journal
        .record_binding(binding(), selection())
        .await
        .unwrap();
    journal
}
fn refusal<T>(result: Result<T>, expected: ErrorCode) {
    assert!(matches!(result, Err(JournalError::Protocol(Error {code, ..})) if code == expected));
}
async fn pending<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|cx| {
        assert!(
            future.as_mut().poll(cx).is_pending(),
            "operation did not remain pending"
        );
        Poll::Ready(())
    })
    .await;
}
async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Elapsed::from_secs(5), future)
        .await
        .expect("bounded test wait")
}

#[tokio::test(flavor = "current_thread")]
async fn persistent_intent_and_empty_root_coverage_round_trip_on_owned_worker() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let journal = bound(&path, 4).await;
    journal
        .execute(|_| {
            assert_eq!(
                std::thread::current().name(),
                Some("pipestream-v2-client-journal")
            );
            Ok(())
        })
        .await
        .unwrap();
    let original = Intent {
        operation: OperationId([1; 16]),
        mutation: Mutation::Declare {
            scope: Number(0),
            entity_ids: vec![],
            seal: true,
        },
    };
    journal.prepare(original.clone()).await.unwrap();
    let identity = journal.identity().await.unwrap();
    let seal = scope_seal(&identity, Number(0), Producer(0), None, Number(0), []).unwrap();
    let receipt = OperationReceipt {
        operation: original.operation,
        request_digest: original
            .mutation
            .digest(&identity, Producer(0), original.operation)
            .unwrap(),
        body: Outcome::Declared {
            scope: Number(0),
            producer: Producer(0),
            accepted_count: BatchCount(0),
            declared: Number(0),
            seal: Some(seal),
        },
    };
    assert_eq!(
        journal
            .unresolved(Number(0), PageLimit(256))
            .await
            .unwrap()
            .len(),
        1
    );
    journal.record_receipt(receipt.clone()).await.unwrap();
    let request = Control::Scope(Scope::Page {
        request: Id(2),
        scope: Number(0),
        after_entity: Number(0),
        limit: PageLimit(256),
    });
    let response = Control::Scope(Scope::PageResponse {
        request: Id(2),
        scope: Number(0),
        producer: Producer(0),
        parent: None,
        sealed: true,
        seal: Some(seal),
        declared: Number(0),
        entries: vec![],
        more: false,
    });
    assert!(
        journal
            .observe_scope_page(request, response)
            .await
            .unwrap()
            .membership_verified
    );
    assert!(journal.covered_scope(Number(0)).await.unwrap().is_none());
    let summary = ScopeSummary {
        scope: Number(0),
        producer: Producer(0),
        parent: None,
        seal,
        declared: Number(0),
        counts: Counts {
            success: Number(0),
            failure: Number(0),
            cancelled: Number(0),
            skipped: Number(0),
        },
        status_root: StatusRoot::default().finish(),
        closed_at: Number(200),
    };
    journal.record_checkpoint(summary.clone()).await.unwrap();
    within(journal.shutdown()).await.unwrap();
    let reopened = open(&path, false, 4).await.unwrap();
    assert_eq!(reopened.intent(original.operation).await.unwrap(), original);
    assert_eq!(
        reopened.receipt(receipt.operation).await.unwrap(),
        Some(receipt)
    );
    assert!(
        reopened
            .unresolved(Number(0), PageLimit(256))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        reopened
            .scope_members(Number(0), Number(0), PageLimit(256))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reopened.covered_scope(Number(0)).await.unwrap(),
        Some(summary.clone())
    );
    assert!(
        matches!(reopened.root_completion(Id(3)).await.unwrap(), Control::Drain(Drain::Complete { root_summary, .. }) if root_summary == summary)
    );
    assert!(reopened.physical_usage().await.unwrap().database_bytes > 0);
    within(reopened.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_waiters_keep_running_and_queued_commits_charged_until_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let journal = bound(&path, 2).await;
    let (started, running) = oneshot::channel();
    let (release, blocked) = mpsc::sync_channel(1);
    let owner = journal.clone();
    let task = tokio::spawn(async move {
        owner
            .execute(move |store| {
                started.send(()).unwrap();
                blocked.recv().unwrap();
                store.prepare(&declaration(1))
            })
            .await
    });
    within(running).await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(journal.in_flight(), 1);
    let mut queued = Box::pin(journal.prepare(declaration(2)));
    pending(queued.as_mut()).await;
    drop(queued); // An accepted queued mutation must not disappear with its waiter.
    assert_eq!(journal.in_flight(), 2);
    refusal(journal.binding().await, ErrorCode::LimitExceeded);
    journal.close();
    let mut closing = Box::pin(journal.closed());
    pending(closing.as_mut()).await;
    // A second actor cannot bypass the first worker's ownership/operation budget.
    refusal(open(&path, false, 2).await, ErrorCode::Conflict);
    release.send(()).unwrap();
    within(closing).await.unwrap();
    assert_eq!(journal.in_flight(), 0);
    refusal(journal.binding().await, ErrorCode::Cancelled);
    let reopened = open(&path, false, 2).await.unwrap();
    assert_eq!(
        reopened.intent(declaration(1).operation).await.unwrap(),
        declaration(1)
    );
    assert_eq!(
        reopened.intent(declaration(2).operation).await.unwrap(),
        declaration(2)
    );
    assert_eq!(
        reopened
            .unresolved(Number(0), PageLimit(256))
            .await
            .unwrap()
            .len(),
        2
    );
    within(reopened.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn completed_but_unconsumed_replies_keep_their_capacity() {
    let directory = tempfile::tempdir().unwrap();
    let journal = bound(&directory.path().join("client.sqlite"), 2).await;
    let (release, blocked) = mpsc::sync_channel(1);
    let mut first = Box::pin(journal.execute(move |store| {
        blocked.recv().unwrap();
        store.binding()
    }));
    pending(first.as_mut()).await;
    let (started, after_first) = oneshot::channel();
    let mut second = Box::pin(journal.execute(move |_| {
        let _ = started.send(());
        Ok(())
    }));
    pending(second.as_mut()).await;
    release.send(()).unwrap();
    within(after_first).await.unwrap(); // FIFO worker has already sent the first reply.
    assert_eq!(journal.in_flight(), 2);
    refusal(journal.identity().await, ErrorCode::LimitExceeded);
    within(second).await.unwrap();
    assert_eq!(journal.in_flight(), 1);
    assert!(within(first).await.unwrap().is_some());
    assert_eq!(journal.in_flight(), 0);
    within(journal.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn last_handle_drop_drains_accepted_commit_before_releasing_file_ownership() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let journal = bound(&path, 2).await;
    let mut exit = journal.inner.exit.subscribe();
    let (started, running) = oneshot::channel();
    let (release, blocked) = mpsc::sync_channel(1);
    let owner = journal.clone();
    let task = tokio::spawn(async move {
        owner
            .execute(move |store| {
                started.send(()).unwrap();
                blocked.recv().unwrap();
                store.prepare(&declaration(1))
            })
            .await
    });
    within(running).await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(journal);
    assert!(*exit.borrow() == Exit::Running);
    refusal(open(&path, false, 2).await, ErrorCode::Conflict);
    release.send(()).unwrap();
    within(async {
        loop {
            if *exit.borrow_and_update() != Exit::Running {
                break;
            }
            exit.changed().await.unwrap();
        }
    })
    .await;
    assert!(*exit.borrow() == Exit::Stopped);
    let reopened = open(&path, false, 2).await.unwrap();
    assert_eq!(
        reopened.intent(declaration(1).operation).await.unwrap(),
        declaration(1)
    );
    within(reopened.shutdown()).await.unwrap();
}

#[test]
fn journal_worker_kill_child() {
    let Some(path) = std::env::var_os("PIPESTREAM_V2_ASYNC_JOURNAL_CHILD") else {
        return;
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let journal = bound(&PathBuf::from(path), 2).await;
            journal.prepare(declaration(1)).await.unwrap();
            use std::io::Write;
            println!("async-journal-committed");
            std::io::stdout().flush().unwrap();
            std::future::pending::<()>().await;
            drop(journal);
        });
}

#[tokio::test(flavor = "current_thread")]
async fn live_process_owns_the_journal_and_forced_exit_preserves_its_commit() {
    use std::io::{BufRead, BufReader};
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let mut child = Child(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "v2_client::journal::tests::journal_worker_kill_child",
                "--nocapture",
            ])
            .env("PIPESTREAM_V2_ASYNC_JOURNAL_CHILD", &path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (marker, observed) = oneshot::channel();
    let reader = std::thread::spawn(move || {
        let found = BufReader::new(stdout)
            .lines()
            .any(|line| line.is_ok_and(|s| s == "async-journal-committed"));
        let _ = marker.send(found);
    });
    assert!(within(observed).await.unwrap());
    assert!(child.0.try_wait().unwrap().is_none());
    refusal(open(&path, false, 2).await, ErrorCode::Conflict);
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    reader.join().unwrap();
    let journal = open(&path, false, 2).await.unwrap();
    assert_eq!(
        journal.intent(declaration(1).operation).await.unwrap(),
        declaration(1)
    );
    assert!(
        journal
            .receipt(declaration(1).operation)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        journal
            .unresolved(Number(0), PageLimit(256))
            .await
            .unwrap()
            .len(),
        1
    );
    within(journal.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn close_is_shared_but_dropping_one_handle_does_not_stop_others() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let journal = bound(&path, 2).await;
    let clone = journal.clone();
    drop(journal);
    assert!(clone.binding().await.unwrap().is_some());
    refusal(open(&path, false, 2).await, ErrorCode::Conflict);
    let observer = clone.clone();
    clone.close();
    within(observer.closed()).await.unwrap();
    refusal(observer.prepare(declaration(1)).await, ErrorCode::Cancelled);
    let reopened = open(&path, false, 2).await.unwrap();
    assert!(
        reopened
            .unresolved(Number(0), PageLimit(1))
            .await
            .unwrap()
            .is_empty()
    );
    within(reopened.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn panic_stops_the_owner_without_erasing_a_finished_commit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let journal = bound(&path, 2).await;
    refusal(
        journal
            .execute::<()>(|store| {
                store.prepare(&declaration(1))?;
                panic!("injected worker fault after durable commit");
            })
            .await,
        ErrorCode::InternalError,
    );
    refusal(within(journal.closed()).await, ErrorCode::InternalError);
    assert_eq!(journal.in_flight(), 0);
    refusal(journal.binding().await, ErrorCode::InternalError);
    let reopened = open(&path, false, 2).await.unwrap();
    assert_eq!(
        reopened.intent(declaration(1).operation).await.unwrap(),
        declaration(1)
    );
    within(reopened.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_arguments_and_options_refuse_without_queued_inventory_or_history_reset() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    for count in [0, 33] {
        refusal(open(&path, true, count).await, ErrorCode::LimitExceeded);
        assert!(!path.exists());
    }
    assert!(open(&path, false, 2).await.is_err());
    let journal = bound(&path, 2).await;
    let bad = Intent {
        operation: OperationId([1; 16]),
        mutation: Mutation::Declare {
            scope: Number(0),
            entity_ids: vec![Id(1); 257],
            seal: false,
        },
    };
    refusal(journal.prepare(bad).await, ErrorCode::FrameError);
    assert_eq!(journal.in_flight(), 0);
    assert!(
        journal
            .unresolved(Number(0), PageLimit(256))
            .await
            .unwrap()
            .is_empty()
    );
    let usage = journal.physical_usage().await.unwrap();
    within(journal.shutdown()).await.unwrap();
    assert!(open(&path, true, 2).await.is_err());
    let mut changed = creation();
    changed.owner.0 = "another-owner".into();
    assert!(
        Journal::open(
            path.clone(),
            changed,
            JournalLimits::default(),
            PhysicalLimits::default(),
            Options::default()
        )
        .await
        .is_err()
    );
    let reopened = open(&path, false, 2).await.unwrap();
    assert_eq!(reopened.binding().await.unwrap(), Some(binding()));
    assert_eq!(
        reopened.physical_usage().await.unwrap().database_bytes,
        usage.database_bytes
    );
    within(reopened.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn ownership_sidecar_refuses_symlinks_and_foreign_data_without_overwriting() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let lock = directory.path().join("client.sqlite.client-lock");
    let foreign = directory.path().join("foreign");
    std::fs::write(&foreign, b"retained").unwrap();
    std::os::unix::fs::symlink(&foreign, &lock).unwrap();
    assert!(open(&path, true, 2).await.is_err());
    assert!(!path.exists());
    assert_eq!(std::fs::read(&foreign).unwrap(), b"retained");
    std::fs::remove_file(&lock).unwrap();
    std::fs::write(&lock, b"retained").unwrap();
    assert!(open(&path, true, 2).await.is_err());
    assert_eq!(std::fs::read(&lock).unwrap(), b"retained");
    assert!(!path.exists());
}
