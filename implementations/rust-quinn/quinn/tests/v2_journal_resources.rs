//! Isolated process: exercise the actual global owner ceiling without sharing
//! a unit-test process's concurrent journals. This is not a heap/RSS benchmark.
use pipestream_quic::{
    persistence::PhysicalLimits,
    v2::*,
    v2_client::journal::{Creation, Journal, JournalError, JournalLimits, Options},
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

#[tokio::test(flavor = "current_thread")]
async fn global_journal_owner_ceiling_refuses_before_io_and_recovers_after_real_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    let mut owners = Vec::with_capacity(64);
    let mut database_bytes = 0;
    for index in 0..64 {
        let journal = Journal::initialize(
            directory.path().join(format!("{index}.sqlite")),
            creation(),
            JournalLimits::default(),
            PhysicalLimits::default(),
            Options { in_flight: 1 },
        )
        .await
        .unwrap();
        database_bytes += journal.physical_usage().await.unwrap().database_bytes;
        owners.push(journal);
    }
    let extra = directory.path().join("extra.sqlite");
    let refused = Journal::initialize(
        extra.clone(),
        creation(),
        JournalLimits::default(),
        PhysicalLimits::default(),
        Options::default(),
    )
    .await;
    assert!(matches!(
        refused,
        Err(JournalError::Protocol(Error {
            code: ErrorCode::LimitExceeded,
            ..
        }))
    ));
    assert!(!extra.exists());
    assert!(!directory.path().join("extra.sqlite.client-lock").exists());
    for journal in &owners {
        journal.close();
    }
    for journal in &owners {
        journal.closed().await.unwrap();
    }
    // Closed handles can remain held; they no longer own a worker or files.
    let replacement = Journal::initialize(
        extra,
        creation(),
        JournalLimits::default(),
        PhysicalLimits::default(),
        Options::default(),
    )
    .await
    .unwrap();
    replacement.shutdown().await.unwrap();
    eprintln!(
        "journal owner saturation: live=64 refused-before-io=1 replacement=1 total-database-bytes={database_bytes}"
    );
}
