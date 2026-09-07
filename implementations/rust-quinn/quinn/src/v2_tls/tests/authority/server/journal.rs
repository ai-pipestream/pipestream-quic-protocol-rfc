//! Real wire replay using an exclusively reopened client journal. This is not
//! the independent cross-language/process-failure driver required by the goal.
use super::*;
use crate::v2_client::journal::{
    Creation, Intent, Journal, JournalLimits, Options as JournalOptions,
};

fn creation() -> Creation {
    Creation {
        authority: IdentityLabel("issuer-a".into()),
        owner: IdentityLabel("alice".into()),
        creation_sequence: Id(1),
        policy: policy(),
        results: true,
    }
}
async fn open(path: &std::path::Path) -> Journal {
    Journal::open(
        path.to_owned(),
        creation(),
        JournalLimits::default(),
        PhysicalLimits::default(),
        JournalOptions::default(),
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
fn declared(response: Control) -> OperationReceipt {
    let Control::Scope(Scope::Declared { receipt, .. }) = response else {
        panic!("expected declaration receipt")
    };
    receipt
}

#[tokio::test]
async fn journal_replays_unrecorded_creation_and_declaration_without_new_identities() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let running = Running::new(options());
    let journal = Journal::initialize(
        path.clone(),
        creation(),
        JournalLimits::default(),
        PhysicalLimits::default(),
        JournalOptions::default(),
    )
    .await
    .unwrap();
    let mut client = running.client(Some(0)).await;
    client.negotiate(offered()).await;
    let first = client
        .call(|id| journal.creation().request(Id(id)).unwrap())
        .await;
    // Received by transport but not committed to the caller's recovery history.
    journal.shutdown().await.unwrap();
    drop(journal);
    drop(client);
    let journal = open(&path).await;
    assert!(journal.binding().await.unwrap().is_none());
    let mut client = running.client(Some(1)).await;
    let selected = client.negotiate(offered()).await;
    let replay = client
        .call(|id| journal.creation().request(Id(id)).unwrap())
        .await;
    assert_eq!(first, replay);
    journal
        .record_binding(replay.clone(), selected)
        .await
        .unwrap();
    journal.prepare(declaration()).await.unwrap();
    let saved_intent = journal.intent(declaration().operation).await.unwrap();
    let original = declared(
        client
            .call(|id| saved_intent.control(Id(id)).unwrap())
            .await,
    );
    assert!(
        journal
            .receipt(declaration().operation)
            .await
            .unwrap()
            .is_none()
    );
    journal.shutdown().await.unwrap();
    drop(journal);
    drop(client);
    let journal = open(&path).await;
    let mut client = running.client(Some(0)).await;
    let selected = client.negotiate(offered()).await;
    let identity = journal.identity().await.unwrap();
    let response = client
        .call(|id| attach(id, "alice", identity.generation.0))
        .await;
    journal.record_binding(response, selected).await.unwrap();
    let saved_intent = journal.intent(declaration().operation).await.unwrap();
    let replay = declared(
        client
            .call(|id| saved_intent.control(Id(id)).unwrap())
            .await,
    );
    assert_eq!(replay, original);
    journal.record_receipt(replay).await.unwrap();
    assert!(
        journal
            .unresolved(Number(0), PageLimit(256))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        client.call(next).await,
        Control::Session(Session::Sequence {
            next_creation_sequence: Id(2),
            ..
        })
    ));
    journal.shutdown().await.unwrap();
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn journal_recovers_unrecorded_input_admission_and_reads_the_original_attempt_output() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let running = Running::new(options());
    let journal = Journal::initialize(
        path.clone(),
        creation(),
        JournalLimits::default(),
        PhysicalLimits::default(),
        JournalOptions::default(),
    )
    .await
    .unwrap();
    let mut client = running.client(Some(0)).await;
    let selected = client.negotiate(offered()).await;
    journal
        .record_binding(client.call(create).await, selected)
        .await
        .unwrap();
    journal.prepare(declaration()).await.unwrap();
    let covering = declared(
        client
            .call(|id| declaration().control(Id(id)).unwrap())
            .await,
    );
    journal.record_receipt(covering).await.unwrap();
    let bytes = vec![0x4d; 65536];
    let header = inputs::header(&bytes);
    let intent = Intent {
        operation: header.operation,
        mutation: Mutation::Admit(header.parameters),
    };
    journal.prepare(intent.clone()).await.unwrap();
    let stored = journal
        .intent(intent.operation)
        .await
        .unwrap()
        .input(journal.identity().await.unwrap().generation)
        .unwrap();
    let mut input = client.flow.open_data().await.unwrap();
    let stream = StreamId(u64::from(input.id()));
    let mut encoded = stored.encode_framed().unwrap();
    encoded.extend_from_slice(&bytes);
    tokio::time::timeout(HANDSHAKE, input.write_all(&encoded))
        .await
        .unwrap()
        .unwrap();
    input.finish().unwrap();
    let Control::Work(Work::Admitted {
        request: RequestTag::Input { stream: actual },
        receipt: original,
    }) = client.receive().await
    else {
        panic!("expected admission")
    };
    assert_eq!(actual, stream);
    drop(input);
    journal.shutdown().await.unwrap();
    drop(journal);
    drop(client);
    let journal = open(&path).await;
    assert!(journal.receipt(intent.operation).await.unwrap().is_none());
    let mut client = running.client(Some(1)).await;
    let selected = client.negotiate(offered()).await;
    journal
        .record_binding(client.call(|id| attach(id, "alice", 1)).await, selected)
        .await
        .unwrap();
    let response = client
        .call(|id| {
            Control::Work(Work::Operation {
                request: Id(id),
                operation: intent.operation,
            })
        })
        .await;
    let Control::Work(Work::OperationResponse { receipt, .. }) = response else {
        panic!("expected recovered receipt")
    };
    assert_eq!(receipt, original);
    journal.record_receipt(receipt).await.unwrap();
    let (revision, view) = client.success().await;
    assert_eq!(view.attempt, Number(1));
    journal.observe_work(revision, view.clone()).await.unwrap();
    journal
        .remember_reference(view.manifest.as_ref().unwrap().clone(), OutputIndex(0))
        .await
        .unwrap();
    journal.shutdown().await.unwrap();
    drop(journal);
    drop(client);
    let journal = open(&path).await;
    assert_eq!(
        journal.observed_work(key()).await.unwrap().unwrap().view,
        view
    );
    let reference = journal
        .retained_reference(key(), Id(1), OutputIndex(0))
        .await
        .unwrap();
    // The trusted fixture endpoint and separately configured rotated certificate
    // come from Running, never from the retained locator hint.
    let mut client = running.client(Some(0)).await;
    let selected = client.negotiate(offered()).await;
    journal
        .record_binding(
            client.call(|id| reference.attach(Id(id)).unwrap()).await,
            selected,
        )
        .await
        .unwrap();
    let request = client.request(|id| reference.read(Id(id)).unwrap()).await;
    assert_eq!(
        client
            .receive_result(request, reference.manifest(), reference.index())
            .await,
        bytes
    );
    assert_eq!(client.success().await, (revision, view));
    assert!(
        journal
            .unresolved(Number(0), PageLimit(256))
            .await
            .unwrap()
            .is_empty()
    );
    let page = |request| {
        Control::Scope(Scope::Page {
            request,
            scope: Number(0),
            after_entity: Number(0),
            limit: PageLimit(256),
        })
    };
    let request = client.request(|id| page(Id(id))).await;
    let observed = journal
        .observe_scope_page(page(request), client.receive().await)
        .await
        .unwrap();
    assert!(observed.membership_verified);
    assert!(journal.covered_scope(Number(0)).await.unwrap().is_none());
    let response = client
        .call(|id| {
            Control::Scope(Scope::Checkpoint {
                request: Id(id),
                scope: Number(0),
                seal: observed.seal.unwrap(),
                wait_ms: WaitMs(30000),
            })
        })
        .await;
    let Control::Scope(Scope::CheckpointResponse { summary, .. }) = response else {
        panic!("expected durable root checkpoint: {response:?}")
    };
    journal.record_checkpoint(summary.clone()).await.unwrap();
    journal.shutdown().await.unwrap();
    drop(journal);
    drop(client);
    let journal = open(&path).await;
    assert_eq!(
        journal.covered_scope(Number(0)).await.unwrap(),
        Some(summary.clone())
    );
    let mut client = running.client(Some(1)).await;
    let selected = client.negotiate(offered()).await;
    journal
        .record_binding(client.call(|id| attach(id, "alice", 1)).await, selected)
        .await
        .unwrap();
    assert_eq!(
        journal
            .scope_members(Number(0), Number(0), PageLimit(1))
            .await
            .unwrap()[0]
            .work,
        key()
    );
    let completion = journal.root_completion(Id(client.next)).await.unwrap();
    let response = client
        .call(|id| {
            assert_eq!(request_id(&completion), Some(Id(id)));
            completion
        })
        .await;
    let Control::Drain(Drain::Completed {
        generation,
        root_summary,
        ..
    }) = response
    else {
        panic!("expected exact completed-session cut: {response:?}")
    };
    assert_eq!(generation, Id(1));
    assert_eq!(root_summary, summary);
    journal.shutdown().await.unwrap();
    drop(client);
    running.finish().await;
}
