//! Real wire replay using an exclusively reopened client journal. This is not
//! the independent cross-language/process-failure driver required by the goal.
use super::*;
use pipestream_core::v2::client::{Creation, Intent, Journal, JournalLimits};

fn creation() -> Creation {
    Creation {
        authority: IdentityLabel("issuer-a".into()),
        owner: IdentityLabel("alice".into()),
        creation_sequence: Id(1),
        policy: policy(),
        results: true,
    }
}
fn open(path: &std::path::Path) -> Journal {
    Journal::open(
        path,
        creation(),
        JournalLimits::default(),
        PhysicalLimits::default(),
    )
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
        &path,
        creation(),
        JournalLimits::default(),
        PhysicalLimits::default(),
    )
    .unwrap();
    let mut client = running.client(Some(0)).await;
    client.negotiate(offered()).await;
    let first = client
        .call(|id| journal.creation().request(Id(id)).unwrap())
        .await;
    // Received by transport but not committed to the caller's recovery history.
    drop(journal);
    drop(client);
    let journal = open(&path);
    assert!(journal.binding().unwrap().is_none());
    let mut client = running.client(Some(1)).await;
    let selected = client.negotiate(offered()).await;
    let replay = client
        .call(|id| journal.creation().request(Id(id)).unwrap())
        .await;
    assert_eq!(first, replay);
    journal.record_binding(&replay, &selected).unwrap();
    journal.prepare(&declaration()).unwrap();
    let original = declared(
        client
            .call(|id| {
                journal
                    .intent(declaration().operation)
                    .unwrap()
                    .control(Id(id))
                    .unwrap()
            })
            .await,
    );
    assert!(journal.receipt(declaration().operation).unwrap().is_none());
    drop(journal);
    drop(client);
    let journal = open(&path);
    let mut client = running.client(Some(0)).await;
    let selected = client.negotiate(offered()).await;
    let response = client
        .call(|id| attach(id, "alice", journal.identity().unwrap().generation.0))
        .await;
    journal.record_binding(&response, &selected).unwrap();
    let replay = declared(
        client
            .call(|id| {
                journal
                    .intent(declaration().operation)
                    .unwrap()
                    .control(Id(id))
                    .unwrap()
            })
            .await,
    );
    assert_eq!(replay, original);
    journal.record_receipt(&replay).unwrap();
    assert!(
        journal
            .unresolved(Number(0), PageLimit(256))
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
    drop(client);
    running.finish().await;
}

#[tokio::test]
async fn journal_recovers_unrecorded_input_admission_and_reads_the_original_attempt_output() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let running = Running::new(options());
    let journal = Journal::initialize(
        &path,
        creation(),
        JournalLimits::default(),
        PhysicalLimits::default(),
    )
    .unwrap();
    let mut client = running.client(Some(0)).await;
    let selected = client.negotiate(offered()).await;
    journal
        .record_binding(&client.call(create).await, &selected)
        .unwrap();
    journal.prepare(&declaration()).unwrap();
    let covering = declared(
        client
            .call(|id| declaration().control(Id(id)).unwrap())
            .await,
    );
    journal.record_receipt(&covering).unwrap();
    let bytes = vec![0x4d; 65536];
    let header = inputs::header(&bytes);
    let intent = Intent {
        operation: header.operation,
        mutation: Mutation::Admit(header.parameters),
    };
    journal.prepare(&intent).unwrap();
    let stored = journal
        .intent(intent.operation)
        .unwrap()
        .input(journal.identity().unwrap().generation)
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
    drop(journal);
    drop(client);
    let journal = open(&path);
    assert!(journal.receipt(intent.operation).unwrap().is_none());
    let mut client = running.client(Some(1)).await;
    let selected = client.negotiate(offered()).await;
    journal
        .record_binding(&client.call(|id| attach(id, "alice", 1)).await, &selected)
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
    journal.record_receipt(&receipt).unwrap();
    let (revision, view) = client.success().await;
    assert_eq!(view.attempt, Number(1));
    journal.observe_work(revision, &view).unwrap();
    journal
        .remember_reference(view.manifest.as_ref().unwrap(), OutputIndex(0))
        .unwrap();
    drop(journal);
    drop(client);
    let journal = open(&path);
    assert_eq!(journal.observed_work(&key()).unwrap().unwrap().view, view);
    let reference = journal
        .retained_reference(&key(), Id(1), OutputIndex(0))
        .unwrap();
    // The trusted fixture endpoint and separately configured rotated certificate
    // come from Running, never from the retained locator hint.
    let mut client = running.client(Some(0)).await;
    let selected = client.negotiate(offered()).await;
    journal
        .record_binding(
            &client.call(|id| reference.attach(Id(id)).unwrap()).await,
            &selected,
        )
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
            .unwrap()
            .is_empty()
    );
    drop(client);
    running.finish().await;
}
