//! Public V2 client against the actual authenticated durable listener. Payloads
//! cross QUIC; no test dispatches a control request directly to the authority.
use super::*;
use crate::v2_client::{
    journal::{self as local, Intent, Journal},
    transport::{self as wire, Reply, Transport},
};
mod adversary;

fn client_options() -> wire::Options {
    wire::Options {
        offer: Capabilities {
            response: ResponseFlag(0),
            ..caps()
        },
        flow: options().flow,
        ..Default::default()
    }
}
async fn connect(running: &Running, options: wire::Options, principal: usize) -> Transport {
    Transport::connect(
        "127.0.0.1:0".parse().unwrap(),
        running.address,
        "localhost",
        wire::Security::new(
            roots(&running.tls.issuer),
            Some(running.tls.clients[principal].identity()),
        )
        .unwrap(),
        options,
    )
    .await
    .unwrap()
}
async fn control(client: &Transport, request: Control) -> Control {
    match client.exchange(request, None).await.unwrap() {
        Reply::Control(c) => c,
        Reply::Object(_) => panic!("expected control"),
    }
}
async fn succeeded(client: &Transport) -> (Id, WorkView) {
    tokio::time::timeout(HANDSHAKE, async {
        loop {
            let c = control(
                client,
                Control::Work(Work::Watch {
                    request: Id(1),
                    work: key(),
                    after_revision: Number(0),
                    wait_ms: WaitMs(0),
                }),
            )
            .await;
            let Control::Work(Work::View { revision, work, .. }) = c else {
                panic!("{c:?}")
            };
            if work.state == State::SUCCEEDED {
                return (revision, *work);
            }
            assert!(!work.state.is_terminal());
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
fn read(manifest: &Manifest) -> Control {
    Control::Result(ResultMessage::Read {
        request: Id(1),
        work: manifest.work.clone(),
        attempt: manifest.attempt,
        index: OutputIndex(0),
        expected_sha256: manifest.outputs[0].sha256,
    })
}
async fn until_pending(client: &Transport, count: usize) {
    tokio::time::timeout(HANDSHAKE, async {
        while client.in_flight() != count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn public_client_streams_real_objects_and_reopens_durable_journal_to_exact_root_cut() {
    let running = Running::new(options());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let journal = Journal::initialize(
        path.clone(),
        local::Creation {
            authority: IdentityLabel("issuer-a".into()),
            owner: IdentityLabel("alice".into()),
            creation_sequence: Id(1),
            policy: policy(),
            results: true,
        },
        local::JournalLimits::default(),
        PhysicalLimits::default(),
        local::Options::default(),
    )
    .await
    .unwrap();
    let client = connect(&running, client_options(), 0).await;
    journal
        .record_binding(
            control(&client, journal.creation().request(Id(1)).unwrap()).await,
            client.selected().clone(),
        )
        .await
        .unwrap();
    let declaration = Intent {
        operation: OperationId([1; 16]),
        mutation: Mutation::Declare {
            scope: Number(0),
            entity_ids: vec![Id(1)],
            seal: true,
        },
    };
    journal.prepare(declaration.clone()).await.unwrap();
    let Control::Scope(Scope::Declared { receipt, .. }) =
        control(&client, declaration.control(Id(1)).unwrap()).await
    else {
        panic!()
    };
    journal.record_receipt(receipt).await.unwrap();
    let bytes: Vec<_> = (0..262144).map(|i| (i % 251) as u8).collect();
    let header = inputs::header(&bytes);
    let admission = Intent {
        operation: header.operation,
        mutation: Mutation::Admit(header.parameters.clone()),
    };
    journal.prepare(admission.clone()).await.unwrap();
    let mut input = client.input(header).await.unwrap();
    assert_eq!(input.stream_id(), StreamId(2));
    for chunk in bytes.chunks(7777) {
        input.write(chunk).await.unwrap();
    }
    input.finish().await.unwrap();
    let Control::Work(Work::Admitted { receipt, .. }) = input.response().await.unwrap() else {
        panic!()
    };
    journal.record_receipt(receipt.clone()).await.unwrap();
    // Header-only replay has the same durable result even though its sender
    // never supplies another copy of the large body.
    let replay = client.input(admission.input(Id(1)).unwrap()).await.unwrap();
    let Control::Work(Work::Admitted { receipt: again, .. }) = replay.response().await.unwrap()
    else {
        panic!()
    };
    assert_eq!(again, receipt);
    let (revision, view) = succeeded(&client).await;
    journal.observe_work(revision, view.clone()).await.unwrap();
    let reference = journal
        .remember_reference(view.manifest.unwrap(), OutputIndex(0))
        .await
        .unwrap();
    client.close();
    client.closed().await;
    journal.shutdown().await.unwrap();
    let journal = Journal::open(
        path,
        journal.creation().clone(),
        local::JournalLimits::default(),
        PhysicalLimits::default(),
        local::Options::default(),
    )
    .await
    .unwrap();
    let client = connect(&running, client_options(), 1).await;
    journal
        .record_binding(
            control(&client, reference.attach(Id(1)).unwrap()).await,
            client.selected().clone(),
        )
        .await
        .unwrap();
    let retained = journal
        .retained_reference(key(), Id(1), OutputIndex(0))
        .await
        .unwrap();
    let Reply::Object(mut output) = client
        .exchange(read(retained.manifest()), Some(retained.manifest()))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(output.verification().is_none());
    // An unread result larger than both the stream and connection windows does
    // not block a subsequent control request on this same connection.
    assert!(matches!(
        control(&client, next(1)).await,
        Control::Session(Session::Sequence {
            next_creation_sequence: Id(2),
            ..
        })
    ));
    let mut actual = Vec::new();
    while let Some(chunk) = output.read_unverified().await.unwrap() {
        assert!(chunk.len() <= 8192);
        actual.extend_from_slice(&chunk);
    }
    assert_eq!(actual, bytes);
    assert_eq!(
        output.verification().unwrap().header().sha256,
        retained.manifest().outputs[0].sha256
    );
    drop(output);
    let page = Control::Scope(Scope::Page {
        request: Id(1),
        scope: Number(0),
        after_entity: Number(0),
        limit: PageLimit(256),
    });
    let response = control(&client, page.clone()).await;
    // The transport allocated a real request ID; observation validation checks
    // the actual correlated request rather than a caller's placeholder number.
    let mut actual_page = page;
    if let Control::Scope(Scope::Page { request, .. }) = &mut actual_page {
        *request = request_id(&response).unwrap();
    }
    let observed = journal
        .observe_scope_page(actual_page, response)
        .await
        .unwrap();
    let c = control(
        &client,
        Control::Scope(Scope::Checkpoint {
            request: Id(1),
            scope: Number(0),
            seal: observed.seal.unwrap(),
            wait_ms: WaitMs(30000),
        }),
    )
    .await;
    let Control::Scope(Scope::CheckpointResponse { summary, .. }) = c else {
        panic!("{c:?}")
    };
    journal.record_checkpoint(summary.clone()).await.unwrap();
    let c = control(&client, journal.root_completion(Id(1)).await.unwrap()).await;
    assert!(
        matches!(c, Control::Drain(Drain::Completed { root_summary, .. }) if root_summary == summary)
    );
    journal.shutdown().await.unwrap();
    client.close();
    client.closed().await;
    running.finish().await;
}

#[tokio::test]
async fn cancelled_waiter_retains_correlation_and_reordered_replies_do_not_block_control() {
    let running = Running::new(options());
    let client = connect(&running, client_options(), 0).await;
    control(&client, create(1)).await;
    let Control::Scope(Scope::Declared { receipt, .. }) =
        control(&client, declare(1, vec![Id(1)], true)).await
    else {
        panic!()
    };
    let Outcome::Declared {
        seal: Some(seal), ..
    } = receipt.body
    else {
        panic!()
    };
    let waiting = {
        let client = client.clone();
        tokio::spawn(async move {
            control(
                &client,
                Control::Scope(Scope::Checkpoint {
                    request: Id(999),
                    scope: Number(0),
                    seal,
                    wait_ms: WaitMs(400),
                }),
            )
            .await
        })
    };
    until_pending(&client, 1).await;
    let fast = control(&client, next(123)).await;
    assert_eq!(request_id(&fast), Some(Id(4)));
    assert!(!waiting.is_finished());
    waiting.abort();
    let _ = waiting.await;
    assert_eq!(client.in_flight(), 1);
    until_pending(&client, 0).await;
    assert_eq!(request_id(&control(&client, next(5)).await), Some(Id(5)));
    let c = control(&client, Control::Drain(Drain::Detach { request: Id(1) })).await;
    assert!(matches!(c, Control::Drain(Drain::Detached { .. })));
    assert_eq!(
        client.exchange(next(1), None).await.err().unwrap().code,
        ErrorCode::NotReady
    );
    client.close();
    client.closed().await;
    running.finish().await;
}

#[tokio::test]
async fn abandoned_input_is_refused_without_poisoning_following_control() {
    let running = Running::new(options());
    let client = connect(&running, client_options(), 0).await;
    control(&client, create(1)).await;
    control(&client, declare(1, vec![Id(1)], true)).await;
    let mut input = client.input(inputs::header(&[42; 65536])).await.unwrap();
    input.write(&[42; 100]).await.unwrap();
    drop(input);
    until_pending(&client, 0).await;
    assert!(matches!(
        control(&client, next(1)).await,
        Control::Session(Session::Sequence { .. })
    ));
    let c = control(
        &client,
        Control::Work(Work::Operation {
            request: Id(1),
            operation: OperationId([2; 16]),
        }),
    )
    .await;
    assert!(matches!(
        c,
        Control::Refusal(Refusal {
            code: ErrorCode::NotFound,
            ..
        })
    ));
    client.close();
    client.closed().await;
    running.finish().await;
}
