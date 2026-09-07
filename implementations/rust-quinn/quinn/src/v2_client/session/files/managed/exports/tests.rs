use super::*;
/// Exercise the real export future with a held single worker. One explicit
/// poll enqueues the copy before its waiter is dropped; no timing assumption.
pub(crate) async fn cancel_after_enqueue(
    mut store: ManagedExports,
    id: OperationId,
    reference: RetainedReference,
    source: LocalCopy,
) -> ManagedExports {
    let workers = Workers::new(1, 4).unwrap();
    store.workers = workers.clone();
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let owner = workers.clone();
    let held = tokio::spawn(async move {
        owner
            .run(move || {
                entered.send(()).unwrap();
                released
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .unwrap();
                Ok(())
            })
            .await
            .unwrap();
    });
    waiting.await.unwrap();
    let mut export = Box::pin(store.export(id, reference, source));
    std::future::poll_fn(|cx| {
        assert!(export.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(export);
    release.send(()).unwrap();
    held.await.unwrap();
    // FIFO barrier proves the accepted copy ended, not just that it started.
    workers.run(|| Ok(())).await.unwrap();
    store
}
