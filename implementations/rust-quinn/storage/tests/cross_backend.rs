//! Cross-backend identity: the same operation sequence must produce
//! identical replay output, cursors, listings, and verification on every
//! backend. The local backends (memory, filesystem) run side by side here;
//! the service backends prove the same identity by passing the shared
//! contract suite, which pins absolute cursor values
//! (`commits_consume_cursors_in_the_global_order`).

use pipestream_storage::{
    Cursor, FsStore, Intent, Journal, MemoryStore, ObjectStore, ReplayEntry, ScopePath, layout,
    sha256,
};

fn intent(id: &str, record: &str) -> Intent {
    Intent::new(id, record.as_bytes().to_vec()).expect("intent")
}

/// Runs one fixed operation script and returns the observable outputs.
fn script<B: ObjectStore + Journal>(
    store: &mut B,
) -> (Vec<Cursor>, Vec<u64>, Vec<ReplayEntry>, Vec<ReplayEntry>) {
    let scope = ScopePath::root(40);
    let child = scope.child(1).expect("child");
    for (entity, payload) in [(1u64, "alpha"), (2, "beta"), (3, "gamma")] {
        store
            .put(&scope, entity, payload.as_bytes(), &sha256(payload.as_bytes()))
            .expect("put");
    }
    store
        .put(&child, 7, b"nested", &sha256(b"nested"))
        .expect("put child");
    let digest = layout::scope_digest(&[
        (1, sha256(b"alpha")),
        (2, sha256(b"beta")),
        (3, sha256(b"gamma")),
    ])
    .expect("scope digest");
    store.seal_scope(&scope, &digest, 3).expect("seal");
    store.verify_scope(&scope).expect("verify");

    let a = store.append_intent(&intent("a", "first")).expect("append");
    let b = store.append_intent(&intent("b", "second")).expect("append");
    store.commit("a", b"a-done").expect("commit");
    let c = store.append_intent(&intent("c", "third")).expect("append");
    // Identical re-append returns the original cursor.
    let a_again = store.append_intent(&intent("a", "first")).expect("re-append");
    assert_eq!(a, a_again);
    let full = store.replay(Cursor(0)).expect("replay");
    let tail = store.replay(b).expect("replay tail");
    (vec![a, b, c], store.list(&scope).expect("list"), full, tail)
}

#[test]
fn memory_and_filesystem_produce_identical_replay() {
    let mut memory = MemoryStore::new();
    let memory_out = script(&mut memory);

    let dir = tempfile::tempdir().expect("tempdir");
    let mut fs = FsStore::open(dir.path()).expect("fs store");
    let fs_out = script(&mut fs);

    assert_eq!(memory_out, fs_out, "memory and filesystem diverged");

    // Pin the absolute expectations: cursors 1, 2, then the commit consumes
    // 3, so the third intent is 4.
    let (cursors, listed, full, tail) = &memory_out;
    assert_eq!(cursors, &[Cursor(1), Cursor(2), Cursor(4)]);
    assert_eq!(listed, &[1, 2, 3]);
    assert_eq!(full.len(), 3);
    assert!(full[0].committed());
    assert!(!full[1].committed());
    assert!(!full[2].committed());
    assert_eq!(full[0].commit.as_deref(), Some(b"a-done".as_slice()));
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].intent.id(), "c");
}
