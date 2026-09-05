use super::*;
use crate::{
    Checkpoint, ERROR_ENTITY_INVALID, ERROR_INTEGRITY, ERROR_LIMIT_EXCEEDED,
    persistence::{SessionStore, SqliteSessionStore, StoreError},
    session::{EntityState, NewEntity},
};

fn frame(ids: &[u32], sequence: u64, sealed_ids: Option<&[u32]>) -> WorkSetFrame {
    WorkSetFrame {
        session_id: "sealed-1".into(),
        producer_id: [1; 16],
        scope_id: 0,
        parent: None,
        sequence,
        entity_ids: ids.to_vec(),
        flags: if sealed_ids.is_some() { SEAL } else { 0 },
        seal_digest: sealed_ids
            .map(|all| seal_digest("sealed-1", [1; 16], 0, None, &all.iter().copied().collect())),
    }
}

fn session() -> Session {
    Session::new_sealed("sealed-1", [1; 16], 7, 1024).unwrap()
}
fn admit(s: &mut Session, id: u32) -> Result<(), ProtocolError> {
    let key = s.add_root(NewEntity {
        entity_id: id,
        layer: 0,
        payload_digest: [2; 32],
        policy: None,
    })?;
    s.transition(key, EntityState::Processing)?;
    s.complete_entity(key, [3; 32])
}
fn checkpoint(id: u32) -> Checkpoint {
    Checkpoint {
        checkpoint_id: "sealed-cut".into(),
        sequence_number: 1,
        checkpoint_entity_id: id,
        scope_id: None,
        flags: 0,
        timeout_ms: None,
    }
}

#[test]
fn checkpoint_cannot_ignore_undeclared_arrival_or_missing_seal() {
    let mut s = session();
    s.declare_work(&frame(&[1, MAX_ENTITY_ID], 0, None), 0)
        .unwrap();
    admit(&mut s, 1).unwrap();
    s.request_checkpoint(&checkpoint(MAX_ENTITY_ID)).unwrap();
    assert!(!s.checkpoint_satisfied(0, 1).unwrap());
    assert!(s.final_lineage_digest().is_err());
    admit(&mut s, MAX_ENTITY_ID).unwrap();
    assert!(!s.checkpoint_satisfied(0, 1).unwrap());
    s.declare_work(&frame(&[], 1, Some(&[1, MAX_ENTITY_ID])), 0)
        .unwrap();
    assert!(s.checkpoint_satisfied(0, 1).unwrap());
    s.acknowledge_checkpoint(0, 1).unwrap();
    assert!(s.final_lineage_digest().is_ok());
    assert!(admit(&mut s, MAX_ENTITY_ID).is_err());
}

#[test]
fn immutable_seal_and_replay_survive_wal_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("work.sqlite3");
    let declaration = frame(&[1, 2], 0, Some(&[1, 2]));
    let store = SqliteSessionStore::open(&path).unwrap();
    store.create(&session()).unwrap();
    let (ack, _) = store
        .transact("sealed-1", |s| s.declare_work(&declaration, 0))
        .unwrap();
    drop(store);
    let reopened = SqliteSessionStore::open(&path).unwrap();
    assert_eq!(
        reopened
            .transact("sealed-1", |s| s.declare_work(&declaration, 0))
            .unwrap()
            .0,
        ack
    );
    let before = reopened.load("sealed-1").unwrap().unwrap();
    assert!(
        reopened
            .transact("sealed-1", |s| s.declare_work(&frame(&[3], 1, None), 0))
            .is_err()
    );
    assert_eq!(reopened.load("sealed-1").unwrap().unwrap(), before);
    assert_eq!(
        before.session.work_sets.unwrap().scopes[&0].seal_digest,
        declaration.seal_digest
    );
    reopened.integrity_check().unwrap();
}

#[test]
fn bad_seals_identity_sequence_and_limits_leave_state_unchanged() {
    let mut s = session();
    s.declare_work(&frame(&[1], 0, None), 0).unwrap();
    let before = s.clone();
    let mut invalid = frame(&[2], 1, Some(&[1, 2]));
    invalid.seal_digest = Some([0; 32]);
    assert_eq!(
        s.declare_work(&invalid, 0).unwrap_err().code,
        ERROR_INTEGRITY
    );
    let mut invalid = frame(&[2], 1, None);
    invalid.producer_id = [2; 16];
    assert_eq!(
        s.declare_work(&invalid, 0).unwrap_err().code,
        ERROR_ENTITY_INVALID
    );
    invalid.producer_id = [1; 16];
    invalid.session_id = "different-session".into();
    assert_eq!(
        s.declare_work(&invalid, 0).unwrap_err().code,
        ERROR_ENTITY_INVALID
    );
    assert!(s.declare_work(&frame(&[2], 0, None), 0).is_err());
    assert!(s.declare_work(&frame(&[2], 2, None), 0).is_err());
    assert!(s.declare_work(&frame(&[1], 1, None), 0).is_err());
    assert!(admit(&mut s, 2).is_err());
    assert_eq!(s, before);
    s.max_entities_per_scope = 1;
    assert_eq!(
        s.declare_work(&frame(&[2], 1, None), 0).unwrap_err().code,
        ERROR_LIMIT_EXCEEDED
    );
}

#[test]
fn wire_roundtrip_is_deterministic_and_bounded() {
    let mut f = frame(&[1, 2], 0, Some(&[1, 2]));
    let bytes = encode(&f).unwrap();
    assert_eq!(decode(crate::decode_ucf(&bytes).unwrap().1).unwrap(), f);
    f.flags |= ACK;
    assert_eq!(
        decode(crate::decode_ucf(&encode(&f).unwrap()).unwrap().1).unwrap(),
        f
    );
    f.entity_ids = vec![1; MAX_BATCH + 1];
    assert!(encode(&f).is_err());
}

#[test]
fn checkpoint_optional_fields_survive_acknowledgement_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoint.sqlite3");
    let mut session = session();
    session
        .declare_work(&frame(&[1, 2], 0, Some(&[1, 2])), 0)
        .unwrap();
    admit(&mut session, 1).unwrap();
    admit(&mut session, 2).unwrap();
    let mut requests = Vec::new();
    for scope_id in [None, Some(0)] {
        for timeout_ms in [None, Some(30_000), Some(u64::MAX)] {
            let mut request = checkpoint(2);
            request.sequence_number = requests.len() as u64;
            request.scope_id = scope_id;
            request.timeout_ms = timeout_ms;
            session.request_checkpoint(&request).unwrap();
            requests.push(request);
        }
    }
    let store = SqliteSessionStore::open(&path).unwrap();
    store.create(&session).unwrap();
    drop(store);
    for _ in 0..2 {
        let reopened = SqliteSessionStore::open(&path).unwrap();
        for request in &requests {
            let mut expected = request.clone();
            expected.flags = crate::CHECKPOINT_ACK;
            let (ack, _) = reopened
                .transact("sealed-1", |session| {
                    session.request_checkpoint(request)?;
                    session.acknowledge_checkpoint(0, request.sequence_number)
                })
                .unwrap();
            assert_eq!(ack, expected);
            let before = reopened.load("sealed-1").unwrap().unwrap();
            let mut changed = request.clone();
            changed.scope_id = if request.scope_id.is_some() {
                None
            } else {
                Some(0)
            };
            assert!(
                matches!(reopened.transact("sealed-1", |session| session.request_checkpoint(&changed)),
                Err(StoreError::Protocol(error)) if error.code == ERROR_ENTITY_INVALID)
            );
            assert_eq!(reopened.load("sealed-1").unwrap().unwrap(), before);
        }
        reopened.integrity_check().unwrap();
    }
}

#[test]
fn prior_session_format_is_refused_without_conversion() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.sqlite3");
    let store = SqliteSessionStore::open(&path).unwrap();
    store.create(&session()).unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute("UPDATE pipestream_sessions SET format_version = 1", [])
        .unwrap();
    let before: Vec<u8> = conn
        .query_row("SELECT state FROM pipestream_sessions", [], |r| r.get(0))
        .unwrap();
    assert!(
        matches!(store.load("sealed-1"),Err(StoreError::Corrupt(message)) if message.contains("unsupported stored session version 1"))
    );
    let after: Vec<u8> = conn
        .query_row("SELECT state FROM pipestream_sessions", [], |r| r.get(0))
        .unwrap();
    assert_eq!(before, after);
    conn.execute("UPDATE pipestream_sessions SET format_version = 6", [])
        .unwrap();
    assert!(
        matches!(store.load("sealed-1"), Err(StoreError::Corrupt(message)) if message.contains("unsupported stored session version 6"))
    );
    assert!(store.save(1, &session()).is_err());
    let after: Vec<u8> = conn
        .query_row("SELECT state FROM pipestream_sessions", [], |r| r.get(0))
        .unwrap();
    assert_eq!(before, after);
    conn.execute("UPDATE pipestream_sessions SET format_version = 5", [])
        .unwrap();
    assert!(
        matches!(store.load("sealed-1"), Err(StoreError::Corrupt(message)) if message.contains("unsupported stored session version 5"))
    );
    assert!(store.save(1, &session()).is_err());
    let after: Vec<u8> = conn
        .query_row("SELECT state FROM pipestream_sessions", [], |r| r.get(0))
        .unwrap();
    assert_eq!(before, after);
    conn.execute("UPDATE pipestream_sessions SET format_version = 2", [])
        .unwrap();
    assert!(
        matches!(store.load("sealed-1"), Err(StoreError::Corrupt(message)) if message.contains("unsupported stored session version 2"))
    );
    let after: Vec<u8> = conn
        .query_row("SELECT state FROM pipestream_sessions", [], |r| r.get(0))
        .unwrap();
    assert_eq!(before, after);
    conn.execute("UPDATE pipestream_sessions SET format_version = 3", [])
        .unwrap();
    assert!(
        matches!(store.load("sealed-1"), Err(StoreError::Corrupt(message)) if message.contains("unsupported stored session version 3"))
    );
    let after: Vec<u8> = conn
        .query_row("SELECT state FROM pipestream_sessions", [], |r| r.get(0))
        .unwrap();
    assert_eq!(before, after);
    conn.execute("UPDATE pipestream_sessions SET format_version = 4", [])
        .unwrap();
    assert!(
        matches!(store.load("sealed-1"), Err(StoreError::Corrupt(message)) if message.contains("unsupported stored session version 4"))
    );
    assert!(store.save(1, &session()).is_err());
    let after: Vec<u8> = conn
        .query_row("SELECT state FROM pipestream_sessions", [], |r| r.get(0))
        .unwrap();
    assert_eq!(before, after);
}

#[test]
fn declaration_batches_do_not_change_the_seal() {
    let ids: Vec<u32> = (1..=1024).collect();
    let mut s = session();
    for (seq, batch) in ids.chunks(MAX_BATCH).enumerate() {
        s.declare_work(&frame(batch, seq as u64, None), 0).unwrap();
    }
    let seal = frame(&[], 4, Some(&ids));
    s.declare_work(&seal, 0).unwrap();
    assert_eq!(s.work_sets.unwrap().scopes[&0].ids.len(), 1024);
}

#[test]
fn frozen_wire_and_independent_seal_vectors() {
    let fixtures = include_str!("../../../../test-vectors/work-sets.tsv");
    let mut count = 0;
    for row in fixtures.lines().skip(1) {
        let fields: Vec<_> = row.split('\t').collect();
        assert_eq!(fields.len(), 3);
        let bytes: Vec<_> = fields[2]
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        let (kind, body) = crate::decode_ucf(&bytes).unwrap();
        assert_eq!(kind, FRAME_WORK_SET);
        let result = decode(body);
        match fields[1] {
            "valid" => {
                let decoded = result.unwrap_or_else(|error| panic!("{}: {error}", fields[0]));
                assert_eq!(encode(&decoded).unwrap(), bytes, "{}", fields[0]);
                if fields[0] == "root-seal" {
                    assert_eq!(
                        seal_digest("sealed-1", [1; 16], 0, None, &BTreeSet::from([1, 2])),
                        decoded.seal_digest.unwrap()
                    );
                }
            }
            "invalid" => assert_eq!(
                result.unwrap_err().code,
                crate::ERROR_FRAME,
                "{}",
                fields[0]
            ),
            _ => panic!("unknown fixture expectation"),
        }
        count += 1;
    }
    assert_eq!(count, 20);
}

#[test]
fn sealed_profile_requires_layer_one_and_excludes_layer_two() {
    for (layer1, layer2) in [(false, false), (true, true)] {
        let caps = crate::Capabilities {
            layer1_recursive: layer1,
            layer2_resilience: layer2,
            extensions: crate::extensions::Extensions {
                supported: vec![EXTENSION_SEALED_WORK_SETS],
                required: vec![EXTENSION_SEALED_WORK_SETS],
            },
            ..crate::Capabilities::default()
        };
        assert_eq!(
            caps.negotiate(&caps).unwrap_err().code,
            crate::ERROR_EXTENSION_UNSUPPORTED
        );
        assert_eq!(
            caps.validate_response(&caps).unwrap_err().code,
            crate::ERROR_FRAME
        );
    }
}

#[test]
fn sealed_declarations_cannot_convert_a_legacy_session() {
    let mut legacy = Session::new("sealed-1", 7, 1024).unwrap();
    let before = legacy.clone();
    assert_eq!(
        legacy
            .declare_work(&frame(&[1], 0, Some(&[1])), 0)
            .unwrap_err()
            .code,
        ERROR_ENTITY_INVALID
    );
    assert_eq!(legacy, before);
}
