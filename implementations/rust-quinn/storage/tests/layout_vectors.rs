//! Literal cross-language layout vectors.
//!
//! Every expected value below was computed independently with python3
//! hashlib (no Rust code in the loop), so the Java lane can assert the same
//! literals and both implementations are proven byte-compatible:
//!
//! - scope path rendering (8-hex components joined by `/`),
//! - fanout bucket (first byte of SHA-256 over the 8-octet big-endian
//!   entity id, 2 lowercase hex chars) and entity name (16-hex),
//! - canonical scope digest (SHA-256 over ascending `0x00 || id-be64 ||
//!   object-sha256`).

use pipestream_storage::layout::{self, ScopePath, sha256};

struct Vector {
    scope_components: &'static [u32],
    entities: &'static [(u64, &'static [u8])],
    expected_path: &'static str,
    expected_objects: &'static [(&'static str, &'static str)], // (bucket, entity name)
    expected_scope_digest: &'static str,
}

const VECTORS: &[Vector] = &[
    // v1: root scope, one entity.
    Vector {
        scope_components: &[0],
        entities: &[(1, b"hello pipestream")],
        expected_path: "00000000",
        expected_objects: &[("cd", "0000000000000001")],
        expected_scope_digest:
            "2d499828d0c72e0238e7916e61eebb42f57436070f196d5f90fa0605b99b44d8",
    },
    // v2: nested scope, one entity with an empty payload.
    Vector {
        scope_components: &[0x0a, 0x0b],
        entities: &[(42, b"")],
        expected_path: "0000000a/0000000b",
        expected_objects: &[("a6", "000000000000002a")],
        expected_scope_digest:
            "85c98419a57a8db102224f99a110bff4a76eda68c91910c8989316feda918ec2",
    },
    // v3: two entities (ascending order matters for the digest; the input
    // here is deliberately unsorted), one id above 32 bits.
    Vector {
        scope_components: &[0xdead_beef],
        entities: &[(0x0102_0304_0506_0708, b"beta"), (5, b"alpha")],
        expected_path: "deadbeef",
        expected_objects: &[
            ("66", "0102030405060708"),
            ("5d", "0000000000000005"),
        ],
        expected_scope_digest:
            "fa41e79d5446b0065fd9e6d284c6dd88bf7dc4219c4f73858053c459469d5598",
    },
];

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn layout_vectors_match_independent_python_computation() {
    for (index, vector) in VECTORS.iter().enumerate() {
        let scope = ScopePath::from_components(vector.scope_components.to_vec()).expect("scope");
        assert_eq!(
            scope.to_path_string(),
            vector.expected_path,
            "vector {}: scope path",
            index + 1
        );
        for ((entity, _payload), (bucket, name)) in
            vector.entities.iter().zip(vector.expected_objects)
        {
            assert_eq!(
                layout::bucket_hex(*entity),
                *bucket,
                "vector {}: bucket of entity {entity}",
                index + 1
            );
            assert_eq!(
                layout::entity_component(*entity),
                *name,
                "vector {}: name of entity {entity}",
                index + 1
            );
        }
        // Canonical digest input: ascending id order, content digests. The
        // vectors intentionally list v3's entities out of order.
        let digests: Vec<(u64, layout::Digest32)> = vector
            .entities
            .iter()
            .map(|(entity, payload)| (*entity, sha256(payload)))
            .collect();
        assert_eq!(
            hex(&layout::scope_digest(&digests).expect("scope digest")),
            vector.expected_scope_digest,
            "vector {}: scope digest",
            index + 1
        );
    }
}

#[test]
fn empty_scope_digest_is_sha256_of_empty_input() {
    let digest = layout::scope_digest(&[]).expect("scope digest");
    assert_eq!(
        hex(&digest),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}
