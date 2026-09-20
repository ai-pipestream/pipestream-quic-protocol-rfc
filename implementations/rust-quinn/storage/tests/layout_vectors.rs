//! Literal cross-language layout vectors.
//!
//! Every expected value below was computed independently with python3
//! hashlib (no Rust code in the loop), so the Java lane can assert the same
//! literals and both implementations are proven byte-compatible:
//!
//! - scope path rendering (8-hex components joined by `/`),
//! - fanout bucket (first byte of SHA-256 over the 4-octet big-endian
//!   entity id, 2 lowercase hex chars) and entity name (8-hex),
//! - canonical content digest (SHA-256 over ascending `0x00 || id-be32 ||
//!   object-sha256`; empty scope = SHA-256 of empty input).

use pipestream_storage::layout::{self, ScopePath, sha256};

struct Vector {
    scope_components: &'static [u32],
    entities: &'static [(u32, &'static [u8])],
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
        expected_objects: &[("b4", "00000001")],
        expected_scope_digest:
            "d36301f18759e453c33a9b7ef0b79400d933fcc1d2a32e866e25af091fda419f",
    },
    // v2: nested scope, one entity with an empty payload.
    Vector {
        scope_components: &[0x0a, 0x0b],
        entities: &[(42, b"")],
        expected_path: "0000000a/0000000b",
        expected_objects: &[("ae", "0000002a")],
        expected_scope_digest:
            "87cd5f2b7c957ae3118cf2b2248a64f0a140a9bbfc3f8b96fe052124415ebc94",
    },
    // v3: two entities (ascending order matters for the digest; the input
    // here is deliberately unsorted), including a high id.
    Vector {
        scope_components: &[0xdead_beef],
        entities: &[(0xf0f0_f0f0, b"beta"), (5, b"alpha")],
        expected_path: "deadbeef",
        expected_objects: &[
            ("72", "f0f0f0f0"),
            ("22", "00000005"),
        ],
        expected_scope_digest:
            "86a05466dccba33b382a0f1e029d03d6348bb96e7a1465eea4918d8766a0dddc",
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
                "vector {}: bucket of entity {entity:#x}",
                index + 1
            );
            assert_eq!(
                layout::entity_component(*entity),
                *name,
                "vector {}: name of entity {entity:#x}",
                index + 1
            );
        }
        // Canonical digest input: ascending id order, content digests. The
        // vectors intentionally list v3's entities out of order.
        let digests: Vec<(u32, layout::Digest32)> = vector
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
