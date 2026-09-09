//! Independently derived expectations. Datasets come from seeds, never from
//! server summaries, journals, or production codecs.

use crate::hex;
use sha2::{Digest, Sha256};

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// Deterministic seed-derived dataset bytes (splitmix64), independent of any
/// subject implementation.
pub fn dataset(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed ^ 0x9E37_79B9_7F4A_7C15;
    let mut bytes = Vec::with_capacity(len);
    while bytes.len() < len {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^= value >> 31;
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes.truncate(len);
    bytes
}

/// A nonzero 16-byte operation identity derived from the run seed, the
/// operation domain, and a per-scenario index. Retry never mints a new one.
pub fn operation_id(seed: u64, domain: &str, index: u32) -> [u8; 16] {
    let mut hash = Sha256::new();
    hash.update(seed.to_le_bytes());
    hash.update(domain.as_bytes());
    hash.update(index.to_le_bytes());
    let digest = hash.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&digest[..16]);
    if id == [0; 16] {
        id[15] = 1;
    }
    id
}

pub fn operation_hex(id: [u8; 16]) -> String {
    hex(&id)
}

// ---------------------------------------------------------------------------
// pipestream-scope-seal-v2 (hand-encoded, no production codec)
// ---------------------------------------------------------------------------
//
// The seal preimage (src/v2/commitments.rs, `ScopeSeal`) is:
//   SHA-256("pipestream-scope-seal-v2" ||
//           det-CBOR array(7) [ text(authority), text(owner), uint(generation),
//                               uint(scope), uint(producer),
//                               parent-or-null,
//                               array(declared)  /* header only */ ]
//           || for each member entity id ascending: minimal CBOR uint)
// where parent-or-null is null (0xf6) for a root scope or det-CBOR
// array(3) [uint(scope), uint(producer), uint(entity)] for a child, and
// `declared` is the array header (major type 4) of the total membership
// count. The session identity contributes three bare items (no inner array;
// see `SessionIdentity::write`). Every integer uses the minimal CBOR
// unsigned form; text strings are definite-length UTF-8 (major type 3).
// This module encodes that fixed structure by hand so the conformance
// crate keeps zero production-codec dependencies.

fn cbor_uint(out: &mut Vec<u8>, n: u64) {
    if n < 24 {
        out.push(n as u8);
    } else if n <= u8::MAX as u64 {
        out.extend_from_slice(&[0x18, n as u8]);
    } else if n <= u16::MAX as u64 {
        out.push(0x19);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else if n <= u32::MAX as u64 {
        out.push(0x1a);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    } else {
        out.push(0x1b);
        out.extend_from_slice(&n.to_be_bytes());
    }
}

fn cbor_array(out: &mut Vec<u8>, n: u64) {
    if n < 24 {
        out.push(0x80 | n as u8);
    } else if n <= u8::MAX as u64 {
        out.extend_from_slice(&[0x98, n as u8]);
    } else if n <= u16::MAX as u64 {
        out.push(0x99);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else if n <= u32::MAX as u64 {
        out.push(0x9a);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    } else {
        out.push(0x9b);
        out.extend_from_slice(&n.to_be_bytes());
    }
}

fn cbor_text(out: &mut Vec<u8>, text: &str) {
    let bytes = text.as_bytes();
    let len = bytes.len() as u64;
    if len < 24 {
        out.push(0x60 | len as u8);
    } else if len <= u8::MAX as u64 {
        out.extend_from_slice(&[0x78, len as u8]);
    } else if len <= u16::MAX as u64 {
        out.push(0x79);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0x7a);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

/// Independently computed `pipestream-scope-seal-v2` digest. `ids` must be
/// the full declared membership, strictly ascending; `parent` is the child
/// work key `[scope, producer, entity]` or `None` for a root scope.
#[allow(clippy::too_many_arguments)]
pub fn scope_seal_hex(
    authority: &str,
    owner: &str,
    generation: u64,
    scope: u64,
    producer: u64,
    parent: Option<[u64; 3]>,
    ids: &[u64],
) -> String {
    let mut preimage = Vec::new();
    cbor_array(&mut preimage, 7);
    cbor_text(&mut preimage, authority);
    cbor_text(&mut preimage, owner);
    cbor_uint(&mut preimage, generation);
    cbor_uint(&mut preimage, scope);
    cbor_uint(&mut preimage, producer);
    match parent {
        Some([parent_scope, parent_producer, parent_entity]) => {
            cbor_array(&mut preimage, 3);
            cbor_uint(&mut preimage, parent_scope);
            cbor_uint(&mut preimage, parent_producer);
            cbor_uint(&mut preimage, parent_entity);
        }
        None => preimage.push(0xf6),
    }
    cbor_array(&mut preimage, ids.len() as u64);
    let mut hasher = Sha256::new();
    hasher.update(b"pipestream-scope-seal-v2");
    hasher.update(&preimage);
    for id in ids {
        let mut encoded = Vec::new();
        cbor_uint(&mut encoded, *id);
        hasher.update(&encoded);
    }
    hex(&hasher.finalize())
}

// ---------------------------------------------------------------------------
// pipestream-result-manifest-v2 / pipestream-status-{leaf,node,empty}-v2
// (hand-encoded, no production codec; preimages per src/v2/commitments.rs and
// test-vectors/v2/commitments.tsv)
// ---------------------------------------------------------------------------

fn cbor_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len() as u64;
    if len < 24 {
        out.push(0x40 | len as u8);
    } else if len <= u8::MAX as u64 {
        out.extend_from_slice(&[0x58, len as u8]);
    } else if len <= u16::MAX as u64 {
        out.push(0x59);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0x5a);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

fn work_key(out: &mut Vec<u8>, work: [u64; 3]) {
    cbor_array(out, 3);
    cbor_uint(out, work[0]);
    cbor_uint(out, work[1]);
    cbor_uint(out, work[2]);
}

/// One output entry of a result manifest, as printed by the subject's
/// MANIFEST observation (`Output { index, length, sha256, content_type,
/// locator }`).
pub struct OutputView<'a> {
    pub index: u64,
    pub length: u64,
    pub sha256: [u8; 32],
    pub content_type: &'a str,
    pub locator: &'a str,
}

/// A decoded result manifest observation, sufficient to re-encode the exact
/// deterministic CBOR the subject committed.
pub struct ManifestView<'a> {
    pub authority: &'a str,
    pub owner: &'a str,
    pub generation: u64,
    pub work: [u64; 3],
    pub attempt: u64,
    pub input_sha256: [u8; 32],
    pub committed_at: u64,
    pub available_until: u64,
    pub outputs: Vec<OutputView<'a>>,
}

/// `pipestream-result-manifest-v2`: SHA-256 over the domain and the
/// det-CBOR array(10) [2, authority, owner, generation, work, attempt,
/// input_sha256, committed_at, available_until, outputs].
pub fn manifest_digest(view: &ManifestView<'_>) -> [u8; 32] {
    let mut body = Vec::new();
    cbor_array(&mut body, 10);
    cbor_uint(&mut body, 2);
    cbor_text(&mut body, view.authority);
    cbor_text(&mut body, view.owner);
    cbor_uint(&mut body, view.generation);
    work_key(&mut body, view.work);
    cbor_uint(&mut body, view.attempt);
    cbor_bytes(&mut body, &view.input_sha256);
    cbor_uint(&mut body, view.committed_at);
    cbor_uint(&mut body, view.available_until);
    cbor_array(&mut body, view.outputs.len() as u64);
    for output in &view.outputs {
        cbor_array(&mut body, 5);
        cbor_uint(&mut body, output.index);
        cbor_uint(&mut body, output.length);
        cbor_bytes(&mut body, &output.sha256);
        cbor_text(&mut body, output.content_type);
        cbor_text(&mut body, output.locator);
    }
    let mut hasher = Sha256::new();
    hasher.update(b"pipestream-result-manifest-v2");
    hasher.update(&body);
    hasher.finalize().into()
}

/// `pipestream-status-leaf-v2`: SHA-256 over the domain and the det-CBOR
/// array(5) [work, state, attempt, manifest_digest-or-null,
/// child_status_root-or-null]. Terminal states other than SUCCEEDED carry a
/// null manifest digest on the wire.
pub fn status_leaf(
    work: [u64; 3],
    state: u64,
    attempt: u64,
    manifest_digest: Option<[u8; 32]>,
    child_status_root: Option<[u8; 32]>,
) -> [u8; 32] {
    let mut body = Vec::new();
    cbor_array(&mut body, 5);
    work_key(&mut body, work);
    cbor_uint(&mut body, state);
    cbor_uint(&mut body, attempt);
    match manifest_digest {
        Some(digest) => cbor_bytes(&mut body, &digest),
        None => body.push(0xf6),
    }
    match child_status_root {
        Some(digest) => cbor_bytes(&mut body, &digest),
        None => body.push(0xf6),
    }
    let mut hasher = Sha256::new();
    hasher.update(b"pipestream-status-leaf-v2");
    hasher.update(&body);
    hasher.finalize().into()
}

pub fn status_node(left: [u8; 32], right: [u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"pipestream-status-node-v2");
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

/// Fold status leaves entity-ascending exactly like the subject's
/// `StatusRoot`: pairwise nodes bottom-up, an odd rightmost subtree
/// duplicated at each level, and the hash of the domain alone for zero
/// leaves.
pub fn status_root(leaves: &[[u8; 32]]) -> [u8; 32] {
    if leaves.is_empty() {
        let mut hasher = Sha256::new();
        hasher.update(b"pipestream-status-empty-v2");
        return hasher.finalize().into();
    }
    let mut level: Vec<[u8; 32]> = leaves.to_vec();
    while level.len() > 1 {
        let mut next = Vec::with_capacity(level.len().div_ceil(2));
        let mut index = 0;
        while index < level.len() {
            let left = level[index];
            let right = if index + 1 < level.len() {
                level[index + 1]
            } else {
                level[index]
            };
            next.push(status_node(left, right));
            index += 2;
        }
        level = next;
    }
    level[0]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_commitments_match_production_frozen_vectors() {
        // test-vectors/v2/commitments.tsv rows result-manifest,
        // success-status-leaf, empty-status-root, two-status-nodes: the
        // digests below are the production subject's frozen answers.
        let manifest = ManifestView {
            authority: "authority-1",
            owner: "owner-1",
            generation: 1,
            work: [0, 0, 1],
            attempt: 1,
            input_sha256: [
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ],
            committed_at: 1_000_200,
            available_until: 1_120_200,
            outputs: vec![OutputView {
                index: 0,
                length: 3,
                sha256: [
                    0xb5, 0xd4, 0x04, 0x5c, 0x3f, 0x46, 0x6f, 0xa9, 0x1f, 0xe2, 0xcc, 0x6a, 0xbe,
                    0x79, 0x23, 0x2a, 0x1a, 0x57, 0xcd, 0xf1, 0x04, 0xf7, 0xa2, 0x6e, 0x71, 0x6e,
                    0x0a, 0x1e, 0x27, 0x89, 0xdf, 0x78,
                ],
                content_type: "text/plain",
                locator: "pipestream://processor.example:9443/v2/sessions/1/scopes/0/producers/0/entities/1/attempts/1/outputs/0",
            }],
        };
        // The frozen leaf vector commits the manifest digest
        // 2a5a87e59a2bec2f086ccac1816a172822ff36d506cb232feb9614ac6886c77a.
        let manifest_digest = manifest_digest(&manifest);
        assert_eq!(
            hex(&manifest_digest),
            "2a5a87e59a2bec2f086ccac1816a172822ff36d506cb232feb9614ac6886c77a"
        );
        let leaf = status_leaf([0, 0, 1], 5, 1, Some(manifest_digest), None);
        assert_eq!(
            hex(&leaf),
            "da1f6989b62111ca873f91678431c689a1d9b9d9a5ff8f3929c0b04b2e68f42d"
        );
        assert_eq!(
            hex(&status_root(&[])),
            "1bcf3ba5ac1c0b66289288a2270e031bbc07b4eb7eca5fa0defef3e493b76386"
        );
        assert_eq!(
            hex(&status_node(leaf, leaf)),
            "35ac7ea6d7bc335441a2b456675ccf93938728754d8b9f0d28fc6fef7c696f55"
        );
        // Odd folds duplicate the rightmost subtree at each level.
        let (a, b, c) = ([0x11; 32], [0x22; 32], [0x33; 32]);
        let ab = status_node(a, b);
        assert_eq!(status_root(&[a, b, c]), status_node(ab, status_node(c, c)));
        assert_eq!(status_root(&[a]), a);
    }

    #[test]
    fn dataset_is_deterministic_seed_dependent_and_exact_length() {
        assert_eq!(dataset(7, 1000), dataset(7, 1000));
        assert_ne!(dataset(7, 64), dataset(8, 64));
        assert_eq!(dataset(7, 0), Vec::<u8>::new());
        for len in [1, 7, 1024] {
            assert_eq!(dataset(3, len).len(), len);
        }
    }

    #[test]
    fn operation_ids_are_stable_nonzero_and_domain_separated() {
        let a = operation_id(1, "declare", 0);
        assert_eq!(a, operation_id(1, "declare", 0));
        assert_ne!(a, operation_id(1, "admit", 0));
        assert_ne!(a, operation_id(2, "declare", 0));
        assert_ne!(a, [0; 16]);
    }

    /// Known-answer vectors produced by the production `scope_seal`
    /// (src/v2/commitments.rs) for session (issuer-a, alice, generation 1).
    /// They pin the hand-encoded replica byte for byte.
    #[test]
    fn scope_seal_matches_production_known_answers() {
        assert_eq!(
            scope_seal_hex("issuer-a", "alice", 1, 0, 0, None, &[1, 2, 3]),
            "6ec1e47e0903f171dee0ca62cac1741850a8fdc3bf3087b00eeff09a518005b6"
        );
        assert_eq!(
            scope_seal_hex("issuer-a", "alice", 1, 0, 0, None, &[]),
            "4468761c89bb33820093d19e7b2a9c2f0c26804d9c282c664ca9846fd53663d1"
        );
        assert_eq!(
            scope_seal_hex("issuer-a", "alice", 1, 1, 1, Some([0, 0, 5]), &[7, 9]),
            "ee2b5325487ef195a712ad06e157ed192ff0529b413f3a2ee71c4f82f113b1c6"
        );
    }

    #[test]
    fn scope_seal_is_order_and_identity_sensitive() {
        let base = scope_seal_hex("issuer-a", "alice", 1, 0, 0, None, &[1, 2, 3]);
        assert_ne!(
            base,
            scope_seal_hex("issuer-a", "alice", 1, 0, 0, None, &[1, 3, 2])
        );
        assert_ne!(
            base,
            scope_seal_hex("issuer-a", "alice", 2, 0, 0, None, &[1, 2, 3])
        );
        assert_ne!(
            base,
            scope_seal_hex("issuer-a", "alice", 1, 0, 1, None, &[1, 2, 3])
        );
        assert_ne!(
            base,
            scope_seal_hex("issuer-a", "alice", 1, 0, 0, Some([0, 0, 1]), &[1, 2, 3])
        );
        assert_ne!(
            base,
            scope_seal_hex("issuer-a", "bob", 1, 0, 0, None, &[1, 2, 3])
        );
    }
}
