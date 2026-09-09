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

#[cfg(test)]
mod tests {
    use super::*;

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
