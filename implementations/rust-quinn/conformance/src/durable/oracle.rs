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
}
