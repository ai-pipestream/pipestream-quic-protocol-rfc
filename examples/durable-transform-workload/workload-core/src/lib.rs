//! Shared deterministic transform, streaming input generator, and
//! independent byte-level verification oracle for the durable-transform
//! workload. Implements contract C1 (`benchmarks/durable-transform/`
//! `contract.md`, sections 2, 3, 6).
//!
//! This crate touches no backend: it is shared by the PipeStream
//! application, the gRPC baseline, and the measurement code. Backend
//! durability, journals, and result streaming live in their own crates.

use sha2::{Digest, Sha256};

/// Frozen chunk payload size in bytes (contract section 1).
pub const CHUNK_SIZE: usize = 65_536;
/// Modulus for the per-offset XOR mask (contract section 2).
pub const XOR_MOD: u16 = 251;

/// Frozen transform: `out[i] = rotl8(b,1) XOR (i mod 251)` with a
/// chunk-relative offset `i` (contract section 2).
pub fn transform_byte(b: u8, offset: u64) -> u8 {
    let rotated = (b << 1) | (b >> 7);
    rotated ^ ((offset % u64::from(XOR_MOD)) as u8)
}

/// Transform one full chunk. `offset_base` is the chunk-relative start
/// (normally 0); output length equals input length.
pub fn transform_chunk(input: &[u8], offset_base: u64) -> Vec<u8> {
    input
        .iter()
        .enumerate()
        .map(|(i, &b)| transform_byte(b, offset_base + i as u64))
        .collect()
}

/// splitmix64 keystream mixer. Deterministic across platforms for the same
/// 64-bit state; the generator advances state by one step per 8 output bytes.
pub fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Derive the generator state for `(seed, chunk_ordinal)` so any chunk can
/// be regenerated or streamed independently of the others.
pub fn chunk_state(seed: u64, ordinal: u64) -> u64 {
    seed ^ ordinal.wrapping_mul(0xD1B5_4DA3_6C96_3F2D)
}

/// Fill `out` with deterministic keystream for chunk `ordinal` of corpus
/// `seed`, starting at chunk-relative byte `start`.
pub fn generate_into(seed: u64, ordinal: u64, start: usize, out: &mut [u8]) {
    let mut state = chunk_state(seed, ordinal);
    // Skip whole 8-byte words strictly below `start`.
    for _ in 0..(start / 8) {
        splitmix64(&mut state);
    }
    // Load the word containing `start`; bytes are consumed little-endian.
    let mut word = splitmix64(&mut state);
    let mut off = start % 8;
    for slot in out.iter_mut() {
        if off == 8 {
            word = splitmix64(&mut state);
            off = 0;
        }
        *slot = (word >> (8 * off)) as u8;
        off += 1;
    }
}

/// Generate one full input chunk (length `len`) for corpus `seed`.
pub fn generate_chunk(seed: u64, ordinal: u64, len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    generate_into(seed, ordinal, 0, &mut out);
    out
}

/// Number of chunks for a corpus of `total_len` bytes.
pub fn chunk_count(total_len: u64) -> u64 {
    total_len.div_ceil(CHUNK_SIZE as u64)
}

/// Length of chunk `ordinal` within a corpus of `total_len` bytes.
pub fn chunk_len(total_len: u64, ordinal: u64) -> usize {
    let start = ordinal * CHUNK_SIZE as u64;
    (total_len.saturating_sub(start).min(CHUNK_SIZE as u64)) as usize
}

/// Expected transformed output for one chunk (oracle primitive).
pub fn expected_chunk(seed: u64, total_len: u64, ordinal: u64) -> Vec<u8> {
    transform_chunk(&generate_chunk(seed, ordinal, chunk_len(total_len, ordinal)), 0)
}

/// SHA-256 over the exact expected final object without materializing it.
/// Feeds chunk transforms incrementally; memory stays bounded by one chunk.
pub fn expected_final_digest(seed: u64, total_len: u64) -> [u8; 32] {
    let mut hash = Sha256::new();
    for ordinal in 0..chunk_count(total_len) {
        hash.update(expected_chunk(seed, total_len, ordinal));
    }
    hash.finalize().into()
}

/// Named contract datasets (contract section 3): (name, size bytes, seed).
pub const DATASETS: &[(&str, u64, u64)] = &[
    ("empty", 0, 0x01),
    ("tiny", 1, 0x02),
    ("boundary", 65_536, 0x03),
    ("boundary-plus-one", 65_537, 0x04),
    ("binary", 200_000, 0x05),
    ("standard", 8 * 1024 * 1024, 0x06),
];

/// Bounded-memory verifier: consumes input and output chunk by chunk and
/// checks length, bytes, and final digest against the oracle.
pub struct StreamVerifier {
    seed: u64,
    total_len: u64,
    ordinal: u64,
    input_seen: u64,
    output_seen: u64,
    hash: Sha256,
}

impl StreamVerifier {
    pub fn new(seed: u64, total_len: u64) -> Self {
        Self {
            seed,
            total_len,
            ordinal: 0,
            input_seen: 0,
            output_seen: 0,
            hash: Sha256::new(),
        }
    }

    /// Feed one input chunk in ordinal order; returns the expected
    /// transformed bytes for cross-checking a worker's output.
    pub fn feed_input(&mut self, input: &[u8]) -> Vec<u8> {
        let expected_in = generate_chunk(self.seed, self.ordinal, input.len());
        assert_eq!(
            input, &expected_in[..],
            "input chunk {} does not match generator",
            self.ordinal
        );
        self.input_seen += input.len() as u64;
        let out = transform_chunk(input, 0);
        self.hash.update(&out);
        self.output_seen += out.len() as u64;
        self.ordinal += 1;
        out
    }

    /// Verify one output chunk against the oracle in ordinal order.
    pub fn feed_output(&mut self, output: &[u8]) {
        let ordinal = self.ordinal;
        let expected = expected_chunk(self.seed, self.total_len, ordinal);
        assert_eq!(
            output, &expected[..],
            "output chunk {ordinal} byte mismatch"
        );
        self.ordinal += 1;
    }

    pub fn finish(self) -> [u8; 32] {
        assert_eq!(self.input_seen, self.total_len, "short input feed");
        assert_eq!(self.output_seen, self.total_len, "short output feed");
        self.hash.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transform_vectors() {
        assert_eq!(transform_byte(0x00, 0), 0x00);
        assert_eq!(transform_byte(0x01, 0), 0x02);
        assert_eq!(transform_byte(0x80, 0), 0x01);
        assert_eq!(transform_byte(0xFF, 1), 0xFE);
        // Offset wraps mod 251: offset 251 behaves like offset 0.
        assert_eq!(transform_byte(0xAB, 251), transform_byte(0xAB, 0));
        assert_eq!(transform_byte(0xAB, 502), transform_byte(0xAB, 0));
    }

    #[test]
    fn transform_is_non_identity_and_length_preserving() {
        let input: Vec<u8> = (0..=255u8).collect();
        let out = transform_chunk(&input, 0);
        assert_eq!(out.len(), input.len());
        assert_ne!(out, input);
        assert!(transform_chunk(&[], 0).is_empty());
    }

    #[test]
    fn generator_is_deterministic_and_chunk_independent() {
        let a = generate_chunk(0x06, 3, 1000);
        let b = generate_chunk(0x06, 3, 1000);
        assert_eq!(a, b);
        assert_ne!(generate_chunk(0x06, 3, 1000), generate_chunk(0x07, 3, 1000));
        assert_ne!(generate_chunk(0x06, 3, 1000), generate_chunk(0x06, 4, 1000));
        // Partial regeneration at an offset matches the full chunk slice.
        let mut part = vec![0u8; 100];
        generate_into(0x06, 3, 137, &mut part);
        assert_eq!(&part[..], &a[137..237]);
    }

    #[test]
    fn generator_covers_binary_alphabet() {
        let mut seen = [false; 256];
        let chunk = generate_chunk(0x05, 0, CHUNK_SIZE);
        for &b in &chunk {
            seen[b as usize] = true;
        }
        assert!(seen.iter().all(|&s| s), "keystream must cover all byte values");
    }

    #[test]
    fn chunking_boundaries() {
        assert_eq!(chunk_count(0), 0);
        assert_eq!(chunk_count(1), 1);
        assert_eq!(chunk_count(65_536), 1);
        assert_eq!(chunk_count(65_537), 2);
        assert_eq!(chunk_len(65_537, 0), 65_536);
        assert_eq!(chunk_len(65_537, 1), 1);
        assert_eq!(chunk_len(0, 0), 0);
    }

    #[test]
    fn oracle_matches_stream_verifier_on_all_small_datasets() {
        for &(name, size, seed) in DATASETS.iter().take(5) {
            let mut verifier = StreamVerifier::new(seed, size);
            let mut outputs = Vec::new();
            for ordinal in 0..chunk_count(size) {
                let input = generate_chunk(seed, ordinal, chunk_len(size, ordinal));
                outputs.push(verifier.feed_input(&input));
            }
            let digest = verifier.finish();
            assert_eq!(digest, expected_final_digest(seed, size), "{name}");
            // Independent output-side verification path.
            let mut out_check = StreamVerifier::new(seed, size);
            out_check.input_seen = size;
            out_check.output_seen = size;
            for output in &outputs {
                out_check.feed_output(output);
            }
        }
    }

    #[test]
    fn verifier_rejects_swapped_order() {
        let (size, seed) = (200_000u64, 0x05u64);
        let c0 = expected_chunk(seed, size, 0);
        let c1 = expected_chunk(seed, size, 1);
        let mut v = StreamVerifier::new(seed, size);
        v.input_seen = size;
        v.output_seen = size;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            v.feed_output(&c1);
            v.feed_output(&c0);
        }));
        assert!(result.is_err(), "swapped chunks must fail verification");
    }

    #[test]
    fn verifier_rejects_wrong_transform() {
        let (size, seed) = (1000u64, 0x02u64);
        let mut bad = expected_chunk(seed, size, 0);
        bad[0] ^= 0xFF;
        let mut v = StreamVerifier::new(seed, size);
        v.input_seen = size;
        v.output_seen = size;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            v.feed_output(&bad);
        }));
        assert!(result.is_err(), "corrupt chunk must fail verification");
    }

    #[test]
    fn empty_corpus_verifies() {
        let v = StreamVerifier::new(0x01, 0);
        let digest = v.finish();
        assert_eq!(digest, expected_final_digest(0x01, 0));
    }

    /// Pinned oracle digests, cross-computed by an independent Python
    /// hashlib mirror (scratch verification, 2026-09-08). Any change to the
    /// transform, keystream, or chunking must change these values.
    #[test]
    fn pinned_oracle_digests() {
        fn hex(s: &str) -> [u8; 32] {
            let mut out = [0u8; 32];
            for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
                out[i] = u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap();
            }
            out
        }
        assert_eq!(
            expected_final_digest(0x01, 0),
            hex("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        );
        assert_eq!(
            expected_final_digest(0x02, 1),
            hex("9d277175737fb50041e75f641acf94d10df9b9721db8fffe874ab57f8ffb062e")
        );
        assert_eq!(
            expected_final_digest(0x03, 65_536),
            hex("fe6f0aa6d69ed8bd6e3a3760ea673a097d7f951756990a98fdb717429f5dd2b7")
        );
        assert_eq!(
            expected_final_digest(0x04, 65_537),
            hex("45768e093c5aac1704af245c0b9893ec7b6ef9755f3998169ead33af68b96e7c")
        );
    }
}
