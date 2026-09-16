//! Shared deterministic core for durable-index-build: corpus generation,
//! term-frequency records, and inverted-index merge. Used by the Rust
//! authority contracts, the coordinator oracle, and (mirrored in Java) the
//! Java contracts. Byte formats are fixed here so both directions agree.

use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;

/// Fixed chunking: every file splits into this many chunks (last takes the
/// remainder).
pub const CHUNKS_PER_FILE: usize = 4;
/// Application labels for the three example contracts.
pub const FILE_LABEL: &str = "index-file/v1";
pub const TF_LABEL: &str = "tf/v1";
pub const MERGE_LABEL: &str = "index-merge/v1";

/// Deterministic PRNG (mulberry32). The corpus must be identical on every
/// run for a given seed; std has no seeded RNG, so this is local.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u32(&mut self) -> u32 {
        self.0 = self.0.wrapping_add(0x6D2B_79F5);
        let mut z = self.0 as u32;
        z = (z ^ (z >> 15)).wrapping_mul(z | 1);
        z ^= z.wrapping_add((z ^ (z >> 7)).wrapping_mul(z | 61));
        z ^ (z >> 14)
    }
}

/// Fixed vocabulary: word0000..word0255. Corpus words are vocabulary draws.
pub fn vocabulary() -> Vec<String> {
    (0..256).map(|i| format!("word{i:04}")).collect()
}

/// Generate one corpus file: `words` vocabulary draws joined by spaces with
/// deterministic punctuation/newlines so chunking is content-agnostic.
pub fn corpus_file(seed: u64, file: u64, words: u64) -> Vec<u8> {
    let vocab = vocabulary();
    let mut rng = Rng::new(seed ^ file.wrapping_mul(0x9E37_79B9));
    let mut out = String::new();
    for w in 0..words {
        if w > 0 {
            out.push(if w % 12 == 0 { '\n' } else { ' ' });
        }
        let word = &vocab[(rng.next_u32() % 256) as usize];
        out.push_str(word);
        if rng.next_u32() % 17 == 0 {
            out.push(',');
        }
    }
    out.push('\n');
    out.into_bytes()
}

/// Split bytes into exactly CHUNKS_PER_FILE chunks (last takes remainder).
pub fn split_chunks(bytes: &[u8]) -> Vec<&[u8]> {
    let per = bytes.len() / CHUNKS_PER_FILE;
    let mut chunks = Vec::with_capacity(CHUNKS_PER_FILE);
    for i in 0..CHUNKS_PER_FILE {
        let start = i * per;
        let end = if i + 1 == CHUNKS_PER_FILE {
            bytes.len()
        } else {
            (i + 1) * per
        };
        chunks.push(&bytes[start..end]);
    }
    chunks
}

/// Terms: maximal runs of ASCII alphanumerics, lowercased.
pub fn terms(bytes: &[u8]) -> Vec<String> {
    let mut terms = Vec::new();
    let mut current = Vec::new();
    for &b in bytes {
        if b.is_ascii_alphanumeric() {
            current.push(b.to_ascii_lowercase());
        } else if !current.is_empty() {
            terms.push(String::from_utf8(std::mem::take(&mut current)).unwrap());
        }
    }
    if !current.is_empty() {
        terms.push(String::from_utf8(current).unwrap());
    }
    terms
}

/// Term-frequency record: `term count` lines, terms sorted byte-wise.
/// This exact byte layout is the cross-implementation contract.
pub fn tf_record(bytes: &[u8]) -> Vec<u8> {
    let mut owned: BTreeMap<String, u64> = BTreeMap::new();
    for term in terms(bytes) {
        *owned.entry(term).or_default() += 1;
    }
    let mut out = Vec::new();
    for (term, count) in &owned {
        out.extend_from_slice(term.as_bytes());
        out.push(b' ');
        out.extend_from_slice(count.to_string().as_bytes());
        out.push(b'\n');
    }
    out
}

/// Parse a TF record back into (term, count) pairs.
pub fn parse_tf(record: &[u8]) -> Vec<(String, u64)> {
    let text = String::from_utf8(record.to_vec()).expect("tf record is ASCII");
    text.lines()
        .map(|line| {
            let (term, count) = line.split_once(' ').expect("tf line has term+count");
            (term.to_string(), count.parse().expect("tf count parses"))
        })
        .collect()
}

/// Merge TF records into one inverted index: `term df doc:tf doc:tf...`
/// lines, terms sorted byte-wise, docs ascending numeric. Each entry carries
/// its explicit doc id so cancelled documents simply never appear (doc ids
/// are NOT positions: callers pass the original file ordinals).
pub fn merge_index(docs: &[(u64, Vec<Vec<u8>>)]) -> Vec<u8> {
    // term -> doc -> total count
    let mut postings: BTreeMap<String, BTreeMap<u64, u64>> = BTreeMap::new();
    for (doc, records) in docs.iter() {
        let mut doc_terms: BTreeMap<String, u64> = BTreeMap::new();
        for record in records {
            for (term, count) in parse_tf(record) {
                *doc_terms.entry(term).or_default() += count;
            }
        }
        for (term, count) in doc_terms {
            postings.entry(term).or_default().insert(*doc, count);
        }
    }
    let mut out = Vec::new();
    for (term, docs) in &postings {
        out.extend_from_slice(term.as_bytes());
        out.push(b' ');
        out.extend_from_slice(docs.len().to_string().as_bytes());
        for (doc, count) in docs {
            out.push(b' ');
            out.extend_from_slice(doc.to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(count.to_string().as_bytes());
        }
        out.push(b'\n');
    }
    out
}

/// Single-process reference: corpus -> index without the protocol.
/// `skip` names documents to exclude (the cancellation demo).
pub fn reference_index(seed: u64, files: u64, words_per_file: u64, skip: &[u64]) -> Vec<u8> {
    let mut docs = Vec::new();
    for file in 0..files {
        if skip.contains(&file) {
            continue;
        }
        let bytes = corpus_file(seed, file, words_per_file);
        let records = split_chunks(&bytes).iter().map(|c| tf_record(c)).collect();
        docs.push((file, records));
    }
    merge_index(&docs)
}

/// One parent-published reference line: `doc scope producer entity digest`.
/// Shared framing so the coordinator and both authorities parse identically.
pub fn format_ref(doc: u64, scope: u64, producer: u64, entity: u64, digest: &[u8; 32]) -> String {
    format!(
        "{doc} {scope} {producer} {entity} {}",
        digest.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}

/// Parse reference lines (without the header) into
/// (doc, scope, producer, entity, digest).
pub fn parse_refs(text: &str) -> Result<Vec<(u64, u64, u64, u64, [u8; 32])>, String> {
    let mut refs = Vec::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split(' ');
        let mut next = || parts.next().ok_or("ref line needs 5 fields");
        let doc: u64 = next()?.parse().map_err(|_| "ref doc parses")?;
        let scope: u64 = next()?.parse().map_err(|_| "ref scope parses")?;
        let producer: u64 = next()?.parse().map_err(|_| "ref producer parses")?;
        let entity: u64 = next()?.parse().map_err(|_| "ref entity parses")?;
        let hex = next()?;
        if hex.len() != 64 {
            return Err("ref digest is 64 hex digits".into());
        }
        let mut digest = [0u8; 32];
        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            digest[i] = u8::from_str_radix(std::str::from_utf8(chunk).map_err(|_| "hex utf8")?, 16)
                .map_err(|_| "hex digits")?;
        }
        refs.push((doc, scope, producer, entity, digest));
    }
    Ok(refs)
}

/// SHA-256 hex digest of bytes.
pub fn digest_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corpus_is_deterministic() {
        assert_eq!(corpus_file(6, 0, 100), corpus_file(6, 0, 100));
        assert_ne!(corpus_file(6, 0, 100), corpus_file(6, 1, 100));
    }

    #[test]
    fn tf_record_shape() {
        // "Hello, hello world!" -> hello x2, world x1, sorted.
        assert_eq!(tf_record(b"Hello, hello world!"), b"hello 2\nworld 1\n");
    }

    #[test]
    fn merge_shape() {
        let docs = vec![
            (0, vec![tf_record(b"a a b"), tf_record(b"a c")]),
            (1, vec![tf_record(b"b c c")]),
        ];
        let index = merge_index(&docs);
        let text = String::from_utf8(index).unwrap();
        assert_eq!(text, "a 1 0:3\nb 2 0:1 1:1\nc 2 0:1 1:2\n");
    }

    #[test]
    fn merge_skips_cancelled_doc_ids() {
        // Doc 1 cancelled: postings keep original ids 0 and 2.
        let docs = vec![
            (0, vec![tf_record(b"a")]),
            (2, vec![tf_record(b"a b")]),
        ];
        let text = String::from_utf8(merge_index(&docs)).unwrap();
        assert_eq!(text, "a 2 0:1 2:1\nb 1 2:1\n");
    }

    #[test]
    fn ref_round_trip() {
        let digest = [0xabu8; 32];
        let line = format_ref(3, 7, 1, 2, &digest);
        assert_eq!(line, format!("3 7 1 2 {}", "ab".repeat(32)));
        let parsed = parse_refs(&line).unwrap();
        assert_eq!(parsed, vec![(3, 7, 1, 2, digest)]);
    }

    #[test]
    fn chunks_cover_exactly() {
        let bytes = corpus_file(6, 3, 500);
        let chunks = split_chunks(&bytes);
        assert_eq!(chunks.len(), CHUNKS_PER_FILE);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, bytes.len());
    }
}
