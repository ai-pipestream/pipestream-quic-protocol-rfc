/**
 * Independent typed implementation of the Section 12 and Appendix F binary contract.
 *
 * <p>This package currently supplies immutable messages, schema-directed deterministic CBOR,
 * incremental control framing, profile-selection validation and streaming commitments. It does not
 * authorize callers, advertise profiles, persist work or provide a version-2 endpoint. Structural
 * validity alone is not evidence of admission, membership, descendant coverage or computation.
 * Connection correlation, authenticated session context and durable state must supply those checks.
 *
 * <p>Public records own their digest arrays and collections. Incremental decoders and commitment
 * builders belong to one caller and cannot resume after a protocol error. No version-1 storage or
 * messages are implicitly converted, and no Rust implementation code is linked into this package.
 */
package ai.pipestream.quic.v2;
