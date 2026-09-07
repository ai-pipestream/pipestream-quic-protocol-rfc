/**
 * Independent typed implementation of the Section 12 and Appendix F binary contract.
 *
 * <p>This package currently supplies immutable messages, schema-directed deterministic CBOR,
 * incremental framing/object verification, bounded client correlation, profile-selection validation
 * and streaming commitments. It does not authorize callers, advertise profiles, persist work or
 * provide a version-2 endpoint. Structural validity or response correlation alone is not evidence
 * of admission, membership, descendant coverage or computation. Authenticated session context and
 * durable state must supply those checks. A real transport must drive deadlines even without read
 * callbacks, reserve control credit and bound stream creation, queues and incomplete headers.
 *
 * <p>Public records own their digest arrays and collections. Incremental decoders and commitment
 * builders belong to one caller and cannot resume after a protocol error. No version-1 storage or
 * messages are implicitly converted, and no Rust implementation code is linked into this package.
 */
package ai.pipestream.quic.v2;
