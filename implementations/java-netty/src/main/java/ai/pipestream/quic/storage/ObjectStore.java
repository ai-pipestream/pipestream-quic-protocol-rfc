package ai.pipestream.quic.storage;

import java.io.InputStream;
import java.util.List;
import java.util.Objects;

/**
 * Durable sealed-object storage: write-once, content-verified entities plus
 * per-scope seal records. Blocking calls; implementations are thread-safe but
 * callers should keep them off transport event loops. All failures are
 * reported as {@link StorageException} with the kind that governs recovery.
 */
public interface ObjectStore extends AutoCloseable {
  /**
   * Stores one entity's bytes after verifying them against the caller's
   * expected SHA-256 digest. The write is idempotent: re-putting identical
   * bytes with the same digest succeeds as a no-op, while bytes that differ
   * from the recorded content for the same identifier fail with
   * {@link StorageException.Kind#CORRUPT}. A digest that does not match the
   * submitted bytes is likewise {@code CORRUPT}, and any put into a sealed
   * scope fails with {@link StorageException.Kind#ALREADY_SEALED}.
   *
   * @param scope logical scope address
   * @param entityId unsigned 32-bit entity identifier within the scope
   * @param bytes complete entity content, not retained by this call
   * @param expectedDigest exactly {@value StoreLayout#DIGEST_BYTES} octets, the SHA-256 of {@code bytes}
   * @throws StorageException {@code CORRUPT} for a digest or content conflict, {@code ALREADY_SEALED} after sealing
   */
  void put(ScopePath scope, long entityId, byte[] bytes, byte[] expectedDigest)
      throws StorageException;

  /**
   * Opens a streaming read of one stored entity. The returned stream holds the
   * object independently of the store: removing the object afterwards, for
   * example through {@link #retire}, never invalidates a read opened before
   * the removal.
   *
   * @param scope logical scope address
   * @param entityId unsigned 32-bit entity identifier within the scope
   * @return stream positioned at the first content octet; closing it releases its handle
   * @throws StorageException {@code NOT_FOUND} if the entity is absent
   */
  InputStream openRead(ScopePath scope, long entityId) throws StorageException;

  /**
   * Atomically marks a scope sealed and records its scope digest and entity
   * count in the scope's own container. Sealing is idempotent: an identical
   * re-seal succeeds as a no-op, while a conflicting digest or count fails
   * with {@code CORRUPT}. After sealing, {@link #put} into the scope fails
   * with {@code ALREADY_SEALED} and {@link #retire} removes the whole scope.
   * The digest is the caller's SCOPE_DIGEST, recorded verbatim; the store
   * cannot recompute it because the protocol digest covers entity statuses
   * the store does not hold.
   *
   * @param scope logical scope address
   * @param digest exactly {@value StoreLayout#DIGEST_BYTES} octets; conventionally the canonical digest from {@link StoreLayout#scopeDigest}
   * @param entityCount non-negative number of direct entities the seal records
   * @throws StorageException {@code CORRUPT} for a conflicting existing seal record
   */
  void sealScope(ScopePath scope, byte[] digest, long entityCount) throws StorageException;

  /**
   * Verifies the state the store owns and returns the recorded seal. Every
   * stored entity payload is re-hashed and compared with the digest recorded
   * at {@link #put} time, and the live entity count is compared with the count
   * recorded by {@link #sealScope}. The recorded scope digest is returned
   * verbatim — it is <em>not</em> recomputed, because the caller's SCOPE_DIGEST
   * covers entity statuses the store does not hold; comparing it against the
   * protocol expectation is the caller's job, optionally using the canonical
   * content digest from {@link StoreLayout#scopeDigest}.
   *
   * @param scope logical scope address
   * @return the recorded seal: verbatim digest and entity count
   * @throws StorageException {@code NOT_FOUND} if the scope was never sealed, {@code CORRUPT} on any store-owned mismatch
   */
  SealRecord verifyScope(ScopePath scope) throws StorageException;

  /**
   * Lists the direct entity identifiers of one scope in ascending order, for
   * audit and orphan reconciliation. A scope that holds no entities yields an
   * empty list.
   *
   * @param scope logical scope address
   * @return ascending direct entity identifiers
   * @throws StorageException {@code CORRUPT} if the scope content cannot be read consistently
   */
  List<Long> list(ScopePath scope) throws StorageException;

  /**
   * Lists every scope that currently holds any state — at least one entity
   * or a seal record — in ascending canonical path order. Scopes that were
   * retired whole, or that never held state, are absent. This is the
   * enumeration primitive recovery sweeps and audits use to find retained
   * state without walking backend-private layouts; what counts as present
   * is consistent with {@link #list} and {@link #verifyScope}.
   *
   * @return ascending scope paths that hold state
   * @throws StorageException {@code CORRUPT} if stored state cannot be read consistently
   */
  List<ScopePath> listScopes() throws StorageException;

  /**
   * Removes retained content. A sealed scope retires whole: every entity and
   * the seal record go away. An unsealed scope retires only the entities at or
   * below the retention cursor, that is every direct entity whose identifier
   * is less than or equal to {@code retentionCursor}. Entities removed under a
   * previously opened read do not invalidate that read.
   *
   * @param scope logical scope address
   * @param retentionCursor highest entity identifier an unsealed scope removes
   * @throws StorageException {@code NOT_FOUND} if the scope holds no entities and no seal record
   */
  void retire(ScopePath scope, long retentionCursor) throws StorageException;

  /**
   * Releases backend resources, including any scratch files created under the
   * configured root. Idempotent. Reads already opened keep their streams.
   *
   * @throws StorageException {@code FAILED} if cleanup itself fails
   */
  @Override void close() throws StorageException;

  /**
   * The seal record of one scope: the caller's scope digest recorded verbatim
   * by {@link #sealScope} alongside the recorded direct entity count.
   * @param digest recorded scope digest, exactly {@value StoreLayout#DIGEST_BYTES} octets
   * @param entityCount recorded direct entity count, non-negative
   */
  record SealRecord(byte[] digest, long entityCount) {
    /**
     * Validates and copies the digest.
     * @param digest recorded scope digest
     * @param entityCount recorded direct entity count
     */
    public SealRecord {
      Objects.requireNonNull(digest, "digest");
      if (digest.length != StoreLayout.DIGEST_BYTES) {
        throw new IllegalArgumentException("digest must be " + StoreLayout.DIGEST_BYTES + " octets");
      }
      if (entityCount < 0) throw new IllegalArgumentException("entity count must be non-negative");
      digest = digest.clone();
    }

    /** @return defensive digest copy */
    @Override public byte[] digest() {
      return digest.clone();
    }
  }
}
