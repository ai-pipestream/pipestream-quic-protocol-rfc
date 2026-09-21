package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adversarial attacks on the scope seal record ({@code [32B digest][8B count]}).
 * Flips every byte of the count field, truncates the seal to every length except the
 * canonical 40, forges impossible counts, and pins the deliberate decision that the
 * caller-supplied SCOPE_DIGEST bytes are opaque and come back verbatim even when
 * tampered (pinned decision 3 of the storage design note: the store verifies only
 * store-owned state). Every refusal asserts the pinned {@link StorageException.Kind}.
 */
final class AdversarialFsSealCorruptionTest {
  @TempDir Path directory;

  private static final byte[] PAYLOAD = bytes("seal-attack-payload");

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static byte[] digest(byte[] bytes) {
    return StoreLayout.sha256().digest(bytes);
  }

  private static Path sealFile(Path root, ScopePath scope) {
    Path leaf = root.resolve("objects");
    for (long component : scope.components()) leaf = leaf.resolve(StoreLayout.scopeComponentName(component));
    return leaf.resolve(FileObjectStore.SEAL_FILE_NAME);
  }

  /** Puts one entity and seals the scope; returns the pristine 40-byte seal record. */
  private byte[] sealedScope(ScopePath scope, long entityId) throws Exception {
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, entityId, PAYLOAD.clone(), digest(PAYLOAD));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(entityId, digest(PAYLOAD)))), 1);
    }
    return Files.readAllBytes(sealFile(directory, scope));
  }

  @Test void everyByteFlipOfSealCountFieldFailsVerificationAsCorrupt() throws Exception {
    var scope = ScopePath.root(0xb001);
    byte[] pristine = sealedScope(scope, 1);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      for (int position = StoreLayout.DIGEST_BYTES; position < pristine.length; position++) {
        byte[] tampered = pristine.clone();
        tampered[position] ^= (byte) (1 << (position % 8));
        Files.write(sealFile(directory, scope), tampered);
        assertKind(StorageException.Kind.CORRUPT,
            () -> store.verifyScope(scope), "count flip at byte " + position);
      }
    }
  }

  @Test void sealTruncatedToAnyLengthExceptCanonicalIsCorrupt() throws Exception {
    var scope = ScopePath.root(0xb002);
    byte[] pristine = sealedScope(scope, 2);
    int[] lengths = {0, 1, 7, 8, 31, 32, 33, 38, 39, 41, 48, 64, 100};
    try (ObjectStore store = FileObjectStore.open(directory)) {
      for (int length : lengths) {
        byte[] tampered = java.util.Arrays.copyOf(pristine, length);
        Files.write(sealFile(directory, scope), tampered);
        assertKind(StorageException.Kind.CORRUPT,
            () -> store.verifyScope(scope), "seal length " + length);
      }
    }
  }

  @Test void sealCountForgedToMaxLongIsCorrupt() throws Exception {
    var scope = ScopePath.root(0xb003);
    byte[] pristine = sealedScope(scope, 3);
    byte[] tampered = pristine.clone();
    ByteBuffer.wrap(tampered).putLong(StoreLayout.DIGEST_BYTES, Long.MAX_VALUE);
    Files.write(sealFile(directory, scope), tampered);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
    }
  }

  @Test void sealCountForgedToNegativeIsCorrupt() throws Exception {
    var scope = ScopePath.root(0xb004);
    byte[] pristine = sealedScope(scope, 4);
    byte[] tampered = pristine.clone();
    ByteBuffer.wrap(tampered).putLong(StoreLayout.DIGEST_BYTES, -7L);
    Files.write(sealFile(directory, scope), tampered);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
    }
  }

  @Test void everyByteFlipOfSealDigestIsReturnedVerbatim() throws Exception {
    var scope = ScopePath.root(0xb005);
    byte[] pristine = sealedScope(scope, 5);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      for (int position = 0; position < StoreLayout.DIGEST_BYTES; position++) {
        byte[] tampered = pristine.clone();
        tampered[position] ^= (byte) (1 << (position % 8));
        Files.write(sealFile(directory, scope), tampered);
        // Pinned decision 3: the SCOPE_DIGEST is the caller's opaque value; the store
        // verifies only the entity count it owns, so tampered digest bytes round-trip.
        ObjectStore.SealRecord record = store.verifyScope(scope);
        assertArrayEquals(java.util.Arrays.copyOf(tampered, StoreLayout.DIGEST_BYTES), record.digest(),
            "digest flip at byte " + position);
        assertEquals(1, record.entityCount());
      }
    }
  }

  @Test void putIntoScopeWithCorruptSealFileIsAlreadySealed() throws Exception {
    var scope = ScopePath.root(0xb006);
    byte[] pristine = sealedScope(scope, 6);
    Files.write(sealFile(directory, scope), java.util.Arrays.copyOf(pristine, 17));
    try (ObjectStore store = FileObjectStore.open(directory)) {
      // Seal presence is checked before content and before digests (pinned decision 4),
      // and presence is defined by a regular file, whatever its (corrupt) content.
      assertStorage(StorageException.Kind.ALREADY_SEALED,
          () -> store.put(scope, 6, PAYLOAD.clone(), digest(PAYLOAD)));
      assertStorage(StorageException.Kind.ALREADY_SEALED,
          () -> store.put(scope, 99, bytes("new"), digest(bytes("new"))));
    }
  }

  @Test void conflictingReSealOfTamperedSealIsCorrupt() throws Exception {
    var scope = ScopePath.root(0xb007);
    byte[] pristine = sealedScope(scope, 7);
    byte[] tampered = pristine.clone();
    tampered[33] ^= 1; // count 1 -> some other value
    Files.write(sealFile(directory, scope), tampered);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.CORRUPT,
          () -> store.sealScope(scope, digest(bytes("re-seal")), 1));
    }
  }

  @Test void emptyScopeSealedWithZeroCountVerifies() throws Exception {
    var scope = ScopePath.root(0xb008);
    byte[] emptyDigest = StoreLayout.scopeDigest(List.of());
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.sealScope(scope, emptyDigest, 0);
      ObjectStore.SealRecord record = store.verifyScope(scope);
      assertArrayEquals(emptyDigest, record.digest());
      assertEquals(0, record.entityCount());
      assertEquals(List.of(scope), store.listScopes());
      // Sealing an already-sealed empty scope with identical bytes is a no-op success.
      store.sealScope(scope, emptyDigest, 0);
    }
  }

  @Test void entityAppendedAfterSealingBreaksCountAsCorrupt() throws Exception {
    var scope = ScopePath.root(0xb009);
    sealedScope(scope, 8);
    // A forged entity file appears after sealing: the live count now exceeds the seal.
    Path leaf = directory.resolve("objects").resolve(StoreLayout.scopeComponentName(0xb009));
    long forgedId = 0xdeadbeefL;
    Path forged = leaf.resolve(StoreLayout.fanoutBucket(forgedId)).resolve(StoreLayout.entityName(forgedId));
    Files.createDirectories(forged.getParent());
    byte[] bytes = bytes("forged-after-seal");
    Files.write(forged, ByteBuffer.allocate(40 + bytes.length).putLong(bytes.length)
        .put(digest(bytes)).put(bytes).array());
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
    }
  }

  @Test void sealFileReplacedByDirectoryIsRefused() throws Exception {
    var scope = ScopePath.root(0xb00a);
    sealedScope(scope, 10);
    Files.delete(sealFile(directory, scope));
    Files.createDirectory(sealFile(directory, scope));
    try (ObjectStore store = FileObjectStore.open(directory)) {
      // A directory cannot carry a 40-byte seal; its size never equals SEAL_BYTES, so
      // the refusal is Corrupt, not an I/O-shaped Transient.
      StorageException failure = assertThrows(StorageException.class, () -> store.verifyScope(scope));
      assertEquals(StorageException.Kind.CORRUPT, failure.kind(), failure.getMessage());
    }
  }

  @Test void retireWholeSealedScopeRemovesForgedSiblingsToo() throws Exception {
    var scope = ScopePath.root(0xb00b);
    sealedScope(scope, 11);
    Path leaf = directory.resolve("objects").resolve(StoreLayout.scopeComponentName(0xb00b));
    Path junk = leaf.resolve("junk-file");
    Files.write(junk, bytes("garbage"));
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.retire(scope, 0);
      // Scope-local retirement: the forged sibling file goes with the
      // scope's own content, while child scope directories would survive
      // (pinned decision 5); only the emptied leaf directory remains.
      assertFalse(Files.exists(junk));
      assertFalse(Files.exists(sealFile(directory, scope)), "seal record removed");
      try (java.util.stream.Stream<Path> remaining = Files.list(leaf)) {
        assertEquals(0, remaining.count(), "scope-owned content and forged siblings are gone");
      }
      assertEquals(List.of(), store.listScopes());
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.verifyScope(scope));
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.retire(scope, 0));
    }
  }

  private static StorageException assertKind(StorageException.Kind kind,
      org.junit.jupiter.api.function.Executable call, String context) {
    StorageException failure = assertThrows(StorageException.class, call, context);
    assertEquals(kind, failure.kind(), context + ": " + failure.getMessage());
    return failure;
  }
}
