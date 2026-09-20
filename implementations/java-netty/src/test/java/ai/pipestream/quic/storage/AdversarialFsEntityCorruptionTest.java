package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adversarial attacks on the entity file format ({@code [8B length][32B sha256][payload]}).
 * Flips every byte of a live entity file, truncates at every byte offset, forges length
 * headers (u32-max, negative, inconsistent), crafts zero-length entities, and swaps an
 * entity for a directory. Every attack asserts the pinned {@link StorageException.Kind} —
 * corruption must surface as {@code CORRUPT}, never as silent acceptance. Where the
 * implementation currently accepts corrupted bytes silently (the streaming read path),
 * the test pins the observed behavior and the class Javadoc of the report calls it out.
 */
final class AdversarialFsEntityCorruptionTest {
  @TempDir Path directory;

  private static final byte[] PAYLOAD = bytes("entity-under-attack-0123456789abcdef"
      + "entity-under-attack-0123456789abcdef");

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static byte[] digest(byte[] bytes) {
    return StoreLayout.sha256().digest(bytes);
  }

  private static StorageException assertKind(StorageException.Kind kind,
      org.junit.jupiter.api.function.Executable call, String context) {
    StorageException failure = assertThrows(StorageException.class, call, context);
    assertEquals(kind, failure.kind(), context + ": " + failure.getMessage());
    return failure;
  }

  private static Path entityFile(Path root, ScopePath scope, long entityId) {
    Path leaf = root.resolve("objects");
    for (long component : scope.components()) leaf = leaf.resolve(StoreLayout.scopeComponentName(component));
    return leaf.resolve(StoreLayout.fanoutBucket(entityId)).resolve(StoreLayout.entityName(entityId));
  }

  private byte[] sealedEntityFile(ScopePath scope, long entityId) throws Exception {
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, entityId, PAYLOAD.clone(), digest(PAYLOAD));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(entityId, digest(PAYLOAD)))), 1);
    }
    return Files.readAllBytes(entityFile(directory, scope, entityId));
  }

  @Test void everyByteFlipOfEntityFileFailsVerificationAsCorrupt() throws Exception {
    var scope = ScopePath.root(0xa001);
    byte[] pristine = sealedEntityFile(scope, 0x01020304);
    assertEquals(StoreLayout.DIGEST_BYTES + 8 + PAYLOAD.length, pristine.length);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      Path entity = entityFile(directory, scope, 0x01020304);
      for (int position = 0; position < pristine.length; position++) {
        byte[] tampered = pristine.clone();
        tampered[position] ^= (byte) (1 << (position % 8));
        Files.write(entity, tampered);
        assertKind(StorageException.Kind.CORRUPT,
            () -> store.verifyScope(scope), "flip at byte " + position);
      }
    }
  }

  @Test void truncationAtEveryByteOffsetFailsVerificationAsCorrupt() throws Exception {
    var scope = ScopePath.root(0xa002);
    byte[] pristine = sealedEntityFile(scope, 9);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      Path entity = entityFile(directory, scope, 9);
      for (int length = 0; length < pristine.length; length++) {
        Files.write(entity, java.util.Arrays.copyOf(pristine, length));
        assertKind(StorageException.Kind.CORRUPT,
            () -> store.verifyScope(scope), "truncated to " + length + " bytes");
      }
    }
  }

  @Test void lengthHeaderForgedToU32MaxIsCorruptNotAnAllocationFailure() throws Exception {
    var scope = ScopePath.root(0xa003);
    byte[] pristine = sealedEntityFile(scope, 11);
    byte[] tampered = pristine.clone();
    ByteBuffer.wrap(tampered).putLong(0, 0xffff_ffffL);
    Files.write(entityFile(directory, scope, 11), tampered);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
    }
  }

  @Test void negativeLengthHeaderIsCorrupt() throws Exception {
    var scope = ScopePath.root(0xa004);
    byte[] pristine = sealedEntityFile(scope, 12);
    byte[] tampered = pristine.clone();
    ByteBuffer.wrap(tampered).putLong(0, -1L);
    Files.write(entityFile(directory, scope, 12), tampered);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
    }
  }

  @Test void lengthHeaderClaimingZeroWithPayloadAppendedIsCorrupt() throws Exception {
    var scope = ScopePath.root(0xa005);
    byte[] pristine = sealedEntityFile(scope, 13);
    byte[] tampered = new byte[pristine.length + 1];
    ByteBuffer.wrap(tampered).putLong(0, 0L).put(digest(PAYLOAD)).put(PAYLOAD).put((byte) 0);
    Files.write(entityFile(directory, scope, 13), tampered);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
    }
  }

  @Test void zeroLengthPayloadRoundTripsThroughPutReadAndVerify() throws Exception {
    var scope = ScopePath.root(0xa006);
    byte[] empty = new byte[0];
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 0xffff_ffffL, empty, digest(empty));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(0xffff_ffffL, digest(empty)))), 1);
      try (InputStream stream = store.openRead(scope, 0xffff_ffffL)) {
        assertArrayEquals(empty, stream.readAllBytes());
      }
      store.verifyScope(scope);
      assertEquals(List.of(0xffff_ffffL), store.list(scope));
    }
  }

  @Test void forgedWellFormedZeroLengthEntityFileIsAccepted() throws Exception {
    var scope = ScopePath.root(0xa007);
    long entityId = 0x11223344;
    byte[] empty = new byte[0];
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, entityId, empty, digest(empty));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(entityId, digest(empty)))), 1);
    }
    Path entity = entityFile(directory, scope, entityId);
    // A header-only file (length 0, digest of the empty input) is byte-identical to
    // what put wrote; nothing distinguishes it from a legitimately written entity.
    assertEquals(40, Files.size(entity));
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.verifyScope(scope);
      try (InputStream stream = store.openRead(scope, entityId)) {
        assertArrayEquals(empty, stream.readAllBytes());
      }
    }
  }

  @Test void zeroLengthEntityFileWithWrongDigestIsCorrupt() throws Exception {
    var scope = ScopePath.root(0xa008);
    byte[] empty = new byte[0];
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 1, empty, digest(empty));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(1, digest(empty)))), 1);
    }
    Path entity = entityFile(directory, scope, 1);
    byte[] forged = ByteBuffer.allocate(40).putLong(0).put(digest(bytes("not-empty"))).array();
    Files.write(entity, forged);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
    }
  }

  @Test void tamperedEntityBlocksConflictingPutAsCorrupt() throws Exception {
    var scope = ScopePath.root(0xa009);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 21, PAYLOAD.clone(), digest(PAYLOAD));
    }
    Path entity = entityFile(directory, scope, 21);
    byte[] tampered = Files.readAllBytes(entity);
    tampered[tampered.length - 1] ^= 1;
    Files.write(entity, tampered);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      // The idempotency/conflict check must read the recorded content, which fails
      // its own recorded digest before any comparison can happen.
      assertStorage(StorageException.Kind.CORRUPT,
          () -> store.put(scope, 21, PAYLOAD.clone(), digest(PAYLOAD)));
      // Even a re-put of the tampered bytes themselves is refused: the recorded
      // digest header no longer matches the recorded payload.
      byte[] tamperedPayload = java.util.Arrays.copyOfRange(tampered, 40, tampered.length);
      assertStorage(StorageException.Kind.CORRUPT,
          () -> store.put(scope, 21, tamperedPayload, digest(tamperedPayload)));
    }
  }

  @Test void entityTruncatedBelowHeaderIsCorruptOnOpenRead() throws Exception {
    var scope = ScopePath.root(0xa00a);
    byte[] pristine = sealedEntityFile(scope, 22);
    Files.write(entityFile(directory, scope, 22), java.util.Arrays.copyOf(pristine, 39));
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.CORRUPT, () -> store.openRead(scope, 22));
    }
  }

  /**
   * Known acceptance gap (report bug list): {@code openRead} checks only that the file
   * holds a full header, then streams payload bytes without ever comparing the recorded
   * length or digest. A payload-truncated entity therefore returns short bytes silently
   * on the read path; only {@code verifyScope} catches it. Pinned here so the gap is
   * documented; the assertion encodes the current behavior, not the desired refusal.
   */
  @Test void openReadSilentlyStreamsPayloadTruncatedEntity() throws Exception {
    var scope = ScopePath.root(0xa00b);
    byte[] pristine = sealedEntityFile(scope, 23);
    Files.write(entityFile(directory, scope, 23),
        java.util.Arrays.copyOf(pristine, pristine.length - 3));
    try (ObjectStore store = FileObjectStore.open(directory)) {
      try (InputStream stream = store.openRead(scope, 23)) {
        byte[] streamed = stream.readAllBytes();
        assertEquals(PAYLOAD.length - 3, streamed.length,
            "read path returns the truncated bytes without any integrity check");
      }
    }
  }

  /**
   * Companion to {@link #openReadSilentlyStreamsPayloadTruncatedEntity()}: a flipped
   * payload byte is streamed verbatim by {@code openRead}; {@code verifyScope} refuses
   * the same bytes as {@code CORRUPT}.
   */
  @Test void openReadSilentlyStreamsDigestFlippedPayload() throws Exception {
    var scope = ScopePath.root(0xa00c);
    byte[] pristine = sealedEntityFile(scope, 24);
    byte[] tampered = pristine.clone();
    tampered[tampered.length - 1] ^= 1;
    Files.write(entityFile(directory, scope, 24), tampered);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      byte[] streamed;
      try (InputStream stream = store.openRead(scope, 24)) {
        streamed = stream.readAllBytes();
      }
      assertFalse(java.util.Arrays.equals(PAYLOAD, streamed), "corrupted bytes stream through openRead");
      assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
    }
  }

  @Test void entitySwappedForDirectoryIsCorruptToVerifyAndListButAbsentToOpenRead() throws Exception {
    var scope = ScopePath.root(0xa00d);
    byte[] pristine = sealedEntityFile(scope, 25);
    Path entity = entityFile(directory, scope, 25);
    Files.delete(entity);
    Files.createDirectory(entity);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.openRead(scope, 25));
      assertStorage(StorageException.Kind.CORRUPT, () -> store.list(scope));
      assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
    }
  }

  @Test void entityFileReplacedWithForeignContentIsCorrupt() throws Exception {
    var scope = ScopePath.root(0xa00e);
    byte[] pristine = sealedEntityFile(scope, 26);
    assertTrue(pristine.length > 40);
    byte[] foreign = new byte[pristine.length - 16];
    new java.util.Random(0x5eedL).nextBytes(foreign);
    Files.write(entityFile(directory, scope, 26), foreign);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
    }
  }

  @Test void staleScratchFileInBucketIsIgnoredByListAndSweptOnOpen() throws Exception {
    var scope = ScopePath.root(0xa00f);
    byte[] pristine = sealedEntityFile(scope, 27);
    Path bucket = entityFile(directory, scope, 27).getParent();
    Path scratch = bucket.resolve("write-00000000-0000-0000-0000-000000000001.tmp");
    Files.write(scratch, pristine);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertFalse(Files.exists(scratch), "stray scratch swept at open");
      assertEquals(List.of(27L), store.list(scope));
      store.verifyScope(scope);
    }
  }
}
