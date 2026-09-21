package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adversarial attacks on the filesystem journal framing
 * ({@code PSJSL01} magic, records of {@code [type][cursor][idLen][payloadLen][id][payload][sha256]}).
 * Flips every byte of a live log, truncates at every byte offset, splices garbage and
 * hand-crafted records (cursor gaps, duplicate intents, dangling or repeated commits,
 * invalid geometry) into the middle of the log, and zeroes the log. Every attack
 * asserts the pinned {@link StorageException.Kind}. Two attacks — a zero-length log
 * and mid-log garbage with plausible geometry — currently succeed silently and are
 * pinned as observed behavior; they are the top entries of the report's bug list.
 */
final class AdversarialFsJournalCorruptionTest {
  @TempDir Path directory;

  private static final byte[] MAGIC = {'P', 'S', 'J', 'S', 'L', '0', '1'};
  private static final int RECORD_HEADER_BYTES = 1 + 8 + 4 + 4;
  private static final int RECORD_BYTES = RECORD_HEADER_BYTES + 2 + 2 + StoreLayout.DIGEST_BYTES;
  private static final byte[] ID_1 = bytes("i1");
  private static final byte[] ID_2 = bytes("i2");
  private static final byte[] ID_3 = bytes("i3");

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private Path logFile() {
    return directory.resolve("journal").resolve("journal.log");
  }

  /** Frames one record exactly as the pinned on-disk format requires. */
  private static byte[] record(byte type, long cursor, byte[] id, byte[] payload) {
    ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER_BYTES)
        .put(type).putLong(cursor).putInt(id.length).putInt(payload.length);
    java.security.MessageDigest checksum = StoreLayout.sha256();
    checksum.update(header.array());
    checksum.update(id);
    checksum.update(payload);
    return ByteBuffer.allocate(RECORD_HEADER_BYTES + id.length + payload.length + StoreLayout.DIGEST_BYTES)
        .put(header.array()).put(id).put(payload).put(checksum.digest()).array();
  }

  private static byte[] intent(long cursor, byte[] id, byte[] payload) {
    return record((byte) 0, cursor, id, payload);
  }

  private static byte[] commit(long cursor, byte[] id, byte[] payload) {
    return record((byte) 1, cursor, id, payload);
  }

  private static byte[] concat(byte[]... parts) {
    int length = 0;
    for (byte[] part : parts) length += part.length;
    ByteBuffer out = ByteBuffer.allocate(length);
    for (byte[] part : parts) out.put(part);
    return out.array();
  }

  /** Appends three intents and one commit through the real journal and returns the raw log. */
  private byte[] durableLog() throws Exception {
    try (Journal journal = FileJournal.open(directory)) {
      assertEquals(1, journal.appendIntent(new Journal.Intent("i1", bytes("p1"))));
      assertEquals(2, journal.appendIntent(new Journal.Intent("i2", bytes("p2"))));
      assertEquals(3, journal.appendIntent(new Journal.Intent("i3", bytes("p3"))));
      journal.commit("i1", bytes("c1"));
    }
    byte[] log = Files.readAllBytes(logFile());
    assertEquals(MAGIC.length + 4 * RECORD_BYTES, log.length);
    return log;
  }

  private static StorageException assertCorrupt(org.junit.jupiter.api.function.Executable call, String context) {
    StorageException failure = assertThrows(StorageException.class, call, context);
    assertEquals(StorageException.Kind.CORRUPT, failure.kind(), context + ": " + failure.getMessage());
    return failure;
  }

  @Test void everyByteFlipOfTheLogIsRefusedOrSilentlyTruncatesExactlyAsPinned() throws Exception {
    byte[] pristine = durableLog();
    assertEquals(MAGIC.length + 4 * RECORD_BYTES, pristine.length);
    int[] recordStarts = {7, 7 + RECORD_BYTES, 7 + 2 * RECORD_BYTES, 7 + 3 * RECORD_BYTES};
    int silentlyTruncated = 0;
    for (int position = 0; position < pristine.length; position++) {
      byte[] tampered = pristine.clone();
      tampered[position] ^= (byte) (1 << (position % 8));
      Files.write(logFile(), tampered);
      Journal reopened = null;
      try {
        reopened = FileJournal.open(directory);
      } catch (StorageException failure) {
        assertEquals(StorageException.Kind.CORRUPT, failure.kind(), "flip at byte " + position);
        continue;
      }
      // Known acceptance gap (report bug list): a flip inside a record's idLen or
      // payloadLen field that enlarges the value past the file end is misclassified
      // as a torn tail and silently truncated, losing committed records. Pinned:
      // recovery must have cut exactly at the start of the record holding the flip,
      // and the flip must sit inside that record's length fields (header offsets 9-16).
      silentlyTruncated++;
      int recordStart = -1;
      for (int start : recordStarts) if (position >= start && position < start + RECORD_BYTES) recordStart = start;
      assertTrue(recordStart >= 0, "silent truncation only inside a record, flip at byte " + position);
      int headerOffset = position - recordStart;
      assertTrue(headerOffset >= 9 && headerOffset <= 16,
          "silent truncation only via length fields, flip at byte " + position);
      assertEquals(recordStart, Files.size(logFile()),
          "log cut at the start of the record holding the flip, byte " + position);
      int retainedRecords = (recordStart - MAGIC.length) / RECORD_BYTES;
      assertEquals(retainedRecords, reopened.replay(0).size(),
          "every record from the flipped one onward is lost, byte " + position);
      reopened.close();
    }
    assertTrue(silentlyTruncated > 0,
        "the length-field silent-truncation gap is exercised by this attack");
  }

  /**
   * Fixed: a zero-length log on an initialized root (the marker file exists)
   * is CORRUPT — the committed records it held cannot silently vanish into a
   * fresh start.
   */
  @Test void zeroLengthLogOnInitializedRootIsCorrupt() throws Exception {
    durableLog();
    Files.write(logFile(), new byte[0]);
    StorageException failure = assertThrows(StorageException.class, () -> FileJournal.open(directory));
    assertEquals(StorageException.Kind.CORRUPT, failure.kind(), failure.getMessage());
  }

  /** Fixed via the same marker: a deleted log on an initialized root is CORRUPT. */
  @Test void deletedLogOnInitializedRootIsCorrupt() throws Exception {
    durableLog();
    Files.delete(logFile());
    StorageException failure = assertThrows(StorageException.class, () -> FileJournal.open(directory));
    assertEquals(StorageException.Kind.CORRUPT, failure.kind(), failure.getMessage());
  }

  @Test void truncationInsideTheMagicMapsToCorrupt() throws Exception {
    durableLog();
    for (int length = 1; length < MAGIC.length; length++) {
      byte[] log = Files.readAllBytes(logFile());
      Files.write(logFile(), java.util.Arrays.copyOf(log, length));
      // Fixed: a partial magic is permanent damage, not a retryable I/O
      // failure; anything shorter than the magic is CORRUPT.
      StorageException failure = assertThrows(StorageException.class,
          () -> FileJournal.open(directory), "magic truncated to " + length);
      assertEquals(StorageException.Kind.CORRUPT, failure.kind(),
          "magic truncated to " + length);
    }
  }

  @Test void truncationAtEveryOffsetDiscardsOnlyTheTornTail() throws Exception {
    byte[] pristine = durableLog();
    int size = pristine.length;
    for (int cut = MAGIC.length; cut < size; cut++) {
      Files.write(logFile(), java.util.Arrays.copyOf(pristine, cut));
      int completeRecords = (cut - MAGIC.length) / RECORD_BYTES;
      try (Journal reopened = FileJournal.open(directory)) {
        // Recovery physically truncates the log back to the last checksummed record
        // end before accepting any new appends.
        assertEquals(MAGIC.length + (long) completeRecords * RECORD_BYTES, Files.size(logFile()),
            "log truncated to last good record, cut at " + cut);
        List<Journal.ReplayEntry> replayed = reopened.replay(0);
        int expectedIntents = Math.min(completeRecords, 3);
        assertEquals(expectedIntents, replayed.size(), "cut at " + cut);
        for (int index = 0; index < expectedIntents; index++) {
          assertEquals("i" + (index + 1), replayed.get(index).intent().intentId(), "cut at " + cut);
        }
        boolean commitIntact = completeRecords == 4;
        if (expectedIntents > 0) {
          assertEquals(commitIntact, replayed.get(0).committed(), "cut at " + cut);
          if (commitIntact) {
            assertArrayEquals(bytes("c1"), replayed.get(0).commitRecord());
          }
        }
        assertEquals(completeRecords + 1,
            reopened.appendIntent(new Journal.Intent("cut-" + cut, bytes("x"))),
            "append continues after the retained prefix, cut at " + cut);
      }
    }
  }

  @Test void garbageTailShorterThanOneRecordIsTornAndDiscarded() throws Exception {
    byte[] pristine = durableLog();
    byte[] withGarbage = concat(pristine, new byte[] {9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9});
    Files.write(logFile(), withGarbage);
    try (Journal reopened = FileJournal.open(directory)) {
      List<Journal.ReplayEntry> replayed = reopened.replay(0);
      assertEquals(List.of("i1", "i2", "i3"),
          replayed.stream().map(entry -> entry.intent().intentId()).toList());
      assertTrue(replayed.get(0).committed());
      assertEquals(pristine.length, Files.size(logFile()), "torn garbage truncated before appends");
      assertEquals(5, reopened.appendIntent(new Journal.Intent("after-garbage", bytes("x"))));
    }
  }

  /**
   * Fixed: the same 49 crafted bytes now fail recovery as CORRUPT. A record
   * claiming bytes past the file end is a torn tail only when nothing
   * parseable follows; here complete checksummed records sit after the
   * splice, so the garbage is mid-log corruption, not a truncatable tail,
   * and the committed records it shadowed are refused rather than silently
   * discarded. Contrasts with
   * {@link #garbageWithPlausibleSmallGeometryMidLogIsCorrupt()}.
   */
  @Test void craftedMidLogGarbageWithHugeClaimedPayloadIsCorrupt() throws Exception {
    byte[] pristine = durableLog();
    byte[] forgedHeader = ByteBuffer.allocate(RECORD_HEADER_BYTES)
        .put((byte) 0).putLong(2L).putInt(1).putInt(Journal.MAX_RECORD_BYTES).array();
    byte[] spliced = concat(java.util.Arrays.copyOfRange(pristine, 0, MAGIC.length + RECORD_BYTES),
        forgedHeader,
        java.util.Arrays.copyOfRange(pristine, MAGIC.length + RECORD_BYTES, pristine.length));
    Files.write(logFile(), spliced);
    StorageException failure = assertThrows(StorageException.class, () -> FileJournal.open(directory));
    assertEquals(StorageException.Kind.CORRUPT, failure.kind(), failure.getMessage());
  }

  @Test void garbageWithPlausibleSmallGeometryMidLogIsCorrupt() throws Exception {
    byte[] pristine = durableLog();
    byte[] forged = record((byte) 0, 2L, bytes("zz"), bytes("yy"));
    forged[forged.length - 1] ^= 1; // break the checksum only; geometry stays plausible
    byte[] spliced = concat(java.util.Arrays.copyOfRange(pristine, 0, MAGIC.length + RECORD_BYTES),
        forged,
        java.util.Arrays.copyOfRange(pristine, MAGIC.length + RECORD_BYTES, pristine.length));
    Files.write(logFile(), spliced);
    assertCorrupt(() -> FileJournal.open(directory), "checksum-breaking splice mid-log");
  }

  @Test void garbageTailWithInvalidGeometryIsCorrupt() throws Exception {
    durableLog();
    byte[] forgedHeader = ByteBuffer.allocate(RECORD_HEADER_BYTES)
        .put((byte) 7).putLong(5L).putInt(2).putInt(2).array();
    byte[] garbage = concat(forgedHeader, new byte[StoreLayout.DIGEST_BYTES + 4]);
    Files.write(logFile(), garbage, java.nio.file.StandardOpenOption.APPEND);
    assertCorrupt(() -> FileJournal.open(directory), "invalid record type at tail");
  }

  @Test void cursorGapInCraftedLogIsCorrupt() throws Exception {
    writeRaw(MAGIC, intent(1, ID_1, bytes("p1")), intent(3, ID_2, bytes("p2")));
    assertCorrupt(() -> FileJournal.open(directory), "cursor jumps 1 -> 3");
  }

  @Test void cursorResetInCraftedLogIsCorrupt() throws Exception {
    writeRaw(MAGIC, intent(1, ID_1, bytes("p1")), intent(1, ID_2, bytes("p2")));
    assertCorrupt(() -> FileJournal.open(directory), "cursor repeats 1");
  }

  @Test void duplicateIntentIdentifierInCraftedLogIsCorrupt() throws Exception {
    writeRaw(MAGIC, intent(1, ID_1, bytes("p1")), intent(2, ID_1, bytes("other")));
    assertCorrupt(() -> FileJournal.open(directory), "same intent id twice");
  }

  @Test void commitWithoutPriorIntentIsCorrupt() throws Exception {
    writeRaw(MAGIC, commit(1, ID_1, bytes("c1")));
    assertCorrupt(() -> FileJournal.open(directory), "dangling commit");
  }

  @Test void doubleCommitInCraftedLogIsCorrupt() throws Exception {
    writeRaw(MAGIC, intent(1, ID_1, bytes("p1")), commit(2, ID_1, bytes("c1")),
        commit(3, ID_1, bytes("c1")));
    assertCorrupt(() -> FileJournal.open(directory), "second commit for one intent");
  }

  @Test void forgedWellFormedRecordsReplayAsRealHistory() throws Exception {
    // A record that passes geometry, cursor order, and checksum is indistinguishable
    // from genuine history: the format pins no writer token. Pinned as accepted.
    writeRaw(MAGIC, intent(1, ID_1, bytes("p1")), commit(2, ID_1, bytes("c1")),
        intent(3, ID_3, bytes("p3")));
    try (Journal reopened = FileJournal.open(directory)) {
      List<Journal.ReplayEntry> replayed = reopened.replay(0);
      assertEquals(2, replayed.size());
      assertTrue(replayed.get(0).committed());
      assertFalse(replayed.get(1).committed());
    }
  }

  @Test void invalidGeometryFieldsAreCorruptEvenWithValidChecksums() throws Exception {
    // Each record is checksummed exactly as a real writer would frame it, so the
    // geometry validation itself must be what refuses.
    byte[] badType = record((byte) 2, 1, ID_1, bytes("p1"));
    writeRaw(MAGIC, badType);
    assertCorrupt(() -> FileJournal.open(directory), "type byte 2");

    byte[] zeroId = record((byte) 0, 1, new byte[0], bytes("p1"));
    writeRaw(MAGIC, zeroId);
    assertCorrupt(() -> FileJournal.open(directory), "id length 0");

    byte[] hugeId = record((byte) 0, 1, new byte[Journal.MAX_INTENT_ID_BYTES + 1], bytes("p1"));
    writeRaw(MAGIC, hugeId);
    assertCorrupt(() -> FileJournal.open(directory), "id length 257");

    byte[] zeroPayload = record((byte) 0, 1, ID_1, new byte[0]);
    writeRaw(MAGIC, zeroPayload);
    assertCorrupt(() -> FileJournal.open(directory), "payload length 0");

    byte[] hugePayload = record((byte) 0, 1, ID_1, new byte[Journal.MAX_RECORD_BYTES + 1]);
    writeRaw(MAGIC, hugePayload);
    assertCorrupt(() -> FileJournal.open(directory), "payload length MAX+1");
  }

  @Test void zeroLengthIntentAndCommitRecordsAreRejected() throws Exception {
    try (Journal journal = FileJournal.open(directory)) {
      assertThrows(IllegalArgumentException.class,
          () -> journal.appendIntent(new Journal.Intent("empty", new byte[0])));
      journal.appendIntent(new Journal.Intent("real", bytes("p1")));
      assertThrows(IllegalArgumentException.class, () -> journal.commit("real", new byte[0]));
    }
  }

  @Test void oversizedIntentIdentifierIsRejected() throws Exception {
    // One surrogate pair encodes as 4 UTF-8 octets: 64 pairs = 256 octets (max),
    // one more code point = 260 octets (over max).
    String maxId = "\uD83D\uDE00".repeat(64);
    String overId = maxId + "x";
    try (Journal journal = FileJournal.open(directory)) {
      assertEquals(256, maxId.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
      assertEquals(1, journal.appendIntent(new Journal.Intent(maxId, bytes("p1"))));
      assertThrows(IllegalArgumentException.class,
          () -> journal.appendIntent(new Journal.Intent(overId, bytes("p2"))));
      assertThrows(IllegalArgumentException.class, () -> journal.appendIntent(new Journal.Intent("", bytes("p2"))));
    }
  }

  @Test void maximumSizedRecordSurvivesReopen() throws Exception {
    byte[] maxPayload = new byte[Journal.MAX_RECORD_BYTES];
    new java.util.Random(42).nextBytes(maxPayload);
    String longId = "intent-" + "x".repeat(200);
    try (Journal journal = FileJournal.open(directory)) {
      assertEquals(1, journal.appendIntent(new Journal.Intent(longId, maxPayload)));
      journal.commit(longId, bytes("done"));
    }
    try (Journal reopened = FileJournal.open(directory)) {
      List<Journal.ReplayEntry> replayed = reopened.replay(0);
      assertEquals(1, replayed.size());
      assertArrayEquals(maxPayload, replayed.get(0).intent().record());
      assertArrayEquals(bytes("done"), replayed.get(0).commitRecord());
    }
  }

  @Test void refusedOpenLeavesTheCorruptLogByteIdentical() throws Exception {
    // Recovery must not truncate or rewrite a log it refused: the caller may need the
    // bytes for forensics, and a retry must fail identically.
    writeRaw(MAGIC, intent(1, ID_1, bytes("p1")));
    byte[] poisoned = Files.readAllBytes(logFile());
    poisoned[poisoned.length - 1] ^= 1;
    Files.write(logFile(), poisoned);
    StorageException first = assertThrows(StorageException.class, () -> FileJournal.open(directory));
    assertEquals(StorageException.Kind.CORRUPT, first.kind());
    assertArrayEquals(poisoned, Files.readAllBytes(logFile()), "refused open must not mutate the log");
    StorageException second = assertThrows(StorageException.class, () -> FileJournal.open(directory));
    assertEquals(StorageException.Kind.CORRUPT, second.kind(), "retry fails identically");
  }

  @Test void concurrentAppendsProduceGapFreeCursorsUnderCorruptionChecks() throws Exception {
    int writers = 8;
    int perWriter = 32;
    var cursors = new java.util.concurrent.ConcurrentSkipListSet<Long>();
    var corrupt = new AtomicInteger();
    try (Journal journal = FileJournal.open(directory)) {
      var pool = java.util.concurrent.Executors.newFixedThreadPool(writers);
      var start = new java.util.concurrent.CountDownLatch(1);
      var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
      for (int writer = 0; writer < writers; writer++) {
        int id = writer;
        futures.add(pool.submit(() -> {
          start.await();
          for (int index = 0; index < perWriter; index++) {
            try {
              cursors.add(journal.appendIntent(
                  new Journal.Intent("w" + id + "-" + index, bytes("record-" + id + "-" + index))));
            } catch (StorageException failure) {
              if (failure.kind() == StorageException.Kind.CORRUPT) corrupt.incrementAndGet();
              else throw new RuntimeException(failure);
            }
          }
          return null;
        }));
      }
      start.countDown();
      for (var future : futures) future.get();
      pool.shutdown();
    }
    assertEquals(0, corrupt.get(), "no append under a healthy journal is ever Corrupt");
    assertEquals(writers * perWriter, cursors.size(), "every append returned a distinct cursor");
    // Reopen and replay: cursor sequence must be dense 1..N with no gap or repeat.
    try (Journal reopened = FileJournal.open(directory)) {
      List<Journal.ReplayEntry> replayed = reopened.replay(0);
      assertEquals(writers * perWriter, replayed.size());
      for (int index = 0; index < replayed.size(); index++) {
        assertEquals(index + 1, replayed.get(index).cursor(), "dense cursor sequence");
      }
    }
  }

  private void writeRaw(byte[]... parts) throws Exception {
    Files.createDirectories(directory.resolve("journal"));
    Files.write(logFile(), concat(parts));
  }
}
