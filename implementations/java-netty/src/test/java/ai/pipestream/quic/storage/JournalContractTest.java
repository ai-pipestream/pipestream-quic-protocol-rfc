package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Role contract every {@link Journal} backend must satisfy. Concrete
 * subclasses instantiate one backend each; append, commit, cursor, and replay
 * semantics of the storage design note are exercised here exactly once.
 */
abstract class JournalContractTest {
  /** Opens one fresh, empty journal for a single test. */
  protected abstract Journal openJournal() throws Exception;

  static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  @Test void appendAssignsStrictlyIncreasingCursors() throws Exception {
    try (Journal journal = openJournal()) {
      assertEquals(1, journal.appendIntent(new Journal.Intent("intent-a", bytes("a"))));
      assertEquals(2, journal.appendIntent(new Journal.Intent("intent-b", bytes("b"))));
      assertEquals(3, journal.appendIntent(new Journal.Intent("intent-c", bytes("c"))));
    }
  }

  @Test void replayFromZeroReturnsIntentsInAppendOrder() throws Exception {
    try (Journal journal = openJournal()) {
      journal.appendIntent(new Journal.Intent("first", bytes("one")));
      journal.appendIntent(new Journal.Intent("second", bytes("two")));
      List<Journal.ReplayEntry> replayed = journal.replay(0);
      assertEquals(2, replayed.size());
      assertEquals(1, replayed.get(0).cursor());
      assertEquals("first", replayed.get(0).intent().intentId());
      assertArrayEquals(bytes("one"), replayed.get(0).intent().record());
      assertFalse(replayed.get(0).committed());
      assertNull(replayed.get(0).commitRecord());
      assertEquals(2, replayed.get(1).cursor());
      assertEquals("second", replayed.get(1).intent().intentId());
    }
  }

  @Test void replaySinceCursorReturnsOnlyTheTail() throws Exception {
    try (Journal journal = openJournal()) {
      journal.appendIntent(new Journal.Intent("one", bytes("1")));
      journal.appendIntent(new Journal.Intent("two", bytes("2")));
      journal.appendIntent(new Journal.Intent("three", bytes("3")));
      List<Journal.ReplayEntry> replayed = journal.replay(2);
      assertEquals(1, replayed.size());
      assertEquals(3, replayed.get(0).cursor());
      assertEquals("three", replayed.get(0).intent().intentId());
      assertEquals(List.of(), journal.replay(3));
    }
  }

  @Test void appendIsIdempotentForIdenticalRecord() throws Exception {
    try (Journal journal = openJournal()) {
      assertEquals(1, journal.appendIntent(new Journal.Intent("retry", bytes("same"))));
      assertEquals(1, journal.appendIntent(new Journal.Intent("retry", bytes("same"))));
      assertEquals(1, journal.replay(0).size());
    }
  }

  @Test void appendConflictingRecordIsCorrupt() throws Exception {
    try (Journal journal = openJournal()) {
      journal.appendIntent(new Journal.Intent("fixed", bytes("original")));
      assertStorage(StorageException.Kind.CORRUPT,
          () -> journal.appendIntent(new Journal.Intent("fixed", bytes("changed"))));
      assertArrayEquals(bytes("original"), journal.replay(0).get(0).intent().record());
    }
  }

  @Test void commitMarksIntentCommittedForReplay() throws Exception {
    try (Journal journal = openJournal()) {
      long cursor = journal.appendIntent(new Journal.Intent("work", bytes("admitted")));
      journal.commit("work", bytes("done"));
      List<Journal.ReplayEntry> replayed = journal.replay(cursor - 1);
      assertEquals(1, replayed.size());
      assertTrue(replayed.get(0).committed());
      assertArrayEquals(bytes("done"), replayed.get(0).commitRecord());
    }
  }

  @Test void commitOfUnknownIntentIsNotFound() throws Exception {
    try (Journal journal = openJournal()) {
      assertStorage(StorageException.Kind.NOT_FOUND, () -> journal.commit("absent", bytes("x")));
    }
  }

  @Test void commitIsIdempotentAndRejectsConflictingBytes() throws Exception {
    try (Journal journal = openJournal()) {
      journal.appendIntent(new Journal.Intent("settled", bytes("record")));
      journal.commit("settled", bytes("commit"));
      journal.commit("settled", bytes("commit"));
      assertStorage(StorageException.Kind.CORRUPT, () -> journal.commit("settled", bytes("other")));
      assertArrayEquals(bytes("commit"), journal.replay(0).get(0).commitRecord());
    }
  }

  @Test void replayInterleavesCommitsInAppendOrder() throws Exception {
    try (Journal journal = openJournal()) {
      journal.appendIntent(new Journal.Intent("a", bytes("a")));
      journal.appendIntent(new Journal.Intent("b", bytes("b")));
      journal.appendIntent(new Journal.Intent("c", bytes("c")));
      journal.commit("a", bytes("a-done"));
      journal.commit("c", bytes("c-done"));
      List<Journal.ReplayEntry> replayed = journal.replay(0);
      assertEquals(List.of("a", "b", "c"),
          replayed.stream().map(entry -> entry.intent().intentId()).toList());
      assertTrue(replayed.get(0).committed());
      assertFalse(replayed.get(1).committed());
      assertTrue(replayed.get(2).committed());
    }
  }

  @Test void replayReturnsImmutableSnapshots() throws Exception {
    try (Journal journal = openJournal()) {
      journal.appendIntent(new Journal.Intent("immutable", bytes("record")));
      List<Journal.ReplayEntry> replayed = journal.replay(0);
      assertThrows(UnsupportedOperationException.class, () -> replayed.add(
          new Journal.ReplayEntry(9, new Journal.Intent("x", bytes("y")), null)));
      byte[] record = replayed.get(0).intent().record();
      record[0] ^= 1;
      assertArrayEquals(bytes("record"), replayed.get(0).intent().record());
    }
  }

  @Test void operationsFailAfterClose() throws Exception {
    Journal journal = openJournal();
    journal.appendIntent(new Journal.Intent("kept", bytes("until-close")));
    journal.close();
    journal.close();
    assertStorage(StorageException.Kind.FAILED,
        () -> journal.appendIntent(new Journal.Intent("late", bytes("x"))));
    assertStorage(StorageException.Kind.FAILED, () -> journal.commit("kept", bytes("x")));
    assertStorage(StorageException.Kind.FAILED, () -> journal.replay(0));
  }

  @Test void replayOutputIsIdenticalAcrossBackends() throws Exception {
    List<Journal.ReplayEntry> first = scriptedReplay(openJournal());
    List<Journal.ReplayEntry> second = scriptedReplay(openJournal());
    assertEquals(first.size(), second.size(), "same script, same entry count");
    for (int i = 0; i < first.size(); i++) {
      Journal.ReplayEntry left = first.get(i);
      Journal.ReplayEntry right = second.get(i);
      assertEquals(left.cursor(), right.cursor(), "cursor " + i);
      assertEquals(left.intent().intentId(), right.intent().intentId());
      assertArrayEquals(left.intent().record(), right.intent().record());
      assertEquals(left.committed(), right.committed());
      assertArrayEquals(left.commitRecord(), right.commitRecord());
      assertEquals(left.commitCursor(), right.commitCursor(), "commit cursor " + i);
    }
  }

  /** Runs one fixed workload and returns its replay; both backends must produce identical output. */
  private static List<Journal.ReplayEntry> scriptedReplay(Journal journal) throws Exception {
    try (journal) {
      journal.appendIntent(new Journal.Intent("intent-a", bytes("record-a")));
      journal.appendIntent(new Journal.Intent("intent-b", bytes("record-b")));
      journal.appendIntent(new Journal.Intent("intent-c", bytes("record-c")));
      journal.commit("intent-a", bytes("commit-a"));
      journal.commit("intent-c", bytes("commit-c"));
      List<Journal.ReplayEntry> replayed = journal.replay(0);
      assertEquals(4, replayed.get(0).commitCursor(), "commit cursors are part of the identity");
      assertEquals(5, replayed.get(2).commitCursor(), "commit cursors are part of the identity");
      assertNull(replayed.get(1).commitCursor());
      assertEquals(6, journal.appendIntent(new Journal.Intent("intent-d", bytes("record-d"))),
          "commits consume cursors in the global append order");
      return journal.replay(0);
    }
  }
}
