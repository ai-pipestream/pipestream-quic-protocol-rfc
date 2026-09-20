package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Filesystem-journal specifics: reopen durability, torn-tail recovery, corruption refusal. */
final class FileJournalBackendTest {
  @TempDir Path directory;

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private Path logFile() {
    return directory.resolve("journal").resolve("journal.log");
  }

  @Test void replaySurvivesCloseAndReopen() throws Exception {
    try (Journal journal = FileJournal.open(directory)) {
      assertEquals(1, journal.appendIntent(new Journal.Intent("one", bytes("first"))));
      assertEquals(2, journal.appendIntent(new Journal.Intent("two", bytes("second"))));
      journal.commit("one", bytes("first-done"));
    }
    try (Journal reopened = FileJournal.open(directory)) {
      assertEquals(4, reopened.appendIntent(new Journal.Intent("three", bytes("third"))));
      List<Journal.ReplayEntry> replayed = reopened.replay(0);
      assertEquals(List.of("one", "two", "three"),
          replayed.stream().map(entry -> entry.intent().intentId()).toList());
      assertArrayEquals(bytes("first-done"), replayed.get(0).commitRecord());
      assertNull(replayed.get(1).commitRecord());
      assertEquals(List.of("three"),
          reopened.replay(2).stream().map(entry -> entry.intent().intentId()).toList());
    }
  }

  @Test void tornTailFromKilledWriterIsDiscardedOnOpen() throws Exception {
    try (Journal journal = FileJournal.open(directory)) {
      journal.appendIntent(new Journal.Intent("complete", bytes("durable")));
    }
    byte[] torn = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0};
    Files.write(logFile(), torn, java.nio.file.StandardOpenOption.APPEND);
    try (Journal reopened = FileJournal.open(directory)) {
      List<Journal.ReplayEntry> replayed = reopened.replay(0);
      assertEquals(1, replayed.size());
      assertEquals("complete", replayed.get(0).intent().intentId());
      assertEquals(2, reopened.appendIntent(new Journal.Intent("next", bytes("continues"))));
      assertEquals(List.of("complete", "next"),
          reopened.replay(0).stream().map(entry -> entry.intent().intentId()).toList());
    }
  }

  @Test void corruptRecordIsRefusedOnOpen() throws Exception {
    try (Journal journal = FileJournal.open(directory)) {
      journal.appendIntent(new Journal.Intent("intact", bytes("payload")));
    }
    byte[] log = Files.readAllBytes(logFile());
    log[log.length - 1] ^= 1;
    Files.write(logFile(), log);
    assertStorage(StorageException.Kind.CORRUPT, () -> FileJournal.open(directory));
  }

  @Test void foreignLogIsRefusedOnOpen() throws Exception {
    Files.createDirectories(directory.resolve("journal"));
    Files.write(logFile(), bytes("not a pipestream journal"));
    assertStorage(StorageException.Kind.CORRUPT, () -> FileJournal.open(directory));
  }

  @Test void secondWriterIsRefusedAndReopenAfterCloseSucceeds() throws Exception {
    Journal first = FileJournal.open(directory);
    assertStorage(StorageException.Kind.FAILED, () -> FileJournal.open(directory));
    first.close();
    try (Journal second = FileJournal.open(directory)) {
      assertEquals(List.of(), second.replay(0));
    }
  }
}
