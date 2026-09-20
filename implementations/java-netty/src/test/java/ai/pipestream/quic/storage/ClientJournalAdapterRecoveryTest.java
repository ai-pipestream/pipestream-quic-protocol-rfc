package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adapter-specific proof that a real v2 client journal kill-recovery flow is
 * observable through the storage SPI: a child process journals intents and
 * commits through {@link ClientJournalAdapter}, announces each durable row,
 * and is either closed cleanly or SIGKILLed while holding the file open. The
 * parent then reopens the file through the adapter, which forces
 * {@link ai.pipestream.quic.v2.ClientJournal#open}'s format, schema, and
 * receipt audit to run, and asserts the SPI replay shows exactly the durable
 * prefix with pinned cursor semantics.
 */
final class ClientJournalAdapterRecoveryTest {
  private static final int INTENTS = 6;

  @TempDir Path directory;

  @Test void replayAfterKillRecoveryShowsExactlyTheDurablePrefix() throws Exception {
    runChild(directory.resolve("killed.sqlite"), false);
    assertReplay(directory.resolve("killed.sqlite"));
  }

  @Test void replayAfterCleanCloseShowsExactlyTheDurablePrefix() throws Exception {
    runChild(directory.resolve("closed.sqlite"), true);
    assertReplay(directory.resolve("closed.sqlite"));
  }

  @Test void secondOpenerIsRefusedWhileOpen() throws Exception {
    Path file = directory.resolve("locked.sqlite");
    try (ClientJournalAdapter journal = ClientJournalAdapter.initialize(file)) {
      journal.appendIntent(new Journal.Intent("held", new byte[] {1}));
      StorageException failure =
          assertThrows(StorageException.class, () -> ClientJournalAdapter.open(file));
      assertEquals(StorageException.Kind.FAILED, failure.kind(), failure.getMessage());
    }
    try (ClientJournalAdapter reopened = ClientJournalAdapter.open(file)) {
      assertEquals(1, reopened.replay(0).size());
    }
  }

  private static void runChild(Path file, boolean cleanClose) throws Exception {
    ProcessBuilder builder = new ProcessBuilder(
        "java", "-cp", System.getProperty("java.class.path"),
        AdapterKillChild.class.getName(), file.toString(), String.valueOf(INTENTS),
        String.valueOf(cleanClose));
    builder.redirectErrorStream(true);
    Process process = builder.start();
    List<String> lines = new ArrayList<>();
    try (BufferedReader stdout = new BufferedReader(
        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = stdout.readLine()) != null) {
        lines.add(line);
        if (line.equals("journaled " + INTENTS)) break;
      }
    }
    assertEquals("journaled " + INTENTS, lines.get(lines.size() - 1), "child journaled the full prefix: " + lines);
    if (cleanClose) {
      assertEquals(0, process.waitFor(), "clean child exited zero");
    } else {
      process.destroyForcibly();
      assertNotEquals(0, process.waitFor(), "killed child exited nonzero");
    }
  }

  /** Six intents with commits on 2, 4, 6: nine rows, cursors 1-9 in pinned global order. */
  private static void assertReplay(Path file) throws Exception {
    try (ClientJournalAdapter journal = ClientJournalAdapter.open(file)) {
      List<Journal.ReplayEntry> replayed = journal.replay(0);
      assertEquals(INTENTS, replayed.size());
      for (int i = 1; i <= INTENTS; i++) {
        Journal.ReplayEntry entry = replayed.get(i - 1);
        assertEquals("kill-" + i, entry.intent().intentId());
        assertArrayEquals(("payload-" + i).getBytes(StandardCharsets.UTF_8), entry.intent().record());
        assertEquals(i % 2 == 0, entry.committed(), "commit recorded for kill-" + i);
      }
      assertEquals(1, replayed.get(0).cursor());
      assertEquals(2, replayed.get(1).cursor());
      assertEquals(3, replayed.get(1).commitCursor(), "commit for kill-2 consumed cursor 3");
      assertEquals(4, replayed.get(2).cursor());
      assertEquals(5, replayed.get(3).cursor());
      assertEquals(6, replayed.get(3).commitCursor(), "commit for kill-4 consumed cursor 6");
      assertEquals(7, replayed.get(4).cursor());
      assertEquals(8, replayed.get(5).cursor());
      assertEquals(9, replayed.get(5).commitCursor(), "commit for kill-6 consumed cursor 9");
      for (int i = 1; i < replayed.size(); i++) {
        assertTrue(replayed.get(i).cursor() > replayed.get(i - 1).cursor(), "ascending cursors");
      }
      assertEquals(List.of(), journal.replay(9));
    }
  }
}
