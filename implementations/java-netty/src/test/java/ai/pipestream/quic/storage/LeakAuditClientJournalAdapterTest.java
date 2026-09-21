package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Leak audit for {@link ClientJournalAdapter}: the same-JVM per-file open
 * registry must survive refused and failed opens (a failed initialize or a
 * refused second open must not poison the file), open/close cycles must not
 * grow file descriptors ({@code /proc/self/fd}), and every write must stay in
 * the journal file's own sibling namespace — no scratch may escape into an
 * ambient temp directory. SQLite runs in-process here; there is no docker,
 * no randomness and no wall-clock waiting.
 */
final class LeakAuditClientJournalAdapterTest {
  @TempDir Path directory;

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static long fdCount() throws Exception {
    try (Stream<Path> fds = Files.list(Path.of("/proc/self/fd"))) {
      return fds.count();
    }
  }

  @Test void failedInitializeAndFailedOpenReleaseTheFileClaim() throws Exception {
    Path occupied = directory.resolve("occupied.sqlite");
    Files.write(occupied, bytes("not a journal"));
    assertThrows(IOException.class, () -> ClientJournalAdapter.initialize(occupied));
    Files.delete(occupied);
    try (ClientJournalAdapter journal = ClientJournalAdapter.initialize(occupied)) {
      journal.appendIntent(new Journal.Intent("after-failure", bytes("record")));
    }
    Path missing = directory.resolve("missing.sqlite");
    assertThrows(IOException.class, () -> ClientJournalAdapter.open(missing));
    try (ClientJournalAdapter journal = ClientJournalAdapter.initialize(missing)) {
      assertEquals(1, journal.appendIntent(new Journal.Intent("usable", bytes("record"))));
    }
  }

  @Test void openCloseCyclesAreDescriptorNeutral() throws Exception {
    long before = fdCount();
    for (int cycle = 0; cycle < 100; cycle++) {
      try (ClientJournalAdapter journal =
          ClientJournalAdapter.initialize(directory.resolve("cycle-" + cycle + ".sqlite"))) {
        journal.appendIntent(new Journal.Intent("i" + cycle, bytes("r" + cycle)));
      }
    }
    assertTrue(fdCount() - before <= 2, "100 adapter cycles leak no file descriptors");
  }

  @Test void refusedSecondAdapterIsDescriptorNeutralAndReopenable() throws Exception {
    Path file = directory.resolve("held.sqlite");
    long before = fdCount();
    try (ClientJournalAdapter first = ClientJournalAdapter.initialize(file)) {
      first.appendIntent(new Journal.Intent("held", bytes("record")));
      for (int round = 0; round < 50; round++) {
        StorageException refusal =
            assertThrows(StorageException.class, () -> ClientJournalAdapter.open(file));
        assertEquals(StorageException.Kind.FAILED, refusal.kind(), refusal.getMessage());
      }
    }
    assertTrue(fdCount() - before <= 2, "refused opens leak no file descriptors");
    try (ClientJournalAdapter reopened = ClientJournalAdapter.open(file)) {
      assertEquals(1, reopened.replay(0).size());
    }
  }

  @Test void everyWriteStaysBesideTheJournalFile() throws Exception {
    Path file = directory.resolve("confined.sqlite");
    try (ClientJournalAdapter journal = ClientJournalAdapter.initialize(file)) {
      journal.appendIntent(new Journal.Intent("one", bytes("first")));
      journal.appendIntent(new Journal.Intent("two", bytes("second")));
      journal.commit("one", bytes("done"));
    }
    String base = file.getFileName().toString();
    try (Stream<Path> paths = Files.list(directory)) {
      List<Path> files = paths.filter(Files::isRegularFile).toList();
      assertFalse(files.isEmpty(), "journal file exists beside nothing else unexpected");
      for (Path sibling : files) {
        assertEquals(directory, sibling.getParent(),
            "write escaped the journal's own directory: " + sibling);
        assertTrue(sibling.getFileName().toString().startsWith(base),
            "unexpected file not derived from the journal name: " + sibling);
      }
      assertTrue(files.stream().anyMatch(path -> path.getFileName().toString().equals(base)),
          "the journal file itself is present");
    }
  }
}
