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

  @Test void rootThatIsARegularFileIsFailedNotTransient() throws Exception {
    Path file = directory.resolve("a-regular-file");
    Files.write(file, bytes("not a directory"));
    assertStorage(StorageException.Kind.FAILED, () -> FileJournal.open(file));
  }

  @Test void failedAppendTruncatesAndReopenYieldsCommittedPrefix() throws Exception {
    var injected = new java.util.concurrent.atomic.AtomicReference<FailingChannel>();
    try (Journal journal = FileJournal.open(directory, path -> {
      FailingChannel channel = new FailingChannel(java.nio.channels.FileChannel.open(path,
          java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE,
          java.nio.file.StandardOpenOption.READ));
      injected.set(channel);
      return channel;
    })) {
      assertEquals(1, journal.appendIntent(new Journal.Intent("one", bytes("durable"))));
      injected.get().writesBeforeFailure = 2;
      assertStorage(StorageException.Kind.TRANSIENT,
          () -> journal.appendIntent(new Journal.Intent("two", bytes("lost"))));
      injected.get().writesBeforeFailure = -1;
      assertEquals(2, journal.appendIntent(new Journal.Intent("two", bytes("retried"))),
          "append continues at the truncated offset");
      assertEquals(List.of("one", "two"),
          journal.replay(0).stream().map(entry -> entry.intent().intentId()).toList());
    }
    try (Journal reopened = FileJournal.open(directory)) {
      assertEquals(List.of("one", "two"),
          reopened.replay(0).stream().map(entry -> entry.intent().intentId()).toList(),
          "committed prefix survives the failed append");
      assertEquals(3, reopened.appendIntent(new Journal.Intent("three", bytes("continues"))));
    }
  }

  /** Delegating channel whose Nth write call throws, to simulate a dying disk mid-append. */
  private static final class FailingChannel extends java.nio.channels.FileChannel {
    private final java.nio.channels.FileChannel delegate;
    volatile int writesBeforeFailure = -1;

    FailingChannel(java.nio.channels.FileChannel delegate) {
      this.delegate = delegate;
    }

    @Override public int read(java.nio.ByteBuffer dst) throws java.io.IOException {
      return delegate.read(dst);
    }

    @Override public long read(java.nio.ByteBuffer[] dsts, int offset, int length) throws java.io.IOException {
      return delegate.read(dsts, offset, length);
    }

    @Override public int write(java.nio.ByteBuffer src) throws java.io.IOException {
      if (writesBeforeFailure == 0) throw new java.io.IOException("injected write failure");
      if (writesBeforeFailure > 0) writesBeforeFailure--;
      return delegate.write(src);
    }

    @Override public long write(java.nio.ByteBuffer[] srcs, int offset, int length) throws java.io.IOException {
      if (writesBeforeFailure == 0) throw new java.io.IOException("injected write failure");
      if (writesBeforeFailure > 0) writesBeforeFailure--;
      return delegate.write(srcs, offset, length);
    }

    @Override public long position() throws java.io.IOException {
      return delegate.position();
    }

    @Override public java.nio.channels.FileChannel position(long newPosition) throws java.io.IOException {
      delegate.position(newPosition);
      return this;
    }

    @Override public long size() throws java.io.IOException {
      return delegate.size();
    }

    @Override public java.nio.channels.FileChannel truncate(long size) throws java.io.IOException {
      delegate.truncate(size);
      return this;
    }

    @Override public void force(boolean metaData) throws java.io.IOException {
      delegate.force(metaData);
    }

    @Override public long transferTo(long position, long count, java.nio.channels.WritableByteChannel target)
        throws java.io.IOException {
      return delegate.transferTo(position, count, target);
    }

    @Override public long transferFrom(java.nio.channels.ReadableByteChannel src, long position, long count)
        throws java.io.IOException {
      return delegate.transferFrom(src, position, count);
    }

    @Override public int read(java.nio.ByteBuffer dst, long position) throws java.io.IOException {
      return delegate.read(dst, position);
    }

    @Override public int write(java.nio.ByteBuffer src, long position) throws java.io.IOException {
      if (writesBeforeFailure == 0) throw new java.io.IOException("injected write failure");
      if (writesBeforeFailure > 0) writesBeforeFailure--;
      return delegate.write(src, position);
    }

    @Override public java.nio.MappedByteBuffer map(MapMode mode, long position, long size)
        throws java.io.IOException {
      return delegate.map(mode, position, size);
    }

    @Override public java.nio.channels.FileLock lock(long position, long size, boolean shared)
        throws java.io.IOException {
      return delegate.lock(position, size, shared);
    }

    @Override public java.nio.channels.FileLock tryLock(long position, long size, boolean shared)
        throws java.io.IOException {
      return delegate.tryLock(position, size, shared);
    }

    @Override protected void implCloseChannel() throws java.io.IOException {
      delegate.close();
    }
  }
}
