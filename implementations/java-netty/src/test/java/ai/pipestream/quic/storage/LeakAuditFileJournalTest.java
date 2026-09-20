package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Leak audit for {@link FileJournal}: file-descriptor growth across open/close
 * cycles, descriptor neutrality of the error paths (corrupt-log refusal,
 * double-writer refusal, failed append with truncation, torn-tail recovery),
 * and the invariant that a refused or failed open leaves the root reusable.
 * Failures are injected through the package-local {@link FileJournal.LogFactory}
 * seam with a deterministic delegating channel; there is no randomness and no
 * wall-clock waiting.
 */
final class LeakAuditFileJournalTest {
  @TempDir Path directory;

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static long fdCount() throws Exception {
    try (Stream<Path> fds = Files.list(Path.of("/proc/self/fd"))) {
      return fds.count();
    }
  }

  private Path logFile() {
    return directory.resolve("journal").resolve("journal.log");
  }

  @Test void openCloseCyclesDoNotGrowFileDescriptors() throws Exception {
    long before = fdCount();
    for (int cycle = 0; cycle < 500; cycle++) {
      FileJournal journal = FileJournal.open(directory);
      journal.close();
    }
    assertTrue(fdCount() - before <= 2,
        "500 open/close cycles leak no file descriptors");
  }

  @Test void corruptLogRefusalIsDescriptorNeutralAndRootStaysReusable() throws Exception {
    Files.createDirectories(directory.resolve("journal"));
    Files.write(logFile(), bytes("not a pipestream journal at all"));
    long before = fdCount();
    for (int round = 0; round < 50; round++) {
      assertStorage(StorageException.Kind.CORRUPT, () -> FileJournal.open(directory));
    }
    assertTrue(fdCount() - before <= 2, "corrupt-log refusals leak no lock or log descriptors");
    Files.delete(logFile());
    try (Journal journal = FileJournal.open(directory)) {
      assertEquals(List.of(), journal.replay(0), "root reusable after refused opens");
    }
  }

  @Test void doubleWriterRefusalIsDescriptorNeutral() throws Exception {
    long before = fdCount();
    Journal first = FileJournal.open(directory);
    for (int round = 0; round < 100; round++) {
      assertStorage(StorageException.Kind.FAILED, () -> FileJournal.open(directory));
    }
    assertTrue(fdCount() - before <= 2, "refused opens leak no file descriptors");
    first.close();
    try (Journal second = FileJournal.open(directory)) {
      assertEquals(List.of(), second.replay(0));
    }
  }

  @Test void failedAppendsTruncateWithoutLosingDescriptors() throws Exception {
    var injected = new AtomicReference<FailingChannel>();
    long before = fdCount();
    try (Journal journal = FileJournal.open(directory, path -> {
      FailingChannel channel = new FailingChannel(FileChannel.open(path,
          StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ));
      injected.set(channel);
      return channel;
    })) {
      assertEquals(1, journal.appendIntent(new Journal.Intent("durable", bytes("kept"))));
      for (int round = 0; round < 30; round++) {
        final String lostId = "lost-" + round;
        injected.get().writesBeforeFailure = 1;
        assertStorage(StorageException.Kind.TRANSIENT,
            () -> journal.appendIntent(new Journal.Intent(lostId, bytes("dropped"))));
      }
      injected.get().writesBeforeFailure = -1;
      assertEquals(2, journal.appendIntent(new Journal.Intent("after", bytes("recovered"))));
      assertEquals(List.of("durable", "after"),
          journal.replay(0).stream().map(entry -> entry.intent().intentId()).toList());
    }
    assertTrue(fdCount() - before <= 2, "failed appends leak no file descriptors");
    try (Journal reopened = FileJournal.open(directory)) {
      assertEquals(List.of("durable", "after"),
          reopened.replay(0).stream().map(entry -> entry.intent().intentId()).toList(),
          "committed prefix survives the failed appends");
    }
  }

  @Test void tornTailRecoveryCyclesAreDescriptorNeutral() throws Exception {
    long before = fdCount();
    for (int cycle = 0; cycle < 40; cycle++) {
      try (Journal journal = FileJournal.open(directory)) {
        assertEquals(cycle + 1, journal.appendIntent(
            new Journal.Intent("intent-" + cycle, bytes("payload-" + cycle))));
      }
      Files.write(logFile(), new byte[] {0, 0, 0, 0, 3, 0}, StandardOpenOption.APPEND);
      try (Journal reopened = FileJournal.open(directory)) {
        assertEquals(cycle + 1, reopened.replay(0).size(), "torn tail discarded each cycle");
      }
    }
    assertTrue(fdCount() - before <= 2, "torn-tail recovery cycles leak no file descriptors");
  }

  @Test void appendCommitReplayCyclesDoNotGrowDescriptors() throws Exception {
    long before = fdCount();
    try (Journal journal = FileJournal.open(directory)) {
      for (int round = 0; round < 200; round++) {
        journal.appendIntent(new Journal.Intent("i" + round, bytes("r" + round)));
        journal.commit("i" + round, bytes("c" + round));
        assertEquals(round + 1, journal.replay(0).size());
      }
    }
    assertTrue(fdCount() - before <= 2, "append/commit/replay cycles leak no file descriptors");
  }

  /** Delegating channel whose Nth write throws, to simulate a dying disk mid-append. */
  private static final class FailingChannel extends FileChannel {
    private final FileChannel delegate;
    volatile int writesBeforeFailure = -1;

    FailingChannel(FileChannel delegate) {
      this.delegate = delegate;
    }

    private void tick() throws IOException {
      if (writesBeforeFailure == 0) throw new IOException("injected write failure");
      if (writesBeforeFailure > 0) writesBeforeFailure--;
    }

    @Override public int read(ByteBuffer dst) throws IOException {
      return delegate.read(dst);
    }

    @Override public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
      return delegate.read(dsts, offset, length);
    }

    @Override public int write(ByteBuffer src) throws IOException {
      tick();
      return delegate.write(src);
    }

    @Override public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
      tick();
      return delegate.write(srcs, offset, length);
    }

    @Override public long position() throws IOException {
      return delegate.position();
    }

    @Override public FileChannel position(long newPosition) throws IOException {
      delegate.position(newPosition);
      return this;
    }

    @Override public long size() throws IOException {
      return delegate.size();
    }

    @Override public FileChannel truncate(long size) throws IOException {
      delegate.truncate(size);
      return this;
    }

    @Override public void force(boolean metaData) throws IOException {
      delegate.force(metaData);
    }

    @Override public long transferTo(long position, long count, java.nio.channels.WritableByteChannel target)
        throws IOException {
      return delegate.transferTo(position, count, target);
    }

    @Override public long transferFrom(java.nio.channels.ReadableByteChannel src, long position, long count)
        throws IOException {
      return delegate.transferFrom(src, position, count);
    }

    @Override public int read(ByteBuffer dst, long position) throws IOException {
      return delegate.read(dst, position);
    }

    @Override public int write(ByteBuffer src, long position) throws IOException {
      tick();
      return delegate.write(src, position);
    }

    @Override public java.nio.MappedByteBuffer map(MapMode mode, long position, long size)
        throws IOException {
      return delegate.map(mode, position, size);
    }

    @Override public java.nio.channels.FileLock lock(long position, long size, boolean shared)
        throws IOException {
      return delegate.lock(position, size, shared);
    }

    @Override public java.nio.channels.FileLock tryLock(long position, long size, boolean shared)
        throws IOException {
      return delegate.tryLock(position, size, shared);
    }

    @Override protected void implCloseChannel() throws IOException {
      delegate.close();
    }
  }
}
