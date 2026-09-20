package ai.pipestream.quic.storage;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Durable filesystem {@link Journal}: one append-only log under the configured
 * root. Every record is checksummed and flushed to stable storage before its
 * append returns success, so success implies survival of a process kill. A
 * torn trailing record left by a killed writer is discarded at open: that
 * append never returned success. A record that fails its checksum anywhere
 * else means corruption. One cooperating process owns a journal root.
 */
public final class FileJournal implements Journal {
  private static final byte[] MAGIC = {'P', 'S', 'J', 'S', 'L', '0', '1'};
  private static final byte TYPE_INTENT = 0;
  private static final byte TYPE_COMMIT = 1;
  private static final int RECORD_HEADER_BYTES = 1 + 8 + 4 + 4;
  private static final String JOURNAL_DIRECTORY = "journal";
  private static final String LOG_FILE_NAME = "journal.log";
  private static final Set<Path> OPEN_ROOTS = ConcurrentHashMap.newKeySet();

  private static final class EntryState {
    final String intentId;
    final long cursor;
    final byte[] record;
    byte[] commit;

    EntryState(String intentId, long cursor, byte[] record) {
      this.intentId = intentId;
      this.cursor = cursor;
      this.record = record;
    }
  }

  private final Path journalDirectory;
  private final FileChannel lockChannel;
  private final FileLock lock;
  private final FileChannel log;
  private final List<EntryState> entries = new ArrayList<>();
  private final Map<String, EntryState> byIntentId = new HashMap<>();
  private long lastCursor;
  private boolean closed;

  private FileJournal(Path journalDirectory, FileChannel lockChannel, FileLock lock, FileChannel log) {
    this.journalDirectory = journalDirectory;
    this.lockChannel = lockChannel;
    this.lock = lock;
    this.log = log;
  }

  /**
   * Opens (creating if necessary) an exclusively owned journal under
   * {@code root/journal}. Retained records are checksum-verified while the
   * in-memory index is rebuilt; a torn tail from a killed writer is
   * truncated, real corruption is refused.
   *
   * @param root configured store root; the log lives under it
   * @return exclusive journal handle
   * @throws StorageException {@code CORRUPT} for a corrupt log, {@code TRANSIENT} for filesystem failure, {@code FAILED} for a second writer
   */
  public static FileJournal open(Path root) throws StorageException {
    Objects.requireNonNull(root, "root");
    Path requested = root.toAbsolutePath().normalize();
    try {
      Files.createDirectories(requested);
      if (!Files.isDirectory(requested, LinkOption.NOFOLLOW_LINKS)) {
        throw StorageException.failed("journal root is not a real directory", null);
      }
      Path base = requested.toRealPath();
      Path directory = base.resolve(JOURNAL_DIRECTORY);
      createDirectoriesDurable(directory);
      // Closing a second channel can release another channel's OS lock in this JVM.
      boolean registered = false;
      synchronized (OPEN_ROOTS) {
        if (!OPEN_ROOTS.add(directory)) throw StorageException.failed("journal already has a writer", null);
        registered = true;
      }
      FileChannel lockChannel = null;
      FileLock held = null;
      FileChannel channel = null;
      try {
        lockChannel = FileChannel.open(directory.resolve("writer.lock"), StandardOpenOption.CREATE,
            StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
        try {
          held = lockChannel.tryLock();
        } catch (OverlappingFileLockException failure) {
          throw StorageException.failed("journal already has a writer", failure);
        }
        if (held == null) throw StorageException.failed("journal already has a writer", null);
        channel = FileChannel.open(directory.resolve(LOG_FILE_NAME), StandardOpenOption.CREATE,
            StandardOpenOption.WRITE, StandardOpenOption.READ);
        var journal = new FileJournal(directory, lockChannel, held, channel);
        journal.recover();
        channel = null;
        lockChannel = null;
        held = null;
        return journal;
      } finally {
        if (held != null) try { held.release(); } catch (IOException cleanup) { /* best effort */ }
        if (lockChannel != null) try { lockChannel.close(); } catch (IOException cleanup) { /* best effort */ }
        if (channel != null) try { channel.close(); } catch (IOException cleanup) { /* best effort */ }
        if (registered) synchronized (OPEN_ROOTS) { OPEN_ROOTS.remove(directory); }
      }
    } catch (IOException failure) {
      throw StorageException.transientFailure("journal open failed", failure);
    }
  }

  @Override public synchronized long appendIntent(Intent intent) throws StorageException {
    Objects.requireNonNull(intent, "intent");
    checkOpen();
    byte[] record = intent.record();
    EntryState existing = byIntentId.get(intent.intentId());
    if (existing != null) {
      if (Arrays.equals(existing.record, record)) return existing.cursor;
      throw StorageException.corrupt("intent " + intent.intentId() + " already exists with different bytes");
    }
    byte[] idBytes = intent.intentId().getBytes(StandardCharsets.UTF_8);
    long cursor = appendRecord(TYPE_INTENT, idBytes, record);
    var entry = new EntryState(intent.intentId(), cursor, record);
    entries.add(entry);
    byIntentId.put(entry.intentId, entry);
    return cursor;
  }

  @Override public synchronized void commit(String intentId, byte[] commitRecord) throws StorageException {
    Objects.requireNonNull(intentId, "intentId");
    Objects.requireNonNull(commitRecord, "commitRecord");
    checkOpen();
    if (commitRecord.length < 1 || commitRecord.length > MAX_RECORD_BYTES) {
      throw new IllegalArgumentException("commit record must be 1 to " + MAX_RECORD_BYTES + " octets");
    }
    EntryState entry = byIntentId.get(intentId);
    if (entry == null) {
      throw StorageException.notFound("intent " + intentId + " has no durable append");
    }
    if (entry.commit != null) {
      if (Arrays.equals(entry.commit, commitRecord)) return;
      throw StorageException.corrupt("intent " + intentId + " already carries a different commit");
    }
    appendRecord(TYPE_COMMIT, intentId.getBytes(StandardCharsets.UTF_8), commitRecord);
    entry.commit = commitRecord.clone();
  }

  @Override public synchronized List<ReplayEntry> replay(long sinceCursor) throws StorageException {
    if (sinceCursor < 0) throw new IllegalArgumentException("replay cursor must be non-negative");
    checkOpen();
    List<ReplayEntry> replayed = new ArrayList<>();
    for (EntryState entry : entries) {
      if (entry.cursor > sinceCursor) {
        replayed.add(new ReplayEntry(entry.cursor,
            new Intent(entry.intentId, entry.record), entry.commit == null ? null : entry.commit.clone()));
      }
    }
    return List.copyOf(replayed);
  }

  /**
   * Flushes and closes the log, then releases the exclusive lock.
   *
   * @throws StorageException {@code FAILED} if the final flush fails
   */
  @Override public synchronized void close() throws StorageException {
    if (closed) return;
    closed = true;
    StorageException cleanup = null;
    try {
      log.force(true);
    } catch (IOException failure) {
      cleanup = StorageException.failed("journal final flush failed", failure);
    }
    try {
      log.close();
    } catch (IOException failure) {
      if (cleanup == null) cleanup = StorageException.failed("journal close failed", failure);
    } finally {
      try {
        lock.release();
      } catch (IOException failure) {
        if (cleanup == null) cleanup = StorageException.failed("lock release failed", failure);
      } finally {
        try {
          lockChannel.close();
        } catch (IOException failure) {
          if (cleanup == null) cleanup = StorageException.failed("lock close failed", failure);
        } finally {
          if (!log.isOpen()) synchronized (OPEN_ROOTS) { OPEN_ROOTS.remove(journalDirectory); }
        }
      }
    }
    if (cleanup != null) throw cleanup;
  }

  private long appendRecord(byte type, byte[] idBytes, byte[] payload) throws StorageException {
    long cursor = lastCursor + 1;
    ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER_BYTES)
        .put(type).putLong(cursor).putInt(idBytes.length).putInt(payload.length);
    MessageDigest checksum = StoreLayout.sha256();
    checksum.update(header.array());
    checksum.update(idBytes);
    checksum.update(payload);
    byte[] recordChecksum = checksum.digest();
    try {
      writeFully(log, header.array());
      writeFully(log, idBytes);
      writeFully(log, payload);
      writeFully(log, recordChecksum);
      log.force(true);
    } catch (IOException failure) {
      throw StorageException.transientFailure("journal append failed", failure);
    }
    lastCursor = cursor;
    return cursor;
  }

  /**
   * Rebuilds the index from the log, truncating at most one torn trailing
   * record: bytes after the last checksummed record never belonged to an
   * append that returned success.
   */
  private void recover() throws StorageException, IOException {
    long size = log.size();
    if (size == 0) {
      writeFully(log, MAGIC);
      log.force(true);
      syncDirectory(journalDirectory);
      return;
    }
    log.position(0);
    ByteBuffer magic = ByteBuffer.allocate(MAGIC.length);
    readFully(log, magic);
    if (!Arrays.equals(magic.array(), MAGIC)) {
      throw StorageException.corrupt("journal log carries a foreign magic");
    }
    long position = MAGIC.length;
    long lastGood = position;
    while (position < size) {
      long scanned = scanRecord(position, size);
      if (scanned < 0) break;
      position = scanned;
      lastGood = position;
    }
    if (lastGood < size) {
      log.truncate(lastGood);
      log.force(true);
    }
    log.position(log.size());
  }

  /** Returns the offset just past the record at {@code offset}, or -1 for a torn tail. */
  private long scanRecord(long offset, long size) throws StorageException, IOException {
    if (size - offset < RECORD_HEADER_BYTES + StoreLayout.DIGEST_BYTES) return -1;
    log.position(offset);
    ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER_BYTES);
    readFully(log, header);
    byte type = header.get(0);
    int idLength = header.getInt(9);
    int payloadLength = header.getInt(13);
    if ((type != TYPE_INTENT && type != TYPE_COMMIT) || idLength < 1 || idLength > MAX_INTENT_ID_BYTES
        || payloadLength < 1 || payloadLength > MAX_RECORD_BYTES) {
      throw StorageException.corrupt("journal record geometry is invalid");
    }
    long recordEnd = offset + RECORD_HEADER_BYTES + (long) idLength + payloadLength + StoreLayout.DIGEST_BYTES;
    if (recordEnd > size) return -1;
    byte[] idBytes = new byte[idLength];
    readFully(log, ByteBuffer.wrap(idBytes));
    byte[] payload = new byte[payloadLength];
    readFully(log, ByteBuffer.wrap(payload));
    byte[] recorded = new byte[StoreLayout.DIGEST_BYTES];
    readFully(log, ByteBuffer.wrap(recorded));
    MessageDigest checksum = StoreLayout.sha256();
    checksum.update(header.array());
    checksum.update(idBytes);
    checksum.update(payload);
    if (!MessageDigest.isEqual(checksum.digest(), recorded)) {
      throw StorageException.corrupt("journal record checksum mismatch");
    }
    long cursor = header.getLong(1);
    if (cursor != lastCursor + 1) {
      throw StorageException.corrupt("journal cursor sequence has a gap");
    }
    lastCursor = cursor;
    String intentId = new String(idBytes, StandardCharsets.UTF_8);
    if (type == TYPE_INTENT) {
      if (byIntentId.containsKey(intentId)) {
        throw StorageException.corrupt("journal replays intent identifier " + intentId);
      }
      var entry = new EntryState(intentId, cursor, payload);
      entries.add(entry);
      byIntentId.put(intentId, entry);
    } else {
      EntryState entry = byIntentId.get(intentId);
      if (entry == null || entry.commit != null) {
        throw StorageException.corrupt("journal commit has no uncommitted intent " + intentId);
      }
      entry.commit = payload;
    }
    return recordEnd;
  }

  private void checkOpen() throws StorageException {
    if (closed) throw StorageException.failed("journal is closed", null);
  }

  private static void writeFully(FileChannel channel, byte[] bytes) throws IOException {
    ByteBuffer remaining = ByteBuffer.wrap(bytes);
    while (remaining.hasRemaining()) channel.write(remaining);
  }

  private static void readFully(FileChannel channel, ByteBuffer bytes) throws IOException {
    while (bytes.hasRemaining()) {
      if (channel.read(bytes) == -1) throw new EOFException("truncated journal log");
    }
  }

  private static void createDirectoriesDurable(Path directory) throws IOException {
    if (Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) return;
    Path parent = directory.getParent();
    if (parent != null) createDirectoriesDurable(parent);
    Files.createDirectory(directory);
    syncDirectory(parent);
  }

  private static void syncDirectory(Path directory) throws IOException {
    if (directory == null) return;
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }
}
