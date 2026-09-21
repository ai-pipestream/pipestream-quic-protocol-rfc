package ai.pipestream.quic.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Volatile in-memory {@link Journal}: the test and development backbone. Append
 * order, cursor assignment, idempotent retries, and replay semantics are
 * identical to the durable backends, but nothing survives process exit.
 * Thread-safe; blocking calls are constant time.
 */
public final class MemoryJournal implements Journal {
  private static final class EntryState {
    final Intent intent;
    final long cursor;
    long commitCursor;
    byte[] commit;

    EntryState(Intent intent, long cursor) {
      this.intent = new Intent(intent.intentId(), intent.record());
      this.cursor = cursor;
    }
  }

  private final List<EntryState> entries = new ArrayList<>();
  private final Map<String, EntryState> byIntentId = new HashMap<>();
  private long lastCursor;
  private boolean closed;

  private MemoryJournal() {}

  /**
   * Creates an empty volatile journal.
   * @return in-memory journal
   */
  public static MemoryJournal create() {
    return new MemoryJournal();
  }

  @Override public synchronized long appendIntent(Intent intent) throws StorageException {
    Objects.requireNonNull(intent, "intent");
    checkOpen();
    EntryState existing = byIntentId.get(intent.intentId());
    if (existing != null) {
      if (Arrays.equals(existing.intent.record(), intent.record())) return existing.cursor;
      throw StorageException.corrupt("intent " + intent.intentId() + " already exists with different bytes");
    }
    EntryState entry = new EntryState(intent, ++lastCursor);
    entries.add(entry);
    byIntentId.put(entry.intent.intentId(), entry);
    return entry.cursor;
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
    lastCursor++;
    entry.commitCursor = lastCursor;
    entry.commit = commitRecord.clone();
  }

  @Override public synchronized List<ReplayEntry> replay(long sinceCursor) throws StorageException {
    if (sinceCursor < 0) throw new IllegalArgumentException("replay cursor must be non-negative");
    checkOpen();
    List<ReplayEntry> replayed = new ArrayList<>();
    for (EntryState entry : entries) {
      if (entry.cursor > sinceCursor) {
        replayed.add(new ReplayEntry(entry.cursor,
            new Intent(entry.intent.intentId(), entry.intent.record()),
            entry.commit == null ? null : entry.commit.clone(),
            entry.commit == null ? null : entry.commitCursor));
      }
    }
    return List.copyOf(replayed);
  }

  @Override public synchronized void close() {
    closed = true;
    entries.clear();
    byIntentId.clear();
  }

  private void checkOpen() throws StorageException {
    if (closed) throw StorageException.failed("journal is closed", null);
  }
}
