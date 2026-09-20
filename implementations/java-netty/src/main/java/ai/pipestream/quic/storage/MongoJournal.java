package ai.pipestream.quic.storage;

import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

/**
 * Durable MongoDB {@link Journal}: one append-only document per intent or
 * commit. The global cursor comes from a counters collection bumped with
 * {@code findOneAndUpdate($inc)} returning the post-increment document: that
 * is the safe single-node sequence allocation (atomic, monotonic, and unique
 * even across retries), and operations allocate cursors under the journal's
 * lock, so commits consume cursors in the global order exactly like intents
 * and the cross-backend replay vectors hold. Cursor values are the journal
 * documents' ids.
 *
 * <p>Every write uses {@code WriteConcern.MAJORITY} with journaling, so an
 * operation returns success only after the server acknowledges the write
 * durably; replication topology is deployment configuration. A partial
 * unique index pins one intent document per intent id; commits for the same
 * id are distinguished by their kind discriminant, and idempotency follows
 * the pinned decisions: an identical re-append or re-commit is a no-op
 * success, changed bytes are Corrupt.
 *
 * <p>The synchronous driver is used, matching the synchronous SPI, and the
 * MongoClient is thread-safe; the journal serializes operations on itself,
 * matching the other backends' single-writer convention. One live journal
 * per server and database may exist in a JVM. Replay attaches commits to
 * intents inside the requested window only; a commit whose intent sits at or
 * below the cursor is invisible, exactly like the other backends.
 */
public final class MongoJournal implements Journal {
  private final String identity;
  private final MongoClient client;
  private final MongoDatabase database;
  private volatile boolean closed;

  private MongoJournal(String identity, MongoClient client, MongoDatabase database) {
    this.identity = identity;
    this.client = client;
    this.database = database;
  }

  /**
   * Opens an exclusively owned journal, creating the collections, indexes,
   * and the counter document when absent. The database itself is the
   * isolation unit; both roles may share one server.
   *
   * @param connectionString MongoDB connection string, for example {@code mongodb://127.0.0.1:27017}
   * @param database database this journal exclusively owns
   * @return exclusive journal handle
   * @throws StorageException {@code TRANSIENT} for server failure, {@code FAILED} for a second live journal on the same identity
   */
  public static MongoJournal open(String connectionString, String database) throws StorageException {
    Objects.requireNonNull(connectionString, "connectionString");
    Objects.requireNonNull(database, "database");
    if (database.isEmpty()) throw new IllegalArgumentException("database must be non-empty");
    String identity = "mongo-journal\n" + connectionString + "\n" + database;
    MongoSupport.acquire(identity);
    try {
      MongoClient client = MongoClients.create(com.mongodb.MongoClientSettings.builder()
          .applyConnectionString(new com.mongodb.ConnectionString(connectionString))
          .writeConcern(com.mongodb.WriteConcern.MAJORITY.withJournal(true))
          .build());
      try {
        MongoDatabase db = client.getDatabase(database);
        db.getCollection(MongoSupport.JOURNAL_COLLECTION).createIndex(
            Indexes.ascending("intent_id"),
            new IndexOptions().unique(true)
                .partialFilterExpression(Filters.eq("kind", MongoSupport.KIND_INTENT)));
        return new MongoJournal(identity, client, db);
      } catch (RuntimeException failure) {
        try {
          client.close();
        } catch (RuntimeException cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
    } catch (MongoException failure) {
      MongoSupport.release(identity);
      throw MongoSupport.mapMongoException(failure, "mongo journal open");
    } catch (RuntimeException failure) {
      MongoSupport.release(identity);
      throw StorageException.failed("mongo journal open failed", failure);
    }
  }

  @Override public synchronized long appendIntent(Intent intent) throws StorageException {
    Objects.requireNonNull(intent, "intent");
    checkOpen();
    byte[] record = intent.record();
    try {
      Document existing = journalRow(MongoSupport.KIND_INTENT, intent.intentId());
      if (existing != null) {
        long cursor = existing.getLong("_id");
        if (Arrays.equals(binary(existing.get("payload")), record)) return cursor;
        throw StorageException.corrupt("intent " + intent.intentId() + " already exists with different bytes");
      }
      long cursor = nextCursor();
      try {
        collection().insertOne(new Document("_id", cursor)
            .append("kind", MongoSupport.KIND_INTENT)
            .append("intent_id", intent.intentId())
            .append("payload", new Binary(record)));
      } catch (MongoWriteException conflict) {
        if (conflict.getError().getCode() != MongoSupport.DUPLICATE_KEY) throw conflict;
        Document raced = journalRow(MongoSupport.KIND_INTENT, intent.intentId());
        if (raced != null && Arrays.equals(binary(raced.get("payload")), record)) {
          return raced.getLong("_id");
        }
        throw StorageException.corrupt("intent " + intent.intentId() + " already exists with different bytes");
      }
      return cursor;
    } catch (MongoException failure) {
      throw MongoSupport.mapMongoException(failure, "appendIntent");
    }
  }

  @Override public synchronized void commit(String intentId, byte[] commitRecord) throws StorageException {
    Objects.requireNonNull(intentId, "intentId");
    Objects.requireNonNull(commitRecord, "commitRecord");
    checkOpen();
    if (commitRecord.length < 1 || commitRecord.length > MAX_RECORD_BYTES) {
      throw new IllegalArgumentException("commit record must be 1 to " + MAX_RECORD_BYTES + " octets");
    }
    try {
      if (journalRow(MongoSupport.KIND_INTENT, intentId) == null) {
        throw StorageException.notFound("intent " + intentId + " has no durable append");
      }
      Document existing = journalRow(MongoSupport.KIND_COMMIT, intentId);
      if (existing != null) {
        if (Arrays.equals(binary(existing.get("payload")), commitRecord)) return;
        throw StorageException.corrupt("intent " + intentId + " already carries a different commit");
      }
      long cursor = nextCursor();
      try {
        collection().insertOne(new Document("_id", cursor)
            .append("kind", MongoSupport.KIND_COMMIT)
            .append("intent_id", intentId)
            .append("payload", new Binary(commitRecord)));
      } catch (MongoWriteException conflict) {
        if (conflict.getError().getCode() != MongoSupport.DUPLICATE_KEY) throw conflict;
        Document raced = journalRow(MongoSupport.KIND_COMMIT, intentId);
        if (raced != null && Arrays.equals(binary(raced.get("payload")), commitRecord)) return;
        throw StorageException.corrupt("intent " + intentId + " already carries a different commit");
      }
    } catch (MongoException failure) {
      throw MongoSupport.mapMongoException(failure, "commit");
    }
  }

  @Override public synchronized List<ReplayEntry> replay(long sinceCursor) throws StorageException {
    if (sinceCursor < 0) throw new IllegalArgumentException("replay cursor must be non-negative");
    checkOpen();
    List<ReplayEntry> replayed = new ArrayList<>();
    Map<String, Integer> positionByIntent = new HashMap<>();
    try {
      FindIterable<Document> rows = collection().find(Filters.gt("_id", sinceCursor)).sort(Sorts.ascending("_id"));
      for (Document row : rows) {
        long cursor = row.getLong("_id");
        int kind = row.getInteger("kind");
        String intentId = row.getString("intent_id");
        byte[] payload = binary(row.get("payload"));
        if (kind == MongoSupport.KIND_INTENT) {
          positionByIntent.put(intentId, replayed.size());
          replayed.add(new ReplayEntry(cursor, new Intent(intentId, payload), null));
        } else if (kind == MongoSupport.KIND_COMMIT) {
          Integer position = positionByIntent.get(intentId);
          // The intent sits at or below sinceCursor, outside this replay
          // window; like the other backends, the commit is invisible.
          if (position == null) continue;
          ReplayEntry entry = replayed.get(position);
          replayed.set(position, new ReplayEntry(entry.cursor(), entry.intent(), payload));
        }
      }
      return List.copyOf(replayed);
    } catch (MongoException failure) {
      throw MongoSupport.mapMongoException(failure, "replay");
    }
  }

  /**
   * Closes the client and releases the role claim; idempotent. A failed close
   * still releases the claim so a later open can succeed.
   *
   * @throws StorageException {@code FAILED} if the client close fails
   */
  @Override public synchronized void close() throws StorageException {
    if (closed) return;
    closed = true;
    try {
      client.close();
    } catch (RuntimeException failure) {
      throw StorageException.failed("mongo journal close failed", failure);
    } finally {
      MongoSupport.release(identity);
    }
  }

  /**
   * Allocates the next global cursor: atomically increments the journal
   * counter and returns the post-increment value, unique and monotonic even
   * across retries. Called under the journal lock.
   */
  private long nextCursor() {
    Bson filter = Filters.eq("_id", MongoSupport.JOURNAL_COUNTER_ID);
    Document counter = database.getCollection(MongoSupport.COUNTERS_COLLECTION).findOneAndUpdate(filter,
        Updates.inc("seq", 1L),
        new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
    return counter.getLong("seq");
  }

  private Document journalRow(int kind, String intentId) {
    return collection().find(Filters.and(Filters.eq("kind", kind), Filters.eq("intent_id", intentId))).first();
  }

  private MongoCollection<Document> collection() {
    return database.getCollection(MongoSupport.JOURNAL_COLLECTION);
  }

  private void checkOpen() throws StorageException {
    if (closed) throw StorageException.failed("journal is closed", null);
  }

  private static byte[] binary(Object value) {
    return ((Binary) value).getData();
  }
}
