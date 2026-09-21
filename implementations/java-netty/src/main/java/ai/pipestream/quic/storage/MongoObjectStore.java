package ai.pipestream.quic.storage;

import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.types.Binary;

/**
 * Durable MongoDB {@link ObjectStore}: one document per entity and one seal
 * document per scope, following the design note's document-per-scope model in
 * its collections form. Entity documents carry the scope path, the 2-hex
 * fanout bucket, and the entity id in the pinned 32-bit formats, with the
 * payload and its put-time SHA-256 as binary data; a unique index pins
 * identity. Every write uses {@code WriteConcern.MAJORITY} with journaling,
 * so an operation returns success only after the server acknowledges the
 * write durably; replication topology is deployment configuration.
 *
 * <p>The synchronous driver is used, matching the synchronous SPI, and the
 * MongoClient is thread-safe; the store still serializes operations on
 * itself, matching the other backends' single-writer convention. One live
 * store per role, server, and database may exist in a JVM. Child scopes are
 * independent documents keyed by their own scope path, so they survive
 * retirement of a parent's entities by construction.
 */
public final class MongoObjectStore implements ObjectStore {
  private final String identity;
  private final MongoClient client;
  private final MongoDatabase database;
  private volatile boolean closed;

  private MongoObjectStore(String identity, MongoClient client, MongoDatabase database) {
    this.identity = identity;
    this.client = client;
    this.database = database;
  }

  /**
   * Opens an exclusively owned object store, creating the collections and
   * indexes when absent. The database itself is the isolation unit; both
   * roles may share one server.
   *
   * @param connectionString MongoDB connection string, for example {@code mongodb://127.0.0.1:27017}
   * @param database database this store exclusively owns
   * @return exclusive store handle
   * @throws StorageException {@code TRANSIENT} for server failure, {@code FAILED} for a second live store on the same identity
   */
  public static MongoObjectStore open(String connectionString, String database) throws StorageException {
    Objects.requireNonNull(connectionString, "connectionString");
    Objects.requireNonNull(database, "database");
    if (database.isEmpty()) throw new IllegalArgumentException("database must be non-empty");
    String identity = "mongo-objects\n" + connectionString + "\n" + database;
    MongoSupport.acquire(identity);
    try {
      MongoClient client = MongoClients.create(com.mongodb.MongoClientSettings.builder()
          .applyConnectionString(new com.mongodb.ConnectionString(connectionString))
          .writeConcern(com.mongodb.WriteConcern.MAJORITY.withJournal(true))
          .build());
      try {
        MongoDatabase db = client.getDatabase(database);
        // Both lanes share one database in certification, and the other
        // lane's documents carry no scope_path at all. A unique index over
        // the full collection would key them all as null and collide, so
        // the indexes cover only documents that hold a scope_path, same as
        // the journal's kind-gated intent index.
        db.getCollection(MongoSupport.OBJECTS_COLLECTION).createIndex(
            Indexes.ascending("scope_path", "bucket", "entity_id"),
            new IndexOptions().unique(true)
                .partialFilterExpression(Filters.exists("scope_path", true)));
        db.getCollection(MongoSupport.SEALS_COLLECTION).createIndex(
            Indexes.ascending("scope_path"),
            new IndexOptions().unique(true)
                .partialFilterExpression(Filters.exists("scope_path", true)));
        return new MongoObjectStore(identity, client, db);
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
      throw MongoSupport.mapMongoException(failure, "mongo store open");
    } catch (RuntimeException failure) {
      MongoSupport.release(identity);
      throw StorageException.failed("mongo store open failed", failure);
    }
  }

  @Override public synchronized void put(ScopePath scope, long entityId, byte[] bytes, byte[] expectedDigest)
      throws StorageException {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(bytes, "bytes");
    validateEntityId(entityId);
    byte[] expected = validateDigest(expectedDigest);
    checkOpen();
    String scopePath = scope.render();
    try {
      if (sealed(scopePath)) {
        throw StorageException.alreadySealed("scope " + scopePath + " is sealed");
      }
      byte[] actual = StoreLayout.sha256().digest(bytes);
      if (!MessageDigest.isEqual(actual, expected)) {
        throw StorageException.corrupt("put digest mismatch for entity " + entityId);
      }
      Document entity = new Document("scope_path", scopePath)
          .append("bucket", StoreLayout.fanoutBucket(entityId))
          .append("entity_id", entityId)
          .append("payload", new Binary(bytes))
          .append("digest", new Binary(expected));
      try {
        collection(MongoSupport.OBJECTS_COLLECTION).insertOne(entity);
        return;
      } catch (MongoWriteException conflict) {
        if (conflict.getError().getCode() != MongoSupport.DUPLICATE_KEY) throw conflict;
      }
      byte[] stored = storedPayload(scopePath, entityId);
      if (Arrays.equals(stored, bytes)) return;
      throw StorageException.corrupt("conflicting content for entity " + entityId);
    } catch (MongoException failure) {
      throw MongoSupport.mapMongoException(failure, "put");
    }
  }

  @Override public synchronized InputStream openRead(ScopePath scope, long entityId) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    validateEntityId(entityId);
    checkOpen();
    try {
      Document entity = collection(MongoSupport.OBJECTS_COLLECTION).find(Filters.and(
          Filters.eq("scope_path", scope.render()),
          Filters.eq("bucket", StoreLayout.fanoutBucket(entityId)),
          Filters.eq("entity_id", entityId))).first();
      if (entity == null) {
        throw StorageException.notFound("entity " + entityId + " is absent from scope " + scope.render());
      }
      // The byte array is an independent snapshot per read, so removal after
      // the read never invalidates it.
      return new ByteArrayInputStream(binary(entity.get("payload")));
    } catch (MongoException failure) {
      throw MongoSupport.mapMongoException(failure, "openRead");
    }
  }

  @Override public synchronized void sealScope(ScopePath scope, byte[] digest, long entityCount)
      throws StorageException {
    Objects.requireNonNull(scope, "scope");
    byte[] recorded = validateDigest(digest);
    if (entityCount < 0) throw new IllegalArgumentException("entity count must be non-negative");
    checkOpen();
    String scopePath = scope.render();
    try {
      Document existing = seal(scopePath);
      if (existing != null) {
        if (existing.getLong("entity_count") == entityCount
            && Arrays.equals(binary(existing.get("digest")), recorded)) {
          return;
        }
        throw StorageException.corrupt("scope " + scopePath + " already carries a different seal");
      }
      try {
        collection(MongoSupport.SEALS_COLLECTION).insertOne(new Document("scope_path", scopePath)
            .append("digest", new Binary(recorded)).append("entity_count", entityCount));
      } catch (MongoWriteException conflict) {
        if (conflict.getError().getCode() != MongoSupport.DUPLICATE_KEY) throw conflict;
        Document raced = seal(scopePath);
        if (raced != null && raced.getLong("entity_count") == entityCount
            && Arrays.equals(binary(raced.get("digest")), recorded)) {
          return;
        }
        throw StorageException.corrupt("scope " + scopePath + " already carries a different seal");
      }
    } catch (MongoException failure) {
      throw MongoSupport.mapMongoException(failure, "sealScope");
    }
  }

  @Override public synchronized SealRecord verifyScope(ScopePath scope) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    checkOpen();
    String scopePath = scope.render();
    try {
      Document seal = seal(scopePath);
      if (seal == null) {
        throw StorageException.notFound("scope " + scopePath + " has no seal record");
      }
      long live = 0;
      for (Document entity : collection(MongoSupport.OBJECTS_COLLECTION).find(Filters.eq("scope_path", scopePath))
          .sort(Sorts.ascending("entity_id"))) {
        byte[] payload = binary(entity.get("payload"));
        byte[] storedDigest = binary(entity.get("digest"));
        if (!MessageDigest.isEqual(StoreLayout.sha256().digest(payload), storedDigest)) {
          throw StorageException.corrupt("entity " + entity.getLong("entity_id") + " fails its recorded digest");
        }
        live++;
      }
      if (live != seal.getLong("entity_count")) {
        throw StorageException.corrupt("scope " + scopePath + " entity count differs from its seal");
      }
      return new SealRecord(binary(seal.get("digest")), seal.getLong("entity_count"));
    } catch (MongoException failure) {
      throw MongoSupport.mapMongoException(failure, "verifyScope");
    }
  }

  @Override public synchronized List<Long> list(ScopePath scope) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    checkOpen();
    try {
      List<Long> ids = new ArrayList<>();
      for (Document entity : collection(MongoSupport.OBJECTS_COLLECTION)
          .find(Filters.eq("scope_path", scope.render()))
          .projection(Projections.include("entity_id")).sort(Sorts.ascending("entity_id"))) {
        ids.add(entity.getLong("entity_id"));
      }
      return List.copyOf(ids);
    } catch (MongoException failure) {
      throw MongoSupport.mapMongoException(failure, "list");
    }
  }

  @Override public synchronized List<ScopePath> listScopes() throws StorageException {
    checkOpen();
    try {
      // Global enumeration must see only this lane's schema: the same
      // database can hold another implementation's documents (the Rust lane
      // keys entities by a compound _id with no top-level scope_path), and
      // distinct() yields null for documents missing the field, which the
      // TreeSet rejects. Restricting to string-typed scope_path documents
      // mirrors the S3 foreign-namespace rule: physical layouts are
      // lane-local, so per-scope operations stay strict while global
      // enumeration skips what cannot be ours.
      TreeSet<String> paths = new TreeSet<>();
      for (String path : collection(MongoSupport.OBJECTS_COLLECTION)
          .distinct("scope_path", Filters.type("scope_path", BsonType.STRING), String.class)) {
        paths.add(path);
      }
      for (String path : collection(MongoSupport.SEALS_COLLECTION)
          .distinct("scope_path", Filters.type("scope_path", BsonType.STRING), String.class)) {
        paths.add(path);
      }
      List<ScopePath> scopes = new ArrayList<>();
      for (String path : paths) scopes.add(ScopePath.parse(path));
      return List.copyOf(scopes);
    } catch (MongoException failure) {
      throw MongoSupport.mapMongoException(failure, "listScopes");
    }
  }

  @Override public synchronized void retire(ScopePath scope, long retentionCursor) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    if (retentionCursor < 0) throw new IllegalArgumentException("retention cursor must be non-negative");
    checkOpen();
    String scopePath = scope.render();
    try {
      if (seal(scopePath) != null) {
        // Retirement is scope-local: child scopes are separate documents and survive.
        collection(MongoSupport.OBJECTS_COLLECTION).deleteMany(Filters.eq("scope_path", scopePath));
        collection(MongoSupport.SEALS_COLLECTION).deleteMany(Filters.eq("scope_path", scopePath));
        return;
      }
      long count = collection(MongoSupport.OBJECTS_COLLECTION)
          .countDocuments(Filters.eq("scope_path", scopePath));
      if (count == 0) {
        throw StorageException.notFound("scope " + scopePath + " holds nothing to retire");
      }
      collection(MongoSupport.OBJECTS_COLLECTION).deleteMany(Filters.and(
          Filters.eq("scope_path", scopePath), Filters.lte("entity_id", retentionCursor)));
    } catch (MongoException failure) {
      throw MongoSupport.mapMongoException(failure, "retire");
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
      throw StorageException.failed("mongo store close failed", failure);
    } finally {
      MongoSupport.release(identity);
    }
  }

  private boolean sealed(String scopePath) {
    return seal(scopePath) != null;
  }

  private Document seal(String scopePath) {
    return collection(MongoSupport.SEALS_COLLECTION).find(Filters.eq("scope_path", scopePath)).first();
  }

  private byte[] storedPayload(String scopePath, long entityId) throws StorageException {
    Document entity = collection(MongoSupport.OBJECTS_COLLECTION).find(Filters.and(
        Filters.eq("scope_path", scopePath),
        Filters.eq("bucket", StoreLayout.fanoutBucket(entityId)),
        Filters.eq("entity_id", entityId))).projection(Projections.include("payload")).first();
    if (entity == null) throw StorageException.failed("put vanished before verification", null);
    return binary(entity.get("payload"));
  }

  private MongoCollection<Document> collection(String name) {
    return database.getCollection(name);
  }

  private void checkOpen() throws StorageException {
    if (closed) throw StorageException.failed("object store is closed", null);
  }

  private static byte[] binary(Object value) {
    return ((Binary) value).getData();
  }

  private static void validateEntityId(long entityId) {
    if (entityId < 0 || entityId > StoreLayout.MAX_ENTITY_ID) {
      throw new IllegalArgumentException("entity identifier is not unsigned 32-bit");
    }
  }

  private static byte[] validateDigest(byte[] digest) {
    Objects.requireNonNull(digest, "digest");
    if (digest.length != StoreLayout.DIGEST_BYTES) {
      throw new IllegalArgumentException("digest must be " + StoreLayout.DIGEST_BYTES + " octets");
    }
    return digest;
  }
}
