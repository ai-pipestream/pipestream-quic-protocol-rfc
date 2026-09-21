package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Payload chunking for the Mongo backend. One BSON document may not exceed 16
 * MiB, so payloads at or above {@link MongoObjectStore#CHUNK_BYTES} are stored
 * as 1 MiB documents in the {@code chunks} collection with the entity document
 * carrying the digest and a {@code chunked} marker, mirroring the Rust lane.
 * These tests pin the round trip above the old document limit, the exact
 * chunk-threshold boundaries, and that retirement leaves no orphan chunk
 * documents. See {@link MongoDocker} for the fixture lifecycle and the docker
 * requirement.
 */
final class MongoObjectStoreChunkingTest {
  private static MongoDocker mongo;

  @BeforeAll static void startMongo() {
    mongo = MongoDocker.shared();
  }

  @AfterAll static void stopMongo() {
    MongoDocker.releaseShared();
  }

  @Test void largePayloadRoundTripsThroughChunksAndLeavesNoOrphans() throws Exception {
    String database = freshDatabase();
    var scope = ScopePath.root(0x1);
    byte[] payload = patterned(16 * MongoObjectStore.CHUNK_BYTES + 7);
    byte[] digest = StoreLayout.sha256().digest(payload);
    byte[] scopeDigest = StoreLayout.scopeDigest(
        List.of(new StoreLayout.EntityDigest(1, digest)));
    try (ObjectStore store = MongoObjectStore.open(mongo.connectionString(), database)) {
      store.put(scope, 1, payload, digest);
      try (InputStream read = store.openRead(scope, 1)) {
        assertArrayEquals(payload, read.readAllBytes(), "chunked read streams the stored bytes");
      }
      try (var client = MongoClients.create(mongo.connectionString())) {
        var objects = client.getDatabase(database).getCollection(MongoSupport.OBJECTS_COLLECTION);
        var chunks = client.getDatabase(database).getCollection(MongoSupport.CHUNKS_COLLECTION);
        Document entity = objects.find(Filters.and(
            Filters.eq("scope_path", scope.render()),
            Filters.eq("entity_id", 1L))).first();
        assertEquals(Boolean.TRUE, entity.getBoolean("chunked"), "entity carries the chunk marker");
        assertFalse(entity.containsKey("payload"), "chunked entity keeps no inline payload");
        assertEquals(17, chunks.countDocuments(), "16 full chunks plus the 7-byte tail");
      }
      store.sealScope(scope, scopeDigest, 1);
      ObjectStore.SealRecord verified = store.verifyScope(scope);
      assertEquals(1, verified.entityCount(), "verify rehashes the chunked payload chunk by chunk");
      assertArrayEquals(scopeDigest, verified.digest());
      store.retire(scope, 0);
      try (var client = MongoClients.create(mongo.connectionString())) {
        assertEquals(0, client.getDatabase(database)
                .getCollection(MongoSupport.CHUNKS_COLLECTION).countDocuments(),
            "sealed retirement deletes every chunk document");
        assertEquals(0, client.getDatabase(database)
                .getCollection(MongoSupport.OBJECTS_COLLECTION).countDocuments());
        assertEquals(0, client.getDatabase(database)
                .getCollection(MongoSupport.SEALS_COLLECTION).countDocuments());
      }
      assertEquals(List.of(), store.listScopes());
    }
  }

  @Test void boundaryPayloadsStoreExactlyAtTheChunkThreshold() throws Exception {
    String database = freshDatabase();
    var scope = ScopePath.root(0x2);
    byte[][] payloads = {
        patterned(MongoObjectStore.CHUNK_BYTES - 1),
        patterned(MongoObjectStore.CHUNK_BYTES),
        patterned(MongoObjectStore.CHUNK_BYTES + 1),
    };
    try (ObjectStore store = MongoObjectStore.open(mongo.connectionString(), database)) {
      for (int i = 0; i < payloads.length; i++) {
        store.put(scope, i + 1, payloads[i], StoreLayout.sha256().digest(payloads[i]));
      }
      assertEquals(List.of(1L, 2L, 3L), store.list(scope));
      for (int i = 0; i < payloads.length; i++) {
        try (InputStream read = store.openRead(scope, i + 1)) {
          assertArrayEquals(payloads[i], read.readAllBytes(), "entity " + (i + 1));
        }
      }
      try (var client = MongoClients.create(mongo.connectionString())) {
        var objects = client.getDatabase(database).getCollection(MongoSupport.OBJECTS_COLLECTION);
        var chunks = client.getDatabase(database).getCollection(MongoSupport.CHUNKS_COLLECTION);
        String rendered = scope.render();
        Document below = objects.find(new Document("scope_path", rendered).append("entity_id", 1L)).first();
        assertFalse(below.getBoolean("chunked", false), "one byte below the threshold stays inline");
        assertTrue(below.containsKey("payload"));
        Document at = objects.find(new Document("scope_path", rendered).append("entity_id", 2L)).first();
        assertEquals(Boolean.TRUE, at.getBoolean("chunked"), "exactly at the threshold chunks");
        assertEquals(1, chunks.countDocuments(new Document("entity_id", 2L)), "one full chunk");
        Document above = objects.find(new Document("scope_path", rendered).append("entity_id", 3L)).first();
        assertEquals(Boolean.TRUE, above.getBoolean("chunked"));
        assertEquals(2, chunks.countDocuments(new Document("entity_id", 3L)), "full chunk plus tail");
      }
      // Idempotent re-put of a chunked entity reassembles and compares; a
      // conflicting rewrite is CORRUPT.
      store.put(scope, 3, payloads[2], StoreLayout.sha256().digest(payloads[2]));
      byte[] altered = payloads[2].clone();
      altered[0] ^= 1;
      byte[] alteredDigest = StoreLayout.sha256().digest(altered);
      assertStorage(StorageException.Kind.CORRUPT, () -> store.put(scope, 3, altered, alteredDigest));
      store.sealScope(scope, new byte[StoreLayout.DIGEST_BYTES], 3);
      assertEquals(3, store.verifyScope(scope).entityCount(),
          "verify spans the inline and chunked entities of one scope");
    }
  }

  @Test void cursorRetirementDeletesChunksOfRetiredEntitiesOnly() throws Exception {
    String database = freshDatabase();
    var scope = ScopePath.root(0x3);
    byte[] small = patterned(11);
    byte[] large = patterned(MongoObjectStore.CHUNK_BYTES + 5);
    try (ObjectStore store = MongoObjectStore.open(mongo.connectionString(), database)) {
      store.put(scope, 1, small, StoreLayout.sha256().digest(small));
      store.put(scope, 2, large, StoreLayout.sha256().digest(large));
      store.retire(scope, 1);
      assertEquals(List.of(2L), store.list(scope));
      try (var client = MongoClients.create(mongo.connectionString())) {
        var chunks = client.getDatabase(database).getCollection(MongoSupport.CHUNKS_COLLECTION);
        assertEquals(2, chunks.countDocuments(), "the surviving entity keeps both of its chunks");
        store.retire(scope, 2);
        assertEquals(0, chunks.countDocuments(), "cursor retirement deletes the entity's chunks too");
      }
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.retire(scope, 1));
    }
  }

  private static String freshDatabase() {
    return "store-" + UUID.randomUUID().toString().replace("-", "");
  }

  /** Deterministic payload big enough to defeat accidental compression. */
  private static byte[] patterned(int size) {
    byte[] bytes = new byte[size];
    for (int i = 0; i < size; i++) bytes[i] = (byte) (i * 31 + 7);
    return bytes;
  }
}
