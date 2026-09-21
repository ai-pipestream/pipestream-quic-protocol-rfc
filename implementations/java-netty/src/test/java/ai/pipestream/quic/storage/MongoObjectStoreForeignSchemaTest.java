package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import java.util.List;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The certification matrix points both subjects at one shared Mongo database,
 * and the Rust lane's schema differs from ours: entities key on a compound
 * {@code _id} of {@code {s: scopePath, e: entityId}} with no top-level
 * {@code scope_path}, and seals key on the scope path alone. This test seeds
 * both collections with those Rust-shaped documents and pins that the Java
 * store's global enumeration sees only its own schema: {@code distinct} yields
 * null for documents missing the field, which used to kill listing with a
 * NullPointerException and refused every open of a database the Rust lane had
 * touched. See {@link MongoDocker} for the fixture lifecycle and the docker
 * requirement.
 */
final class MongoObjectStoreForeignSchemaTest {
  private static MongoDocker mongo;

  @BeforeAll static void startMongo() {
    mongo = MongoDocker.shared();
  }

  @AfterAll static void stopMongo() {
    MongoDocker.releaseShared();
  }

  @Test void listScopesSkipsRustSchemaDocuments() throws Exception {
    String database = "store-" + UUID.randomUUID().toString().replace("-", "");
    var javaScope = ScopePath.root(0x51);
    byte[] payload = "java-payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    try (var client = MongoClients.create(mongo.connectionString())) {
      var objects = client.getDatabase(database).getCollection(MongoSupport.OBJECTS_COLLECTION);
      var seals = client.getDatabase(database).getCollection(MongoSupport.SEALS_COLLECTION);
      // Rust lane shapes: compound _id, digest/count under short keys, no scope_path.
      objects.insertOne(new Document(
          "_id", new Document("s", "0").append("e", 1L))
          .append("d", new byte[StoreLayout.DIGEST_BYTES])
          .append("p", new byte[] {(byte) 0xab}));
      objects.insertOne(new Document(
          "_id", new Document("s", "1/2").append("e", 9L))
          .append("d", new byte[StoreLayout.DIGEST_BYTES])
          .append("p", new byte[] {(byte) 0xcd}));
      seals.insertOne(new Document("_id", "0")
          .append("d", new byte[StoreLayout.DIGEST_BYTES])
          .append("n", 1L));
    }
    try (ObjectStore store = MongoObjectStore.open(mongo.connectionString(), database)) {
      store.put(javaScope, 7, payload, StoreLayout.sha256().digest(payload));
      assertEquals(List.of(javaScope), store.listScopes(),
          "only Java-schema scopes enumerate; the Rust documents neither throw nor appear");
      assertEquals(List.of(7L), store.list(javaScope), "per-scope operations stay strict");
    }
    try (var client = MongoClients.create(mongo.connectionString())) {
      assertEquals(2, client.getDatabase(database).getCollection(MongoSupport.OBJECTS_COLLECTION)
              .countDocuments(Filters.exists("_id.s")),
          "Rust entity documents coexist untouched");
      assertEquals(1, client.getDatabase(database).getCollection(MongoSupport.SEALS_COLLECTION)
              .countDocuments(Filters.eq("_id", "0")),
          "Rust seal document coexists untouched");
    }
  }
}
