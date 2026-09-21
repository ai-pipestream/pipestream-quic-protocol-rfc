package ai.pipestream.quic.storage;

import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Certifies {@link MongoObjectStore} against the object-store contract on a
 * real MongoDB server (see {@link MongoDocker} for the fixture lifecycle and
 * the docker requirement). Every test gets a fresh store on its own
 * database, so isolation needs no cleanup.
 */
final class MongoObjectStoreContractTest extends ObjectStoreContractTest {
  private static MongoDocker mongo;

  private String database;

  @BeforeAll static void startMongo() {
    mongo = MongoDocker.shared();
  }

  @AfterAll static void stopMongo() {
    MongoDocker.releaseShared();
  }

  @Override protected ObjectStore openStore() throws StorageException {
    database = "store-" + UUID.randomUUID().toString().replace("-", "");
    return MongoObjectStore.open(mongo.connectionString(), database);
  }

  @Override protected boolean supportsRecordTamper() {
    return true;
  }

  @Override protected void tamperRecord(ScopePath scope, long entityId) throws Exception {
    try (var client = com.mongodb.client.MongoClients.create(mongo.connectionString())) {
      var result = client.getDatabase(database).getCollection(MongoSupport.OBJECTS_COLLECTION)
          .updateOne(
              com.mongodb.client.model.Filters.and(
                  com.mongodb.client.model.Filters.eq("scope_path", scope.render()),
                  com.mongodb.client.model.Filters.eq("entity_id", entityId)),
              com.mongodb.client.model.Updates.set("digest", new org.bson.types.Binary(new byte[32])));
      org.junit.jupiter.api.Assertions.assertEquals(1, result.getModifiedCount());
    }
  }

  @Override protected void tamperSealCount(ScopePath scope) throws Exception {
    try (var client = com.mongodb.client.MongoClients.create(mongo.connectionString())) {
      var result = client.getDatabase(database).getCollection(MongoSupport.SEALS_COLLECTION)
          .updateOne(com.mongodb.client.model.Filters.eq("scope_path", scope.render()),
              com.mongodb.client.model.Updates.inc("entity_count", 1L));
      org.junit.jupiter.api.Assertions.assertEquals(1, result.getModifiedCount());
    }
  }
}
