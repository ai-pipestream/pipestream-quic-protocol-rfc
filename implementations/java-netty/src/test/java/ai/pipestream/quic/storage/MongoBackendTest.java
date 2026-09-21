package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Mongo-backend specifics on a real MongoDB server: container stop/start
 * persistence for both roles, cursor continuation across reopen, second-opener
 * behavior, and database isolation between stores. See {@link MongoDocker}
 * for the fixture lifecycle and the docker requirement.
 */
final class MongoBackendTest {
  private static final String DATABASE = "backend-" + UUID.randomUUID().toString().replace("-", "");
  private static MongoDocker mongo;

  @BeforeAll static void startMongo() {
    mongo = MongoDocker.shared();
  }

  @AfterAll static void stopMongo() {
    MongoDocker.releaseShared();
  }

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static byte[] digest(byte[] bytes) {
    return StoreLayout.sha256().digest(bytes);
  }

  @Test void contentSurvivesContainerStopAndStart() throws Exception {
    mongo.clearDatabase(DATABASE);
    var scope = ScopePath.of(0x1111, 0x2222);
    byte[] payload = bytes("mongo-survives-restart");
    try (ObjectStore store = MongoObjectStore.open(mongo.connectionString(), DATABASE);
         Journal journal = MongoJournal.open(mongo.connectionString(), DATABASE)) {
      store.put(scope, 3, payload, digest(payload));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(3, digest(payload)))), 1);
      journal.appendIntent(new Journal.Intent("durability", bytes("admitted")));
      journal.commit("durability", bytes("committed"));
      mongo.stopServer();
    }
    mongo.startServer();
    try (ObjectStore store = MongoObjectStore.open(mongo.connectionString(), DATABASE);
         Journal journal = MongoJournal.open(mongo.connectionString(), DATABASE)) {
      ObjectStore.SealRecord seal = store.verifyScope(scope);
      assertEquals(1, seal.entityCount());
      try (var stream = store.openRead(scope, 3)) {
        assertArrayEquals(payload, stream.readAllBytes());
      }
      List<Journal.ReplayEntry> replayed = journal.replay(0);
      assertEquals(1, replayed.size());
      assertEquals("durability", replayed.get(0).intent().intentId());
      assertArrayEquals(bytes("committed"), replayed.get(0).commitRecord());
      assertEquals(3, journal.appendIntent(new Journal.Intent("continues", bytes("next"))),
          "cursors continue across reopen");
    }
  }

  @Test void secondOpenerIsRefusedUntilClose() throws Exception {
    mongo.clearDatabase(DATABASE);
    ObjectStore store = MongoObjectStore.open(mongo.connectionString(), DATABASE);
    assertStorage(StorageException.Kind.FAILED,
        () -> MongoObjectStore.open(mongo.connectionString(), DATABASE));
    store.close();
    try (ObjectStore second = MongoObjectStore.open(mongo.connectionString(), DATABASE)) {
      assertEquals(List.of(), second.list(ScopePath.root(0x3333)));
    }
    Journal journal = MongoJournal.open(mongo.connectionString(), DATABASE);
    assertStorage(StorageException.Kind.FAILED,
        () -> MongoJournal.open(mongo.connectionString(), DATABASE));
    journal.close();
    try (Journal second = MongoJournal.open(mongo.connectionString(), DATABASE)) {
      assertEquals(List.of(), second.replay(0));
    }
  }

  @Test void databasesIsolateStoresOnOneServer() throws Exception {
    String leftDb = DATABASE + "-a";
    String rightDb = DATABASE + "-b";
    var scope = ScopePath.root(0x4444);
    byte[] fromA = bytes("db-a"), fromB = bytes("db-b");
    try (ObjectStore left = MongoObjectStore.open(mongo.connectionString(), leftDb);
         ObjectStore right = MongoObjectStore.open(mongo.connectionString(), rightDb)) {
      left.put(scope, 1, fromA, digest(fromA));
      assertEquals(List.of(), right.list(scope), "other database sees nothing");
      assertStorage(StorageException.Kind.NOT_FOUND, () -> right.openRead(scope, 1));
      right.put(scope, 1, fromB, digest(fromB));
      try (var stream = left.openRead(scope, 1)) {
        assertArrayEquals(fromA, stream.readAllBytes());
      }
      try (var stream = right.openRead(scope, 1)) {
        assertArrayEquals(fromB, stream.readAllBytes());
      }
    }
  }
}
