package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Postgres-backend specifics on a real {@code postgres:16-alpine} container:
 * close/reopen persistence, server kill/resume durability, second-opener
 * behavior, and both roles sharing one database. See {@link PostgresDocker}
 * for the fixture lifecycle and the docker requirement.
 */
final class PostgresBackendTest {
  private static PostgresDocker postgres;

  @BeforeAll static void startPostgres() {
    postgres = PostgresDocker.shared();
  }

  @AfterAll static void stopPostgres() {
    PostgresDocker.releaseShared();
  }

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static byte[] digest(byte[] bytes) {
    return StoreLayout.sha256().digest(bytes);
  }

  private static void resetTables() throws Exception {
    try (Connection connection = PostgresSupport.connectAndEnsureSchema(
        postgres.url(), postgres.user(), postgres.password());
         Statement statement = connection.createStatement()) {
      statement.execute("TRUNCATE TABLE ps_object_entities, ps_object_seals");
      statement.execute("TRUNCATE TABLE ps_journal_records RESTART IDENTITY");
    }
  }

  @Test void contentSurvivesCloseAndReopen() throws Exception {
    resetTables();
    var scope = ScopePath.of(0x1111, 0x2222);
    byte[] first = bytes("pg-first"), second = bytes("pg-second");
    try (ObjectStore store = PostgresObjectStore.open(postgres.url(), postgres.user(), postgres.password())) {
      store.put(scope, 1, first, digest(first));
      store.put(scope, 2, second, digest(second));
      store.sealScope(scope, StoreLayout.scopeDigest(List.of(
          new StoreLayout.EntityDigest(1, digest(first)), new StoreLayout.EntityDigest(2, digest(second)))), 2);
    }
    try (ObjectStore reopened = PostgresObjectStore.open(postgres.url(), postgres.user(), postgres.password())) {
      reopened.verifyScope(scope);
      assertEquals(List.of(1L, 2L), reopened.list(scope));
      try (var stream = reopened.openRead(scope, 1)) {
        assertArrayEquals(first, stream.readAllBytes());
      }
    }
  }

  @Test void journalReplaySurvivesCloseAndCursorsContinue() throws Exception {
    resetTables();
    try (Journal journal = PostgresJournal.open(postgres.url(), postgres.user(), postgres.password())) {
      assertEquals(1, journal.appendIntent(new Journal.Intent("a", bytes("record-a"))));
      journal.commit("a", bytes("commit-a"));
      assertEquals(3, journal.appendIntent(new Journal.Intent("b", bytes("record-b"))));
    }
    try (Journal reopened = PostgresJournal.open(postgres.url(), postgres.user(), postgres.password())) {
      List<Journal.ReplayEntry> replayed = reopened.replay(0);
      assertEquals(List.of("a", "b"),
          replayed.stream().map(entry -> entry.intent().intentId()).toList());
      assertArrayEquals(bytes("commit-a"), replayed.get(0).commitRecord());
      assertNull(replayed.get(1).commitRecord());
      assertEquals(4, reopened.appendIntent(new Journal.Intent("c", bytes("record-c"))),
          "the identity sequence continues across reopen");
    }
  }

  @Test void contentSurvivesServerKillAndResume() throws Exception {
    resetTables();
    var scope = ScopePath.root(0x3333);
    byte[] payload = bytes("survives-kill");
    try (ObjectStore store = PostgresObjectStore.open(postgres.url(), postgres.user(), postgres.password());
         Journal journal = PostgresJournal.open(postgres.url(), postgres.user(), postgres.password())) {
      store.put(scope, 1, payload, digest(payload));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(1, digest(payload)))), 1);
      journal.appendIntent(new Journal.Intent("durability", bytes("admitted")));
      journal.commit("durability", bytes("committed"));
      postgres.stopServer();
    }
    postgres.startServer();
    try (ObjectStore store = PostgresObjectStore.open(postgres.url(), postgres.user(), postgres.password());
         Journal journal = PostgresJournal.open(postgres.url(), postgres.user(), postgres.password())) {
      store.verifyScope(scope);
      try (var stream = store.openRead(scope, 1)) {
        assertArrayEquals(payload, stream.readAllBytes());
      }
      List<Journal.ReplayEntry> replayed = journal.replay(0);
      assertEquals(1, replayed.size());
      assertEquals("durability", replayed.get(0).intent().intentId());
      assertArrayEquals(bytes("committed"), replayed.get(0).commitRecord());
    }
  }

  @Test void secondOpenerIsRefusedUntilClose() throws Exception {
    resetTables();
    ObjectStore first = PostgresObjectStore.open(postgres.url(), postgres.user(), postgres.password());
    assertStorage(StorageException.Kind.FAILED,
        () -> PostgresObjectStore.open(postgres.url(), postgres.user(), postgres.password()));
    first.close();
    try (ObjectStore second = PostgresObjectStore.open(postgres.url(), postgres.user(), postgres.password())) {
      assertEquals(List.of(), second.list(ScopePath.root(0x4444)));
    }
    Journal journal = PostgresJournal.open(postgres.url(), postgres.user(), postgres.password());
    assertStorage(StorageException.Kind.FAILED,
        () -> PostgresJournal.open(postgres.url(), postgres.user(), postgres.password()));
    journal.close();
    try (Journal second = PostgresJournal.open(postgres.url(), postgres.user(), postgres.password())) {
      assertEquals(List.of(), second.replay(0));
    }
  }

  @Test void bothRolesShareOneDatabase() throws Exception {
    resetTables();
    var scope = ScopePath.root(0x5555);
    byte[] payload = bytes("two-roles");
    try (ObjectStore store = PostgresObjectStore.open(postgres.url(), postgres.user(), postgres.password());
         Journal journal = PostgresJournal.open(postgres.url(), postgres.user(), postgres.password())) {
      store.put(scope, 1, payload, digest(payload));
      long cursor = journal.appendIntent(new Journal.Intent("two-system", bytes("record")));
      journal.commit("two-system", bytes("done"));
      assertEquals(1, cursor);
    }
  }
}
