package ai.pipestream.quic.storage;

import java.sql.Connection;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Certifies {@link PostgresObjectStore} against the object-store contract on a
 * real {@code postgres:16-alpine} container (see {@link PostgresDocker} for
 * the fixture lifecycle and the docker requirement). Every test gets a fresh
 * store with emptied tables.
 */
final class PostgresObjectStoreContractTest extends ObjectStoreContractTest {
  private static PostgresDocker postgres;

  @BeforeAll static void startPostgres() {
    postgres = PostgresDocker.shared();
  }

  @AfterAll static void stopPostgres() {
    PostgresDocker.releaseShared();
  }

  @Override protected ObjectStore openStore() throws Exception {
    PostgresObjectStore store =
        PostgresObjectStore.open(postgres.url(), postgres.user(), postgres.password());
    try (Connection connection = postgres.connect(); Statement statement = connection.createStatement()) {
      statement.execute("TRUNCATE TABLE ps_object_entities, ps_object_seals");
    }
    return store;
  }

  @Override protected boolean supportsRecordTamper() {
    return true;
  }

  @Override protected void tamperRecord(ScopePath scope, long entityId) throws Exception {
    try (Connection connection = postgres.connect();
         java.sql.PreparedStatement update = connection.prepareStatement(
             "UPDATE ps_object_entities SET digest = ? WHERE scope_path = ? AND entity_id = ?")) {
      update.setBytes(1, new byte[32]);
      update.setString(2, scope.render());
      update.setLong(3, entityId);
      org.junit.jupiter.api.Assertions.assertEquals(1, update.executeUpdate());
    }
  }

  @Override protected void tamperSealCount(ScopePath scope) throws Exception {
    try (Connection connection = postgres.connect();
         java.sql.PreparedStatement update = connection.prepareStatement(
             "UPDATE ps_object_seals SET entity_count = entity_count + 1 WHERE scope_path = ?")) {
      update.setString(1, scope.render());
      org.junit.jupiter.api.Assertions.assertEquals(1, update.executeUpdate());
    }
  }
}
