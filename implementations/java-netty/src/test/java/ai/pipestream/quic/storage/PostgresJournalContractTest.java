package ai.pipestream.quic.storage;

import java.sql.Connection;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Certifies {@link PostgresJournal} against the journal contract on a real
 * {@code postgres:16-alpine} container (see {@link PostgresDocker} for the
 * fixture lifecycle and the docker requirement). Every test gets a fresh
 * journal with emptied tables and restarted identity, so cursors start at 1
 * exactly like the other backends' fresh stores.
 */
final class PostgresJournalContractTest extends JournalContractTest {
  private static PostgresDocker postgres;

  @BeforeAll static void startPostgres() {
    postgres = PostgresDocker.shared();
  }

  @AfterAll static void stopPostgres() {
    PostgresDocker.releaseShared();
  }

  @Override protected Journal openJournal() throws Exception {
    PostgresJournal journal =
        PostgresJournal.open(postgres.url(), postgres.user(), postgres.password());
    try (Connection connection = postgres.connect(); Statement statement = connection.createStatement()) {
      statement.execute("TRUNCATE TABLE ps_journal_records RESTART IDENTITY");
    }
    return journal;
  }
}
