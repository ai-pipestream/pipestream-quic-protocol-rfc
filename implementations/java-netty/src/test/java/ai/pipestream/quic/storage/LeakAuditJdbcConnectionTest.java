package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Leak audit for the JDBC lifecycle of the Postgres backends
 * ({@link PostgresObjectStore}, {@link PostgresJournal}) without a database:
 * a deterministic in-memory JDBC driver serves {@code jdbc:leakaudit:} URLs
 * and counts every opened and closed {@link Connection}. The audit attacks
 * the open-error paths (connection refusal, schema failure), the
 * close/idempotency path, and the dead-connection replacement path, and pins
 * the single-writer identity registry so a failed open must not poison later
 * opens. The schema-failure path closes the bootstrap connection it opened
 * (LEAK-1) so a failed open leaves nothing behind.
 */
final class LeakAuditJdbcConnectionTest {
  private static final AtomicInteger opened = new AtomicInteger();
  private static final AtomicInteger closed = new AtomicInteger();
  private static final AtomicBoolean refuseConnections = new AtomicBoolean();
  private static final AtomicBoolean failSchema = new AtomicBoolean();
  private static volatile boolean connectionsValid = true;

  @BeforeAll
  static void registerDriver() throws SQLException {
    DriverManager.registerDriver(new LeakAuditDriver());
  }

  @BeforeEach
  void resetDriver() {
    opened.set(0);
    closed.set(0);
    refuseConnections.set(false);
    failSchema.set(false);
    connectionsValid = true;
  }

  @Test void refusedConnectLeavesNothingOpenAndReleasesBothIdentities() throws Exception {
    String storeUrl = "jdbc:leakaudit:refused-store";
    String journalUrl = "jdbc:leakaudit:refused-journal";
    refuseConnections.set(true);
    assertStorage(StorageException.Kind.TRANSIENT,
        () -> PostgresObjectStore.open(storeUrl, "user", "password"));
    assertStorage(StorageException.Kind.TRANSIENT,
        () -> PostgresJournal.open(journalUrl, "user", "password"));
    assertEquals(0, opened.get(), "a refused connect must not create a connection");
    refuseConnections.set(false);
    try (ObjectStore store = PostgresObjectStore.open(storeUrl, "user", "password");
         Journal journal = PostgresJournal.open(journalUrl, "user", "password")) {
      assertNotNull(store);
      assertNotNull(journal);
      assertEquals(2, opened.get(), "both identities reopened after the refusals");
    }
    assertEquals(opened.get(), closed.get(), "every opened connection closes");
  }

  @Test void schemaFailureClosesTheBootstrapConnection_objectStore() throws Exception {
    String url = "jdbc:leakaudit:schema-store";
    failSchema.set(true);
    assertStorage(StorageException.Kind.FAILED,
        () -> PostgresObjectStore.open(url, "user", "password"));
    failSchema.set(false);
    // LEAK-1 fixed (PostgresSupport.connectAndEnsureSchema): a schema
    // failure closes the bootstrap Connection before the open fails.
    assertEquals(0, opened.get() - closed.get(),
        "schema failure closes the bootstrap connection (LEAK-1)");
    try (ObjectStore store = PostgresObjectStore.open(url, "user", "password")) {
      assertNotNull(store);
      assertEquals(2, opened.get(), "identity released despite the failed first open");
    }
    assertEquals(0, opened.get() - closed.get(), "no connection leaks");
  }

  @Test void schemaFailureClosesTheBootstrapConnection_journal() throws Exception {
    String url = "jdbc:leakaudit:schema-journal";
    failSchema.set(true);
    assertStorage(StorageException.Kind.FAILED,
        () -> PostgresJournal.open(url, "user", "password"));
    failSchema.set(false);
    // Same LEAK-1 fix as the object-store variant; the journal open path
    // shares PostgresSupport.connectAndEnsureSchema.
    assertEquals(0, opened.get() - closed.get(),
        "schema failure closes the bootstrap connection (LEAK-1)");
    try (Journal journal = PostgresJournal.open(url, "user", "password")) {
      assertNotNull(journal);
      assertEquals(2, opened.get(), "identity released despite the failed first open");
    }
    assertEquals(0, opened.get() - closed.get(), "no connection leaks");
  }

  @Test void closeIsIdempotentAndClosesEachConnectionExactlyOnce() throws Exception {
    String storeUrl = "jdbc:leakaudit:close-store";
    String journalUrl = "jdbc:leakaudit:close-journal";
    ObjectStore store = PostgresObjectStore.open(storeUrl, "user", "password");
    store.close();
    store.close();
    Journal journal = PostgresJournal.open(journalUrl, "user", "password");
    journal.close();
    journal.close();
    assertEquals(2, opened.get());
    assertEquals(2, closed.get(), "idempotent close still closes each connection once");
    try (ObjectStore reopened = PostgresObjectStore.open(storeUrl, "user", "password");
         Journal journalAgain = PostgresJournal.open(journalUrl, "user", "password")) {
      assertNotNull(reopened);
      assertNotNull(journalAgain);
      assertEquals(4, opened.get(), "identities released by close");
    }
    assertEquals(opened.get(), closed.get());
  }

  @Test void deadConnectionIsClosedBeforeItsReplacement() throws Exception {
    String url = "jdbc:leakaudit:reconnect-store";
    try (ObjectStore store = PostgresObjectStore.open(url, "user", "password")) {
      assertEquals(1, opened.get());
      connectionsValid = false;
      assertEquals(java.util.List.of(), store.list(ScopePath.root(0x0d)));
      assertEquals(2, opened.get(), "dead connection replaced lazily");
      assertEquals(1, closed.get(), "the dead connection is closed before replacement");
      connectionsValid = true;
    }
    assertEquals(opened.get(), closed.get(), "store close releases the live replacement");
  }

  /** In-memory JDBC driver: deterministic connect refusal and schema failure. */
  private static final class LeakAuditDriver implements Driver {
    @Override public Connection connect(String url, Properties info) throws SQLException {
      if (!acceptsURL(url)) return null;
      if (refuseConnections.get()) throw new SQLException("injected connect refusal", "08001");
      return newConnection(url);
    }

    @Override public boolean acceptsURL(String url) {
      return url != null && url.startsWith("jdbc:leakaudit:");
    }

    @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
      return new DriverPropertyInfo[0];
    }

    @Override public int getMajorVersion() {
      return 1;
    }

    @Override public int getMinorVersion() {
      return 0;
    }

    @Override public boolean jdbcCompliant() {
      return false;
    }

    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return Logger.getLogger("leakaudit.jdbc");
    }
  }

  private static Connection newConnection(String url) {
    opened.incrementAndGet();
    AtomicBoolean connectionClosed = new AtomicBoolean();
    return proxy(Connection.class, (method, args) -> {
      switch (method.getName()) {
        case "close":
          if (connectionClosed.compareAndSet(false, true)) closed.incrementAndGet();
          return null;
        case "isClosed":
          return connectionClosed.get();
        case "isValid":
          return connectionsValid && !connectionClosed.get();
        case "createStatement":
          return newStatement();
        case "prepareStatement":
        case "prepareCall":
          return newPreparedStatement();
        case "getAutoCommit":
          return true;
        default:
          return defaultValue(method, args, "LeakAuditConnection[" + url + "]");
      }
    });
  }

  private static Statement newStatement() {
    return proxy(Statement.class, (method, args) -> {
      switch (method.getName()) {
        case "execute":
          if (failSchema.get()) throw new SQLException("injected schema failure", "42501");
          return false;
        case "executeQuery":
          return newResultSet();
        case "executeUpdate":
          return 0;
        case "getConnection":
          return null;
        default:
          return defaultValue(method, args, "LeakAuditStatement");
      }
    });
  }

  private static java.sql.PreparedStatement newPreparedStatement() {
    return proxy(java.sql.PreparedStatement.class, (method, args) -> {
      switch (method.getName()) {
        case "execute":
          if (failSchema.get()) throw new SQLException("injected schema failure", "42501");
          return false;
        case "executeQuery":
          return newResultSet();
        case "executeUpdate":
          return 0;
        default:
          return defaultValue(method, args, "LeakAuditPreparedStatement");
      }
    });
  }

  private static ResultSet newResultSet() {
    return proxy(ResultSet.class, (method, args) -> {
      switch (method.getName()) {
        case "next":
          return false;
        case "getBytes":
          return new byte[0];
        case "getLong":
          return 0L;
        case "getInt":
          return 0;
        default:
          return defaultValue(method, args, "LeakAuditResultSet");
      }
    });
  }

  @SuppressWarnings("unchecked")
  private static <T> T proxy(Class<T> type, Body body) {
    InvocationHandler handler = (proxy, method, args) -> body.apply(method, args);
    return (T) Proxy.newProxyInstance(
        LeakAuditJdbcConnectionTest.class.getClassLoader(), new Class<?>[] {type}, handler);
  }

  private interface Body {
    Object apply(Method method, Object[] args) throws SQLException;
  }

  private static Object defaultValue(Method method, Object[] args, String label) {
    if (method.getDeclaringClass() == Object.class) {
      return switch (method.getName()) {
        case "toString" -> label;
        case "hashCode" -> label.hashCode();
        case "equals" -> args != null && args[0] != null && args[0].toString().equals(label);
        default -> null;
      };
    }
    Class<?> returns = method.getReturnType();
    if (returns == boolean.class) return false;
    if (returns == int.class || returns == short.class || returns == byte.class) return 0;
    if (returns == long.class) return 0L;
    if (returns == double.class) return 0d;
    if (returns == float.class) return 0f;
    if (returns == char.class) return (char) 0;
    return null;
  }
}
