package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import ai.pipestream.quic.SealedSessionStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(10)
final class SessionStoreTest {
  private static final Records.Limits LIMITS = new Records.Limits(8, 64, 256, 1 << 20, 1 << 20, 4);
  private static final Records.Policy MAXIMUM = new Records.Policy(60_000, 120_000, 180_000);
  private static final Records.Policy POLICY = new Records.Policy(10_000, 20_000, 30_000);

  @TempDir Path directory;

  @Test
  void replayAndAttachRetainImmutableBindingWithFreshCorrelation() throws Exception {
    Path path = database("replay");
    SessionStore store = SessionStore.initialize(path, configuration(8, 8, 4));
    SessionStore.Access alice = access("alice");

    assertEquals(
        new Messages.Sequence(1, 1),
        store.nextSequence(alice, durable(8192, 1 << 20), new Messages.NextSequence(1)));
    Messages.Binding created =
        store.create(alice, durable(8192, 1 << 20), new Messages.Create(2, 1, POLICY));
    assertEquals(2, created.request());
    assertEquals("issuer-a", created.authority());
    assertEquals("alice", created.owner());
    assertEquals(1, created.generation());
    assertEquals(LIMITS, created.limits());

    Messages.Binding replay =
        store.create(alice, durable(8192, 1 << 20), new Messages.Create(9, 1, POLICY));
    assertEquals(created, withRequest(replay, 2));
    assertEquals(9, replay.request());
    Messages.Binding attached =
        store.attach(
            alice, durable(8192, 512 << 10), new Messages.Attach(10, "issuer-a", "alice", 1));
    assertEquals(created, withRequest(attached, 2));

    assertCode(
        ProtocolError.Code.EXTENSION_UNSUPPORTED,
        () ->
            store.attach(
                alice,
                durableWithResults(8192, 1 << 20),
                new Messages.Attach(11, "issuer-a", "alice", 1)));

    Path resultPath = database("result-replay");
    SessionStore resultStore = SessionStore.initialize(resultPath, configuration(8, 8, 4));
    resultStore.create(alice, durableWithResults(8192, 1 << 20), new Messages.Create(1, 1, POLICY));
    assertCode(
        ProtocolError.Code.EXTENSION_UNSUPPORTED,
        () -> resultStore.create(alice, durable(8192, 1 << 20), new Messages.Create(2, 1, POLICY)));
    assertCode(
        ProtocolError.Code.CONFLICT,
        () ->
            store.create(
                alice,
                durable(8192, 1 << 20),
                new Messages.Create(12, 1, new Records.Policy(9_000, 20_000, 30_000))));
    assertCode(
        ProtocolError.Code.CONFLICT,
        () -> store.create(alice, durable(8192, 1 << 20), new Messages.Create(13, 3, POLICY)));
    assertCode(
        ProtocolError.Code.LIMIT_EXCEEDED,
        () ->
            store.attach(
                alice, durable(4096, 1 << 20), new Messages.Attach(14, "issuer-a", "alice", 1)));
  }

  @Test
  void authorizationPrecedesExistenceAndConfigurationDisclosure() throws Exception {
    SessionStore store = SessionStore.initialize(database("authorization"), configuration(8, 8, 4));
    SessionStore.Access alice = access("alice");
    store.create(alice, durable(8192, 1 << 20), new Messages.Create(1, 1, POLICY));

    SessionStore.Access denied =
        new SessionStore.Access(
            "mallory",
            () -> {
              throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "revoked");
            });
    assertCode(
        ProtocolError.Code.UNAUTHORIZED,
        () -> store.attach(denied, core(), new Messages.Attach(2, "wrong", "mallory", 999)));
    assertCode(
        ProtocolError.Code.UNAUTHORIZED,
        () ->
            store.attach(
                access("mallory"),
                durable(8192, 1 << 20),
                new Messages.Attach(3, "issuer-a", "alice", 1)));
    assertCode(
        ProtocolError.Code.NOT_FOUND,
        () ->
            store.attach(
                alice, durable(8192, 1 << 20), new Messages.Attach(4, "issuer-a", "alice", 99)));
    assertCode(
        ProtocolError.Code.CONFLICT,
        () ->
            store.attach(
                alice, durable(8192, 1 << 20), new Messages.Attach(5, "issuer-b", "alice", 1)));
    assertCode(
        ProtocolError.Code.EXTENSION_UNSUPPORTED,
        () -> store.nextSequence(alice, core(), new Messages.NextSequence(6)));
  }

  @Test
  void policyAndCapacityRefusalsDoNotAdvanceEitherAllocator() throws Exception {
    Path path = database("capacity");
    SessionStore store = SessionStore.initialize(path, configuration(1, 1, 1));
    SessionStore.Access alice = access("alice");
    assertCode(
        ProtocolError.Code.LIMIT_EXCEEDED,
        () ->
            store.create(
                alice,
                durable(8192, 1 << 20),
                new Messages.Create(1, 1, new Records.Policy(60_001, 1, 1))));
    assertEquals(
        1,
        store
            .nextSequence(alice, durable(8192, 1 << 20), new Messages.NextSequence(2))
            .nextCreationSequence());

    Messages.Binding first =
        store.create(alice, durable(8192, 1 << 20), new Messages.Create(3, 1, POLICY));
    assertEquals(1, first.generation());
    assertCode(
        ProtocolError.Code.LIMIT_EXCEEDED,
        () ->
            store.create(access("bob"), durable(8192, 1 << 20), new Messages.Create(4, 1, POLICY)));
    assertEquals(
        1,
        store
            .nextSequence(access("bob"), durable(8192, 1 << 20), new Messages.NextSequence(5))
            .nextCreationSequence());
    assertCode(
        ProtocolError.Code.NOT_FOUND,
        () ->
            store.attach(
                alice, durable(8192, 1 << 20), new Messages.Attach(6, "issuer-a", "alice", 2)));

    SessionStore perOwner = SessionStore.initialize(database("per-owner"), configuration(8, 8, 1));
    perOwner.create(alice, durable(8192, 1 << 20), new Messages.Create(7, 1, POLICY));
    assertCode(
        ProtocolError.Code.LIMIT_EXCEEDED,
        () -> perOwner.create(alice, durable(8192, 1 << 20), new Messages.Create(8, 2, POLICY)));
    assertEquals(
        2,
        perOwner
            .nextSequence(alice, durable(8192, 1 << 20), new Messages.NextSequence(9))
            .nextCreationSequence());

    SessionStore owners = SessionStore.initialize(database("owners"), configuration(1, 8, 4));
    owners.create(alice, durable(8192, 1 << 20), new Messages.Create(10, 1, POLICY));
    assertCode(
        ProtocolError.Code.LIMIT_EXCEEDED,
        () ->
            owners.create(
                access("bob"), durable(8192, 1 << 20), new Messages.Create(11, 1, POLICY)));
    assertEquals(
        1,
        owners
            .nextSequence(access("bob"), durable(8192, 1 << 20), new Messages.NextSequence(12))
            .nextCreationSequence());
  }

  @Test
  void concurrentDuplicateCreationCommitsOneGenerationAndOneSequence() throws Exception {
    Path path = database("concurrent");
    SessionStore first = SessionStore.initialize(path, configuration(8, 8, 4));
    SessionStore second = SessionStore.open(path, configuration(8, 8, 4));
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(2)) {
      var one = executor.submit(() -> createTogether(first, ready, start, 1));
      var two = executor.submit(() -> createTogether(second, ready, start, 2));
      assertTrue(ready.await(2, TimeUnit.SECONDS));
      start.countDown();
      Messages.Binding a = one.get(5, TimeUnit.SECONDS);
      Messages.Binding b = two.get(5, TimeUnit.SECONDS);
      assertEquals(a.generation(), b.generation());
      assertEquals(a.creationSequence(), b.creationSequence());
      assertNotEquals(a.request(), b.request());
      assertEquals(
          2,
          first
              .nextSequence(access("alice"), durable(8192, 1 << 20), new Messages.NextSequence(3))
              .nextCreationSequence());
    }
  }

  @Test
  void authorizationFailureImmediatelyBeforeCommitRollsBack() throws Exception {
    SessionStore store =
        SessionStore.initialize(database("authorization-rollback"), configuration(8, 8, 4));
    AtomicInteger checks = new AtomicInteger();
    SessionStore.Access revokedAtCommit =
        new SessionStore.Access(
            "alice",
            () -> {
              if (checks.incrementAndGet() == 3)
                throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "revoked before commit");
            });
    assertCode(
        ProtocolError.Code.UNAUTHORIZED,
        () ->
            store.create(
                revokedAtCommit, durable(8192, 1 << 20), new Messages.Create(1, 1, POLICY)));
    assertEquals(3, checks.get());
    assertEquals(
        1,
        store
            .nextSequence(access("alice"), durable(8192, 1 << 20), new Messages.NextSequence(2))
            .nextCreationSequence());
    Messages.Binding retry =
        store.create(access("alice"), durable(8192, 1 << 20), new Messages.Create(3, 1, POLICY));
    assertEquals(1, retry.generation());
  }

  @Test
  void rootInsertionFailureRollsBackSessionAndBothHighWaterMarks() throws Exception {
    Path path = database("sql-rollback");
    SessionStore.Configuration config = configuration(8, 8, 4);
    SessionStore store = SessionStore.initialize(path, config);
    try (var connection = BoundedSqlite.open(path, config.files()).connect();
        var statement = connection.createStatement()) {
      statement.execute(
          "CREATE TRIGGER fail_root BEFORE INSERT ON ps_v2_scopes BEGIN SELECT RAISE(ABORT,'test"
              + " root commit interruption'); END");
    }
    SQLException failure =
        assertThrows(
            SQLException.class,
            () ->
                store.create(
                    access("alice"), durable(8192, 1 << 20), new Messages.Create(1, 1, POLICY)));
    assertTrue(failure.getMessage().contains("test root commit interruption"), failure::toString);
    try (var connection = BoundedSqlite.open(path, config.files()).connect();
        var statement = connection.createStatement()) {
      statement.execute("DROP TRIGGER fail_root");
    }
    assertEquals(
        1,
        store
            .nextSequence(access("alice"), durable(8192, 1 << 20), new Messages.NextSequence(2))
            .nextCreationSequence());
    Messages.Binding retry =
        store.create(access("alice"), durable(8192, 1 << 20), new Messages.Create(3, 1, POLICY));
    assertEquals(1, retry.generation());
    assertEquals(
        1,
        SessionStore.open(path, config)
            .attach(
                access("alice"),
                durable(8192, 1 << 20),
                new Messages.Attach(4, "issuer-a", "alice", 1))
            .generation());
  }

  @Test
  void reopenRequiresExactConfigurationAndRefusesForeignOrV1Schema() throws Exception {
    Path path = database("reopen");
    SessionStore.Configuration config = configuration(8, 8, 4);
    SessionStore store = SessionStore.initialize(path, config);
    store.create(access("alice"), durable(8192, 1 << 20), new Messages.Create(1, 1, POLICY));
    assertEquals(
        1,
        SessionStore.open(path, config)
            .attach(
                access("alice"),
                durable(8192, 1 << 20),
                new Messages.Attach(2, "issuer-a", "alice", 1))
            .generation());
    assertThrows(SQLException.class, () -> SessionStore.open(path, configuration(8, 7, 4)));

    Path foreign = database("foreign");
    try (var connection = BoundedSqlite.open(foreign, BoundedSqlite.Limits.defaults()).connect();
        var statement = connection.createStatement()) {
      statement.execute("CREATE TABLE sessions(id INTEGER PRIMARY KEY)");
    }
    assertThrows(SQLException.class, () -> SessionStore.open(foreign, config));

    Path v1 = database("v1");
    SealedSessionStore.open(v1);
    assertThrows(SQLException.class, () -> SessionStore.open(v1, config));
    assertNotNull(SealedSessionStore.open(v1));
  }

  @Test
  void initializationIsExclusiveAndRecoveryNeverCreatesMissingOrTruncatedState() throws Exception {
    SessionStore.Configuration config = configuration(8, 8, 4);
    Path absent = database("absent");
    assertThrows(IOException.class, () -> SessionStore.open(absent, config));

    Path empty = database("empty");
    Files.createFile(empty);
    assertThrows(IOException.class, () -> SessionStore.open(empty, config));

    Path initialized = database("initialized");
    SessionStore store = SessionStore.initialize(initialized, config);
    store.create(access("alice"), durable(8192, 1 << 20), new Messages.Create(1, 1, POLICY));
    assertThrows(IOException.class, () -> SessionStore.initialize(initialized, config));
    assertEquals(
        2,
        SessionStore.open(initialized, config)
            .nextSequence(access("alice"), durable(8192, 1 << 20), new Messages.NextSequence(2))
            .nextCreationSequence());

    Path bare = database("bare");
    Files.write(bare, new byte[] {0x53, 0x51, 0x4c});
    IOException bareFailure =
        assertThrows(IOException.class, () -> SessionStore.open(bare, config));
    assertTrue(
        bareFailure.getMessage().contains("nonempty JDBC store lacks file policy"),
        bareFailure::toString);

    Path truncated = database("truncated");
    SessionStore.initialize(truncated, config);
    Files.write(truncated, new byte[] {0x53, 0x51, 0x4c});
    assertThrows(SQLException.class, () -> SessionStore.open(truncated, config));
  }

  @Test
  void exhaustedOwnerAndAuthorityAllocatorsRefuseWithoutWraparound() throws Exception {
    SessionStore.Configuration config = configuration(8, 8, 4);

    Path ownerPath = database("owner-exhaustion");
    SessionStore ownerStore = SessionStore.initialize(ownerPath, config);
    ownerStore.create(access("alice"), durable(8192, 1 << 20), new Messages.Create(1, 1, POLICY));
    try (var connection = BoundedSqlite.open(ownerPath, config.files()).connect();
        var statement = connection.createStatement()) {
      assertEquals(
          1,
          statement.executeUpdate(
              "UPDATE ps_v2_owners SET high_water=9223372036854775807 WHERE owner='alice'"));
    }
    assertCode(
        ProtocolError.Code.LIMIT_EXCEEDED,
        () ->
            ownerStore.nextSequence(
                access("alice"), durable(8192, 1 << 20), new Messages.NextSequence(2)));

    Path authorityPath = database("authority-exhaustion");
    SessionStore authorityStore = SessionStore.initialize(authorityPath, config);
    try (var connection = BoundedSqlite.open(authorityPath, config.files()).connect();
        var statement = connection.createStatement()) {
      assertEquals(
          1,
          statement.executeUpdate(
              "UPDATE ps_v2_meta SET high_water=9223372036854775807 WHERE singleton=1"));
    }
    assertCode(
        ProtocolError.Code.LIMIT_EXCEEDED,
        () ->
            authorityStore.create(
                access("bob"), durable(8192, 1 << 20), new Messages.Create(3, 1, POLICY)));
    assertEquals(
        1,
        authorityStore
            .nextSequence(access("bob"), durable(8192, 1 << 20), new Messages.NextSequence(4))
            .nextCreationSequence());
  }

  @Test
  void physicalDatabaseLimitRefusesAtomicallyWithoutEvictingCommittedHistory() throws Exception {
    Path path = database("physical-full");
    BoundedSqlite.Limits files = new BoundedSqlite.Limits(128 << 10, 64L << 20, 65_536, 512 << 10);
    SessionStore.Configuration config =
        new SessionStore.Configuration("issuer-a", LIMITS, MAXIMUM, 1, 1024, 1024, files);
    SessionStore store = SessionStore.initialize(path, config);
    long initialBytes = Files.size(path);
    long initialPages;
    try (var connection = BoundedSqlite.open(path, files).connect()) {
      initialPages = scalar(connection, "PRAGMA page_count");
    }

    int committed = 0;
    ProtocolError refusal = null;
    for (int sequence = 1; sequence <= 1024; sequence++) {
      try {
        store.create(
            access("alice"),
            durable(8192, 1 << 20),
            new Messages.Create(sequence, sequence, POLICY));
        committed = sequence;
      } catch (ProtocolError failure) {
        refusal = failure;
        break;
      }
    }
    assertTrue(committed > 0);
    System.out.printf(
        "session physical-full initialPages=%d initialBytes=%d committed=%d%n",
        initialPages, initialBytes, committed);
    assertNotNull(refusal, "physical file policy did not exhaust within bounded attempts");
    assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, refusal.code(), refusal::getMessage);
    assertInstanceOf(SQLException.class, refusal.getCause());
    int refusedSequence = committed + 1;
    assertEquals(
        refusedSequence,
        store
            .nextSequence(access("alice"), durable(8192, 1 << 20), new Messages.NextSequence(2000))
            .nextCreationSequence());
    Messages.Binding replay =
        store.create(access("alice"), durable(8192, 1 << 20), new Messages.Create(2001, 1, POLICY));
    assertEquals(1, replay.generation());
    assertEquals(
        1,
        store
            .attach(
                access("alice"),
                durable(8192, 1 << 20),
                new Messages.Attach(2002, "issuer-a", "alice", 1))
            .generation());
    assertCode(
        ProtocolError.Code.NOT_FOUND,
        () ->
            store.attach(
                access("alice"),
                durable(8192, 1 << 20),
                new Messages.Attach(2003, "issuer-a", "alice", refusedSequence)));

    SessionStore reopened = SessionStore.open(path, config);
    assertEquals(
        refusedSequence,
        reopened
            .nextSequence(access("alice"), durable(8192, 1 << 20), new Messages.NextSequence(2004))
            .nextCreationSequence());
    assertTrue(Files.size(path) <= 128 << 10);
  }

  @Test
  void corruptReceiptIsHiddenFromAnotherOwnerAndRejectedForItsOwner() throws Exception {
    Path path = database("corrupt-receipt");
    SessionStore.Configuration config = configuration(8, 8, 4);
    SessionStore store = SessionStore.initialize(path, config);
    store.create(access("alice"), durable(8192, 1 << 20), new Messages.Create(1, 1, POLICY));
    try (var connection = BoundedSqlite.open(path, config.files()).connect();
        var statement = connection.createStatement()) {
      assertEquals(
          1,
          statement.executeUpdate(
              "UPDATE ps_v2_sessions SET receipt_hash=zeroblob(32) WHERE generation=1"));
    }
    assertCode(
        ProtocolError.Code.UNAUTHORIZED,
        () ->
            store.attach(
                access("mallory"),
                durable(8192, 1 << 20),
                new Messages.Attach(2, "issuer-a", "mallory", 1)));
    assertThrows(
        SQLException.class,
        () ->
            store.attach(
                access("alice"),
                durable(8192, 1 << 20),
                new Messages.Attach(3, "issuer-a", "alice", 1)));
  }

  @Test
  void processDeathBeforeAndAfterCommitPreservesAtomicRecovery() throws Exception {
    Path before = database("halt-before");
    assertEquals(23, runChild("halt-before", before));
    SessionStore beforeStore = SessionStore.open(before, configuration(8, 8, 4));
    assertEquals(
        1,
        beforeStore
            .nextSequence(access("alice"), durable(8192, 1 << 20), new Messages.NextSequence(1))
            .nextCreationSequence());
    assertCode(
        ProtocolError.Code.NOT_FOUND,
        () ->
            beforeStore.attach(
                access("alice"),
                durable(8192, 1 << 20),
                new Messages.Attach(2, "issuer-a", "alice", 1)));

    Path after = database("halt-after");
    assertEquals(24, runChild("halt-after", after));
    SessionStore afterStore = SessionStore.open(after, configuration(8, 8, 4));
    Messages.Binding replay =
        afterStore.create(
            access("alice"), durable(8192, 1 << 20), new Messages.Create(3, 1, POLICY));
    assertEquals(1, replay.generation());
    assertEquals(
        2,
        afterStore
            .nextSequence(access("alice"), durable(8192, 1 << 20), new Messages.NextSequence(4))
            .nextCreationSequence());
  }

  public static void main(String[] args) throws Exception {
    Path path = Path.of(args[1]);
    SessionStore store = SessionStore.initialize(path, configuration(8, 8, 4));
    if (args[0].equals("halt-before")) {
      AtomicInteger checks = new AtomicInteger();
      SessionStore.Access access =
          new SessionStore.Access(
              "alice",
              () -> {
                if (checks.incrementAndGet() == 3) Runtime.getRuntime().halt(23);
              });
      store.create(access, durable(8192, 1 << 20), new Messages.Create(1, 1, POLICY));
      throw new AssertionError("pre-commit halt did not run");
    }
    if (args[0].equals("halt-after")) {
      store.create(access("alice"), durable(8192, 1 << 20), new Messages.Create(1, 1, POLICY));
      Runtime.getRuntime().halt(24);
    }
    throw new IllegalArgumentException(args[0]);
  }

  private Path database(String name) {
    return directory.resolve(name + ".sqlite");
  }

  private int runChild(String mode, Path path) throws Exception {
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                SessionStoreTest.class.getName(),
                mode,
                path.toString())
            .redirectError(directory.resolve(mode + ".err").toFile())
            .redirectOutput(directory.resolve(mode + ".out").toFile())
            .start();
    try {
      assertTrue(process.waitFor(5, TimeUnit.SECONDS), "child process did not terminate");
      return process.exitValue();
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS), "owned child resisted forced cleanup");
      }
    }
  }

  private static SessionStore.Configuration configuration(int owners, int sessions, int perOwner) {
    return new SessionStore.Configuration(
        "issuer-a", LIMITS, MAXIMUM, owners, sessions, perOwner, BoundedSqlite.Limits.defaults());
  }

  private static SessionStore.Access access(String owner) {
    return new SessionStore.Access(owner, () -> {});
  }

  private static Messages.Capabilities durable(int controlLimit, long objectLimit) {
    return selected(List.of(DURABLE_WORK), controlLimit, objectLimit);
  }

  private static Messages.Capabilities durableWithResults(int controlLimit, long objectLimit) {
    return selected(List.of(DURABLE_WORK, RESULT_DELIVERY), controlLimit, objectLimit);
  }

  private static Messages.Capabilities core() {
    return selected(List.of(), 8192, 1 << 20);
  }

  private static Messages.Capabilities selected(
      List<Integer> profiles, int controlLimit, long objectLimit) {
    return new Messages.Capabilities(
        true, profiles, List.of(), controlLimit, 4, 16, objectLimit, 1_000, 5_000);
  }

  private static Messages.Binding withRequest(Messages.Binding binding, long request) {
    return new Messages.Binding(
        request,
        binding.authority(),
        binding.owner(),
        binding.generation(),
        binding.creationSequence(),
        binding.policy(),
        binding.limits());
  }

  private static long scalar(java.sql.Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement();
        var rows = statement.executeQuery(sql)) {
      assertTrue(rows.next());
      return rows.getLong(1);
    }
  }

  private static Messages.Binding createTogether(
      SessionStore store, CountDownLatch ready, CountDownLatch start, long request)
      throws Exception {
    ready.countDown();
    assertTrue(start.await(2, TimeUnit.SECONDS));
    return store.create(
        access("alice"), durable(8192, 1 << 20), new Messages.Create(request, 1, POLICY));
  }

  private static ProtocolError assertCode(ProtocolError.Code code, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(code, error.code(), error::getMessage);
    return error;
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
