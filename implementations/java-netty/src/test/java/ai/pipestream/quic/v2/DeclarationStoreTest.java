package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(15)
final class DeclarationStoreTest {
  private static final Records.Policy POLICY = new Records.Policy(1000, 1000, 1000);
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65536, 4, 16, 1 << 20, 1000, 5000);
  @TempDir Path directory;

  @Test
  void declareReplayLookupPageAndImmediateSnapshotAreExact() throws Exception {
    SessionStore store = initialized("basic", 16, 16);
    Records.OperationId operation = operation(1);
    var response =
        store.declare(
            access(),
            SELECTED,
            1,
            new Messages.Declare(2, operation, 0, List.of(2L, 7L, Long.MAX_VALUE), true));
    var declared = (Records.Declared) response.receipt().outcome();
    assertEquals(3, declared.acceptedCount());
    assertEquals(3, declared.declared());
    Commitments.Seal seal =
        new Commitments.Seal(new Commitments.Context("issuer-a", "alice", 1), 0, 0, null, 3);
    for (long id : List.of(2L, 7L, Long.MAX_VALUE)) seal.add(id);
    assertEquals(seal.finish(), declared.seal());

    var replay =
        store.declare(
            access(),
            SELECTED,
            1,
            new Messages.Declare(9, operation, 0, List.of(2L, 7L, Long.MAX_VALUE), true));
    assertEquals(response.receipt(), replay.receipt());
    assertEquals(9, replay.request());
    assertEquals(
        response.receipt(),
        store
            .lookupOperation(access(), SELECTED, 1, new Messages.LookupOperation(10, operation))
            .receipt());

    var first = store.page(access(), SELECTED, 1, new Messages.Page(11, 0, 0, 2));
    assertEquals(
        List.of(
            new Messages.Entry(2, Records.State.DECLARED),
            new Messages.Entry(7, Records.State.DECLARED)),
        first.entries());
    assertTrue(first.more());
    var last = store.page(access(), SELECTED, 1, new Messages.Page(12, 0, 7, 2));
    assertEquals(
        List.of(new Messages.Entry(Long.MAX_VALUE, Records.State.DECLARED)), last.entries());
    assertFalse(last.more());
    var snapshot =
        store.snapshot(
            access(), SELECTED, 1, new Messages.Watch(13, new Records.WorkKey(0, 0, 7), 0, 0));
    assertEquals(1, snapshot.revision());
    assertEquals(Records.State.DECLARED, snapshot.work().state());
    assertEquals(0, snapshot.work().attempt());
    assertCode(
        ProtocolError.Code.CONFLICT,
        () ->
            store.snapshot(
                access(), SELECTED, 1, new Messages.Watch(14, new Records.WorkKey(0, 0, 7), 2, 0)));
  }

  @Test
  void conflictsSealingAndNotFoundDoNotMutateMembership() throws Exception {
    SessionStore store = initialized("conflicts", 16, 16);
    Records.OperationId op = operation(1);
    store.declare(access(), SELECTED, 1, new Messages.Declare(2, op, 0, List.of(1L), false));
    assertCode(
        ProtocolError.Code.CONFLICT,
        () ->
            store.declare(
                access(), SELECTED, 1, new Messages.Declare(3, op, 0, List.of(2L), false)));
    assertCode(
        ProtocolError.Code.NOT_FOUND,
        () ->
            store.lookupOperation(
                access(), SELECTED, 1, new Messages.LookupOperation(4, operation(9))));
    assertCode(
        ProtocolError.Code.NOT_FOUND,
        () -> store.page(access(), SELECTED, 1, new Messages.Page(5, 99, 0, 1)));
    assertCode(
        ProtocolError.Code.NOT_FOUND,
        () ->
            store.snapshot(
                access(), SELECTED, 1, new Messages.Watch(6, new Records.WorkKey(0, 0, 99), 0, 0)));
    store.declare(access(), SELECTED, 1, new Messages.Declare(7, operation(2), 0, List.of(), true));
    assertCode(
        ProtocolError.Code.CONFLICT,
        () ->
            store.declare(
                access(),
                SELECTED,
                1,
                new Messages.Declare(8, operation(3), 0, List.of(2L), false)));
    assertEquals(1, store.page(access(), SELECTED, 1, new Messages.Page(9, 0, 0, 10)).declared());
  }

  @Test
  void thousandMembersAcrossBatchesHaveStableStreamedSealAfterReopen() throws Exception {
    Path path = directory.resolve("large.sqlite");
    SessionStore.Configuration config =
        new SessionStore.Configuration(
            "issuer-a",
            new Records.Limits(1, 1000, 8, 1 << 20, 1 << 20, 1),
            new Records.Policy(10000, 10000, 10000),
            4,
            4,
            4,
            new BoundedSqlite.Limits(256L << 20, 512L << 20, 64L << 20, 4L << 20));
    SessionStore store = SessionStore.initialize(path, config);
    store.create(access(), SELECTED, new Messages.Create(1, 1, POLICY));
    List<Long> ids = new ArrayList<>(1000);
    for (long id = 1; id < 1000; id++) ids.add(id * 10);
    ids.add(Long.MAX_VALUE);
    Records.Digest expected;
    Commitments.Seal hasher =
        new Commitments.Seal(new Commitments.Context("issuer-a", "alice", 1), 0, 0, null, 1000);
    for (long id : ids) hasher.add(id);
    expected = hasher.finish();
    for (int from = 0, batch = 0; from < ids.size(); from += 250, batch++) {
      int to = Math.min(ids.size(), from + 250);
      var result =
          store.declare(
              access(),
              SELECTED,
              1,
              new Messages.Declare(
                  10 + batch, operation(batch + 1), 0, ids.subList(from, to), to == ids.size()));
      if (to == ids.size())
        assertEquals(expected, ((Records.Declared) result.receipt().outcome()).seal());
    }
    SessionStore reopened = SessionStore.open(path, config);
    assertEquals(
        expected, reopened.page(access(), SELECTED, 1, new Messages.Page(20, 0, 9990, 2)).seal());
  }

  @Test
  void concurrentDuplicateAndConflictingOperationsSerialize() throws Exception {
    Path path = directory.resolve("concurrent.sqlite");
    SessionStore one = SessionStore.initialize(path, configuration(16, 16));
    one.create(access(), SELECTED, new Messages.Create(1, 1, POLICY));
    SessionStore two = SessionStore.open(path, configuration(16, 16));
    CountDownLatch ready = new CountDownLatch(2), start = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(2)) {
      var a = executor.submit(() -> together(one, ready, start, List.of(1L)));
      var b = executor.submit(() -> together(two, ready, start, List.of(1L)));
      assertTrue(ready.await(2, TimeUnit.SECONDS));
      start.countDown();
      assertEquals(a.get(5, TimeUnit.SECONDS).receipt(), b.get(5, TimeUnit.SECONDS).receipt());
    }
    Records.OperationId conflicting = operation(2);
    CountDownLatch ready2 = new CountDownLatch(2), start2 = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(2)) {
      var a = executor.submit(() -> together(one, ready2, start2, conflicting, List.of(2L)));
      var b = executor.submit(() -> together(two, ready2, start2, conflicting, List.of(3L)));
      assertTrue(ready2.await(2, TimeUnit.SECONDS));
      start2.countDown();
      int success = 0, conflict = 0;
      for (var future : List.of(a, b))
        try {
          future.get(5, TimeUnit.SECONDS);
          success++;
        } catch (java.util.concurrent.ExecutionException e) {
          assertEquals(ProtocolError.Code.CONFLICT, ((ProtocolError) e.getCause()).code());
          conflict++;
        }
      assertEquals(1, success);
      assertEquals(1, conflict);
    }
  }

  @Test
  void entityAndOperationQuotasRefuseWithoutPartialMembership() throws Exception {
    SessionStore entities = initialized("entity-quota", 1, 8);
    entities.declare(
        access(), SELECTED, 1, new Messages.Declare(2, operation(1), 0, List.of(1L), false));
    assertCode(
        ProtocolError.Code.LIMIT_EXCEEDED,
        () ->
            entities.declare(
                access(),
                SELECTED,
                1,
                new Messages.Declare(3, operation(2), 0, List.of(2L), false)));
    assertEquals(
        List.of(new Messages.Entry(1, Records.State.DECLARED)),
        entities.page(access(), SELECTED, 1, new Messages.Page(4, 0, 0, 10)).entries());
    assertCode(
        ProtocolError.Code.NOT_FOUND,
        () ->
            entities.lookupOperation(
                access(), SELECTED, 1, new Messages.LookupOperation(5, operation(2))));

    SessionStore operations = initialized("operation-quota", 8, 1);
    operations.declare(
        access(), SELECTED, 1, new Messages.Declare(6, operation(1), 0, List.of(1L), false));
    assertCode(
        ProtocolError.Code.LIMIT_EXCEEDED,
        () ->
            operations.declare(
                access(),
                SELECTED,
                1,
                new Messages.Declare(7, operation(2), 0, List.of(2L), false)));
    assertEquals(
        1, operations.page(access(), SELECTED, 1, new Messages.Page(8, 0, 0, 10)).declared());
  }

  @Test
  void precommitDenialAndOperationInsertFailureRollBackEveryDeclarationWrite() throws Exception {
    Path deniedPath = directory.resolve("denied.sqlite");
    SessionStore denied = SessionStore.initialize(deniedPath, configuration(8, 8));
    denied.create(access(), SELECTED, new Messages.Create(1, 1, POLICY));
    AtomicInteger checks = new AtomicInteger();
    SessionStore.Access revoked =
        new SessionStore.Access(
            "alice",
            () -> {
              if (checks.incrementAndGet() == 3)
                throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "revoked");
            });
    assertCode(
        ProtocolError.Code.UNAUTHORIZED,
        () ->
            denied.declare(
                revoked,
                SELECTED,
                1,
                new Messages.Declare(2, operation(1), 0, List.of(1L), false)));
    assertTrue(
        denied.page(access(), SELECTED, 1, new Messages.Page(3, 0, 0, 10)).entries().isEmpty());
    denied.declare(
        access(), SELECTED, 1, new Messages.Declare(4, operation(1), 0, List.of(1L), false));

    Path sqlPath = directory.resolve("sql.sqlite");
    SessionStore.Configuration config = configuration(8, 8);
    SessionStore sql = SessionStore.initialize(sqlPath, config);
    sql.create(access(), SELECTED, new Messages.Create(1, 1, POLICY));
    try (var connection = BoundedSqlite.open(sqlPath, config.files()).connect();
        var statement = connection.createStatement()) {
      statement.execute(
          "CREATE TRIGGER fail_operation BEFORE INSERT ON ps_v2_operations BEGIN SELECT"
              + " RAISE(ABORT,'test operation interruption'); END");
      assertEquals(2, scalar(connection, "SELECT count(*) FROM ps_v2_slots"));
    }
    SQLException failure =
        assertThrows(
            SQLException.class,
            () ->
                sql.declare(
                    access(),
                    SELECTED,
                    1,
                    new Messages.Declare(2, operation(1), 0, List.of(1L), false)));
    assertTrue(failure.getMessage().contains("test operation interruption"));
    assertTrue(sql.page(access(), SELECTED, 1, new Messages.Page(3, 0, 0, 10)).entries().isEmpty());
    try (var connection = BoundedSqlite.open(sqlPath, config.files()).connect()) {
      assertEquals(2, scalar(connection, "SELECT count(*) FROM ps_v2_slots"));
    }
    assertCode(
        ProtocolError.Code.NOT_FOUND,
        () ->
            sql.lookupOperation(
                access(), SELECTED, 1, new Messages.LookupOperation(4, operation(1))));
    try (var connection = BoundedSqlite.open(sqlPath, config.files()).connect();
        var statement = connection.createStatement()) {
      statement.execute("DROP TRIGGER fail_operation");
    }
    sql.declare(
        access(), SELECTED, 1, new Messages.Declare(5, operation(1), 0, List.of(1L), false));
  }

  @Test
  void authorizationAndRetainedIntegrityPrecedeOrBlockDeclarationReads() throws Exception {
    Path path = directory.resolve("integrity.sqlite");
    SessionStore.Configuration config = configuration(8, 8);
    SessionStore store = SessionStore.initialize(path, config);
    store.create(access(), SELECTED, new Messages.Create(1, 1, POLICY));
    Records.OperationId op = operation(1);
    store.declare(access(), SELECTED, 1, new Messages.Declare(2, op, 0, List.of(1L), false));
    SessionStore.Access denied =
        new SessionStore.Access(
            "alice",
            () -> {
              throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "revoked");
            });
    assertCode(
        ProtocolError.Code.UNAUTHORIZED,
        () -> store.lookupOperation(denied, SELECTED, 999, new Messages.LookupOperation(3, op)));
    assertCode(
        ProtocolError.Code.CONFLICT,
        () ->
            store.snapshot(
                access(), SELECTED, 1, new Messages.Watch(4, new Records.WorkKey(0, 1, 1), 0, 0)));

    try (var connection = BoundedSqlite.open(path, config.files()).connect();
        var statement = connection.createStatement()) {
      assertThrows(
          SQLException.class,
          () -> statement.executeUpdate("DELETE FROM ps_v2_operations WHERE generation=1"));
      assertEquals(
          1,
          statement.executeUpdate(
              "UPDATE ps_v2_operations SET receipt=zeroblob(length(receipt)) WHERE generation=1"));
    }
    assertThrows(
        SQLException.class,
        () -> store.lookupOperation(access(), SELECTED, 1, new Messages.LookupOperation(5, op)));
    assertThrows(SQLException.class, () -> SessionStore.open(path, config));
  }

  @Test
  void processDeathAroundDeclarationCommitHasExactRecoveryBoundary() throws Exception {
    Path before = directory.resolve("halt-before.sqlite");
    assertEquals(31, runChild("before", before));
    SessionStore beforeStore = SessionStore.open(before, configuration(8, 8));
    assertTrue(
        beforeStore
            .page(access(), SELECTED, 1, new Messages.Page(1, 0, 0, 10))
            .entries()
            .isEmpty());
    assertCode(
        ProtocolError.Code.NOT_FOUND,
        () ->
            beforeStore.lookupOperation(
                access(), SELECTED, 1, new Messages.LookupOperation(2, operation(1))));

    Path after = directory.resolve("halt-after.sqlite");
    assertEquals(32, runChild("after", after));
    SessionStore afterStore = SessionStore.open(after, configuration(8, 8));
    assertEquals(
        1, afterStore.page(access(), SELECTED, 1, new Messages.Page(3, 0, 0, 10)).declared());
    assertEquals(
        expectedReceipt(),
        afterStore
            .lookupOperation(access(), SELECTED, 1, new Messages.LookupOperation(4, operation(1)))
            .receipt());
    assertEquals(
        expectedReceipt(),
        afterStore
            .declare(
                access(), SELECTED, 1, new Messages.Declare(5, operation(1), 0, List.of(1L), false))
            .receipt());
  }

  @Test
  void physicalFileLimitLeavesPriorDeclarationReplayableAndNoPartialOperation() throws Exception {
    Path path = directory.resolve("physical-full.sqlite");
    BoundedSqlite.Limits files = new BoundedSqlite.Limits(128 << 10, 64L << 20, 65_536, 512 << 10);
    SessionStore.Configuration config =
        new SessionStore.Configuration(
            "issuer-a",
            new Records.Limits(1, 8192, 8192, 1 << 20, 1 << 20, 1),
            new Records.Policy(10_000, 10_000, 10_000),
            1,
            1,
            1,
            files);
    SessionStore store = SessionStore.initialize(path, config);
    long initialBytes = java.nio.file.Files.size(path);
    long initialPages;
    try (var connection = BoundedSqlite.open(path, files).connect()) {
      initialPages = scalar(connection, "PRAGMA page_count");
    }
    store.create(access(), SELECTED, new Messages.Create(1, 1, POLICY));
    int committed = 0;
    ProtocolError refusal = null;
    for (int entity = 1; entity <= 1024; entity++) {
      try {
        store.declare(
            access(),
            SELECTED,
            1,
            new Messages.Declare(entity + 1, operation(entity), 0, List.of((long) entity), false));
        committed = entity;
      } catch (ProtocolError failure) {
        refusal = failure;
        break;
      }
    }
    assertTrue(committed > 0);
    System.out.printf(
        "declaration physical-full initialPages=%d initialBytes=%d committed=%d%n",
        initialPages, initialBytes, committed);
    assertNotNull(refusal, "declaration storage did not reach its physical bound");
    assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, refusal.code());
    assertInstanceOf(SQLException.class, refusal.getCause());
    int refused = committed + 1;
    assertEquals(
        committed,
        store.page(access(), SELECTED, 1, new Messages.Page(2000, 0, 0, 256)).declared());
    assertCode(
        ProtocolError.Code.NOT_FOUND,
        () ->
            store.lookupOperation(
                access(), SELECTED, 1, new Messages.LookupOperation(2001, operation(refused))));
    var first =
        store.lookupOperation(
            access(), SELECTED, 1, new Messages.LookupOperation(2002, operation(1)));
    assertEquals(
        first.receipt(),
        store
            .declare(
                access(),
                SELECTED,
                1,
                new Messages.Declare(2003, operation(1), 0, List.of(1L), false))
            .receipt());
    SessionStore reopened = SessionStore.open(path, config);
    assertEquals(
        first.receipt(),
        reopened
            .lookupOperation(
                access(), SELECTED, 1, new Messages.LookupOperation(2004, operation(1)))
            .receipt());
    assertTrue(java.nio.file.Files.size(path) <= 128 << 10);
  }

  @Test
  void declarationReceiptCannotReplayAfterItsAcceptedMemberDisappears() throws Exception {
    Path path = directory.resolve("missing-member.sqlite");
    SessionStore.Configuration config = configuration(8, 8);
    SessionStore store = SessionStore.initialize(path, config);
    store.create(access(), SELECTED, new Messages.Create(1, 1, POLICY));
    Records.OperationId operation = operation(1);
    Messages.Declare declaration = new Messages.Declare(2, operation, 0, List.of(1L), false);
    store.declare(access(), SELECTED, 1, declaration);
    try (var connection = BoundedSqlite.open(path, config.files()).connect();
        var statement = connection.createStatement()) {
      long viewSlot =
          scalar(
              connection,
              "SELECT view_slot FROM ps_v2_entities WHERE generation=1 AND scope=0 AND id=1");
      long fenceSlot =
          scalar(
              connection,
              "SELECT fence_slot FROM ps_v2_entities WHERE generation=1 AND scope=0 AND id=1");
      assertEquals(
          1,
          statement.executeUpdate(
              "DELETE FROM ps_v2_entities WHERE generation=1 AND scope=0 AND id=1"));
      assertEquals(
          2,
          statement.executeUpdate(
              "DELETE FROM ps_v2_slots WHERE id IN (" + viewSlot + "," + fenceSlot + ")"));
    }
    assertThrows(
        SQLException.class,
        () ->
            store.lookupOperation(
                access(), SELECTED, 1, new Messages.LookupOperation(3, operation)));
    assertThrows(
        SQLException.class,
        () ->
            store.declare(
                access(), SELECTED, 1, new Messages.Declare(4, operation, 0, List.of(1L), false)));
  }

  @Test
  void recoveryRejectsMissingDeclarationMemberEvenWhenCountersAreAdjusted() throws Exception {
    Path path = directory.resolve("missing-member-adjusted.sqlite");
    SessionStore.Configuration config = configuration(8, 8);
    SessionStore store = SessionStore.initialize(path, config);
    store.create(access(), SELECTED, new Messages.Create(1, 1, POLICY));
    store.declare(
        access(), SELECTED, 1, new Messages.Declare(2, operation(1), 0, List.of(1L), false));
    Messages.Binding binding =
        store.attach(access(), SELECTED, new Messages.Attach(99, "issuer-a", "alice", 1));
    try (var connection = BoundedSqlite.open(path, config.files()).connect();
        var statement = connection.createStatement()) {
      long viewSlot =
          scalar(
              connection,
              "SELECT view_slot FROM ps_v2_entities WHERE generation=1 AND scope=0 AND id=1");
      long fenceSlot =
          scalar(
              connection,
              "SELECT fence_slot FROM ps_v2_entities WHERE generation=1 AND scope=0 AND id=1");
      assertEquals(
          1,
          statement.executeUpdate(
              "DELETE FROM ps_v2_entities WHERE generation=1 AND scope=0 AND id=1"));
      assertEquals(
          2,
          statement.executeUpdate(
              "DELETE FROM ps_v2_slots WHERE id IN (" + viewSlot + "," + fenceSlot + ")"));
      statement.execute("BEGIN IMMEDIATE");
      long slot =
          scalar(connection, "SELECT state_slot FROM ps_v2_scopes WHERE generation=1 AND id=0");
      FixedRecords.Snapshot image =
          FixedRecords.read(
              connection,
              slot,
              FixedRecords.Kind.SCOPE,
              FixedRecords.key(binding, FixedRecords.Kind.SCOPE, 0, 0, 0, null));
      ScopeState state = ScopeState.decode(image.body());
      FixedRecords.replace(
          connection,
          config.files(),
          slot,
          FixedRecords.Kind.SCOPE,
          image.header().key(),
          image.header().revision(),
          state.members(0, 0, null).encode(),
          false);
      assertEquals(
          1,
          statement.executeUpdate("UPDATE ps_v2_sessions SET entity_count=0 WHERE generation=1"));
      statement.execute("COMMIT");
    }
    assertThrows(SQLException.class, () -> SessionStore.open(path, config));
  }

  @Test
  void emptyRootSealPersistsAsMembershipMetadataWithoutInventingClosure() throws Exception {
    Path path = directory.resolve("empty-seal.sqlite");
    SessionStore.Configuration config = configuration(8, 8);
    SessionStore store = SessionStore.initialize(path, config);
    store.create(access(), SELECTED, new Messages.Create(1, 1, POLICY));
    Records.Digest expected =
        new Commitments.Seal(new Commitments.Context("issuer-a", "alice", 1), 0, 0, null, 0)
            .finish();
    Records.Declared declared =
        (Records.Declared)
            store
                .declare(
                    access(),
                    SELECTED,
                    1,
                    new Messages.Declare(2, operation(1), 0, List.of(), true))
                .receipt()
                .outcome();
    assertEquals(0, declared.acceptedCount());
    assertEquals(0, declared.declared());
    assertEquals(expected, declared.seal());

    SessionStore reopened = SessionStore.open(path, config);
    Messages.PageResponse page =
        reopened.page(access(), SELECTED, 1, new Messages.Page(3, 0, Long.MAX_VALUE, 1));
    assertTrue(page.sealed());
    assertEquals(expected, page.seal());
    assertEquals(0, page.declared());
    assertTrue(page.entries().isEmpty());
    assertFalse(page.more());
  }

  public static void main(String[] args) throws Exception {
    Path path = Path.of(args[1]);
    SessionStore store = SessionStore.initialize(path, configuration(8, 8));
    store.create(access(), SELECTED, new Messages.Create(1, 1, POLICY));
    Messages.Declare declaration = new Messages.Declare(2, operation(1), 0, List.of(1L), false);
    if (args[0].equals("before")) {
      AtomicInteger checks = new AtomicInteger();
      store.declare(
          new SessionStore.Access(
              "alice",
              () -> {
                if (checks.incrementAndGet() == 3) Runtime.getRuntime().halt(31);
              }),
          SELECTED,
          1,
          declaration);
      throw new AssertionError("precommit halt did not run");
    }
    store.declare(access(), SELECTED, 1, declaration);
    Runtime.getRuntime().halt(32);
  }

  private int runChild(String mode, Path path) throws Exception {
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                DeclarationStoreTest.class.getName(),
                mode,
                path.toString())
            .redirectError(directory.resolve(mode + ".err").toFile())
            .redirectOutput(directory.resolve(mode + ".out").toFile())
            .start();
    try {
      assertTrue(process.waitFor(8, TimeUnit.SECONDS));
      return process.exitValue();
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS));
      }
    }
  }

  private SessionStore initialized(String name, long entities, long operations) throws Exception {
    SessionStore store =
        SessionStore.initialize(
            directory.resolve(name + ".sqlite"), configuration(entities, operations));
    store.create(access(), SELECTED, new Messages.Create(1, 1, POLICY));
    return store;
  }

  private static SessionStore.Configuration configuration(long entities, long operations) {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(1, entities, operations, 1 << 20, 1 << 20, 1),
        new Records.Policy(10000, 10000, 10000),
        4,
        4,
        4,
        BoundedSqlite.Limits.defaults());
  }

  private static SessionStore.Access access() {
    return new SessionStore.Access("alice", () -> {});
  }

  private static Records.OperationId operation(int value) {
    byte[] b = new byte[16];
    b[12] = (byte) (value >>> 24);
    b[13] = (byte) (value >>> 16);
    b[14] = (byte) (value >>> 8);
    b[15] = (byte) value;
    return new Records.OperationId(b);
  }

  private static Records.OperationReceipt expectedReceipt() {
    Messages.Declare declaration = new Messages.Declare(1, operation(1), 0, List.of(1L), false);
    return new Records.OperationReceipt(
        operation(1),
        Commitments.operation(new Commitments.Context("issuer-a", "alice", 1), 0, declaration),
        new Records.Declared(0, 0, 1, 1, null));
  }

  private static Messages.DeclarationResponse together(
      SessionStore s, CountDownLatch r, CountDownLatch start, List<Long> ids) throws Exception {
    return together(s, r, start, operation(1), ids);
  }

  private static Messages.DeclarationResponse together(
      SessionStore s,
      CountDownLatch r,
      CountDownLatch start,
      Records.OperationId op,
      List<Long> ids)
      throws Exception {
    r.countDown();
    assertTrue(start.await(2, TimeUnit.SECONDS));
    return s.declare(
        access(), SELECTED, 1, new Messages.Declare(ids.getFirst() + 100, op, 0, ids, false));
  }

  private static void assertCode(ProtocolError.Code code, Throwing action) {
    ProtocolError e = assertThrows(ProtocolError.class, action::run);
    assertEquals(code, e.code(), e::getMessage);
  }

  private static long scalar(java.sql.Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement();
        var rows = statement.executeQuery(sql)) {
      assertTrue(rows.next());
      return rows.getLong(1);
    }
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
