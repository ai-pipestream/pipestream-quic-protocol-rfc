package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class BranchExecutionTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true,
          List.of(DURABLE_WORK, RESULT_DELIVERY),
          List.of(),
          1 << 20,
          16,
          32,
          1 << 20,
          1000,
          5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(16L << 20, 128, 1 << 20, 16);
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void reconstructsTwoPagedChildOutputsAndRetainsExactParentAndRootEvidence() throws Exception {
    AtomicLong now = new AtomicLong(1000);
    try (Fixture fixture = new Fixture(directory, "success", now)) {
      fixture.createParent(new byte[] {42}, true);
      AtomicInteger prematureCalls = new AtomicInteger();
      assertCode(
          ProtocolError.Code.NOT_READY,
          () ->
              fixture
                  .runtime(
                      context -> {
                        prematureCalls.incrementAndGet();
                        return ExecutionRuntime.Outcome.succeeded();
                      })
                  .run(execAccess(), fixture.generation, fixture.parent));
      assertEquals(0, prematureCalls.get());

      fixture.declareChildren(List.of(10L, 20L), true);
      byte[] first = {1, 2, 3};
      byte[] second = {4, 5};
      fixture.publishChild(10, first);
      fixture.publishChild(20, second);
      assertEquals(
          1100, fixture.view(new Records.WorkKey(fixture.child.scope(), 0, 10)).outputUntil());
      assertEquals(
          1100, fixture.view(new Records.WorkKey(fixture.child.scope(), 0, 20)).outputUntil());
      fixture.closeChild();
      now.set(1400); // Child result delivery expired at 1100; internal branch reads remain valid.

      AtomicReference<ExecutionStore.Lease> parentLease = new AtomicReference<>();
      Records.WorkView parent =
          fixture
              .runtime(
                  context -> {
                    parentLease.set(context.lease());
                    byte[] parentInput = new byte[2];
                    assertEquals(1, context.readInput(parentInput, 0, parentInput.length));
                    assertEquals(42, parentInput[0]);
                    assertEquals(-1, context.readInput(parentInput, 0, parentInput.length));
                    BranchStore.Page page = context.children(0, 1);
                    assertEquals(
                        List.of(new Records.WorkKey(fixture.child.scope(), 0, 10)), page.members());
                    assertTrue(page.more());
                    BranchStore.Page tail = context.children(10, 1);
                    assertEquals(
                        List.of(new Records.WorkKey(fixture.child.scope(), 0, 20)), tail.members());
                    assertFalse(tail.more());
                    context.beginOutput(first.length + second.length, "application/octet-stream");
                    copyChild(context, 10, first);
                    copyChild(context, 20, second);
                    assertEquals(0, context.finishOutput());
                    return ExecutionRuntime.Outcome.succeeded();
                  })
              .run(execAccess(), fixture.generation, fixture.parent);
      assertEquals(Records.State.SUCCEEDED, parent.state());
      byte[] reconstructed = {1, 2, 3, 4, 5};
      assertEquals(digest(reconstructed), parent.manifest().outputs().get(0).sha256());
      OutputStore.Stored stored =
          fixture
              .inputs
              .findOutput(fixture.context(), fixture.parentHeader, parentLease.get(), 0)
              .orElseThrow();
      try (InputStream stream = stored.openStream()) {
        assertArrayEquals(reconstructed, stream.readAllBytes());
      }
      Records.ScopeSummary childSummary =
          fixture.sessions.scopeSummary(
              access(), SELECTED, fixture.generation, fixture.child.scope(), fixture.childSeal());
      fixture.closeRoot();
      Records.ScopeSummary root =
          fixture.sessions.scopeSummary(
              access(), SELECTED, fixture.generation, 0, fixture.rootSeal());
      assertEquals(new Records.Counts(1, 0, 0, 0), root.counts());
      Commitments.StatusTree status = new Commitments.StatusTree(0, 0, 1);
      status.add(parent, childSummary.statusRoot());
      assertEquals(status.finish().root(), root.statusRoot());
      fixture.reopen();
      assertEquals(parent, fixture.view(fixture.parent));
      assertEquals(
          root,
          fixture.sessions.scopeSummary(
              access(), SELECTED, fixture.generation, 0, fixture.rootSeal()));
      assertEquals(0, fixture.inputs.usage().handles());
    }
  }

  @Test
  void leafUnknownChildAndUnfinishedChildReadsAreStickyAndCannotPublishSuccess() throws Exception {
    AtomicLong now = new AtomicLong(1000);
    try (Fixture fixture = new Fixture(directory, "leaf-negative", now)) {
      Records.WorkKey leaf = fixture.createLeaf(2, new byte[0], 0, 0);
      Records.WorkView leafFailure =
          fixture
              .runtime(
                  context -> {
                    context.children(0, 1);
                    throw new AssertionError("leaf child enumeration unexpectedly succeeded");
                  })
              .run(execAccess(), fixture.generation, leaf);
      assertEquals(Records.State.FAILED, leafFailure.state());
      assertEquals(ProtocolError.Code.CONFLICT.value(), leafFailure.diagnostic().code());
      assertNull(leafFailure.manifest());
    }

    try (Fixture fixture = new Fixture(directory, "unknown-negative", now)) {
      fixture.createParent(new byte[0], false);
      fixture.declareChildren(List.of(10L), true);
      fixture.publishChild(10, new byte[] {1, 2, 3});
      fixture.closeChild();
      AtomicBoolean returnedAfterRefusal = new AtomicBoolean();
      Records.WorkView unknown =
          fixture
              .runtime(
                  context -> {
                    assertCode(ProtocolError.Code.NOT_FOUND, () -> context.beginChildOutput(11, 0));
                    returnedAfterRefusal.set(true);
                    return ExecutionRuntime.Outcome.succeeded();
                  })
              .run(execAccess(), fixture.generation, fixture.parent);
      assertEquals(Records.State.FAILED, unknown.state());
      assertEquals(ProtocolError.Code.NOT_FOUND.value(), unknown.diagnostic().code());
      assertTrue(returnedAfterRefusal.get());
      assertNull(unknown.manifest());
    }

    try (Fixture fixture = new Fixture(directory, "unfinished", now)) {
      fixture.createParent(new byte[0], false);
      fixture.declareChildren(List.of(10L), true);
      fixture.publishChild(10, new byte[] {1, 2, 3});
      fixture.closeChild();
      Records.WorkView unfinished =
          fixture
              .runtime(
                  context -> {
                    Records.Output output = context.beginChildOutput(10, 0);
                    assertEquals(3, output.length());
                    assertEquals(1, context.readChildOutput(new byte[1], 0, 1));
                    context.finishChildOutput();
                    throw new AssertionError("partial child output unexpectedly reached EOF");
                  })
              .run(execAccess(), fixture.generation, fixture.parent);
      assertEquals(Records.State.FAILED, unfinished.state());
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR.value(), unfinished.diagnostic().code());
      assertNull(unfinished.manifest());
      assertEquals(0, fixture.inputs.usage().handles());
    }

    try (Fixture fixture = new Fixture(directory, "absent-index", now)) {
      fixture.createParent(new byte[0], false);
      fixture.declareChildren(List.of(10L), true);
      fixture.publishChild(10, new byte[] {1});
      fixture.closeChild();
      Records.WorkView absent =
          fixture
              .runtime(
                  context -> {
                    context.beginChildOutput(10, 1);
                    throw new AssertionError("absent child output index unexpectedly opened");
                  })
              .run(execAccess(), fixture.generation, fixture.parent);
      assertEquals(Records.State.FAILED, absent.state());
      assertEquals(ProtocolError.Code.NOT_FOUND.value(), absent.diagnostic().code());
      assertNull(absent.manifest());
    }
  }

  @Test
  void abandonedConcurrentInvalidAndZeroLengthChildReadsCannotPublishSuccess() throws Exception {
    AtomicLong now = new AtomicLong(1000);
    try (Fixture fixture = prepared("abandoned", now, new byte[] {1, 2, 3})) {
      Records.WorkView view =
          fixture
              .runtime(
                  context -> {
                    context.beginChildOutput(10, 0);
                    assertEquals(1, context.readChildOutput(new byte[1], 0, 1));
                    return ExecutionRuntime.Outcome.succeeded();
                  })
              .run(execAccess(), fixture.generation, fixture.parent);
      assertEquals(Records.State.FAILED, view.state());
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR.value(), view.diagnostic().code());
      assertEquals(0, fixture.inputs.usage().handles());
    }
    try (Fixture fixture = prepared("simultaneous", now, new byte[] {1})) {
      Records.WorkView view =
          fixture
              .runtime(
                  context -> {
                    context.beginChildOutput(10, 0);
                    context.beginChildOutput(10, 0);
                    throw new AssertionError("a second child reader unexpectedly opened");
                  })
              .run(execAccess(), fixture.generation, fixture.parent);
      assertEquals(ProtocolError.Code.CONFLICT.value(), view.diagnostic().code());
      assertNull(view.manifest());
    }
    try (Fixture fixture = prepared("index-range", now, new byte[] {1})) {
      Records.WorkView view =
          fixture
              .runtime(
                  context -> {
                    context.beginChildOutput(10, 256);
                    throw new AssertionError("invalid child output index unexpectedly opened");
                  })
              .run(execAccess(), fixture.generation, fixture.parent);
      assertEquals(ProtocolError.Code.FRAME_ERROR.value(), view.diagnostic().code());
      assertNull(view.manifest());
    }
    try (Fixture fixture = prepared("zero-read", now, new byte[] {1})) {
      Records.WorkView view =
          fixture
              .runtime(
                  context -> {
                    context.beginChildOutput(10, 0);
                    assertEquals(0, context.readChildOutput(new byte[1], 0, 0));
                    context.finishChildOutput();
                    throw new AssertionError("zero-length read unexpectedly established EOF");
                  })
              .run(execAccess(), fixture.generation, fixture.parent);
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR.value(), view.diagnostic().code());
      assertNull(view.manifest());
    }
  }

  @Test
  void lostSettledChildOutputAccountingPropagatesSqlCorruptionWithoutFailingParent()
      throws Exception {
    AtomicLong now = new AtomicLong(1000);
    try (Fixture fixture = prepared("lost-output-accounting", now, new byte[] {1, 2, 3})) {
      Records.WorkKey childWork = new Records.WorkKey(fixture.child.scope(), 0, 10);
      try (Connection connection =
          BoundedSqlite.open(fixture.database, configuration(fixture.application).files())
              .connect()) {
        execute(connection, "BEGIN IMMEDIATE");
        AdmissionStore.StoredJob stored =
            AdmissionStore.job(connection, fixture.binding, childWork);
        JobRecord source = stored.record();
        assertEquals(JobRecord.Stage.SETTLED, source.stage());
        assertTrue(source.outputsLive());
        JobRecord corrupt =
            new JobRecord(
                source.input(),
                source.safety(),
                source.attempt(),
                source.lease(),
                source.leaseUntil(),
                source.stage(),
                source.inputReference(),
                source.outputReference(),
                source.objectLimit(),
                source.inputLive(),
                false,
                source.executorLive(),
                source.expansionComplete(),
                source.inputReleaseAt(),
                // Structurally valid refund evidence; retained parent/interval audit must reject
                // it.
                0L);
        Records.WorkKey work = source.input().parameters().work();
        FixedRecords.replace(
            connection,
            configuration(fixture.application).files(),
            stored.slot(),
            FixedRecords.Kind.JOB,
            FixedRecords.key(
                fixture.context(),
                FixedRecords.Kind.JOB,
                work.scope(),
                work.producer(),
                work.entity(),
                source.input().operation().bytes()),
            stored.geometry().revision(),
            corrupt.encode(),
            false);
        execute(connection, "COMMIT");
      }

      AtomicBoolean returnedAfterCorruption = new AtomicBoolean();
      java.sql.SQLException failure =
          assertThrows(
              java.sql.SQLException.class,
              () ->
                  fixture
                      .runtime(
                          context -> {
                            try {
                              context.beginChildOutput(10, 0);
                            } catch (java.sql.SQLException expected) {
                              returnedAfterCorruption.set(true);
                            }
                            return ExecutionRuntime.Outcome.succeeded();
                          })
                      .run(execAccess(), fixture.generation, fixture.parent));
      assertTrue(returnedAfterCorruption.get());
      assertTrue(failure.getMessage().contains("dependency"), failure::getMessage);
      Records.WorkView parent = fixture.view(fixture.parent);
      assertEquals(Records.State.ACTIVE, parent.state());
      assertNull(parent.manifest());
      assertEquals(0, fixture.inputs.usage().handles());
    }
  }

  @Test
  void emptyChildOutputRequiresNonemptyReadToObserveEofAndThenAllowsParentSuccess()
      throws Exception {
    AtomicLong now = new AtomicLong(1000);
    try (Fixture fixture = prepared("empty-child-output", now, new byte[0])) {
      Records.WorkView parent =
          fixture
              .runtime(
                  context -> {
                    Records.Output output = context.beginChildOutput(10, 0);
                    assertEquals(0, output.length());
                    byte[] byteBuffer = new byte[1];
                    assertEquals(0, context.readChildOutput(byteBuffer, 0, 0));
                    assertEquals(-1, context.readChildOutput(byteBuffer, 0, 1));
                    context.finishChildOutput();
                    context.beginOutput(0, "application/octet-stream");
                    assertEquals(0, context.finishOutput());
                    return ExecutionRuntime.Outcome.succeeded();
                  })
              .run(execAccess(), fixture.generation, fixture.parent);
      assertEquals(Records.State.SUCCEEDED, parent.state());
      assertEquals(0, parent.manifest().outputs().get(0).length());
      assertEquals(0, fixture.inputs.usage().handles());
    }
  }

  private Fixture prepared(String name, AtomicLong now, byte[] output) throws Exception {
    Fixture fixture = new Fixture(directory, name, now);
    boolean success = false;
    try {
      fixture.createParent(new byte[0], false);
      fixture.declareChildren(List.of(10L), true);
      fixture.publishChild(10, output);
      fixture.closeChild();
      success = true;
      return fixture;
    } finally {
      if (!success) fixture.close();
    }
  }

  private static void execute(Connection connection, String sql) throws Exception {
    try (var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static void copyChild(ExecutionRuntime.Context context, long entity, byte[] expected)
      throws Exception {
    Records.Output output = context.beginChildOutput(entity, 0);
    assertEquals(expected.length, output.length());
    assertEquals(digest(expected), output.sha256());
    byte[] buffer = new byte[2];
    int offset = 0;
    for (int read; (read = context.readChildOutput(buffer, 0, buffer.length)) != -1; ) {
      assertArrayEquals(
          java.util.Arrays.copyOfRange(expected, offset, offset + read),
          java.util.Arrays.copyOf(buffer, read));
      context.writeOutput(ByteBuffer.wrap(buffer, 0, read));
      offset += read;
    }
    assertEquals(expected.length, offset);
    context.finishChildOutput();
  }

  static final class Fixture implements AutoCloseable {
    final Path database;
    final Path inputPath;
    final AdmissionStore.Application application =
        new AdmissionStore.Application(
            "copy", Set.of(0, 1), AdmissionStore.RestartSafety.IDEMPOTENT);
    final AtomicLong now;
    final Messages.Binding binding;
    final long generation;
    final Records.WorkKey parent = new Records.WorkKey(0, 0, 1);
    SessionStore sessions;
    InputStore inputs;
    Records.ChildScope child;
    List<Long> childIds = List.of();
    Records.InputHeader parentHeader;
    int request = 10;

    Fixture(Path directory, String name, AtomicLong now) throws Exception {
      this.now = now;
      database = directory.resolve(name + ".sqlite");
      inputPath = directory.resolve(name + "-inputs");
      sessions = SessionStore.initialize(database, configuration(application));
      binding =
          sessions.create(
              access(),
              SELECTED,
              new Messages.Create(1, 1, new Records.Policy(10_000, 100, 30_000)));
      generation = binding.generation();
      inputs = InputStore.initializeForAuthority(inputPath, INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
    }

    void createParent(byte[] input, boolean sealRoot) throws Exception {
      sessions.declare(
          access(),
          SELECTED,
          generation,
          new Messages.Declare(request++, operation(request++), 0, List.of(1L), sealRoot));
      parentHeader = admit(parent, input, 1, 1, 5, operation(request++));
      child = view(parent).child();
      assertNotNull(child);
    }

    Records.WorkKey createLeaf(long entity, byte[] input, int outputs, long bytes)
        throws Exception {
      Records.WorkKey work = new Records.WorkKey(0, 0, entity);
      sessions.declare(
          access(),
          SELECTED,
          generation,
          new Messages.Declare(request++, operation(request++), 0, List.of(entity), false));
      admit(work, input, 0, outputs, bytes, operation(request++));
      return work;
    }

    void declareChildren(List<Long> ids, boolean seal) throws Exception {
      childIds = List.copyOf(ids);
      sessions.declare(
          access(),
          SELECTED,
          generation,
          new Messages.Declare(request++, operation(request++), child.scope(), ids, seal));
    }

    void publishChild(long entity, byte[] output) throws Exception {
      Records.WorkKey work = new Records.WorkKey(child.scope(), child.producer(), entity);
      admit(work, new byte[0], 0, 1, output.length, operation(request++));
      Records.WorkView published =
          runtime(
                  context -> {
                    context.beginOutput(output.length, "application/octet-stream");
                    context.writeOutput(ByteBuffer.wrap(output));
                    assertEquals(0, context.finishOutput());
                    return ExecutionRuntime.Outcome.succeeded();
                  })
              .run(execAccess(), generation, work);
      assertEquals(Records.State.SUCCEEDED, published.state());
      assertEquals(digest(output), published.manifest().outputs().get(0).sha256());
    }

    Records.WorkKey admitChild(long entity, long outputBytes) throws Exception {
      Records.WorkKey work = new Records.WorkKey(child.scope(), child.producer(), entity);
      admit(work, new byte[0], 0, 1, outputBytes, operation(request++));
      return work;
    }

    Records.InputHeader admit(
        Records.WorkKey work,
        byte[] input,
        int mode,
        int outputs,
        long outputBytes,
        Records.OperationId operation)
        throws Exception {
      Records.InputHeader header =
          new Records.InputHeader(
              generation,
              operation,
              new Records.AdmitParameters(
                  work,
                  new Records.Input(input.length, digest(input), "application/octet-stream"),
                  "copy",
                  mode,
                  5000,
                  new Records.OutputBudget(outputs, outputBytes)));
      Commitments.Context context = new Commitments.Context("issuer-a", "alice", generation);
      try (InputStore.Receiver receiver = inputs.begin(context, header, SELECTED, now.get())) {
        receiver.write(ByteBuffer.wrap(input), now.get());
        receiver.finish(now.get());
      }
      sessions.admit(
          access(), SELECTED, generation, inputs, header, request++, clock(now.get()), ALLOW);
      return header;
    }

    Commitments.Context context() {
      return new Commitments.Context("issuer-a", "alice", generation);
    }

    void closeChild() throws Exception {
      ClosureStore.Cursor cursor = new ClosureStore.Cursor();
      for (int calls = 0; calls < 8; calls++) {
        sessions.reconcileClosures(cursor, 1, clock(now.get()));
        try {
          sessions.scopeSummary(access(), SELECTED, generation, child.scope(), childSeal());
          return;
        } catch (ProtocolError refusal) {
          if (refusal.code() != ProtocolError.Code.NOT_READY) throw refusal;
        }
      }
      fail("child scope did not close");
    }

    void closeRoot() throws Exception {
      ClosureStore.Cursor cursor = new ClosureStore.Cursor();
      for (int calls = 0; calls < 8; calls++) {
        sessions.reconcileClosures(cursor, 1, clock(now.get()));
        try {
          sessions.scopeSummary(access(), SELECTED, generation, 0, rootSeal());
          return;
        } catch (ProtocolError refusal) {
          if (refusal.code() != ProtocolError.Code.NOT_READY) throw refusal;
        }
      }
      fail("root scope did not close");
    }

    Records.Digest childSeal() {
      return seal(child.scope(), child.producer(), parent, childIds);
    }

    Records.Digest rootSeal() {
      return seal(0, 0, null, List.of(1L));
    }

    Records.Digest seal(long scope, int producer, Records.WorkKey parent, List<Long> ids) {
      Commitments.Seal seal =
          new Commitments.Seal(
              new Commitments.Context("issuer-a", "alice", generation),
              scope,
              producer,
              parent,
              ids.size());
      for (long id : ids) seal.add(id);
      return seal.finish();
    }

    ExecutionRuntime runtime(ExecutionRuntime.Callback callback) {
      return new ExecutionRuntime(
          sessions,
          inputs,
          List.of(new ExecutionRuntime.Registration(application, callback)),
          ENDPOINT,
          clock(now.get()),
          ALLOW,
          new ExecutionRuntime.Limits(1, 1, 500, 128));
    }

    Records.WorkView view(Records.WorkKey work) throws Exception {
      return sessions
          .snapshot(access(), SELECTED, generation, new Messages.Watch(request++, work, 0, 0))
          .work();
    }

    void reopen() throws Exception {
      inputs.close();
      sessions = SessionStore.open(database, configuration(application));
      inputs = InputStore.open(inputPath, INPUT_LIMITS);
      sessions.verifyInputs(inputs);
    }

    @Override
    public void close() throws java.io.IOException {
      inputs.close();
    }
  }

  private static SessionStore.Configuration configuration(AdmissionStore.Application app) {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(32, 64, 64, 1 << 20, 1 << 20, 8),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        16,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(app), 16, 16));
  }

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[12] = (byte) (value >>> 24);
    bytes[13] = (byte) (value >>> 16);
    bytes[14] = (byte) (value >>> 8);
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static AdmissionStore.Clock clock(long utc) {
    return () -> new AdmissionStore.Time(utc, true);
  }

  private static SessionStore.Access access() {
    return new SessionStore.Access("alice", () -> {});
  }

  private static ExecutionStore.Access execAccess() {
    return new ExecutionStore.Access("alice", () -> {});
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
