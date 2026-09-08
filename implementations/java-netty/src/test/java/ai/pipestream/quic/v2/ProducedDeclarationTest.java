package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class ProducedDeclarationTest {
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
      new InputStore.Limits(8L << 20, 64, 1 << 20, 8);
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void realProducerOneDeclarationSealsReplaysAndKeepsCallerOperationNamespaceIsolated()
      throws Exception {
    try (Fixture fixture = new Fixture("replay")) {
      Parent parent = fixture.parent(1, 1000);
      Records.OperationId shared = operation(50);
      fixture.sessions.declare(
          access(),
          SELECTED,
          fixture.generation,
          new Messages.Declare(20, shared, 0, List.of(3L), false));
      Messages.Declare produced =
          new Messages.Declare(21, shared, parent.child().scope(), List.of(10L, 20L), true);
      Messages.DeclarationResponse first = fixture.declare(parent.lease(), produced, 1100, ALLOW);
      Records.Declared declared =
          assertInstanceOf(Records.Declared.class, first.receipt().outcome());
      assertEquals(parent.child().scope(), declared.scope());
      assertEquals(1, declared.producer());
      assertEquals(2, declared.acceptedCount());
      assertEquals(2, declared.declared());
      assertNotNull(declared.seal());

      Messages.DeclarationResponse replay =
          fixture.declare(
              parent.lease(),
              new Messages.Declare(22, shared, parent.child().scope(), List.of(10L, 20L), true),
              1100,
              ALLOW);
      assertEquals(first.receipt(), replay.receipt());
      assertEquals(22, replay.request());
      ExecutionStore.Lease replacement =
          fixture.sessions.claimExecution(
              execAccess(),
              fixture.generation,
              parent.work(),
              fixture.inputs,
              500,
              clock(parent.lease().until()),
              ALLOW);
      Messages.DeclarationResponse replacementReplay =
          fixture.declare(
              replacement,
              new Messages.Declare(23, shared, parent.child().scope(), List.of(10L, 20L), true),
              parent.lease().until(),
              ALLOW);
      assertEquals(first.receipt(), replacementReplay.receipt());
      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              fixture.declare(
                  parent.lease(),
                  new Messages.Declare(24, shared, parent.child().scope(), List.of(10L, 20L), true),
                  parent.lease().until(),
                  ALLOW));
      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              fixture.declare(
                  replacement,
                  new Messages.Declare(25, shared, parent.child().scope(), List.of(10L), false),
                  parent.lease().until(),
                  ALLOW));

      Messages.OperationResponse caller =
          fixture.sessions.lookupOperation(
              access(), SELECTED, fixture.generation, new Messages.LookupOperation(26, shared));
      Records.Declared callerOutcome =
          assertInstanceOf(Records.Declared.class, caller.receipt().outcome());
      assertEquals(0, callerOutcome.scope());
      assertEquals(0, callerOutcome.producer());
      assertNotEquals(first.receipt(), caller.receipt());
      Messages.PageResponse page =
          fixture.sessions.page(
              access(),
              SELECTED,
              fixture.generation,
              new Messages.Page(27, parent.child().scope(), 0, 8));
      assertEquals(1, page.producer());
      assertEquals(List.of(10L, 20L), page.entries().stream().map(Messages.Entry::entity).toList());
    }
  }

  @Test
  void anotherParentScopeStaleLeaseAndDeadlineAreRefusedWithoutMutation() throws Exception {
    try (Fixture fixture = new Fixture("fences")) {
      Parent first = fixture.parent(1, 1000);
      Parent second = fixture.parent(2, 1100);
      Messages.Declare identityRequest =
          new Messages.Declare(28, operation(28), first.child().scope(), List.of(10L), false);
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.declareProduced(
                  new ExecutionStore.Access("bob", () -> {}),
                  first.lease(),
                  SELECTED,
                  fixture.inputs,
                  identityRequest,
                  clock(1200),
                  ALLOW));
      ExecutionStore.Lease foreignInstallation =
          new ExecutionStore.Lease(
              java.util.UUID.fromString("20000000-0000-0000-0000-000000000002"),
              first.lease().owner(),
              first.lease().generation(),
              first.lease().work(),
              first.lease().attempt(),
              first.lease().number(),
              first.lease().until());
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.declare(foreignInstallation, identityRequest, first.lease().until(), ALLOW));
      assertOpenEmpty(fixture, first.child().scope(), 29);
      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              fixture.declare(
                  second.lease(),
                  new Messages.Declare(
                      30, operation(30), first.child().scope(), List.of(10L), false),
                  1200,
                  ALLOW));
      assertOpenEmpty(fixture, first.child().scope(), 31);

      ExecutionStore.Lease replacement =
          fixture.sessions.claimExecution(
              execAccess(),
              fixture.generation,
              first.work(),
              fixture.inputs,
              800,
              clock(first.lease().until()),
              ALLOW);
      assertEquals(2, replacement.number());
      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              fixture.declare(
                  first.lease(),
                  new Messages.Declare(
                      32, operation(32), first.child().scope(), List.of(10L), false),
                  first.lease().until(),
                  ALLOW));
      assertCode(
          ProtocolError.Code.DEADLINE_EXCEEDED,
          () ->
              fixture.declare(
                  replacement,
                  new Messages.Declare(
                      33, operation(33), first.child().scope(), List.of(10L), false),
                  2000,
                  ALLOW));
      assertOpenEmpty(fixture, first.child().scope(), 34);
    }
  }

  @Test
  void finalAuthorizationRollbackLeavesSequenceAndScopeUnchangedForExactRetry() throws Exception {
    try (Fixture fixture = new Fixture("authorization")) {
      Parent parent = fixture.parent(1, 1000);
      AtomicInteger checks = new AtomicInteger();
      AdmissionStore.Authorization deny =
          (binding, parameters) -> {
            if (checks.incrementAndGet() == 2)
              throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "revoked before commit");
          };
      Messages.Declare request =
          new Messages.Declare(40, operation(40), parent.child().scope(), List.of(10L, 20L), false);
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () -> fixture.declare(parent.lease(), request, 1100, deny));
      assertEquals(2, checks.get());
      assertOpenEmpty(fixture, parent.child().scope(), 41);
      Messages.DeclarationResponse retried = fixture.declare(parent.lease(), request, 1100, ALLOW);
      Records.Declared accepted =
          assertInstanceOf(Records.Declared.class, retried.receipt().outcome());
      assertEquals(2, accepted.acceptedCount());
      assertEquals(2, accepted.declared());
      assertCode(
          ProtocolError.Code.NOT_FOUND,
          () ->
              fixture.sessions.lookupOperation(
                  access(),
                  SELECTED,
                  fixture.generation,
                  new Messages.LookupOperation(42, operation(40))));
    }
  }

  @Test
  void emptyProducerSealDoesNotCompleteExpansionOrPermitSuccess() throws Exception {
    try (Fixture fixture = new Fixture("empty-seal")) {
      Parent parent = fixture.parent(1, 1000);
      Messages.DeclarationResponse response =
          fixture.declare(
              parent.lease(),
              new Messages.Declare(50, operation(50), parent.child().scope(), List.of(), true),
              1100,
              ALLOW);
      Records.Declared declared =
          assertInstanceOf(Records.Declared.class, response.receipt().outcome());
      assertEquals(0, declared.declared());
      assertNotNull(declared.seal());
      assertCode(
          ProtocolError.Code.NOT_READY,
          () ->
              fixture.sessions.succeedExecution(
                  execAccess(), parent.lease(), fixture.inputs, 0, ENDPOINT, clock(1100), ALLOW));
      Records.WorkView retained = fixture.view(parent.work());
      assertEquals(Records.State.ACTIVE, retained.state());
      assertNull(retained.manifest());
    }
  }

  private final class Fixture implements AutoCloseable {
    final Path database;
    final Path inputsPath;
    final SessionStore sessions;
    final InputStore inputs;
    final long generation;
    int request = 100;

    Fixture(String name) throws Exception {
      database = directory.resolve(name + ".sqlite");
      inputsPath = directory.resolve(name + "-inputs");
      sessions = SessionStore.initialize(database, configuration());
      Messages.Binding binding =
          sessions.create(
              access(),
              SELECTED,
              new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
      generation = binding.generation();
      sessions.declare(
          access(),
          SELECTED,
          generation,
          new Messages.Declare(2, operation(1), 0, List.of(1L, 2L), false));
      inputs = InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
    }

    Parent parent(long entity, long admittedAt) throws Exception {
      Records.WorkKey work = new Records.WorkKey(0, 0, entity);
      Records.InputHeader header =
          new Records.InputHeader(
              generation,
              operation(request++),
              new Records.AdmitParameters(
                  work,
                  new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
                  "expand",
                  2,
                  1000,
                  new Records.OutputBudget(0, 0)));
      Commitments.Context context = new Commitments.Context("issuer-a", "alice", generation);
      try (InputStore.Receiver receiver = inputs.begin(context, header, SELECTED, admittedAt)) {
        receiver.write(ByteBuffer.allocate(0), admittedAt);
        receiver.finish(admittedAt);
      }
      sessions.admit(
          access(), SELECTED, generation, inputs, header, request++, clock(admittedAt), ALLOW);
      ExecutionStore.Lease lease =
          sessions.claimExecution(
              execAccess(), generation, work, inputs, 100, clock(admittedAt + 100), ALLOW);
      Records.ChildScope child = view(work).child();
      assertNotNull(child);
      assertEquals(1, child.producer());
      return new Parent(work, child, lease);
    }

    Messages.DeclarationResponse declare(
        ExecutionStore.Lease lease,
        Messages.Declare request,
        long utc,
        AdmissionStore.Authorization authorization)
        throws Exception {
      return sessions.declareProduced(
          execAccess(), lease, SELECTED, inputs, request, clock(utc), authorization);
    }

    Records.WorkView view(Records.WorkKey work) throws Exception {
      return sessions
          .snapshot(access(), SELECTED, generation, new Messages.Watch(request++, work, 0, 0))
          .work();
    }

    @Override
    public void close() throws java.io.IOException {
      inputs.close();
    }
  }

  private static SessionStore.Configuration configuration() {
    AdmissionStore.Application application =
        new AdmissionStore.Application(
            "expand", Set.of(2), AdmissionStore.RestartSafety.IDEMPOTENT);
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(32, 64, 64, 1 << 20, 1 << 20, 16),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        16,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(application), 16, 16));
  }

  private static void assertOpenEmpty(Fixture fixture, long scope, long request) throws Exception {
    Messages.PageResponse page =
        fixture.sessions.page(
            access(), SELECTED, fixture.generation, new Messages.Page(request, scope, 0, 8));
    assertEquals(1, page.producer());
    assertEquals(0, page.declared());
    assertFalse(page.sealed());
    assertTrue(page.entries().isEmpty());
  }

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static Records.OperationId operation(int value) {
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

  private record Parent(
      Records.WorkKey work, Records.ChildScope child, ExecutionStore.Lease lease) {}

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
