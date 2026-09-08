package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class BranchExecutionCapacityTest {
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
  private static final AdmissionStore.Application APPLICATION =
      new AdmissionStore.Application("copy", Set.of(0, 1), AdmissionStore.RestartSafety.IDEMPOTENT);
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void threeHandlesProtectSequentialChildReadsAndParentWriterFromOrdinaryReaders()
      throws Exception {
    AtomicLong now = new AtomicLong(1000);
    try (Fixture fixture = new Fixture("three", 3, now)) {
      byte[] first = {1, 2, 3};
      byte[] second = {4, 5};
      fixture.prepareBranch(List.of(first, second), 1);

      InputStore.Usage funded = fixture.inputs.usage();
      Records.WorkView parent =
          fixture
              .runtime(
                  ALLOW,
                  context -> {
                    assertEquals(3, fixture.inputs.usage().handles());
                    assertCode(
                        ProtocolError.Code.LIMIT_EXCEEDED,
                        () ->
                            fixture
                                .inputs
                                .find(fixture.context(), fixture.parentHeader)
                                .orElseThrow()
                                .openStream());
                    context.beginOutput(first.length + second.length, "application/octet-stream");
                    copy(context, 10, first);
                    assertEquals(3, fixture.inputs.usage().handles());
                    copy(context, 20, second);
                    assertEquals(0, context.finishOutput());
                    return ExecutionRuntime.Outcome.succeeded();
                  })
              .run(execAccess(), fixture.generation, fixture.parent);

      assertEquals(Records.State.SUCCEEDED, parent.state());
      assertEquals(digest(new byte[] {1, 2, 3, 4, 5}), parent.manifest().outputs().get(0).sha256());
      assertEquals(0, fixture.inputs.usage().handles());
      assertEquals(funded.bytes(), fixture.inputs.usage().bytes());
      assertEquals(funded.files(), fixture.inputs.usage().files());
    }
  }

  @Test
  void admissionRefusesIntrinsicallyInsufficientBranchGeometryBeforeFunding() throws Exception {
    AtomicLong now = new AtomicLong(1000);
    try (Fixture fixture = new Fixture("insufficient", 2, now)) {
      fixture.declareParent();
      Records.InputHeader header = fixture.receive(fixture.parent, new byte[] {7}, 1, 1, 1);
      InputStore.Usage before = fixture.inputs.usage();

      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () ->
              fixture.sessions.admit(
                  access(),
                  SELECTED,
                  fixture.generation,
                  fixture.inputs,
                  header,
                  fixture.request++,
                  clock(now.get()),
                  ALLOW));

      assertEquals(before, fixture.inputs.usage());
      assertTrue(fixture.inputs.find(fixture.context(), header).isPresent());
      assertTrue(fixture.inputs.findReservation(fixture.context(), header).isEmpty());
      assertEquals(Records.State.DECLARED, fixture.view(fixture.parent).state());
    }
  }

  @Test
  void readerCreditIsStoreBoundAndOneChargeCanBeBorrowedSequentially() throws Exception {
    AtomicLong now = new AtomicLong(1000);
    try (Fixture first = new Fixture("reader-first", 3, now);
        Fixture foreign = new Fixture("reader-foreign", 3, now)) {
      first.prepareBranch(List.of(new byte[] {1, 2, 3}), 0);
      foreign.prepareBranch(List.of(new byte[] {4}), 0);
      ExecutionStore.Lease firstLease = first.claimParent();
      ExecutionStore.Lease foreignLease = foreign.claimParent();
      BranchStore.Source source =
          first.sessions.childOutput(execAccess(), firstLease, 10, 0, clock(1100), ALLOW);
      BranchStore.Source foreignSource =
          foreign.sessions.childOutput(execAccess(), foreignLease, 10, 0, clock(1100), ALLOW);
      InputStore.Usage funded = first.inputs.usage();

      OutputStore.ReaderCredit credit = first.inputs.reserveOutputReader();
      assertEquals(1, first.inputs.usage().handles());
      InputStream reader = BranchStore.open(first.inputs, source, credit);
      assertEquals(1, first.inputs.usage().handles());
      assertThrows(java.io.IOException.class, credit::close);
      assertCode(ProtocolError.Code.CONFLICT, () -> BranchStore.open(first.inputs, source, credit));
      assertEquals(1, first.inputs.usage().handles());
      assertEquals(1, reader.read());
      reader.close();
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> BranchStore.open(foreign.inputs, foreignSource, credit));
      assertEquals(0, foreign.inputs.usage().handles());
      try (InputStream again = BranchStore.open(first.inputs, source, credit)) {
        assertArrayEquals(new byte[] {1, 2, 3}, again.readAllBytes());
      }
      credit.close();
      credit.close();
      assertCode(ProtocolError.Code.CONFLICT, () -> BranchStore.open(first.inputs, source, credit));
      assertEquals(0, first.inputs.usage().handles());
      assertEquals(funded.bytes(), first.inputs.usage().bytes());
      assertEquals(funded.files(), first.inputs.usage().files());
    }
  }

  @Test
  void unfinishedReadAndRevokedOrExpiredGrantReleaseEveryPhysicalHandle() throws Exception {
    for (String loss : List.of("unfinished", "revoked", "deadline")) {
      AtomicLong now = new AtomicLong(1000);
      try (Fixture fixture = new Fixture(loss, 2, now)) {
        fixture.prepareBranch(List.of(new byte[] {1, 2, 3}), 0);
        AtomicLong revoked = new AtomicLong();
        AtomicInteger reached = new AtomicInteger();
        AdmissionStore.Authorization authorization =
            (binding, parameters) -> {
              if (revoked.get() != 0)
                throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "revoked");
            };
        ExecutionRuntime runtime =
            fixture.runtime(
                authorization,
                context -> {
                  context.beginChildOutput(10, 0);
                  assertEquals(1, context.readChildOutput(new byte[1], 0, 1));
                  reached.incrementAndGet();
                  if (loss.equals("unfinished")) return ExecutionRuntime.Outcome.succeeded();
                  if (loss.equals("revoked")) revoked.set(1);
                  else now.set(6000);
                  context.readChildOutput(new byte[1], 0, 1);
                  return ExecutionRuntime.Outcome.succeeded();
                });

        if (loss.equals("unfinished")) {
          Records.WorkView failed = runtime.run(execAccess(), fixture.generation, fixture.parent);
          assertEquals(1, reached.get());
          assertEquals(Records.State.FAILED, failed.state());
          assertNull(failed.manifest());
          assertEquals(ProtocolError.Code.INTEGRITY_ERROR.value(), failed.diagnostic().code());
        } else {
          assertCode(
              loss.equals("revoked")
                  ? ProtocolError.Code.UNAUTHORIZED
                  : ProtocolError.Code.DEADLINE_EXCEEDED,
              () -> runtime.run(execAccess(), fixture.generation, fixture.parent));
          assertEquals(1, reached.get());
          assertEquals(Records.State.ACTIVE, fixture.view(fixture.parent).state());
          assertNull(fixture.view(fixture.parent).manifest());
        }
        assertEquals(0, fixture.inputs.usage().handles(), loss);
      }
    }
  }

  private static void copy(ExecutionRuntime.Context context, long entity, byte[] expected)
      throws Exception {
    assertEquals(expected.length, context.beginChildOutput(entity, 0).length());
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

  private final class Fixture implements AutoCloseable {
    final AtomicLong now;
    final SessionStore sessions;
    final InputStore inputs;
    final Messages.Binding binding;
    final long generation;
    final Records.WorkKey parent = new Records.WorkKey(0, 0, 1);
    int request = 10;
    Records.InputHeader parentHeader;
    Records.ChildScope child;

    Fixture(String name, int handles, AtomicLong now) throws Exception {
      this.now = now;
      sessions = SessionStore.initialize(directory.resolve(name + ".sqlite"), configuration());
      binding =
          sessions.create(
              access(),
              SELECTED,
              new Messages.Create(1, 1, new Records.Policy(10_000, 100, 30_000)));
      generation = binding.generation();
      inputs =
          InputStore.initializeForAuthority(
              directory.resolve(name + "-inputs"),
              new InputStore.Limits(16L << 20, 128, 1 << 20, handles),
              sessions.identity());
      sessions.bindInputs(inputs);
    }

    void prepareBranch(List<byte[]> outputs, int parentOutputs) throws Exception {
      declareParent();
      parentHeader = admit(parent, new byte[] {42}, 1, parentOutputs, parentOutputs == 0 ? 0 : 5);
      child = view(parent).child();
      assertNotNull(child);
      List<Long> ids = outputs.size() == 1 ? List.of(10L) : List.of(10L, 20L);
      sessions.declare(
          access(),
          SELECTED,
          generation,
          new Messages.Declare(request++, operation(request++), child.scope(), ids, true));
      for (int index = 0; index < outputs.size(); index++) {
        long entity = ids.get(index);
        byte[] output = outputs.get(index);
        Records.WorkKey work = new Records.WorkKey(child.scope(), child.producer(), entity);
        admit(work, new byte[0], 0, 1, output.length);
        Records.WorkView result =
            runtime(
                    ALLOW,
                    context -> {
                      context.beginOutput(output.length, "application/octet-stream");
                      context.writeOutput(ByteBuffer.wrap(output));
                      context.finishOutput();
                      return ExecutionRuntime.Outcome.succeeded();
                    })
                .run(execAccess(), generation, work);
        assertEquals(Records.State.SUCCEEDED, result.state());
      }
      ClosureStore.Cursor cursor = new ClosureStore.Cursor();
      for (int calls = 0; calls < 8; calls++) {
        sessions.reconcileClosures(cursor, 1, clock(now.get()));
        try {
          sessions.scopeSummary(access(), SELECTED, generation, child.scope(), childSeal(ids));
          return;
        } catch (ProtocolError refusal) {
          if (refusal.code() != ProtocolError.Code.NOT_READY) throw refusal;
        }
      }
      fail("child scope did not close");
    }

    void declareParent() throws Exception {
      sessions.declare(
          access(),
          SELECTED,
          generation,
          new Messages.Declare(request++, operation(request++), 0, List.of(1L), false));
    }

    Records.InputHeader admit(
        Records.WorkKey work, byte[] input, int mode, int outputs, long outputBytes)
        throws Exception {
      Records.InputHeader header = receive(work, input, mode, outputs, outputBytes);
      sessions.admit(
          access(), SELECTED, generation, inputs, header, request++, clock(now.get()), ALLOW);
      return header;
    }

    Records.InputHeader receive(
        Records.WorkKey work, byte[] input, int mode, int outputs, long outputBytes)
        throws Exception {
      Records.InputHeader header =
          new Records.InputHeader(
              generation,
              operation(request++),
              new Records.AdmitParameters(
                  work,
                  new Records.Input(input.length, digest(input), "application/octet-stream"),
                  "copy",
                  mode,
                  5000,
                  new Records.OutputBudget(outputs, outputBytes)));
      try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, now.get())) {
        receiver.write(ByteBuffer.wrap(input), now.get());
        receiver.finish(now.get());
      }
      return header;
    }

    Records.Digest childSeal(List<Long> ids) {
      Commitments.Seal seal =
          new Commitments.Seal(context(), child.scope(), child.producer(), parent, ids.size());
      for (long id : ids) seal.add(id);
      return seal.finish();
    }

    ExecutionStore.Lease claimParent() throws Exception {
      return sessions.claimExecution(
          execAccess(), generation, parent, inputs, 500, clock(1100), ALLOW);
    }

    ExecutionRuntime runtime(
        AdmissionStore.Authorization authorization, ExecutionRuntime.Callback callback) {
      return new ExecutionRuntime(
          sessions,
          inputs,
          List.of(new ExecutionRuntime.Registration(APPLICATION, callback)),
          ENDPOINT,
          () -> new AdmissionStore.Time(now.get(), true),
          authorization,
          new ExecutionRuntime.Limits(1, 1, 500, 128));
    }

    Records.WorkView view(Records.WorkKey work) throws Exception {
      return sessions
          .snapshot(access(), SELECTED, generation, new Messages.Watch(request++, work, 0, 0))
          .work();
    }

    Commitments.Context context() {
      return new Commitments.Context("issuer-a", "alice", generation);
    }

    @Override
    public void close() throws java.io.IOException {
      inputs.close();
    }
  }

  private static SessionStore.Configuration configuration() {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(32, 64, 64, 1 << 20, 1 << 20, 8),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        16,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(APPLICATION), 16, 16));
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

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
