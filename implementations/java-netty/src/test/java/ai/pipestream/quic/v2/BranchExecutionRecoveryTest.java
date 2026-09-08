package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(45)
final class BranchExecutionRecoveryTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true,
          List.of(DURABLE_WORK, RESULT_DELIVERY),
          List.of(),
          1 << 20,
          8,
          16,
          1 << 20,
          1000,
          10_000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(8L << 20, 64, 1 << 20, 3);
  private static final Records.WorkKey PARENT = new Records.WorkKey(0, 0, 1);
  private static final byte[] FIRST = {1, 2, 3};
  private static final byte[] SECOND = {4, 5};
  private static final byte[] COMBINED = {1, 2, 3, 4, 5};
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void interruptedOpenChildReaderIsRecoveredOnlyAfterLeaseExpiry() throws Exception {
    Path database = directory.resolve("authority.sqlite");
    Path inputPath = directory.resolve("inputs");
    Path output = directory.resolve("child.out");
    Path error = directory.resolve("child.err");
    Process child =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                BranchExecutionRecoveryTest.class.getName(),
                database.toString(),
                inputPath.toString())
            .redirectOutput(output.toFile())
            .redirectError(error.toFile())
            .start();
    try {
      assertTrue(child.waitFor(10, TimeUnit.SECONDS), "branch crash child did not exit");
      assertEquals(126, child.exitValue(), boundedText(error, 8192));
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        assertTrue(child.waitFor(2, TimeUnit.SECONDS));
      }
    }
    ChildReport report = report(output);
    assertEquals(1, report.attempt());
    assertEquals(1, report.lease());
    assertEquals(2100, report.until());

    SessionStore sessions = SessionStore.open(database, configuration());
    InputStore.Usage funded;
    Records.WorkView firstChild;
    Records.WorkView secondChild;
    Records.ScopeSummary root;
    try (InputStore inputs = InputStore.open(inputPath, INPUT_LIMITS)) {
      sessions.verifyInputs(inputs);
      funded = inputs.usage();
      assertEquals(report.bytes(), funded.bytes());
      assertEquals(report.files(), funded.files());
      assertEquals(0, funded.handles());
      Records.WorkView retained = view(sessions, PARENT, 80);
      assertEquals(Records.State.ACTIVE, retained.state());
      assertEquals(1, retained.attempt());
      assertNull(retained.manifest());
      firstChild =
          view(sessions, new Records.WorkKey(report.childScope(), report.childProducer(), 10), 81);
      secondChild =
          view(sessions, new Records.WorkKey(report.childScope(), report.childProducer(), 20), 82);
      assertEquals(digest(FIRST), firstChild.manifest().outputs().get(0).sha256());
      assertEquals(digest(SECOND), secondChild.manifest().outputs().get(0).sha256());

      AtomicInteger early = new AtomicInteger();
      assertCode(
          ProtocolError.Code.NOT_READY,
          () ->
              runtime(
                      sessions,
                      inputs,
                      context -> {
                        early.incrementAndGet();
                        return ExecutionRuntime.Outcome.succeeded();
                      },
                      clock(2099),
                      2000)
                  .run(execAccess(), 1, PARENT));
      assertEquals(0, early.get());
      assertEquals(0, inputs.usage().handles());

      ExecutionStore.Lease[] replacement = new ExecutionStore.Lease[1];
      Records.WorkView succeeded =
          runtime(
                  sessions,
                  inputs,
                  context -> {
                    replacement[0] = context.lease();
                    context.beginOutput(COMBINED.length, "application/octet-stream");
                    copy(context, 10);
                    copy(context, 20);
                    context.finishOutput();
                    return ExecutionRuntime.Outcome.succeeded();
                  },
                  clock(2101),
                  2000)
              .run(execAccess(), 1, PARENT);
      assertNotNull(replacement[0]);
      assertEquals(1, replacement[0].attempt());
      assertEquals(2, replacement[0].number());
      assertEquals(4101, replacement[0].until());
      assertEquals(1, succeeded.attempt());
      assertEquals(Records.State.SUCCEEDED, succeeded.state());
      assertEquals(digest(COMBINED), succeeded.manifest().outputs().get(0).sha256());
      assertEquals(firstChild, view(sessions, firstChild.work(), 83));
      assertEquals(secondChild, view(sessions, secondChild.work(), 84));
      assertEquals(funded, inputs.usage());

      Records.Digest rootSeal = seal(0, 0, null, List.of(1L));
      ClosureStore.Cursor cursor = new ClosureStore.Cursor();
      root = null;
      for (int calls = 0; calls < 8 && root == null; calls++) {
        sessions.reconcileClosures(cursor, 1, clock(2200));
        try {
          root = sessions.scopeSummary(sessionAccess(), SELECTED, 1, 0, rootSeal);
        } catch (ProtocolError refusal) {
          if (refusal.code() != ProtocolError.Code.NOT_READY) throw refusal;
        }
      }
      assertNotNull(root);
      assertEquals(new Records.Counts(1, 0, 0, 0), root.counts());
      assertEquals(0, inputs.usage().handles());
    }

    SessionStore reopened = SessionStore.open(database, configuration());
    try (InputStore inputs = InputStore.open(inputPath, INPUT_LIMITS)) {
      reopened.verifyInputs(inputs);
      Records.WorkView retained = view(reopened, PARENT, 90);
      assertEquals(Records.State.SUCCEEDED, retained.state());
      assertEquals(digest(COMBINED), retained.manifest().outputs().get(0).sha256());
      assertEquals(firstChild, view(reopened, firstChild.work(), 91));
      assertEquals(secondChild, view(reopened, secondChild.work(), 92));
      assertEquals(root, reopened.scopeSummary(sessionAccess(), SELECTED, 1, 0, root.seal()));
      assertEquals(funded, inputs.usage());
    }
  }

  @Test
  void swallowedMissingChildBytesRemainStorageFailureWithoutComputedOutcome() throws Exception {
    Path database = directory.resolve("missing.sqlite");
    Path inputPath = directory.resolve("missing-inputs");
    try (Fixture fixture = Fixture.create(database, inputPath, List.of(FIRST), 1000)) {
      Path installed;
      try (var files = Files.list(inputPath.resolve("outputs"))) {
        installed = files.findFirst().orElseThrow();
      }
      Files.move(installed, inputPath.resolve("hidden-output"));
      AtomicInteger caught = new AtomicInteger();

      assertThrows(
          IOException.class,
          () ->
              runtime(
                      fixture.sessions,
                      fixture.inputs,
                      context -> {
                        try {
                          context.beginChildOutput(10, 0);
                        } catch (IOException expected) {
                          caught.incrementAndGet();
                        }
                        return ExecutionRuntime.Outcome.succeeded();
                      },
                      clock(1100),
                      1000)
                  .run(execAccess(), 1, PARENT));
      assertEquals(1, caught.get());
      Records.WorkView retained = view(fixture.sessions, PARENT, 70);
      assertEquals(Records.State.ACTIVE, retained.state());
      assertNull(retained.manifest());
      assertEquals(0, fixture.inputs.usage().handles());
    }
  }

  public static void main(String[] args) throws Exception {
    try (Fixture fixture =
        Fixture.create(Path.of(args[0]), Path.of(args[1]), List.of(FIRST, SECOND), 1000)) {
      InputStore.Usage funded = fixture.inputs.usage();
      runtime(
              fixture.sessions,
              fixture.inputs,
              context -> {
                Records.Output selected = context.beginChildOutput(10, 0);
                if (selected.length() != FIRST.length)
                  throw new AssertionError("wrong child output");
                if (context.readChildOutput(new byte[1], 0, 1) != 1)
                  throw new AssertionError("child reader did not open");
                ExecutionStore.Lease lease = context.lease();
                System.out.printf(
                    "attempt=%d lease=%d until=%d child=%d producer=%d bytes=%d files=%d%n",
                    lease.attempt(),
                    lease.number(),
                    lease.until(),
                    fixture.child.scope(),
                    fixture.child.producer(),
                    funded.bytes(),
                    funded.files());
                System.out.flush();
                Runtime.getRuntime().halt(126);
                throw new AssertionError("halt returned");
              },
              clock(1100),
              1000)
          .run(execAccess(), 1, PARENT);
      throw new AssertionError("branch crash callback was not reached");
    }
  }

  private static void copy(ExecutionRuntime.Context context, long entity) throws Exception {
    context.beginChildOutput(entity, 0);
    byte[] buffer = new byte[2];
    for (int read; (read = context.readChildOutput(buffer, 0, buffer.length)) != -1; )
      context.writeOutput(ByteBuffer.wrap(buffer, 0, read));
    context.finishChildOutput();
  }

  private static final class Fixture implements AutoCloseable {
    final SessionStore sessions;
    final InputStore inputs;
    final Messages.Binding binding;
    Records.ChildScope child;
    int request = 10;

    static Fixture create(Path database, Path inputPath, List<byte[]> children, long now)
        throws Exception {
      SessionStore sessions = SessionStore.initialize(database, configuration());
      Messages.Binding binding =
          sessions.create(
              sessionAccess(),
              SELECTED,
              new Messages.Create(1, 1, new Records.Policy(20_000, 20_000, 30_000)));
      assertEquals(1, binding.generation());
      InputStore inputs = null;
      try {
        inputs = InputStore.initializeForAuthority(inputPath, INPUT_LIMITS, sessions.identity());
        sessions.bindInputs(inputs);
        Fixture fixture = new Fixture(sessions, inputs, binding);
        fixture.prepare(children, now);
        return fixture;
      } catch (Exception | Error failure) {
        if (inputs != null) {
          try {
            inputs.close();
          } catch (IOException cleanup) {
            failure.addSuppressed(cleanup);
          }
        }
        throw failure;
      }
    }

    private Fixture(SessionStore sessions, InputStore inputs, Messages.Binding binding)
        throws Exception {
      this.sessions = sessions;
      this.inputs = inputs;
      this.binding = binding;
    }

    private void prepare(List<byte[]> children, long now) throws Exception {
      sessions.declare(
          sessionAccess(),
          SELECTED,
          1,
          new Messages.Declare(request++, operation(request++), 0, List.of(1L), true));
      Records.InputHeader parent = header(PARENT, operation(request++), new byte[] {42}, 1, 1, 5);
      receiveAndAdmit(parent, new byte[] {42}, now);
      Records.ChildScope allocated = view(sessions, PARENT, request++).child();
      assertNotNull(allocated);
      child = allocated;
      List<Long> ids = children.size() == 1 ? List.of(10L) : List.of(10L, 20L);
      sessions.declare(
          sessionAccess(),
          SELECTED,
          1,
          new Messages.Declare(request++, operation(request++), allocated.scope(), ids, true));
      for (int index = 0; index < children.size(); index++) {
        byte[] payload = children.get(index);
        Records.WorkKey work =
            new Records.WorkKey(allocated.scope(), allocated.producer(), ids.get(index));
        Records.InputHeader input =
            header(work, operation(request++), new byte[0], 0, 1, payload.length);
        receiveAndAdmit(input, new byte[0], now);
        Records.WorkView published =
            runtime(
                    sessions,
                    inputs,
                    context -> {
                      context.beginOutput(payload.length, "application/octet-stream");
                      context.writeOutput(ByteBuffer.wrap(payload));
                      context.finishOutput();
                      return ExecutionRuntime.Outcome.succeeded();
                    },
                    clock(now),
                    1000)
                .run(execAccess(), 1, work);
        if (published.state() != Records.State.SUCCEEDED)
          throw new AssertionError("child publication failed");
      }
      ClosureStore.Cursor cursor = new ClosureStore.Cursor();
      Records.Digest expected = seal(allocated.scope(), allocated.producer(), PARENT, ids);
      for (int calls = 0; calls < 8; calls++) {
        sessions.reconcileClosures(cursor, 1, clock(now));
        try {
          sessions.scopeSummary(sessionAccess(), SELECTED, 1, allocated.scope(), expected);
          return;
        } catch (ProtocolError refusal) {
          if (refusal.code() != ProtocolError.Code.NOT_READY) throw refusal;
        }
      }
      throw new AssertionError("child scope did not close");
    }

    private void receiveAndAdmit(Records.InputHeader header, byte[] payload, long now)
        throws Exception {
      try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, now)) {
        receiver.write(ByteBuffer.wrap(payload), now);
        receiver.finish(now);
      }
      sessions.admit(sessionAccess(), SELECTED, 1, inputs, header, request++, clock(now), ALLOW);
    }

    @Override
    public void close() throws IOException {
      inputs.close();
    }
  }

  private static ExecutionRuntime runtime(
      SessionStore sessions,
      InputStore inputs,
      ExecutionRuntime.Callback callback,
      AdmissionStore.Clock clock,
      long leaseMillis) {
    return new ExecutionRuntime(
        sessions,
        inputs,
        List.of(new ExecutionRuntime.Registration(application(), callback)),
        ENDPOINT,
        clock,
        ALLOW,
        new ExecutionRuntime.Limits(1, 1, leaseMillis, 128));
  }

  private static SessionStore.Configuration configuration() {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(16, 32, 32, 1 << 20, 1 << 20, 4),
        new Records.Policy(60_000, 60_000, 60_000),
        3,
        12,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(application()), 8, 8));
  }

  private static AdmissionStore.Application application() {
    return new AdmissionStore.Application(
        "copy", Set.of(0, 1), AdmissionStore.RestartSafety.IDEMPOTENT);
  }

  private static Records.InputHeader header(
      Records.WorkKey work,
      Records.OperationId operation,
      byte[] input,
      int mode,
      int outputs,
      long outputBytes)
      throws Exception {
    return new Records.InputHeader(
        1,
        operation,
        new Records.AdmitParameters(
            work,
            new Records.Input(input.length, digest(input), "application/octet-stream"),
            "copy",
            mode,
            10_000,
            new Records.OutputBudget(outputs, outputBytes)));
  }

  private static Records.WorkView view(SessionStore sessions, Records.WorkKey work, int request)
      throws Exception {
    return sessions
        .snapshot(sessionAccess(), SELECTED, 1, new Messages.Watch(request, work, 0, 0))
        .work();
  }

  private static Records.Digest seal(
      long scope, int producer, Records.WorkKey parent, List<Long> ids) {
    Commitments.Seal seal = new Commitments.Seal(context(), scope, producer, parent, ids.size());
    for (long id : ids) seal.add(id);
    return seal.finish();
  }

  private static ChildReport report(Path output) throws Exception {
    byte[] bytes;
    try (InputStream input = Files.newInputStream(output)) {
      bytes = input.readNBytes(320);
      assertEquals(-1, input.read());
    }
    String text = new String(bytes, StandardCharsets.UTF_8);
    assertTrue(
        text.matches(
            "attempt=[0-9]+ lease=[0-9]+ until=[0-9]+ child=[0-9]+ producer=[0-9]+ bytes=[0-9]+"
                + " files=[0-9]+\\n"),
        text);
    String[] fields = text.trim().split(" ");
    return new ChildReport(
        value(fields[0]),
        value(fields[1]),
        value(fields[2]),
        value(fields[3]),
        Math.toIntExact(value(fields[4])),
        value(fields[5]),
        value(fields[6]));
  }

  private static long value(String field) {
    return Long.parseLong(field.substring(field.indexOf('=') + 1));
  }

  private static String boundedText(Path path, int limit) throws Exception {
    if (!Files.exists(path)) return "";
    try (InputStream input = Files.newInputStream(path)) {
      byte[] bytes = input.readNBytes(limit + 1);
      return new String(bytes, 0, Math.min(bytes.length, limit), StandardCharsets.UTF_8);
    }
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

  private static Commitments.Context context() {
    return new Commitments.Context("issuer-a", "alice", 1);
  }

  private static SessionStore.Access sessionAccess() {
    return new SessionStore.Access("alice", () -> {});
  }

  private static ExecutionStore.Access execAccess() {
    return new ExecutionStore.Access("alice", () -> {});
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  private record ChildReport(
      long attempt,
      long lease,
      long until,
      long childScope,
      int childProducer,
      long bytes,
      long files) {}

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
