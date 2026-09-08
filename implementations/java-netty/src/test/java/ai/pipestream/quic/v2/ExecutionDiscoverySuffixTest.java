package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class ExecutionDiscoverySuffixTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 1 << 20, 8, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(8L << 20, 128, 1 << 20, 8);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void suffixStartsAfterHintKeepsFixedCeilingAndFreshSweepSeesConcurrentTail() throws Exception {
    Path database = directory.resolve("suffix.sqlite");
    Path inputPath = directory.resolve("suffix-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration());
    Messages.Binding binding =
        sessions.create(
            access(),
            SELECTED,
            new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    try (InputStore inputs =
        InputStore.initializeForAuthority(inputPath, INPUT_LIMITS, sessions.identity())) {
      sessions.bindInputs(inputs);
      sessions.declare(
          access(),
          SELECTED,
          binding.generation(),
          new Messages.Declare(2, operation(1), 0, List.of(1L, 2L, 3L), false));
      for (int entity = 1; entity <= 3; entity++) admit(sessions, inputs, binding, entity);

      ExecutionStore.Position one = position(binding, 1);
      ExecutionStore.Position two = position(binding, 2);
      ExecutionStore.Position three = position(binding, 3);
      ExecutionStore.Page first = sessions.scanExecutions(null, one, 1);
      assertEquals(List.of(two), positions(first));
      assertNotNull(first.next());
      assertEquals(three, first.next().through());

      sessions.declare(
          access(),
          SELECTED,
          binding.generation(),
          new Messages.Declare(3, operation(10), 0, List.of(4L), false));
      admit(sessions, inputs, binding, 4);
      ExecutionStore.Position four = position(binding, 4);

      ExecutionStore.Page fixed =
          sessions.scanExecutions(
              first.next(), new ExecutionStore.Position(binding.generation(), 0, 999), 8);
      assertEquals(List.of(three), positions(fixed));
      assertNull(fixed.next());

      ExecutionStore.Page freshSuffix = sessions.scanExecutions(null, one, 8);
      assertEquals(List.of(two, three, four), positions(freshSuffix));
      assertNull(freshSuffix.next());
      ExecutionStore.Page beyond =
          sessions.scanExecutions(
              null, new ExecutionStore.Position(binding.generation(), 0, 999), 8);
      assertTrue(beyond.entries().isEmpty());
      assertNull(beyond.next());

      ExecutionStore.Page ordinary = sessions.scanExecutions(null, 8);
      assertEquals(List.of(one, two, three, four), positions(ordinary));
      assertNull(ordinary.next());
    }
  }

  private static void admit(
      SessionStore sessions, InputStore inputs, Messages.Binding binding, long entity)
      throws Exception {
    Records.WorkKey work = new Records.WorkKey(0, 0, entity);
    Records.InputHeader header =
        new Records.InputHeader(
            binding.generation(),
            operation(100 + (int) entity),
            new Records.AdmitParameters(
                work,
                new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
                "worker",
                0,
                1000,
                new Records.OutputBudget(0, 0)));
    Commitments.Context context =
        new Commitments.Context("issuer-a", binding.owner(), binding.generation());
    try (InputStore.Receiver receiver = inputs.begin(context, header, SELECTED, 1)) {
      receiver.write(ByteBuffer.allocate(0), 2);
      receiver.finish(3);
    }
    sessions.admit(
        access(), SELECTED, binding.generation(), inputs, header, 200 + entity, clock(), ALLOW);
  }

  private static ExecutionStore.Position position(Messages.Binding binding, long entity) {
    return new ExecutionStore.Position(binding.generation(), 0, entity);
  }

  private static List<ExecutionStore.Position> positions(ExecutionStore.Page page) {
    return page.entries().stream().map(ExecutionStore.Candidate::position).toList();
  }

  private static SessionStore.Configuration configuration() {
    AdmissionStore.Application application =
        new AdmissionStore.Application(
            "worker", Set.of(0), AdmissionStore.RestartSafety.IDEMPOTENT);
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(16, 64, 64, 1 << 20, 1 << 20, 8),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        16,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(application), 8, 8));
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

  private static AdmissionStore.Clock clock() {
    return () -> new AdmissionStore.Time(1000, true);
  }

  private static SessionStore.Access access() {
    return new SessionStore.Access("alice", () -> {});
  }
}
