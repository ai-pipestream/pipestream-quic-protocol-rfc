package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;

/** Real paired metadata/object-store publication fixture for result tests. */
final class ResultFixture implements AutoCloseable {
  static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);
  static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true,
          List.of(DURABLE_WORK, RESULT_DELIVERY),
          List.of(),
          1 << 20,
          8,
          16,
          1 << 20,
          1000,
          5000);
  static final InputStore.Limits INPUT_LIMITS = new InputStore.Limits(8L << 20, 64, 1 << 20, 8);
  static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");
  static final AdmissionStore.Authorization ALLOW_EXECUTION = (binding, parameters) -> {};

  final Path database;
  final Path inputsPath;
  final byte[] payload;
  final Records.Digest payloadDigest;
  final Records.InputHeader inputHeader;
  final Messages.Binding binding;
  final ExecutionStore.Lease lease;
  final Records.WorkView published;
  SessionStore sessions;
  InputStore inputs;

  ResultFixture(Path directory, String name, byte[] payload) throws Exception {
    this(directory, name, new byte[0], payload, true, 1);
  }

  ResultFixture(Path directory, String name, byte[] payload, boolean publish) throws Exception {
    this(directory, name, new byte[0], payload, publish, 1);
  }

  ResultFixture(Path directory, String name, byte[] inputPayload, byte[] payload) throws Exception {
    this(directory, name, inputPayload, payload, true, 1);
  }

  ResultFixture(Path directory, String name, byte[] inputPayload, byte[] payload, boolean publish)
      throws Exception {
    this(directory, name, inputPayload, payload, publish, 1);
  }

  ResultFixture(Path directory, String name, byte[] inputPayload, byte[] payload, int outputCount)
      throws Exception {
    this(directory, name, inputPayload, payload, true, outputCount);
  }

  private ResultFixture(
      Path directory,
      String name,
      byte[] inputPayload,
      byte[] payload,
      boolean publish,
      int outputCount)
      throws Exception {
    database = directory.resolve(name + ".sqlite");
    inputsPath = directory.resolve(name + "-inputs");
    this.payload = payload.clone();
    payloadDigest = digest(payload);
    sessions = SessionStore.initialize(database, configuration());
    binding =
        sessions.create(
            sessionAccess("alice"),
            SELECTED,
            new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    sessions.declare(
        sessionAccess("alice"),
        SELECTED,
        binding.generation(),
        new Messages.Declare(2, operation(1), 0, List.of(1L), false));
    inputs = InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
    sessions.bindInputs(inputs);
    inputHeader =
        new Records.InputHeader(
            binding.generation(),
            operation(2),
            new Records.AdmitParameters(
                WORK,
                new Records.Input(
                    inputPayload.length, digest(inputPayload), "application/octet-stream"),
                "copy",
                0,
                1000,
                new Records.OutputBudget(outputCount, payload.length)));
    try (InputStore.Receiver receiver = inputs.begin(context(), inputHeader, SELECTED, 1)) {
      receiver.write(ByteBuffer.wrap(inputPayload), 2);
      receiver.finish(3);
    }
    sessions.admit(
        sessionAccess("alice"),
        SELECTED,
        binding.generation(),
        inputs,
        inputHeader,
        3,
        clock(1000),
        ALLOW_EXECUTION);
    lease =
        sessions.claimExecution(
            executionAccess("alice"),
            binding.generation(),
            WORK,
            inputs,
            500,
            clock(1100),
            ALLOW_EXECUTION);
    if (publish && outputCount > 0) {
      try (OutputStore.Writer writer =
          inputs.beginOutput(
              context(),
              inputHeader,
              lease,
              0,
              payload.length,
              "application/octet-stream",
              payload.length)) {
        writer.write(ByteBuffer.wrap(payload));
        writer.finish();
      }
    }
    if (publish) {
      published =
          sessions.succeedExecution(
              executionAccess("alice"),
              lease,
              inputs,
              outputCount,
              ENDPOINT,
              clock(1200),
              ALLOW_EXECUTION);
    } else {
      published =
          sessions
              .snapshot(
                  sessionAccess("alice"),
                  SELECTED,
                  binding.generation(),
                  new Messages.Watch(4, WORK, 0, 0))
              .work();
    }
  }

  Records.Output output() {
    return published.manifest().outputs().get(0);
  }

  Commitments.Context context() {
    return new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
  }

  void reopen() throws Exception {
    inputs.close();
    sessions = SessionStore.open(database, configuration());
    inputs = InputStore.open(inputsPath, INPUT_LIMITS);
    sessions.verifyInputs(inputs);
  }

  @Override
  public void close() throws java.io.IOException {
    inputs.close();
  }

  static SessionStore.Configuration configuration() {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(8, 16, 16, 1 << 20, 1 << 20, 4),
        new Records.Policy(60_000, 60_000, 60_000),
        2,
        8,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(
            List.of(
                new AdmissionStore.Application(
                    "copy", Set.of(0, 1), AdmissionStore.RestartSafety.IDEMPOTENT)),
            4,
            4));
  }

  static Records.Digest digest(byte[] bytes) throws Exception {
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

  static AdmissionStore.Clock clock(long utc) {
    return () -> new AdmissionStore.Time(utc, true);
  }

  static SessionStore.Access sessionAccess(String owner) {
    return new SessionStore.Access(owner, () -> {});
  }

  static ExecutionStore.Access executionAccess(String owner) {
    return new ExecutionStore.Access(owner, () -> {});
  }
}
