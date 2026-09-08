package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(150)
final class ExecutionRuntimeResourceTest {
  private static final long LENGTH = 64L << 20;
  private static final int CHUNK = 8192;
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(300L << 20, 16, LENGTH, 4);
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true,
          List.of(DURABLE_WORK, RESULT_DELIVERY),
          List.of(),
          1 << 20,
          8,
          16,
          LENGTH,
          1000,
          300_000);
  private static final AdmissionStore.Application APP =
      new AdmissionStore.Application("copy", Set.of(0), AdmissionStore.RestartSafety.IDEMPOTENT);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void sixtyFourMiBCopyRunsThroughThirtyTwoMiBHeapAndReopensExactly() throws Exception {
    Path root = directory.resolve("resource");
    Path output = directory.resolve("child.out");
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx32m",
                "-cp",
                System.getProperty("java.class.path"),
                ExecutionRuntimeResourceTest.class.getName(),
                root.toString())
            .redirectErrorStream(true)
            .redirectOutput(output.toFile())
            .start();
    try {
      assertTrue(process.waitFor(120, TimeUnit.SECONDS), "runtime resource child did not finish");
      String report = Files.readString(output);
      assertEquals(0, process.exitValue(), report);
      System.out.print(report);
      assertEquals(LENGTH, field(report, "bytes"), report);
      assertEquals(CHUNK, field(report, "buffer"), report);
      long heap = field(report, "heapMax");
      assertTrue(heap > 0 && heap <= 32L << 20, report);
      assertTrue(field(report, "observedRssKiB") > 0, report);
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    long started = System.nanoTime();
    Path root = Path.of(args[0]);
    Path database = root.resolveSibling(root.getFileName() + ".sqlite");
    SessionStore sessions = SessionStore.initialize(database, configuration());
    sessions.create(
        sessionAccess(),
        SELECTED,
        new Messages.Create(1, 1, new Records.Policy(300_000, 300_000, 300_000)));
    sessions.declare(
        sessionAccess(), SELECTED, 1, new Messages.Declare(2, operation(1), 0, List.of(1L), false));
    Records.InputHeader header = header();
    try (InputStore inputs =
        InputStore.initializeForAuthority(root, INPUT_LIMITS, sessions.identity())) {
      sessions.bindInputs(inputs);
      try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, 1)) {
        for (long offset = 0; offset < LENGTH; offset += CHUNK)
          receiver.write(ByteBuffer.wrap(chunk(offset)), offset + 1);
        receiver.finish(LENGTH + 1);
      }
      sessions.admit(
          sessionAccess(),
          SELECTED,
          1,
          inputs,
          header,
          2,
          () -> new AdmissionStore.Time(1000, true),
          ALLOW);
      ExecutionRuntime runtime =
          new ExecutionRuntime(
              sessions,
              inputs,
              List.of(
                  new ExecutionRuntime.Registration(
                      APP,
                      context -> {
                        byte[] buffer = new byte[CHUNK];
                        context.beginOutput(LENGTH, "application/octet-stream");
                        for (int read; (read = context.readInput(buffer, 0, buffer.length)) != -1; )
                          context.writeOutput(ByteBuffer.wrap(buffer, 0, read));
                        context.finishOutput();
                        return ExecutionRuntime.Outcome.succeeded();
                      })),
              new PublicationStore.Endpoint("results.example:7443"),
              () -> new AdmissionStore.Time(1000, true),
              ALLOW,
              new ExecutionRuntime.Limits(1, 1, 300_000, CHUNK));
      Records.WorkView succeeded = runtime.run(execAccess(), 1, new Records.WorkKey(0, 0, 1));
      assertEquals(Records.State.SUCCEEDED, succeeded.state());
      assertEquals(digest(), succeeded.manifest().outputs().get(0).sha256());
    }

    long bytes = 0;
    MessageDigest observed = MessageDigest.getInstance("SHA-256");
    SessionStore reopened = SessionStore.open(database, configuration());
    try (InputStore inputs = InputStore.open(root, INPUT_LIMITS)) {
      reopened.verifyInputs(inputs);
      Records.WorkView view =
          reopened
              .snapshot(
                  sessionAccess(),
                  SELECTED,
                  1,
                  new Messages.Watch(9, new Records.WorkKey(0, 0, 1), 0, 0))
              .work();
      Records.Output descriptor = view.manifest().outputs().get(0);
      ExecutionStore.Lease locator =
          new ExecutionStore.Lease(
              reopened.identity(), "alice", 1, descriptor.locator().target().work(), 1, 1, 1);
      try (var input =
          inputs.findOutput(context(), header, locator, 0).orElseThrow().openStream()) {
        byte[] buffer = new byte[CHUNK];
        for (int read; (read = input.read(buffer)) != -1; ) {
          observed.update(buffer, 0, read);
          bytes += read;
        }
      }
      assertEquals(0, inputs.usage().handles());
    }
    assertEquals(LENGTH, bytes);
    byte[] actualDigest = observed.digest();
    assertArrayEquals(digest().bytes(), actualDigest);
    System.out.printf(
        "bytes=%d digest=%s buffer=%d heapMax=%d observedRssKiB=%d elapsedMillis=%d%n",
        bytes,
        HexFormat.of().formatHex(actualDigest),
        CHUNK,
        Runtime.getRuntime().maxMemory(),
        rssKiB(),
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
  }

  private static SessionStore.Configuration configuration() {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(4, 4, 4, LENGTH, LENGTH, 1),
        new Records.Policy(300_000, 300_000, 300_000),
        1,
        2,
        1,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(APP), 1, 1));
  }

  private static Records.InputHeader header() throws Exception {
    return new Records.InputHeader(
        1,
        operation(2),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, 1),
            new Records.Input(LENGTH, digest(), "application/octet-stream"),
            "copy",
            0,
            300_000,
            new Records.OutputBudget(1, LENGTH)));
  }

  private static Records.Digest digest() throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    for (long offset = 0; offset < LENGTH; offset += CHUNK) digest.update(chunk(offset));
    return new Records.Digest(digest.digest());
  }

  private static byte[] chunk(long offset) {
    byte[] bytes = new byte[CHUNK];
    for (int index = 0; index < bytes.length; index++)
      bytes[index] = (byte) ((offset + index) * 31 + 17);
    return bytes;
  }

  private static long field(String report, String name) {
    var matcher = java.util.regex.Pattern.compile("(?:^|\\s)" + name + "=([0-9]+)").matcher(report);
    assertTrue(matcher.find(), report);
    return Long.parseLong(matcher.group(1));
  }

  private static long rssKiB() throws Exception {
    try (BufferedReader status =
        new BufferedReader(
            new InputStreamReader(Files.newInputStream(Path.of("/proc/self/status"))))) {
      for (String line; (line = status.readLine()) != null; )
        if (line.startsWith("VmRSS:")) return Long.parseLong(line.replaceAll("[^0-9]", ""));
    }
    throw new AssertionError("VmRSS absent");
  }

  private static Commitments.Context context() {
    return new Commitments.Context("issuer-a", "alice", 1);
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static SessionStore.Access sessionAccess() {
    return new SessionStore.Access("alice", () -> {});
  }

  private static ExecutionStore.Access execAccess() {
    return new ExecutionStore.Access("alice", () -> {});
  }
}
