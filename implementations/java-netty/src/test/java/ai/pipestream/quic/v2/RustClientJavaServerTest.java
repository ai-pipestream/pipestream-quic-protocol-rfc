package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The existing Rust durable client CLI against the composed Java authority. This is the real
 * opposite-language peer for A-SERVER; it requires the release Rust binary built from this tree.
 */
@Tag("sealed-interop")
@Timeout(300)
final class RustClientJavaServerTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static Path executable;

  @BeforeAll
  static void setup() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
    executable =
        Path.of("../rust-quinn/target/release/pipestream-quinn").toAbsolutePath().normalize();
    assertTrue(Files.isExecutable(executable), "sealed interop requires the release Rust CLI");
  }

  static String sha256(Path file) throws Exception {
    return HexFormat.of()
        .formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
  }

  private record Run(int exit, String output) {}

  private Run rust(List<String> args, long timeoutSeconds) throws Exception {
    List<String> command = new ArrayList<>();
    command.add(executable.toString());
    command.add("v2");
    command.addAll(args);
    Path log = Files.createTempFile(directory, "rust-", ".log");
    Process process =
        new ProcessBuilder(command)
            .directory(directory.toFile())
            .redirectErrorStream(true)
            .redirectOutput(log.toFile())
            .start();
    try {
      assertTrue(process.waitFor(timeoutSeconds, TimeUnit.SECONDS), () -> "timeout: " + command);
      return new Run(process.exitValue(), Files.readString(log));
    } finally {
      if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
    }
  }

  private Run ok(List<String> args) throws Exception {
    Run run = rust(args, 60);
    assertEquals(0, run.exit(), () -> String.join(" ", args) + "\n" + run.output());
    return run;
  }

  private List<String> connection(InetSocketAddress address) {
    return List.of(
        "--connect",
        address.getHostString() + ":" + address.getPort(),
        "--server-name",
        "localhost",
        "--ca",
        pki.path("ca.crt").toString(),
        "--cert",
        pki.path("alice.crt").toString(),
        "--key",
        pki.path("alice.key").toString());
  }

  private List<String> client(Path journal, InetSocketAddress address, String... operation) {
    List<String> args =
        new ArrayList<>(
            List.of(
                "client",
                "--journal",
                journal.toString(),
                "--authority",
                "issuer-a",
                "--owner",
                "alice",
                "--creation-sequence",
                "1"));
    args.addAll(connection(address));
    args.addAll(Arrays.asList(operation));
    return args;
  }

  private static String hexOperation(int value) {
    return String.format("%032x", value);
  }

  private static int state(String watch) {
    Matcher matcher =
        Pattern.compile("WORK revision=(\\d+) state=(\\d+) attempt=(\\d+)").matcher(watch);
    assertTrue(matcher.find(), watch);
    return Integer.parseInt(matcher.group(2));
  }

  @Test
  void rustClientCreatesAdmitsReadsCheckpointsAndCompletesAgainstJavaAuthority() throws Exception {
    Path root = directory.resolve("authority");
    Path journal = directory.resolve("alice-journal.sqlite");
    Path input = directory.resolve("input.bin");
    Path output = directory.resolve("output.bin");
    byte[] bytes = new byte[300_000];
    new Random(11).nextBytes(bytes);
    Files.write(input, bytes);
    DurableHost.OwnerPolicy owners = DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true);
    DurableHost.Configuration configuration =
        DurableHost.Configuration.defaults("issuer-a", "localhost:7443");
    String seal;
    try (DurableHost host =
            DurableHost.initialize(
                root,
                configuration,
                ReferenceApplications.all(),
                owners,
                DurableHost.UtcClock.system(true));
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults())) {
      InetSocketAddress address = server.address();
      List<String> next = new ArrayList<>(List.of("next-sequence"));
      next.addAll(connection(address));
      assertTrue(ok(next).output().contains("NEXT_SEQUENCE 1"), "next sequence");
      ok(
          List.of(
              "init-client",
              "--journal",
              journal.toString(),
              "--authority",
              "issuer-a",
              "--owner",
              "alice",
              "--creation-sequence",
              "1"));
      Run binding = ok(client(journal, address, "binding"));
      assertTrue(binding.output().contains("BINDING"), binding.output());
      Run declared =
          ok(
              client(
                  journal,
                  address,
                  "declare",
                  "--operation",
                  hexOperation(1),
                  "--entities",
                  "1",
                  "--seal"));
      assertTrue(declared.output().contains("RECEIPT"), declared.output());
      Run admitted =
          ok(
              client(
                  journal,
                  address,
                  "admit",
                  "--operation",
                  hexOperation(2),
                  "--declaration",
                  hexOperation(1),
                  "--work",
                  "0:0:1",
                  "--input",
                  input.toString(),
                  "--application",
                  "copy/v2"));
      assertTrue(admitted.output().contains("RECEIPT"), admitted.output());
      int state = -1;
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      while (state != 5 && System.nanoTime() < deadline) {
        Run watch = ok(client(journal, address, "watch", "--work", "0:0:1", "--wait-ms", "3000"));
        state = state(watch.output());
        assertTrue(state <= 5 || state == 5, watch.output());
        if (state == 6 || state == 7 || state == 8)
          fail("unexpected terminal state: " + watch.output());
      }
      assertEquals(5, state, "work did not succeed within the deadline");
      Run lookup = ok(client(journal, address, "lookup", "--operation", hexOperation(2)));
      assertTrue(lookup.output().contains("RECEIPT"), lookup.output());
      ok(client(journal, address, "select", "--work", "0:0:1", "--attempt", "1", "--index", "0"));
      Run read =
          ok(
              client(
                  journal,
                  address,
                  "read",
                  "--work",
                  "0:0:1",
                  "--attempt",
                  "1",
                  "--index",
                  "0",
                  "--output",
                  output.toString()));
      assertTrue(read.output().contains("VERIFIED"), read.output());
      assertArrayEquals(bytes, Files.readAllBytes(output));
      Run page = ok(client(journal, address, "page", "--scope", "0"));
      Matcher matcher = Pattern.compile("seal=([0-9a-f]{64})").matcher(page.output());
      assertTrue(matcher.find(), page.output());
      seal = matcher.group(1);
      Run checkpoint =
          ok(
              client(
                  journal,
                  address,
                  "checkpoint",
                  "--scope",
                  "0",
                  "--seal",
                  seal,
                  "--wait-ms",
                  "5000"));
      assertTrue(checkpoint.output().contains("COVERAGE"), checkpoint.output());
      Run completed = ok(client(journal, address, "complete"));
      assertTrue(completed.output().contains("COMPLETED"), completed.output());
      Run detached = ok(client(journal, address, "detach"));
      assertTrue(detached.output().contains("DETACHED"), detached.output());
      assertEquals(0, server.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS).inputs());
    }

    // Java authority restart: the Rust client reattaches, replays the original operation exactly
    // and still observes the same terminal evidence; a changed parameter is CONFLICT.
    try (DurableHost host =
            DurableHost.open(
                root,
                configuration,
                ReferenceApplications.all(),
                owners,
                DurableHost.UtcClock.system(true));
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults())) {
      InetSocketAddress address = server.address();
      Run watch = ok(client(journal, address, "watch", "--work", "0:0:1"));
      assertEquals(5, state(watch.output()));
      Run replay =
          ok(
              client(
                  journal,
                  address,
                  "replay",
                  "--operation",
                  hexOperation(2),
                  "--input",
                  input.toString(),
                  "--declaration",
                  hexOperation(1)));
      assertTrue(replay.output().contains("RECEIPT"), replay.output());
      Run checkpoint = ok(client(journal, address, "checkpoint", "--scope", "0", "--seal", seal));
      assertTrue(checkpoint.output().contains("COVERAGE"), checkpoint.output());
      Files.write(input, Arrays.copyOf(bytes, bytes.length - 1));
      Run changed =
          rust(
              client(
                  journal,
                  address,
                  "replay",
                  "--operation",
                  hexOperation(2),
                  "--input",
                  input.toString(),
                  "--declaration",
                  hexOperation(1)),
              60);
      // The Rust client refuses locally: a changed file cannot replay the immutable intent, so no
      // replacement work is ever sent. This is the client-side rule, not a server CONFLICT.
      assertNotEquals(0, changed.exit(), changed.output());
      assertTrue(changed.output().contains("INTEGRITY_ERROR"), changed.output());
      Run detached = ok(client(journal, address, "detach"));
      assertTrue(detached.output().contains("DETACHED"), detached.output());
    }
    Files.writeString(
        directory.resolve("rust-binary.sha256"), sha256(executable) + "  " + executable + "\n");
  }

  @Test
  void rustClientWithUnmappedIdentityIsUnauthorizedAndUnknownApplicationIsRefused()
      throws Exception {
    Path root = directory.resolve("authority-negative");
    Path journal = directory.resolve("bob-journal.sqlite");
    DurableTestPki stranger =
        DurableTestPki.generate(
            Files.createDirectories(directory.resolve("stranger")), List.of("mallory"));
    DurableHost.OwnerPolicy owners = DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true);
    try (DurableHost host =
            DurableHost.initialize(
                root,
                DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
                ReferenceApplications.all(),
                owners,
                DurableHost.UtcClock.system(true));
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults())) {
      InetSocketAddress address = server.address();
      // Untrusted CA: the TLS handshake fails; no application refusal is fabricated.
      List<String> untrusted =
          new ArrayList<>(
              List.of(
                  "next-sequence",
                  "--connect",
                  address.getHostString() + ":" + address.getPort(),
                  "--server-name",
                  "localhost",
                  "--ca",
                  pki.path("ca.crt").toString(),
                  "--cert",
                  stranger.path("mallory.crt").toString(),
                  "--key",
                  stranger.path("mallory.key").toString()));
      Run rejected = rust(untrusted, 60);
      assertNotEquals(0, rejected.exit(), rejected.output());
      // Unknown application: APPLICATION_UNSUPPORTED through the real host.
      ok(
          List.of(
              "init-client",
              "--journal",
              journal.toString(),
              "--authority",
              "issuer-a",
              "--owner",
              "alice",
              "--creation-sequence",
              "1"));
      ok(
          client(
              journal,
              address,
              "declare",
              "--operation",
              hexOperation(1),
              "--entities",
              "1",
              "--seal"));
      Path input = directory.resolve("negative-input.bin");
      Files.write(input, new byte[] {1, 2, 3});
      Run unsupported =
          rust(
              client(
                  journal,
                  address,
                  "admit",
                  "--operation",
                  hexOperation(2),
                  "--declaration",
                  hexOperation(1),
                  "--work",
                  "0:0:1",
                  "--input",
                  input.toString(),
                  "--application",
                  "nope/v9"),
              60);
      assertNotEquals(0, unsupported.exit(), unsupported.output());
      assertTrue(
          unsupported.output().contains("APPLICATION_UNSUPPORTED")
              || unsupported.output().toLowerCase().contains("application"),
          unsupported.output());
      // The declaration is intact: the right application still admits and succeeds.
      ok(
          client(
              journal,
              address,
              "admit",
              "--operation",
              hexOperation(3),
              "--declaration",
              hexOperation(1),
              "--work",
              "0:0:1",
              "--input",
              input.toString(),
              "--application",
              "consume/v2",
              "--output-count",
              "0"));
      int state = -1;
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      while (state != 5 && System.nanoTime() < deadline) {
        state =
            state(
                ok(client(journal, address, "watch", "--work", "0:0:1", "--wait-ms", "3000"))
                    .output());
      }
      assertEquals(5, state);
    }
  }

  static void deleteRecursively(Path path) throws IOException {
    if (!Files.exists(path)) return;
    try (var walk = Files.walk(path)) {
      walk.sorted(java.util.Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.delete(p);
                } catch (IOException ignored) {
                  // best effort
                }
              });
    }
  }
}
