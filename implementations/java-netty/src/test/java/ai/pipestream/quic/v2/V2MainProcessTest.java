package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The shipped V2 launcher as real processes: {@code V2Main serve} drained by SIGTERM and {@code
 * V2Main client} operations against it, all Java, all separate JVMs. This is the process contract
 * the neutral driver consumes.
 */
@Timeout(300)
final class V2MainProcessTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static String classpath;
  static String javaBinary;

  @BeforeAll
  static void setup() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    classpath = System.getProperty("java.class.path");
    javaBinary = Path.of(System.getProperty("java.home"), "bin", "java").toString();
  }

  private record Run(int exit, String output) {}

  private static Run java(List<String> args, long timeoutSeconds) throws Exception {
    List<String> command =
        new ArrayList<>(
            List.of(
                javaBinary,
                "--enable-native-access=ALL-UNNAMED",
                "-cp",
                classpath,
                "ai.pipestream.quic.v2.V2Main"));
    command.addAll(args);
    Path log = Files.createTempFile(directory, "v2main-", ".log");
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

  private static Run ok(List<String> args) throws Exception {
    Run run = java(args, 120);
    assertEquals(0, run.exit(), () -> String.join(" ", args) + "\n" + run.output());
    return run;
  }

  private static String boundedLog(Path log) {
    try {
      String text = Files.exists(log) ? Files.readString(log) : "";
      return text.substring(Math.max(0, text.length() - 8192));
    } catch (Exception failure) {
      return failure.toString();
    }
  }

  /** A {@code V2Main serve} process with readiness marker and SIGTERM drain. */
  static final class JavaServer implements AutoCloseable {
    final Process process;
    final Path log;
    final InetSocketAddress address;

    JavaServer(Path root, Path principals, String tag, List<String> extra) throws Exception {
      Path ready = directory.resolve("java-ready-" + tag);
      log = directory.resolve("java-server-" + tag + ".log");
      List<String> command =
          new ArrayList<>(
              List.of(
                  javaBinary,
                  "--enable-native-access=ALL-UNNAMED",
                  "-cp",
                  classpath,
                  "ai.pipestream.quic.v2.V2Main",
                  "serve",
                  "--root",
                  root.toString(),
                  "--authority",
                  "issuer-a",
                  "--result-authority",
                  "localhost:7443",
                  "--bind",
                  "127.0.0.1:0",
                  "--cert",
                  pki.path("server.crt").toString(),
                  "--key",
                  pki.path("server.key").toString(),
                  "--client-ca",
                  pki.path("ca.crt").toString(),
                  "--principal-map",
                  principals.toString(),
                  "--trust-system-clock",
                  "--ready-file",
                  ready.toString()));
      command.addAll(extra);
      process =
          new ProcessBuilder(command)
              .directory(directory.toFile())
              .redirectErrorStream(true)
              .redirectOutput(log.toFile())
              .start();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      InetSocketAddress bound = null;
      while (System.nanoTime() < deadline) {
        if (!process.isAlive()) fail("Java server exited before readiness: " + boundedLog(log));
        if (Files.isRegularFile(ready)) {
          String[] parts = Files.readString(ready).trim().split(":");
          if (parts.length == 2) {
            bound = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
            break;
          }
        }
        Thread.sleep(20);
      }
      assertNotNull(bound, () -> "Java server readiness timeout: " + boundedLog(log));
      address = bound;
    }

    @Override
    public void close() {
      process.destroy();
      boolean exited;
      try {
        exited = process.waitFor(60, TimeUnit.SECONDS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new AssertionError(interrupted);
      }
      assertTrue(exited, () -> "Java server did not drain after SIGTERM: " + boundedLog(log));
      assertEquals(0, process.exitValue(), () -> boundedLog(log));
      assertTrue(boundedLog(log).contains("DRAINED"), () -> boundedLog(log));
    }
  }

  private static List<String> connection(InetSocketAddress address) {
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

  private static List<String> client(Path journal, InetSocketAddress address, String... operation) {
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
  void launcherProcessesCompleteTheWholeCombinationAndSurviveServerRestart() throws Exception {
    Path root = directory.resolve("authority");
    Path principals = directory.resolve("principals.tsv");
    pki.principalMap(principals, List.of("alice"));
    Path journal = directory.resolve("journal.sqlite");
    Path input = directory.resolve("input.bin");
    Path output = directory.resolve("output.bin");
    byte[] bytes = new byte[150_000];
    new Random(3).nextBytes(bytes);
    Files.write(input, bytes);

    // Reopen before initialization is an error, never an empty authority.
    Run missing =
        java(
            List.of(
                "serve",
                "--root",
                root.toString(),
                "--authority",
                "issuer-a",
                "--result-authority",
                "localhost:7443",
                "--cert",
                pki.path("server.crt").toString(),
                "--key",
                pki.path("server.key").toString(),
                "--client-ca",
                pki.path("ca.crt").toString(),
                "--principal-map",
                principals.toString(),
                "--trust-system-clock"),
            60);
    assertNotEquals(0, missing.exit(), missing.output());
    Run initialized =
        ok(
            List.of(
                "init-authority",
                "--root",
                root.toString(),
                "--authority",
                "issuer-a",
                "--result-authority",
                "localhost:7443"));
    assertTrue(initialized.output().contains("INITIALIZED"), initialized.output());
    Run again =
        java(
            List.of(
                "init-authority",
                "--root",
                root.toString(),
                "--authority",
                "issuer-a",
                "--result-authority",
                "localhost:7443"),
            60);
    assertNotEquals(0, again.exit(), "initialization must not overwrite existing history");
    // Serving without the explicit clock assertion is refused.
    Run noClock =
        java(
            List.of(
                "serve",
                "--root",
                root.toString(),
                "--authority",
                "issuer-a",
                "--result-authority",
                "localhost:7443",
                "--cert",
                pki.path("server.crt").toString(),
                "--key",
                pki.path("server.key").toString(),
                "--client-ca",
                pki.path("ca.crt").toString(),
                "--principal-map",
                principals.toString()),
            60);
    assertNotEquals(0, noClock.exit(), noClock.output());

    String seal;
    try (JavaServer server = new JavaServer(root, principals, "first", List.of())) {
      List<String> next = new ArrayList<>(List.of("next-sequence"));
      next.addAll(connection(server.address));
      assertTrue(ok(next).output().contains("NEXT_SEQUENCE 1"));
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
      assertTrue(ok(client(journal, server.address, "binding")).output().contains("BINDING"));
      assertTrue(
          ok(client(
                  journal,
                  server.address,
                  "declare",
                  "--operation",
                  hexOperation(1),
                  "--entities",
                  "1",
                  "--seal"))
              .output()
              .contains("RECEIPT"));
      Run admitted =
          ok(
              client(
                  journal,
                  server.address,
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
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
      while (state != 5 && System.nanoTime() < deadline) {
        Run watch =
            ok(client(journal, server.address, "watch", "--work", "0:0:1", "--wait-ms", "3000"));
        state = state(watch.output());
        assertTrue(state < 6, watch.output());
      }
      assertEquals(5, state);
      assertTrue(
          ok(client(journal, server.address, "unresolved")).output().isBlank()
              || !ok(client(journal, server.address, "unresolved"))
                  .output()
                  .contains("UNRESOLVED"));
      ok(
          client(
              journal,
              server.address,
              "select",
              "--work",
              "0:0:1",
              "--attempt",
              "1",
              "--index",
              "0"));
      Run read =
          ok(
              client(
                  journal,
                  server.address,
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
      Run page = ok(client(journal, server.address, "page", "--scope", "0"));
      Matcher matcher = Pattern.compile("seal=([0-9a-f]{64})").matcher(page.output());
      assertTrue(matcher.find(), page.output());
      seal = matcher.group(1);
      assertTrue(
          ok(client(
                  journal,
                  server.address,
                  "checkpoint",
                  "--scope",
                  "0",
                  "--seal",
                  seal,
                  "--wait-ms",
                  "5000"))
              .output()
              .contains("COVERAGE"));
      assertTrue(ok(client(journal, server.address, "complete")).output().contains("COMPLETED"));
      assertTrue(ok(client(journal, server.address, "detach")).output().contains("DETACHED"));
    }

    try (JavaServer server = new JavaServer(root, principals, "second", List.of())) {
      assertEquals(
          5, state(ok(client(journal, server.address, "watch", "--work", "0:0:1")).output()));
      Run replay =
          ok(
              client(
                  journal,
                  server.address,
                  "replay",
                  "--operation",
                  hexOperation(2),
                  "--input",
                  input.toString()));
      assertTrue(replay.output().contains("RECEIPT"), replay.output());
      Files.write(input, Arrays.copyOf(bytes, bytes.length - 1));
      Run changed =
          java(
              client(
                  journal,
                  server.address,
                  "replay",
                  "--operation",
                  hexOperation(2),
                  "--input",
                  input.toString()),
              60);
      assertNotEquals(0, changed.exit(), changed.output());
      assertTrue(changed.output().contains("INTEGRITY_ERROR"), changed.output());
      assertFalse(
          changed.output().contains("REFUSED"), "a local check is not an authority refusal");
      // An authority refusal is named on stdout for the driver, with its code and diagnostic.
      Run stale =
          java(
              client(
                  journal,
                  server.address,
                  "retry",
                  "--operation",
                  hexOperation(7),
                  "--work",
                  "0:0:1",
                  "--expected-attempt",
                  "9"),
              60);
      assertNotEquals(0, stale.exit(), stale.output());
      assertTrue(stale.output().contains("REFUSED code="), stale.output());
      assertTrue(stale.output().contains(" detail="), stale.output());
      assertTrue(
          ok(client(journal, server.address, "checkpoint", "--scope", "0", "--seal", seal))
              .output()
              .contains("COVERAGE"));
    }
  }
}
