package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The Rust authority CLI ({@code pipestream-quinn v2 serve}) as a child process for sealed interop
 * tests: readiness through its ready file, SIGTERM drain on close, bounded log capture.
 */
final class RustAuthorityProcess implements AutoCloseable {
  private final Process process;
  private final Path log;
  private final InetSocketAddress address;

  RustAuthorityProcess(
      Path executable, DurableTestPki pki, Path directory, List<String> storage, String tag)
      throws Exception {
    Path ready = directory.resolve("ready-" + tag);
    log = directory.resolve("rust-server-" + tag + ".log");
    List<String> serve = new ArrayList<>(List.of(executable.toString(), "v2", "serve"));
    serve.addAll(storage);
    serve.addAll(
        List.of(
            "--cert",
            pki.path("server.crt").toString(),
            "--key",
            pki.path("server.key").toString(),
            "--client-ca",
            pki.path("ca.crt").toString(),
            "--result-authority",
            "localhost:7443",
            "--ready-file",
            ready.toString()));
    process =
        new ProcessBuilder(serve)
            .directory(directory.toFile())
            .redirectErrorStream(true)
            .redirectOutput(log.toFile())
            .start();
    address = awaitReady(process, ready, log);
  }

  /**
   * The bound address from the ready file.
   *
   * @return listener address
   */
  InetSocketAddress address() {
    return address;
  }

  /**
   * Run one Rust CLI command to completion and require exit zero.
   *
   * @param directory working directory and log location
   * @param args full command vector
   * @throws Exception on start failure or timeout
   */
  static void command(Path directory, List<String> args) throws Exception {
    Path log = Files.createTempFile(directory, "rust-", ".log");
    Process process =
        new ProcessBuilder(args)
            .directory(directory.toFile())
            .redirectErrorStream(true)
            .redirectOutput(log.toFile())
            .start();
    try {
      assertTrue(process.waitFor(60, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue(), () -> boundedLog(log));
    } finally {
      if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
    }
  }

  static String boundedLog(Path log) {
    try {
      String text = Files.exists(log) ? Files.readString(log) : "";
      return text.substring(Math.max(0, text.length() - 8192));
    } catch (Exception failure) {
      return failure.toString();
    }
  }

  private static InetSocketAddress awaitReady(Process process, Path ready, Path log)
      throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (System.nanoTime() < deadline) {
      if (!process.isAlive()) fail("Rust server exited before readiness: " + boundedLog(log));
      if (Files.isRegularFile(ready)) {
        String[] address = Files.readString(ready).trim().split(":");
        if (address.length == 2)
          return new InetSocketAddress(address[0], Integer.parseInt(address[1]));
      }
      Thread.sleep(10);
    }
    throw new AssertionError("Rust server readiness timeout: " + boundedLog(log));
  }

  /** SIGTERM, then require a clean drain within the CLI's documented grace. */
  @Override
  public void close() {
    process.destroy();
    boolean exited;
    try {
      exited = process.waitFor(20, TimeUnit.SECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new AssertionError("interrupted while draining the Rust server", interrupted);
    }
    assertTrue(exited, "Rust server did not drain after SIGTERM");
    assertEquals(0, process.exitValue(), () -> boundedLog(log));
    assertTrue(boundedLog(log).contains("DRAINED"), () -> boundedLog(log));
  }
}
