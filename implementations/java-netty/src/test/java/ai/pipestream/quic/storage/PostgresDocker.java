package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Shared {@code postgres:16-alpine} container for the Postgres backend tests,
 * matching the project convention of real child processes instead of
 * Testcontainers. One container serves all Postgres test classes; the first
 * class starts it, the last one stops and removes it, and a shutdown hook
 * guarantees cleanup even when a test run aborts. The container deliberately
 * has no {@code --rm} so the kill/resume test can stop and restart it without
 * losing its data. Docker is required: when the {@code docker} CLI is
 * unavailable every Postgres test class is skipped with an explicit assumption
 * message, keeping the suite portable.
 */
final class PostgresDocker implements AutoCloseable {
  private static final String IMAGE = "postgres:16-alpine";
  private static final String USER = "postgres";
  private static final String PASSWORD = "pipestream";
  private static final String DATABASE = "pipestream";
  private static PostgresDocker shared;
  private static int sharedUsers;

  private final String container;
  private final String url;
  private boolean closed;

  private PostgresDocker(String container, String url) {
    this.container = container;
    this.url = url;
  }

  /** Starts (or reuses) the shared container; aborts the test class with a clear message when docker is unavailable. */
  static synchronized PostgresDocker shared() {
    assumeTrue(dockerAvailable(), "docker CLI is required for the Postgres backend tests");
    if (shared == null) {
      shared = start();
      PostgresDocker hookTarget = shared;
      Runtime.getRuntime().addShutdownHook(new Thread(() -> hookTarget.closeContainer()));
    }
    sharedUsers++;
    return shared;
  }

  /** Releases one {@link #shared()} claim; stops and removes the container when the last claim goes away. */
  static synchronized void releaseShared() {
    if (sharedUsers <= 0) return;
    sharedUsers--;
    if (sharedUsers == 0 && shared != null) {
      shared.closeContainer();
      shared = null;
    }
  }

  private static boolean dockerAvailable() {
    try {
      return run(List.of("docker", "version", "--format", "{{.Server.Version}}")).exitCode() == 0;
    } catch (IOException | InterruptedException failure) {
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      return false;
    }
  }

  private static PostgresDocker start() {
    String container = "pipestream-pg-test-" + UUID.randomUUID().toString().substring(0, 8);
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    } catch (IOException failure) {
      throw new IllegalStateException("no free port for the postgres container", failure);
    }
    try {
      ProcessResult run = run(List.of("docker", "run", "-d", "--name", container,
          "-e", "POSTGRES_PASSWORD=" + PASSWORD, "-e", "POSTGRES_DB=" + DATABASE,
          "-p", "127.0.0.1:" + port + ":5432", IMAGE));
      assertTrue(run.exitCode() == 0, "postgres container start failed: " + run.output());
      var docker = new PostgresDocker(container,
          "jdbc:postgresql://127.0.0.1:" + port + "/" + DATABASE);
      docker.awaitReady();
      return docker;
    } catch (IOException | InterruptedException failure) {
      runQuiet(List.of("docker", "rm", "-f", container));
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      throw new IllegalStateException("postgres container start failed", failure);
    }
  }

  /** JDBC URL of the running container. */
  String url() {
    return url;
  }

  /** Database role. */
  String user() {
    return USER;
  }

  /** Database password. */
  String password() {
    return PASSWORD;
  }

  /** Opens a fresh administrative JDBC connection for test setup and tamper helpers. */
  Connection connect() throws java.sql.SQLException {
    return DriverManager.getConnection(url, USER, PASSWORD);
  }

  /** Stops the postgres server without removing the container or its data; the kill/resume durability proof. */
  void stopServer() throws Exception {
    ProcessResult stopped = run(List.of("docker", "stop", container));
    assertTrue(stopped.exitCode() == 0, "docker stop failed: " + stopped.output());
  }

  /** Restarts a container previously stopped via {@link #stopServer()}. */
  void startServer() throws Exception {
    ProcessResult started = run(List.of("docker", "start", container));
    assertTrue(started.exitCode() == 0, "docker start failed: " + started.output());
    awaitReady();
  }

  private void awaitReady() throws IOException, InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
    Exception last = null;
    while (System.nanoTime() < deadline) {
      try {
        ProcessResult ready = run(List.of("docker", "exec", container,
            "pg_isready", "-U", USER, "-d", DATABASE));
        if (ready.exitCode() == 0) {
          try (Connection connection = connect()) {
            if (connection.isValid(2)) return;
          } catch (java.sql.SQLException notReady) {
            last = notReady;
          }
        }
      } catch (IOException failure) {
        last = failure;
      }
      Thread.sleep(250);
    }
    throw new IllegalStateException("postgres container did not become ready", last);
  }

  private synchronized void closeContainer() {
    if (closed) return;
    closed = true;
    runQuiet(List.of("docker", "rm", "-f", container));
  }

  /** Releases this handle; the shared container stops when the last handle goes away. Idempotent. */
  @Override public void close() {
    releaseShared();
  }

  private static void runQuiet(List<String> command) {
    try {
      run(command);
    } catch (IOException | InterruptedException failure) {
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
    }
  }

  private static ProcessResult run(List<String> command) throws IOException, InterruptedException {
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    byte[] output = process.getInputStream().readAllBytes();
    if (!process.waitFor(90, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      throw new IOException("command timed out: " + String.join(" ", command));
    }
    return new ProcessResult(process.exitValue(), new String(output, StandardCharsets.UTF_8).trim());
  }

  private record ProcessResult(int exitCode, String output) {}
}
