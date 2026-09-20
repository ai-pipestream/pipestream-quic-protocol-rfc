package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Shared MongoDB container for the Mongo backend tests, matching the project
 * convention of real child processes instead of Testcontainers. One server
 * serves all Mongo test classes; the first class starts it, the last one
 * stops and removes it, and a shutdown hook guarantees cleanup even when a
 * test run aborts. The container deliberately has no {@code --rm} so the
 * restart-persistence test can stop and start it without losing its data
 * files. The server runs unauthenticated on localhost, which is acceptable
 * for an ephemeral test container. The image is {@code mongo:8.2} because
 * {@code mongo:8.0} refuses to start on Linux kernels 6.19 and newer
 * (SERVER-121912, this host runs 7.0); the backends target the stable MongoDB
 * server API, not one image build. Docker is required: when the {@code
 * docker} CLI is unavailable every Mongo test class is skipped with an
 * explicit assumption message, keeping the suite portable.
 */
final class MongoDocker implements AutoCloseable {
  private static final String IMAGE = "mongo:8.2";
  private static MongoDocker shared;
  private static int sharedUsers;

  private final String container;
  private final int port;
  private boolean closed;

  private MongoDocker(String container, int port) {
    this.container = container;
    this.port = port;
  }

  /** Starts (or reuses) the shared server; aborts the test class with a clear message when docker is unavailable. */
  static synchronized MongoDocker shared() {
    assumeTrue(dockerAvailable(), "docker CLI is required for the Mongo backend tests");
    if (shared == null) {
      shared = start();
      MongoDocker hookTarget = shared;
      Runtime.getRuntime().addShutdownHook(new Thread(() -> hookTarget.closeContainer()));
    }
    sharedUsers++;
    return shared;
  }

  /** Releases one {@link #shared()} claim; stops and removes the server when the last claim goes away. */
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

  private static MongoDocker start() {
    String container = "pipestream-mongo-test-" + UUID.randomUUID().toString().substring(0, 8);
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    } catch (IOException failure) {
      throw new IllegalStateException("no free port for the mongo container", failure);
    }
    try {
      ProcessResult run = run(List.of("docker", "run", "-d", "--name", container,
          "-p", "127.0.0.1:" + port + ":27017", IMAGE));
      assertTrue(run.exitCode() == 0, "mongo container start failed: " + run.output());
      var mongo = new MongoDocker(container, port);
      mongo.awaitReady();
      return mongo;
    } catch (IOException | InterruptedException failure) {
      runQuiet(List.of("docker", "rm", "-f", container));
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      throw new IllegalStateException("mongo container start failed", failure);
    }
  }

  /** MongoDB connection string of the running server. */
  String connectionString() {
    return "mongodb://127.0.0.1:" + port;
  }

  /** Drops every collection in one database; backend tests reset their shared database this way. */
  void clearDatabase(String database) throws Exception {
    try (var client = com.mongodb.client.MongoClients.create(connectionString())) {
      for (String collection : client.getDatabase(database).listCollectionNames()) {
        client.getDatabase(database).getCollection(collection).deleteMany(new org.bson.Document());
      }
    }
  }

  /** Stops the server without removing the container or its data files; the restart-persistence proof. */
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

  private void awaitReady() throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
    Exception last = null;
    while (System.nanoTime() < deadline) {
      try {
        ProcessResult ready = run(List.of("docker", "exec", container,
            "mongosh", "--quiet", "--eval", "db.runCommand({ping: 1}).ok"));
        if (ready.exitCode() == 0 && ready.output().contains("1")) return;
      } catch (IOException failure) {
        last = failure;
      }
      Thread.sleep(250);
    }
    throw new IllegalStateException("mongo container did not become ready", last);
  }

  private synchronized void closeContainer() {
    if (closed) return;
    closed = true;
    runQuiet(List.of("docker", "rm", "-f", container));
  }

  /** Releases this handle; the shared server stops when the last handle goes away. Idempotent. */
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
