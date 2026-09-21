package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Shared Apache Kafka broker container for the Kafka journal tests, matching
 * the project convention of real child processes instead of Testcontainers.
 * One broker serves all Kafka test classes; the first class starts it, the
 * last one stops and removes it, and a shutdown hook guarantees cleanup even
 * when a test run aborts. The container deliberately has no {@code --rm} so
 * the restart-persistence test can stop and start it without losing its log.
 * The broker listens on the same port inside and outside the container so the
 * advertised listener matches the published port.
 *
 * <p>The apache/kafka:4.2.0 image cannot be configured through its wrapper on
 * container restart: the wrapper regenerates the effective server.properties
 * and reverts to the default listeners whenever storage is already formatted,
 * and any {@code KAFKA_*} environment variable makes the wrapper drop
 * {@code process.roles} entirely (both verified empirically). The fixture
 * therefore bypasses the wrapper: it mounts a complete {@code
 * server.properties} and runs {@code kafka-storage format --standalone} only
 * when the storage directory is unformatted, then execs {@code
 * kafka-server-start.sh} against the mounted config, so every start, initial
 * or restarted, uses the identical configuration. Docker is required: when
 * the {@code docker} CLI is unavailable every Kafka test class is skipped
 * with an explicit assumption message, keeping the suite portable.
 */
final class KafkaDocker implements AutoCloseable {
  private static final String IMAGE = "apache/kafka:4.2.0";
  private static KafkaDocker shared;
  private static int sharedUsers;

  private final String container;
  private final int port;
  private final java.nio.file.Path configDirectory;
  private boolean closed;

  private KafkaDocker(String container, int port, java.nio.file.Path configDirectory) {
    this.container = container;
    this.port = port;
    this.configDirectory = configDirectory;
  }

  /** Starts (or reuses) the shared broker; aborts the test class with a clear message when docker is unavailable. */
  static synchronized KafkaDocker shared() {
    assumeTrue(dockerAvailable(), "docker CLI is required for the Kafka backend tests");
    if (shared == null) {
      shared = start();
      KafkaDocker hookTarget = shared;
      Runtime.getRuntime().addShutdownHook(new Thread(() -> hookTarget.closeContainer()));
    }
    sharedUsers++;
    return shared;
  }

  /** Releases one {@link #shared()} claim; stops and removes the broker when the last claim goes away. */
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

  private static KafkaDocker start() {
    String container = "pipestream-kafka-test-" + UUID.randomUUID().toString().substring(0, 8);
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    } catch (IOException failure) {
      throw new IllegalStateException("no free port for the kafka container", failure);
    }
    java.nio.file.Path configDirectory = writeServerProperties(port);
    String clusterId = java.util.Base64.getUrlEncoder().withoutPadding()
        .encodeToString(randomBytes(16));
    try {
      ProcessResult run = run(List.of("docker", "run", "-d", "--name", container,
          "-v", configDirectory + ":/mnt/shared/config:ro",
          "-p", "127.0.0.1:" + port + ":" + port, IMAGE,
          "sh", "-c",
          "test -f /tmp/kraft-combined-logs/meta.properties"
              + " || /opt/kafka/bin/kafka-storage.sh format"
              + " -t " + clusterId + " -c /mnt/shared/config/server.properties;"
              + " exec /opt/kafka/bin/kafka-server-start.sh /mnt/shared/config/server.properties"));
      assertTrue(run.exitCode() == 0, "kafka container start failed: " + run.output());
      var kafka = new KafkaDocker(container, port, configDirectory);
      kafka.awaitReady();
      return kafka;
    } catch (IOException | InterruptedException failure) {
      runQuiet(List.of("docker", "rm", "-f", container));
      deleteQuiet(configDirectory);
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      throw new IllegalStateException("kafka container start failed", failure);
    }
  }

  private static byte[] randomBytes(int count) {
    byte[] bytes = new byte[count];
    new java.security.SecureRandom().nextBytes(bytes);
    return bytes;
  }

  /**
   * Writes the broker's complete server.properties for the wrapper's mounted
   * config directory: KRaft combined node, one listener on the published port,
   * single-node replication factors for local development.
   */
  private static java.nio.file.Path writeServerProperties(int port) {
    String properties = """
        process.roles=broker,controller
        node.id=1
        controller.quorum.voters=1@localhost:9093
        listeners=PLAINTEXT://:%d,CONTROLLER://:9093
        inter.broker.listener.name=PLAINTEXT
        advertised.listeners=PLAINTEXT://localhost:%d
        controller.listener.names=CONTROLLER
        listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
        num.network.threads=3
        num.io.threads=8
        socket.send.buffer.bytes=102400
        socket.receive.buffer.bytes=102400
        socket.request.max.bytes=104857600
        log.dirs=/tmp/kraft-combined-logs
        num.partitions=1
        num.recovery.threads.per.data.dir=2
        offsets.topic.replication.factor=1
        share.coordinator.state.topic.replication.factor=1
        share.coordinator.state.topic.min.isr=1
        transaction.state.log.replication.factor=1
        transaction.state.log.min.isr=1
        log.retention.hours=168
        log.segment.bytes=1073741824
        log.retention.check.interval.ms=300000
        """.formatted(port, port);
    try {
      java.nio.file.Path directory =
          Files.createTempDirectory("pipestream-kafka-config-");
      Files.writeString(directory.resolve("server.properties"), properties);
      return directory;
    } catch (IOException failure) {
      throw new IllegalStateException("kafka server.properties generation failed", failure);
    }
  }

  private static void deleteQuiet(java.nio.file.Path directory) {
    try (var stream = Files.list(directory)) {
      for (java.nio.file.Path file : stream.toList()) Files.deleteIfExists(file);
      Files.deleteIfExists(directory);
    } catch (IOException failure) {
      /* the directory is empty after startup; leftover files are harmless */
    }
  }

  /** Broker bootstrap servers for journal configuration. */
  String bootstrapServers() {
    return "127.0.0.1:" + port;
  }

  /** Creates a topic with the given partition count; tests use this to pin the single-partition requirement. */
  void createTopic(String topic, int partitions) throws Exception {
    try (org.apache.kafka.clients.admin.AdminClient admin =
        org.apache.kafka.clients.admin.AdminClient.create(adminConfig())) {
      admin.createTopics(List.of(
          new org.apache.kafka.clients.admin.NewTopic(topic, partitions, (short) 1)))
          .all().get(30, TimeUnit.SECONDS);
    }
  }

  /** Stops the broker without removing the container or its log; the restart-persistence proof. */
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

  private Properties adminConfig() {
    Properties config = new Properties();
    config.put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    return config;
  }

  private void awaitReady() throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
    Exception last = null;
    while (System.nanoTime() < deadline) {
      try (org.apache.kafka.clients.admin.AdminClient admin =
          org.apache.kafka.clients.admin.AdminClient.create(adminConfig())) {
        admin.listTopics().listings().get(5, TimeUnit.SECONDS);
        return;
      } catch (Exception failure) {
        last = failure;
        if (failure instanceof InterruptedException) throw (InterruptedException) failure;
      }
      Thread.sleep(500);
    }
    throw new IllegalStateException("kafka container did not become ready", last);
  }

  private synchronized void closeContainer() {
    if (closed) return;
    closed = true;
    runQuiet(List.of("docker", "rm", "-f", container));
    deleteQuiet(configDirectory);
  }

  /** Releases this handle; the shared broker stops when the last handle goes away. Idempotent. */
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
