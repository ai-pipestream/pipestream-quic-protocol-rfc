package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Shared MinIO container for the S3 backend tests, matching the project
 * convention of real child processes instead of Testcontainers. One container
 * serves all S3 test classes; the first class starts it, the last one stops
 * and removes it, and a shutdown hook guarantees cleanup even when a test run
 * aborts. The image is {@code quay.io/minio/minio} because MinIO no longer
 * publishes community server images to Docker Hub. The container deliberately
 * has no {@code --rm} so the restart-persistence test can stop and start it
 * without losing its data volume. Docker is required: when the {@code docker}
 * CLI is unavailable every S3 test class is skipped with an explicit
 * assumption message, keeping the suite portable.
 */
public final class MinioDocker implements AutoCloseable {
  private static final String IMAGE = "quay.io/minio/minio:latest";
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";
  private static final String BUCKET = "pipestream-test";
  private static MinioDocker shared;
  private static int sharedUsers;

  private final String container;
  private final int port;
  private boolean closed;

  private MinioDocker(String container, int port) {
    this.container = container;
    this.port = port;
  }

  /** Starts (or reuses) the shared container; aborts the test class with a clear message when docker is unavailable. */
  public static synchronized MinioDocker shared() {
    assumeTrue(dockerAvailable(), "docker CLI is required for the S3 backend tests");
    if (shared == null) {
      shared = start();
      MinioDocker hookTarget = shared;
      Runtime.getRuntime().addShutdownHook(new Thread(() -> hookTarget.closeContainer()));
    }
    sharedUsers++;
    return shared;
  }

  /** Releases one {@link #shared()} claim; stops and removes the container when the last claim goes away. */
  public static synchronized void releaseShared() {
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

  private static MinioDocker start() {
    String container = "pipestream-minio-test-" + UUID.randomUUID().toString().substring(0, 8);
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    } catch (IOException failure) {
      throw new IllegalStateException("no free port for the minio container", failure);
    }
    try {
      ProcessResult run = run(List.of("docker", "run", "-d", "--name", container,
          "-e", "MINIO_ROOT_USER=" + ACCESS_KEY, "-e", "MINIO_ROOT_PASSWORD=" + SECRET_KEY,
          "-p", "127.0.0.1:" + port + ":9000", IMAGE, "server", "/data"));
      assertTrue(run.exitCode() == 0, "minio container start failed: " + run.output());
      var docker = new MinioDocker(container, port);
      docker.awaitReady();
      docker.createBucket();
      return docker;
    } catch (IOException | InterruptedException failure) {
      runQuiet(List.of("docker", "rm", "-f", container));
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      throw new IllegalStateException("minio container start failed", failure);
    }
  }

  /** S3 service endpoint of the running container. */
  public URI endpoint() {
    return URI.create("http://127.0.0.1:" + port);
  }

  /** Service access key. */
  public String accessKey() {
    return ACCESS_KEY;
  }

  /** Service secret key. */
  public String secretKey() {
    return SECRET_KEY;
  }

  /** Shared bucket created at startup; every test store uses a unique key prefix inside it. */
  public String bucket() {
    return BUCKET;
  }

  /** Stops the MinIO server without removing the container or its data; the restart-persistence proof. */
  public void stopServer() throws Exception {
    ProcessResult stopped = run(List.of("docker", "stop", container));
    assertTrue(stopped.exitCode() == 0, "docker stop failed: " + stopped.output());
  }

  /** Restarts a container previously stopped via {@link #stopServer()}. */
  public void startServer() throws Exception {
    ProcessResult started = run(List.of("docker", "start", container));
    assertTrue(started.exitCode() == 0, "docker start failed: " + started.output());
    awaitReady();
  }

  /** Builds a short-lived raw S3 client against the container, for test assertions and tamper helpers. */
  software.amazon.awssdk.services.s3.S3Client newClient() {
    return software.amazon.awssdk.services.s3.S3Client.builder()
        .endpointOverride(endpoint())
        .region(software.amazon.awssdk.regions.Region.of("us-east-1"))
        .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
            software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
        .serviceConfiguration(software.amazon.awssdk.services.s3.S3Configuration.builder()
            .pathStyleAccessEnabled(true).build())
        .build();
  }

  private void createBucket() throws IOException, InterruptedException {
    try (var s3 = newClient()) {
      s3.createBucket(request -> request.bucket(BUCKET));
    } catch (software.amazon.awssdk.services.s3.model.S3Exception failure) {
      if (failure.statusCode() != 409) throw failure; // bucket already exists
    }
  }

  private void awaitReady() throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
    Exception last = null;
    HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    while (System.nanoTime() < deadline) {
      try {
        HttpResponse<Void> response = client.send(HttpRequest.newBuilder()
            .uri(endpoint().resolve("/minio/health/ready"))
            .timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() == 200) return;
      } catch (IOException | InterruptedException failure) {
        last = failure;
        if (failure instanceof InterruptedException) throw (InterruptedException) failure;
      }
      Thread.sleep(250);
    }
    throw new IllegalStateException("minio container did not become ready", last);
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
