package ai.pipestream.quic.v2;

import ai.pipestream.quic.storage.FileObjectStore;
import ai.pipestream.quic.storage.MongoObjectStore;
import ai.pipestream.quic.storage.ObjectStore;
import ai.pipestream.quic.storage.PostgresObjectStore;
import ai.pipestream.quic.storage.S3ObjectStore;
import ai.pipestream.quic.storage.StorageException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

/**
 * Constructs the SPI object plane's {@link ObjectStore} for one recorded
 * {@link DurableHost.SpiObjectBackend} selection. Non-secret coordinates
 * travel with the selection; credentials are resolved per open from the
 * operator environment, reusing the storage probe's {@code PROBE_*}
 * conventions: {@code PROBE_PG_USER}, {@code PROBE_PG_PASSWORD},
 * {@code PROBE_S3_ACCESS_KEY}, {@code PROBE_S3_SECRET_KEY}, and optionally
 * {@code PROBE_S3_REGION} (default {@code us-east-1}) and
 * {@code PROBE_S3_PREFIX} (default {@code v2-plane/}). Secrets are never
 * persisted or compared.
 */
final class SpiPlaneStores {
  private SpiPlaneStores() {}

  /**
   * Opens the selected backend.
   *
   * @param backend recorded backend selection
   * @param fsRoot filesystem backend root inside the installation
   * @param environment credential and option environment, normally
   *     {@code System.getenv()} merged with operator flags
   * @return exclusively owned object store
   * @throws StorageException construction failure
   */
  static ObjectStore open(
      DurableHost.SpiObjectBackend backend, Path fsRoot, Map<String, String> environment)
      throws StorageException {
    Objects.requireNonNull(backend, "backend");
    Objects.requireNonNull(fsRoot, "fsRoot");
    Objects.requireNonNull(environment, "environment");
    return switch (backend.kind()) {
      case "fs" -> FileObjectStore.open(fsRoot);
      case "postgres" ->
          PostgresObjectStore.open(
              backend.endpoint(), required(environment, "PROBE_PG_USER"),
              required(environment, "PROBE_PG_PASSWORD"));
      case "s3" ->
          S3ObjectStore.open(
              s3Endpoint(backend.endpoint()),
              environment.getOrDefault("PROBE_S3_REGION", "us-east-1"),
              required(environment, "PROBE_S3_ACCESS_KEY"),
              required(environment, "PROBE_S3_SECRET_KEY"),
              true,
              backend.bucket(),
              environment.getOrDefault("PROBE_S3_PREFIX", "v2-plane/"));
      case "mongo" -> MongoObjectStore.open(backend.endpoint(), backend.bucket());
      default -> throw new IllegalArgumentException("unknown SPI backend: " + backend.kind());
    };
  }

  /**
   * Normalizes one S3 service endpoint. Operators and harnesses often pass
   * {@code host:port} without a scheme; MinIO-style endpoints are HTTP, so a
   * missing scheme defaults to {@code http://}. Anything that is not an
   * absolute http(s) URI is refused with a message naming the flag.
   */
  static URI s3Endpoint(String endpoint) {
    if (endpoint == null || endpoint.isEmpty()) {
      throw new IllegalArgumentException("missing --spi-s3-endpoint (or S3 endpoint coordinate)");
    }
    String normalized = endpoint.contains("://") ? endpoint : "http://" + endpoint;
    URI uri = URI.create(normalized);
    if (!uri.isAbsolute()
        || !("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))) {
      throw new IllegalArgumentException(
          "--spi-s3-endpoint must be an absolute http(s) URI: " + endpoint);
    }
    return uri;
  }

  private static String required(Map<String, String> environment, String key) {
    String value = environment.get(key);
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException(
          "missing SPI backend configuration " + key + " (operator flag or environment)");
    }
    return value;
  }
}
