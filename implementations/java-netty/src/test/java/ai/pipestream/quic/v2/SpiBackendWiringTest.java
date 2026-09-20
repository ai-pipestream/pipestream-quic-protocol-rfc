package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.storage.MinioDocker;
import ai.pipestream.quic.storage.MongoDocker;
import ai.pipestream.quic.storage.ObjectStore;
import ai.pipestream.quic.storage.PostgresDocker;
import ai.pipestream.quic.storage.ScopePath;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * SPI object backend selection wiring: {@link SpiPlaneStores} constructs each
 * backend kind from a recorded {@link DurableHost.SpiObjectBackend} selection
 * and the operator environment; installed payload bytes land in the selected
 * backend (verified independently through {@code listScopes}); and reopen
 * refuses a changed selection with the installation's config-exactness idiom.
 * Docker fixtures follow the shared lifecycle and skip with an explicit
 * assumption when docker is unavailable.
 */
final class SpiBackendWiringTest {
  private static final InputStore.Limits LIMITS = new InputStore.Limits(16L << 20, 32, 8 << 20, 8);
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65536, 4, 16, 1 << 20, 1000, 5000);

  @TempDir Path directory;

  @Test
  void filesystemBackendInstallsAndVerifiesIndependently() throws Exception {
    Path root = directory.resolve("fs-store");
    Path backendRoot = directory.resolve("fs-backend");
    DurableHost.SpiObjectBackend selection = DurableHost.SpiObjectBackend.fs();
    try (ObjectStore backend = SpiPlaneStores.open(selection, backendRoot, Map.of())) {
      assertEquals(List.of(), backend.listScopes());
      install(root, backend, payload());
    }
    try (ObjectStore independent = SpiPlaneStores.open(selection, backendRoot, Map.of())) {
      assertInstalled(independent, payload());
    }
    DurableHost.SpiObjectBackend postgresElsewhere =
        new DurableHost.SpiObjectBackend("postgres", "jdbc:postgresql://127.0.0.1:1/unused", null);
    hostRoundTrip(root.resolve("host"), selection, postgresElsewhere, Map.of());
  }

  @Test
  void postgresBackendInstallsVerifiesAndRefusesSelectionChange() throws Exception {
    PostgresDocker postgres = PostgresDocker.shared();
    try {
      String database = "spibackend" + UUID.randomUUID().toString().replace("-", "");
      try (java.sql.Connection admin = postgres.connect();
          java.sql.Statement statement = admin.createStatement()) {
        statement.execute("CREATE DATABASE \"" + database + "\"");
      }
      String serverUrl =
          postgres.url().substring(0, postgres.url().lastIndexOf('/') + 1);
      DurableHost.SpiObjectBackend selection =
          new DurableHost.SpiObjectBackend("postgres", serverUrl + database, null);
      Map<String, String> environment =
          Map.of("PROBE_PG_USER", postgres.user(), "PROBE_PG_PASSWORD", postgres.password());
      try (ObjectStore backend = SpiPlaneStores.open(selection, directory.resolve("unused"), environment)) {
        install(directory.resolve("pg-store"), backend, payload());
      }
      try (ObjectStore independent =
          SpiPlaneStores.open(selection, directory.resolve("unused"), environment)) {
        assertInstalled(independent, payload());
      }
      hostRoundTrip(directory.resolve("pg-host"), selection, DurableHost.SpiObjectBackend.fs(), environment);
    } finally {
      PostgresDocker.releaseShared();
    }
  }

  @Test
  void s3BackendInstallsVerifiesAndRefusesSelectionChange() throws Exception {
    MinioDocker minio = MinioDocker.shared();
    try {
      String prefix = "backend-" + UUID.randomUUID() + "/";
      DurableHost.SpiObjectBackend selection =
          new DurableHost.SpiObjectBackend("s3", minio.endpoint().toString(), minio.bucket());
      Map<String, String> environment =
          Map.of(
              "PROBE_S3_ACCESS_KEY", minio.accessKey(),
              "PROBE_S3_SECRET_KEY", minio.secretKey(),
              "PROBE_S3_REGION", "us-east-1",
              "PROBE_S3_PREFIX", prefix);
      try (ObjectStore backend = SpiPlaneStores.open(selection, directory.resolve("unused"), environment)) {
        install(directory.resolve("s3-store"), backend, payload());
      }
      try (ObjectStore independent =
          SpiPlaneStores.open(selection, directory.resolve("unused"), environment)) {
        assertInstalled(independent, payload());
      }
      hostRoundTrip(directory.resolve("s3-host"), selection, DurableHost.SpiObjectBackend.fs(), environment);
    } finally {
      MinioDocker.releaseShared();
    }
  }

  @Test
  void mongoBackendInstallsVerifiesAndRefusesSelectionChange() throws Exception {
    MongoDocker mongo = MongoDocker.shared();
    try {
      String database = "spibackend" + UUID.randomUUID().toString().replace("-", "");
      DurableHost.SpiObjectBackend selection =
          new DurableHost.SpiObjectBackend("mongo", mongo.connectionString(), database);
      try (ObjectStore backend = SpiPlaneStores.open(selection, directory.resolve("unused"), Map.of())) {
        install(directory.resolve("mongo-store"), backend, payload());
      }
      try (ObjectStore independent =
          SpiPlaneStores.open(selection, directory.resolve("unused"), Map.of())) {
        assertInstalled(independent, payload());
      }
      hostRoundTrip(directory.resolve("mongo-host"), selection, DurableHost.SpiObjectBackend.fs(), Map.of());
    } finally {
      MongoDocker.releaseShared();
    }
  }

  @Test
  void postgresLibpqUrlFormOpensThroughV2MainFlags() throws Exception {
    PostgresDocker postgres = PostgresDocker.shared();
    try {
      String database = "spibackend" + UUID.randomUUID().toString().replace("-", "");
      try (java.sql.Connection admin = postgres.connect();
          java.sql.Statement statement = admin.createStatement()) {
        statement.execute("CREATE DATABASE \"" + database + "\"");
      }
      String serverUrl =
          postgres.url().substring(0, postgres.url().lastIndexOf('/') + 1);
      // The conformance harness passes PROBE_PG_URL in libpq form.
      String authority = postgres.url().substring("jdbc:postgresql://".length());
      String hostPort = authority.substring(0, authority.indexOf('/'));
      String libpqUrl = "postgres://ignored:ignored@" + hostPort + "/" + database;
      Map<String, String> options =
          Map.of(
              "authority", "issuer-a",
              "result-authority", "localhost:7443",
              "object-plane", "spi",
              "spi-backend", "postgres",
              "spi-pg-url", libpqUrl);
      DurableHost.Configuration configuration = V2Main.configuration(options);
      assertEquals(libpqUrl, configuration.spiBackend().endpoint());
      Map<String, String> environment =
          Map.of("PROBE_PG_USER", postgres.user(), "PROBE_PG_PASSWORD", postgres.password());
      Path root = directory.resolve("libpq-host");
      try (DurableHost host =
          DurableHost.initialize(
              root,
              configuration,
              ReferenceApplications.all(),
              DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
              DurableHost.UtcClock.system(true),
              environment)) {
        assertEquals("postgres", host.configuration().spiBackend().kind());
      }
      try (DurableHost reopened =
          DurableHost.open(
              root,
              configuration,
              ReferenceApplications.all(),
              DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
              DurableHost.UtcClock.system(true),
              environment)) {
        assertEquals("postgres", reopened.configuration().spiBackend().kind());
      }
    } finally {
      PostgresDocker.releaseShared();
    }
  }

  @Test
  void s3InitThroughV2MainFlagsAcceptsSchemelessEndpoints() throws Exception {
    MinioDocker minio = MinioDocker.shared();
    try {
      String prefix = "backend-" + UUID.randomUUID() + "/";
      // Harnesses pass endpoints as host:port without a scheme; the wiring
      // must normalize (MinIO-style endpoints are HTTP) instead of failing
      // inside the SDK with an opaque wrapper.
      String schemeless =
          "127.0.0.1:" + minio.endpoint().getPort() + minio.endpoint().getPath();
      Map<String, String> options =
          Map.of(
              "authority", "issuer-a",
              "result-authority", "localhost:7443",
              "object-plane", "spi",
              "spi-backend", "s3",
              "spi-s3-endpoint", schemeless,
              "spi-s3-bucket", minio.bucket(),
              "spi-s3-access-key", minio.accessKey(),
              "spi-s3-secret-key", minio.secretKey());
      DurableHost.Configuration configuration = V2Main.configuration(options);
      assertEquals(schemeless, configuration.spiBackend().endpoint(), "raw coordinate is recorded");
      Map<String, String> environment =
          V2Main.spiEnvironment(
              new java.util.HashMap<>(
                  Map.of(
                      "spi-s3-access-key", minio.accessKey(),
                      "spi-s3-secret-key", minio.secretKey(),
                      "spi-s3-region", "us-east-1",
                      "spi-s3-prefix", prefix)));
      Path root = directory.resolve("s3-flags-host");
      try (DurableHost host =
          DurableHost.initialize(
              root,
              configuration,
              ReferenceApplications.all(),
              DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
              DurableHost.UtcClock.system(true),
              environment)) {
        assertEquals("s3", host.configuration().spiBackend().kind());
      }
      try (DurableHost reopened =
          DurableHost.open(
              root,
              configuration,
              ReferenceApplications.all(),
              DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
              DurableHost.UtcClock.system(true),
              environment)) {
        assertEquals("s3", reopened.configuration().spiBackend().kind());
      }
    } finally {
      MinioDocker.releaseShared();
    }
  }

  @Test
  void s3InitWithoutAnyRegionSucceedsOnMinio() throws Exception {
    MinioDocker minio = MinioDocker.shared();
    try {
      Map<String, String> options =
          Map.of(
              "authority", "issuer-a",
              "result-authority", "localhost:7443",
              "object-plane", "spi",
              "spi-backend", "s3",
              "spi-s3-endpoint", minio.endpoint().toString(),
              "spi-s3-bucket", minio.bucket(),
              "spi-s3-access-key", minio.accessKey(),
              "spi-s3-secret-key", minio.secretKey());
      DurableHost.Configuration configuration = V2Main.configuration(options);
      // No flag and no environment region: the subject default applies, and
      // a blank value must behave exactly like an unset one.
      Map<String, String> withoutRegion = V2Main.spiEnvironment(new java.util.HashMap<>(options));
      assertFalse(withoutRegion.containsKey("PROBE_S3_REGION"));
      Map<String, String> blankRegion =
          V2Main.spiEnvironment(
              new java.util.HashMap<>(
                  Map.of(
                      "spi-s3-access-key", minio.accessKey(),
                      "spi-s3-secret-key", minio.secretKey(),
                      "spi-s3-region", "  ")));
      assertFalse(blankRegion.containsKey("PROBE_S3_REGION"), "blank region behaves as unset");
      int variant = 0;
      for (Map<String, String> environment :
          List.of(
              withoutRegion,
              blankRegion,
              Map.of(
                  "PROBE_S3_ACCESS_KEY", minio.accessKey(),
                  "PROBE_S3_SECRET_KEY", minio.secretKey(),
                  "PROBE_S3_REGION", ""))) {
        Path root = directory.resolve("s3-noregion-" + variant++);
        try (DurableHost host =
            DurableHost.initialize(
                root,
                configuration,
                ReferenceApplications.all(),
                DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
                DurableHost.UtcClock.system(true),
                environment)) {
          assertEquals("s3", host.configuration().spiBackend().kind());
        }
      }
    } finally {
      MinioDocker.releaseShared();
    }
  }

  @Test
  void nonFilesystemSelectionRequiresSpiPlaneAndCredentials() {
    DurableHost.SpiObjectBackend missing =
        new DurableHost.SpiObjectBackend("postgres", "jdbc:postgresql://127.0.0.1:1/x", null);
    assertThrows(
        IllegalArgumentException.class,
        () -> SpiPlaneStores.open(missing, directory.resolve("unused"), Map.of()));
    Map<String, String> options = Map.of("spi-backend", "s3", "spi-s3-endpoint", "http://127.0.0.1:9",
        "spi-s3-bucket", "bucket");
    assertThrows(IllegalArgumentException.class, () -> V2Main.spiBackend(options));
    assertEquals(
        "value",
        V2Main.spiEnvironment(Map.of("spi-pg-user", "value")).get("PROBE_PG_USER"));
  }

  private void hostRoundTrip(
      Path root,
      DurableHost.SpiObjectBackend selection,
      DurableHost.SpiObjectBackend changed,
      Map<String, String> environment)
      throws Exception {
    DurableHost.Configuration configuration =
        new DurableHost.Configuration(
            DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
            DurableHost.ObjectPlane.SPI,
            selection);
    try (DurableHost host =
        DurableHost.initialize(
            root,
            configuration,
            ReferenceApplications.all(),
            DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
            DurableHost.UtcClock.system(true),
            environment)) {
      assertEquals(selection, host.configuration().spiBackend());
    }
    try (DurableHost reopened =
        DurableHost.open(
            root,
            configuration,
            ReferenceApplications.all(),
            DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
            DurableHost.UtcClock.system(true),
            environment)) {
      assertEquals(selection, reopened.configuration().spiBackend());
    }
    IOException refusal =
        assertThrows(
            IOException.class,
            () ->
                DurableHost.open(
                    root,
                    new DurableHost.Configuration(
                        DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
                        DurableHost.ObjectPlane.SPI,
                        changed),
                    ReferenceApplications.all(),
                    DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
                    DurableHost.UtcClock.system(true),
                    environment));
    assertTrue(refusal.getMessage().contains("SPI backend differs"), refusal.getMessage());
  }

  private static byte[] payload() {
    byte[] payload = new byte[513];
    for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i * 29 + 3);
    return payload;
  }

  private static void install(Path root, ObjectStore backend, byte[] payload) throws Exception {
    Commitments.Context context = new Commitments.Context("issuer-a", "alice", 1);
    Records.InputHeader header = header(payload);
    try (InputStore store =
            InputStore.initializeForAuthority(root, LIMITS, UUID.randomUUID(), backend);
        InputStore.Receiver receiver = store.begin(context, header, SELECTED, 1)) {
      receiver.write(ByteBuffer.wrap(payload), 2);
      receiver.finish(3);
    }
  }

  private static void assertInstalled(ObjectStore backend, byte[] payload) throws Exception {
    List<ScopePath> scopes = backend.listScopes();
    assertEquals(1, scopes.size(), "payload bytes landed in the selected backend");
    try (InputStream entity = backend.openRead(scopes.get(0), 0)) {
      assertArrayEquals(payload, entity.readAllBytes());
    }
  }

  private static Records.InputHeader header(byte[] payload) throws Exception {
    byte[] operation = new byte[16];
    operation[15] = 3;
    return new Records.InputHeader(
        1,
        new Records.OperationId(operation),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, 3),
            new Records.Input(
                payload.length,
                new Records.Digest(MessageDigest.getInstance("SHA-256").digest(payload)),
                "application/octet-stream"),
            "backend-wiring",
            0,
            1000,
            new Records.OutputBudget(0, 0)));
  }
}
