package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Docker-backed layout-probe pins: every docker backend must produce a report
 * that byte-matches {@code expected-report-v1.txt}, filtered exactly as the
 * README allows. S3 implements the object role only, so its report is the
 * expected file without the journal section; Kafka implements the journal
 * role only, so its report is the expected file from the {@code journal}
 * marker onward. Fixtures follow the shared lifecycle and skip with an
 * explicit assumption when docker is unavailable.
 */
final class ProbeDockerTest {
  private static final Path SCRIPT = Path.of("../../test-vectors/storage-spi/probe-script-v1.txt");
  private static final Path EXPECTED = Path.of("../../test-vectors/storage-spi/expected-report-v1.txt");

  @Test void postgresProbeMatchesExpectedReport() throws Exception {
    PostgresDocker postgres = PostgresDocker.shared();
    try {
      try (Connection connection = PostgresSupport.connectAndEnsureSchema(
          postgres.url(), postgres.user(), postgres.password());
           Statement statement = connection.createStatement()) {
        statement.execute("TRUNCATE TABLE ps_object_entities, ps_object_seals");
        statement.execute("TRUNCATE TABLE ps_journal_records RESTART IDENTITY");
      }
      ProbeContractTest.assertReport("postgres", Probe.config(
          "PROBE_PG_URL", postgres.url(),
          "PROBE_PG_USER", postgres.user(),
          "PROBE_PG_PASSWORD", postgres.password()));
    } finally {
      PostgresDocker.releaseShared();
    }
  }

  @Test void s3ProbeMatchesExpectedReportWithoutJournal() throws Exception {
    MinioDocker minio = MinioDocker.shared();
    try {
      ByteArrayOutputStream out = runProbe("s3", Probe.config(
          "PROBE_S3_ENDPOINT", minio.endpoint().toString(),
          "PROBE_S3_REGION", "us-east-1",
          "PROBE_S3_ACCESS_KEY", minio.accessKey(),
          "PROBE_S3_SECRET_KEY", minio.secretKey(),
          "PROBE_S3_BUCKET", minio.bucket(),
          "PROBE_S3_PREFIX", "probe-" + UUID.randomUUID() + "/"));
      byte[] expected = Files.readAllBytes(EXPECTED);
      int journal = indexOf(expected, "journal\n");
      assertTrue(journal > 0, "expected report has a journal section");
      assertArrayEquals(java.util.Arrays.copyOf(expected, journal), out.toByteArray(),
          "s3 report is the expected report without the journal section");
    } finally {
      MinioDocker.releaseShared();
    }
  }

  @Test void kafkaProbeMatchesExpectedReportWithoutScopes() throws Exception {
    KafkaDocker kafka = KafkaDocker.shared();
    try {
      ByteArrayOutputStream out = runProbe("kafka", Probe.config(
          "PROBE_KAFKA_BOOTSTRAP", kafka.bootstrapServers(),
          "PROBE_KAFKA_TOPIC", "probe-" + UUID.randomUUID()));
      byte[] expected = Files.readAllBytes(EXPECTED);
      int journal = indexOf(expected, "journal\n");
      assertTrue(journal > 0, "expected report has a journal section");
      assertArrayEquals(java.util.Arrays.copyOfRange(expected, journal, expected.length), out.toByteArray(),
          "kafka report is the expected report from the journal marker onward");
    } finally {
      KafkaDocker.releaseShared();
    }
  }

  @Test void mongoProbeMatchesExpectedReport() throws Exception {
    MongoDocker mongo = MongoDocker.shared();
    try {
      ProbeContractTest.assertReport("mongo", Probe.config(
          "PROBE_MONGO_URI", mongo.connectionString(),
          "PROBE_MONGO_DATABASE", "probe-" + UUID.randomUUID().toString().replace("-", "")));
    } finally {
      MongoDocker.releaseShared();
    }
  }

  private static ByteArrayOutputStream runProbe(String backend, java.util.Map<String, String> config)
      throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int code = Probe.execute(new String[] {backend, SCRIPT.toString()}, config,
        new PrintStream(out, true, StandardCharsets.UTF_8),
        new PrintStream(err, true, StandardCharsets.UTF_8));
    assertEquals(Probe.OK, code, err.toString(StandardCharsets.UTF_8));
    return out;
  }

  private static int indexOf(byte[] bytes, String marker) {
    byte[] needle = marker.getBytes(StandardCharsets.UTF_8);
    for (int offset = 0; offset + needle.length <= bytes.length; offset++) {
      boolean match = true;
      for (int i = 0; i < needle.length; i++) {
        if (bytes[offset + i] != needle[i]) {
          match = false;
          break;
        }
      }
      if (match) return offset;
    }
    return -1;
  }
}
