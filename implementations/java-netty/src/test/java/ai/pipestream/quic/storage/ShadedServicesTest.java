package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;

/**
 * Regression pins for running the SPI backends from the shaded standalone
 * jar: the maven-shade configuration must merge {@code META-INF/services}
 * files so JDBC driver registration survives shading, and the PostgreSQL
 * backend must accept the libpq URL form the conformance harness passes.
 */
final class ShadedServicesTest {
  /**
   * Pins the shade configuration: without the services transformer the
   * shaded jar keeps only the first dependency's driver registration and
   * Postgres SPI backends fail with "No suitable driver".
   */
  @Test void shadeConfigurationMergesServiceRegistrations() throws IOException {
    String pom = Files.readString(Path.of("pom.xml"));
    assertTrue(
        pom.contains("org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"),
        "maven-shade must merge META-INF/services files (JDBC driver registration)");
  }

  /**
   * When the shaded jar has been built, its Driver registrations must
   * include every SQL backend in the module. Skipped in plain
   * {@code mvn test} runs, exercised by package-level verification.
   */
  @Test void shadedJarRegistersEverySqlDriverWhenBuilt() throws IOException {
    try (var jars =
        Files.list(Path.of("target"))) {
      var built =
          jars
              .filter(path -> path.getFileName().toString().endsWith("-all.jar"))
              .findFirst();
      if (built.isEmpty()) return; // package was not run in this build
      try (JarFile jar = new JarFile(built.get().toFile())) {
        var entry = jar.getJarEntry("META-INF/services/java.sql.Driver");
        assertNotNull(entry, "shaded jar lacks the JDBC driver services entry");
        String services = new String(jar.getInputStream(entry).readAllBytes());
        assertTrue(services.contains("org.sqlite.JDBC"), services);
        assertTrue(services.contains("org.postgresql.Driver"), services);
      }
    }
  }

  @Test void postgresUrlNormalizationAcceptsJdbcAndLibpqForms() {
    assertEquals(
        "jdbc:postgresql://db.example:5432/pipestream",
        PostgresObjectStore.normalizeConnectionUrl(
            "jdbc:postgresql://db.example:5432/pipestream"));
    assertEquals(
        "jdbc:postgresql://db.example:5432/pipestream",
        PostgresObjectStore.normalizeConnectionUrl(
            "postgres://alice:secret@db.example:5432/pipestream"));
    assertEquals(
        "jdbc:postgresql://127.0.0.1:5432/pipestream",
        PostgresObjectStore.normalizeConnectionUrl(
            "postgresql://postgres@127.0.0.1:5432/pipestream"));
    assertEquals(
        "jdbc:postgresql://127.0.0.1:5432/pipestream",
        PostgresObjectStore.normalizeConnectionUrl(
            "postgres://127.0.0.1:5432/pipestream"));
    assertEquals("jdbc:other://x", PostgresObjectStore.normalizeConnectionUrl("jdbc:other://x"));
  }
}
