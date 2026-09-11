package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code --db-mib} and {@code --wal-mib} storage funding on the V2 launcher: absent options
 * reproduce the reference file policy exactly, present options fund the database and WAL files
 * in whole MiB, out-of-range values are refused before any file is touched, and a funded root
 * reopens only with the same funding.
 */
class V2MainStorageFundingTest {
  private static Map<String, String> options(String... pairs) {
    Map<String, String> options = new java.util.LinkedHashMap<>();
    options.put("authority", "issuer-a");
    options.put("result-authority", "localhost:7443");
    for (int i = 0; i < pairs.length; i += 2) options.put(pairs[i], pairs[i + 1]);
    return options;
  }

  @Test
  void absentOptionsReproduceTheReferenceFilePolicy() {
    DurableHost.Configuration configuration = V2Main.configuration(options());
    assertEquals(BoundedSqlite.Limits.defaults(), configuration.files());
    assertEquals(
        DurableHost.Configuration.defaults("issuer-a", "localhost:7443"), configuration);
  }

  @Test
  void presentOptionsFundOnlyTheNamedFiles() {
    DurableHost.Configuration funded =
        V2Main.configuration(options("db-mib", "1024", "wal-mib", "256"));
    BoundedSqlite.Limits defaults = BoundedSqlite.Limits.defaults();
    assertEquals(1024L << 20, funded.files().databaseBytes());
    assertEquals(256L << 20, funded.files().walBytes());
    assertEquals(defaults.journalBytes(), funded.files().journalBytes());
    assertEquals(defaults.sharedMemoryBytes(), funded.files().sharedMemoryBytes());
    DurableHost.Configuration databaseOnly = V2Main.configuration(options("db-mib", "16384"));
    assertEquals(16L << 30, databaseOnly.files().databaseBytes());
    assertEquals(defaults.walBytes(), databaseOnly.files().walBytes());
    // Everything that is not a file bound is untouched.
    DurableHost.Configuration reference =
        DurableHost.Configuration.defaults("issuer-a", "localhost:7443");
    assertEquals(reference.sessionLimits(), funded.sessionLimits());
    assertEquals(reference.objects(), funded.objects());
    assertEquals(reference.execution(), funded.execution());
    assertEquals(reference.producer(), funded.producer());
  }

  @Test
  void outOfRangeFundingIsRefused() {
    for (String value : new String[] {"0", "-1", "16385", "1.5", "256M", ""}) {
      IllegalArgumentException refused =
          assertThrows(
              IllegalArgumentException.class,
              () -> V2Main.configuration(options("db-mib", value)),
              value);
      assertTrue(refused.getMessage().contains("db-mib"), refused.getMessage());
    }
    assertThrows(
        IllegalArgumentException.class, () -> V2Main.configuration(options("wal-mib", "20000")));
  }

  @Test
  void fundedRootReopensOnlyWithTheSameFunding(@TempDir Path directory) throws Exception {
    Path root = directory.resolve("authority");
    Map<String, String> funded = options("root", root.toString(), "db-mib", "512", "wal-mib", "128");
    V2Main.initAuthority(funded);
    // Same funding: the retained file policy matches and the root opens.
    DurableHost host =
        DurableHost.open(
            root,
            V2Main.configuration(funded),
            ReferenceApplications.all(),
            DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
            DurableHost.UtcClock.system(true));
    host.close();
    // Default funding on a root initialized with explicit funding is refused, never widened or
    // narrowed silently.
    Exception changed =
        assertThrows(
            Exception.class,
            () ->
                DurableHost.open(
                        root,
                        V2Main.configuration(options("root", root.toString())),
                        ReferenceApplications.all(),
                        DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
                        DurableHost.UtcClock.system(true))
                    .close());
    String chain = changed.toString() + (changed.getCause() == null ? "" : " / " + changed.getCause());
    assertTrue(chain.contains("file policy"), chain);
  }
}
