package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adoption plan stage 2: the per-installation object plane is selected in the
 * authority configuration, recorded durably with the installation, and reopen
 * refuses a changed selection. Default is v2 with zero behavioral delta; the
 * SPI plane is refused by this build until stage 3 wires it.
 */
final class ObjectPlaneBindingTest {
  @TempDir Path directory;

  @Test void defaultsUseTheV2Plane() {
    assertEquals(
        DurableHost.ObjectPlane.V2,
        DurableHost.Configuration.defaults("issuer-a", "localhost:7443").objectPlane());
  }

  @Test void initializeRecordsPlaneAndReopenAgrees() throws Exception {
    Path root = directory.resolve("agree");
    try (DurableHost host = initialize(root, DurableHost.ObjectPlane.V2)) {
      assertEquals(DurableHost.ObjectPlane.V2, host.configuration().objectPlane());
    }
    try (DurableHost reopened = open(root, DurableHost.ObjectPlane.V2)) {
      assertEquals(DurableHost.ObjectPlane.V2, reopened.configuration().objectPlane());
    }
  }

  @Test void spiPlaneInitializesRecordsAndReopens() throws Exception {
    Path root = directory.resolve("spi-init");
    DurableHost.Configuration configuration =
        new DurableHost.Configuration(
            DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
            DurableHost.ObjectPlane.SPI);
    try (DurableHost host =
        DurableHost.initialize(
            root,
            configuration,
            ReferenceApplications.all(),
            DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
            DurableHost.UtcClock.system(true))) {
      assertEquals(DurableHost.ObjectPlane.SPI, host.configuration().objectPlane());
    }
    assertTrue(Files.isDirectory(root.resolve(DurableHost.SPI_OBJECTS)), "SPI payload backend root exists");
    try (DurableHost reopened = open(root, DurableHost.ObjectPlane.SPI)) {
      assertEquals(DurableHost.ObjectPlane.SPI, reopened.configuration().objectPlane());
    }
    IOException refusal = assertThrows(IOException.class, () -> open(root, DurableHost.ObjectPlane.V2));
    assertTrue(refusal.getMessage().contains("object plane differs"), refusal.getMessage());
  }

  @Test void openRefusesAChangedRecordedPlane() throws Exception {
    Path root = directory.resolve("mismatch");
    try (DurableHost host = initialize(root, DurableHost.ObjectPlane.V2)) {
      assertEquals(DurableHost.ObjectPlane.V2, host.configuration().objectPlane());
    }
    // A recorded spi installation cannot arise through stage-2 initialize
    // (it is refused), so record it at the store level to pin the open seam.
    SessionStore sessions =
        SessionStore.open(
            root.resolve(DurableHost.DATABASE), storeConfiguration(DurableHost.ObjectPlane.SPI));
    sessions.recordObjectPlane(DurableHost.ObjectPlane.SPI, DurableHost.SpiObjectBackend.fs());
    sessions.close();

    IOException refusal = assertThrows(IOException.class, () -> open(root, DurableHost.ObjectPlane.V2));
    assertTrue(refusal.getMessage().contains("object plane differs"), refusal.getMessage());
    // The matching spi selection reopens against the SPI payload plane (stage 3).
    try (DurableHost reopened = open(root, DurableHost.ObjectPlane.SPI)) {
      assertEquals(DurableHost.ObjectPlane.SPI, reopened.configuration().objectPlane());
    }
  }

  @Test void storeLevelPlaneRecordDefaultsToV2ForLegacyInstallations() throws Exception {
    SessionStore legacy =
        SessionStore.initialize(
            directory.resolve("legacy.sqlite"), storeConfiguration(DurableHost.ObjectPlane.V2));
    assertEquals(DurableHost.ObjectPlane.V2, legacy.objectPlane().plane());
    assertEquals(DurableHost.SpiObjectBackend.fs(), legacy.objectPlane().backend());
    legacy.recordObjectPlane(DurableHost.ObjectPlane.SPI, DurableHost.SpiObjectBackend.fs());
    assertEquals(DurableHost.ObjectPlane.SPI, legacy.objectPlane().plane());
    legacy.close();
    SessionStore reopened =
        SessionStore.open(directory.resolve("legacy.sqlite"), storeConfiguration(DurableHost.ObjectPlane.SPI));
    assertEquals(DurableHost.ObjectPlane.SPI, reopened.objectPlane().plane());
    reopened.close();
  }

  @Test void v2MainParsesTheObjectPlaneOption() {
    Map<String, String> base =
        Map.of("authority", "issuer-a", "result-authority", "localhost:7443");
    assertEquals(
        DurableHost.ObjectPlane.V2, V2Main.configuration(base).objectPlane());
    assertEquals(
        DurableHost.ObjectPlane.SPI,
        V2Main.configuration(with(base, "object-plane", "spi")).objectPlane());
    assertThrows(
        IllegalArgumentException.class,
        () -> V2Main.configuration(with(base, "object-plane", "nonsense")));
  }

  private static DurableHost initialize(Path root, DurableHost.ObjectPlane plane) throws Exception {
    return DurableHost.initialize(
        root,
        new DurableHost.Configuration(
            DurableHost.Configuration.defaults("issuer-a", "localhost:7443"), plane),
        ReferenceApplications.all(),
        DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
        DurableHost.UtcClock.system(true));
  }

  private static DurableHost open(Path root, DurableHost.ObjectPlane plane) throws Exception {
    return DurableHost.open(
        root,
        new DurableHost.Configuration(
            DurableHost.Configuration.defaults("issuer-a", "localhost:7443"), plane),
        ReferenceApplications.all(),
        DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
        DurableHost.UtcClock.system(true));
  }

  /** Mirrors DurableHost.storeConfiguration for a defaults-based host configuration. */
  private static SessionStore.Configuration storeConfiguration(DurableHost.ObjectPlane plane) {
    DurableHost.Configuration host =
        new DurableHost.Configuration(
            DurableHost.Configuration.defaults("issuer-a", "localhost:7443"), plane);
    java.util.List<AdmissionStore.Application> contracts = new java.util.ArrayList<>();
    for (DurableHost.Application application : ReferenceApplications.all()) {
      contracts.add(application.contract());
    }
    return new SessionStore.Configuration(
        host.authority(),
        host.sessionLimits(),
        host.maximumPolicy(),
        host.maxOwners(),
        host.maxSessions(),
        host.maxSessionsPerOwner(),
        host.files(),
        new AdmissionStore.ExecutionPolicy(contracts, host.maxJobs(), host.maxJobsPerOwner()));
  }

  private static Map<String, String> with(Map<String, String> base, String key, String value) {
    Map<String, String> options = new java.util.HashMap<>(base);
    options.put(key, value);
    return options;
  }
}
