package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.v2.DurableHost;
import ai.pipestream.quic.v2.ReferenceApplications;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Leak audit for the v2 SPI-plane wiring around {@link DurableHost}: each
 * cycle opens and closes the authority database, the {@code InputStore}, and
 * the SPI payload {@link ObjectStore} (filesystem kind) under the host root.
 * The audit counts file descriptors ({@code /proc/self/fd}) and live
 * {@code pipestream-v2-*} pool threads across host cycles, proves the SPI
 * payload backend and the input store are actually released by host close
 * (their same-JVM open registries accept a new open immediately), checks the
 * plane-mismatch refusal path for descriptor neutrality, and verifies no
 * scratch name ever escapes the host root. No docker and no network: the fs
 * SPI backend is the only backend exercised here.
 */
final class LeakAuditSpiPlaneHostTest {
  private static final String AUTHORITY = "issuer-a";
  private static final String RESULT_AUTHORITY = "localhost:7443";

  @TempDir Path directory;

  private static DurableHost.Configuration spiConfiguration() {
    return new DurableHost.Configuration(
        DurableHost.Configuration.defaults(AUTHORITY, RESULT_AUTHORITY),
        DurableHost.ObjectPlane.SPI);
  }

  private static DurableHost.Configuration v2Configuration() {
    return DurableHost.Configuration.defaults(AUTHORITY, RESULT_AUTHORITY);
  }

  private static DurableHost initialize(Path root) throws Exception {
    return DurableHost.initialize(
        root,
        spiConfiguration(),
        ReferenceApplications.all(),
        DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
        DurableHost.UtcClock.system(true));
  }

  private static DurableHost open(Path root) throws Exception {
    return DurableHost.open(
        root,
        spiConfiguration(),
        ReferenceApplications.all(),
        DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
        DurableHost.UtcClock.system(true));
  }

  private static long fdCount() throws Exception {
    try (Stream<Path> fds = Files.list(Path.of("/proc/self/fd"))) {
      return fds.count();
    }
  }

  private static long v2Threads() {
    ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    long count = 0;
    for (long id : threads.getAllThreadIds()) {
      java.lang.management.ThreadInfo info = threads.getThreadInfo(id);
      if (info != null
          && info.getThreadName() != null
          && info.getThreadName().startsWith("pipestream-v2-")) {
        count++;
      }
    }
    return count;
  }

  @Test void hostCyclesDoNotGrowFileDescriptorsOrV2Threads() throws Exception {
    Path reusable = directory.resolve("reusable");
    long fdsBefore = fdCount();
    long threadsBefore = v2Threads();
    for (int cycle = 0; cycle < 10; cycle++) {
      try (DurableHost host = initialize(directory.resolve("fresh-" + cycle))) {
        assertEquals(DurableHost.ObjectPlane.SPI, host.configuration().objectPlane());
      }
    }
    try (DurableHost host = initialize(reusable)) {
      assertEquals(DurableHost.ObjectPlane.SPI, host.configuration().objectPlane());
    }
    for (int cycle = 0; cycle < 10; cycle++) {
      try (DurableHost host = open(reusable)) {
        assertEquals(DurableHost.ObjectPlane.SPI, host.configuration().objectPlane());
      }
    }
    assertTrue(fdCount() - fdsBefore <= 8,
        "20 host cycles leak no file descriptors beyond the one-time bootstrap");
    assertTrue(v2Threads() - threadsBefore <= 2,
        "20 host cycles leave no pipestream-v2 worker, scheduler, retention or result thread");
  }

  @Test void hostCloseReleasesTheSpiPayloadBackendAndTheInputStore() throws Exception {
    Path root = directory.resolve("release");
    try (DurableHost host = initialize(root)) {
      assertEquals(DurableHost.ObjectPlane.SPI, host.configuration().objectPlane());
    }
    // If host close did not chain into InputStore.close() -> spi.close(), the
    // filesystem SPI backend would still hold its writer lock and its
    // same-JVM open registration, and both opens below would be refused.
    try (ObjectStore backend = FileObjectStore.open(root.resolve(DurableHost.SPI_OBJECTS))) {
      assertEquals(List.of(), backend.listScopes(), "SPI payload backend reopened after host close");
    }
    try (DurableHost reopened = open(root)) {
      assertEquals(DurableHost.ObjectPlane.SPI, reopened.configuration().objectPlane());
    }
  }

  @Test void planeMismatchRefusalIsDescriptorNeutralAndReopenable() throws Exception {
    Path root = directory.resolve("mismatch");
    try (DurableHost host = initialize(root)) {
      assertEquals(DurableHost.ObjectPlane.SPI, host.configuration().objectPlane());
    }
    long before = fdCount();
    for (int round = 0; round < 25; round++) {
      IOException refusal = assertThrows(IOException.class,
          () -> DurableHost.open(
              root,
              v2Configuration(),
              ReferenceApplications.all(),
              DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
              DurableHost.UtcClock.system(true)));
      assertTrue(refusal.getMessage().contains("object plane differs"), refusal.getMessage());
    }
    assertTrue(fdCount() - before <= 2, "refused opens leak no session-store descriptors");
    try (DurableHost reopened = open(root)) {
      assertEquals(DurableHost.ObjectPlane.SPI, reopened.configuration().objectPlane());
    }
  }

  @Test void noScratchEscapesTheHostRootAcrossCycles() throws Exception {
    Path root = directory.resolve("contained");
    try (DurableHost host = initialize(root)) {
      assertEquals(DurableHost.ObjectPlane.SPI, host.configuration().objectPlane());
    }
    try (DurableHost reopened = open(root)) {
      assertEquals(DurableHost.ObjectPlane.SPI, reopened.configuration().objectPlane());
    }
    try (Stream<Path> paths = Files.walk(root)) {
      for (Path file : paths.filter(Files::isRegularFile).toList()) {
        assertTrue(file.startsWith(root), "write escaped the host root: " + file);
        String name = file.getFileName().toString();
        assertFalse(name.endsWith(".tmp"), "scratch left under the host root: " + file);
        assertFalse(name.endsWith(".part"), "staging left under the host root: " + file);
      }
    }
  }
}
