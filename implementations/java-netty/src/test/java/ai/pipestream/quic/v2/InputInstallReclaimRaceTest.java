package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12.5: a complete, validated input that the authority has installed and is admitting is
 * not an orphan. The transfer is parked between installation and the admission transaction while
 * a retention sweep runs its orphan page; the admission must still succeed with the installed
 * bytes rather than refuse NOT_READY because the sweep reclaimed them (handoff defect 10, seen by
 * Meta's coordinator as intermittent {@code NOT_READY: complete validated input is unavailable}).
 */
@Timeout(120)
class InputInstallReclaimRaceTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(20_000, 60_000, 120_000);
  static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);
  static final Records.OperationId ADMIT = DurableServerTest.operation(2);

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
  }

  /** Parks the admission of {@link #ADMIT} after its input is installed, before its commit. */
  static final class Park implements Boundaries {
    final CountDownLatch installed = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);

    @Override
    public void committed(Boundary boundary, Details details) {
      if (boundary != Boundary.INPUT_INSTALLED || !ADMIT.equals(details.operation())) return;
      installed.countDown();
      try {
        assertTrue(release.await(60, TimeUnit.SECONDS), "park never released");
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void sent(Boundary boundary, Details details) {}

    @Override
    public boolean withhold(Boundary boundary) {
      return false;
    }
  }

  /** The host sweeps retention every millisecond, so orphan pages run throughout the park. */
  static DurableHost.Configuration quietRetention() {
    DurableHost.Configuration defaults =
        DurableHost.Configuration.defaults("issuer-a", "localhost:7443");
    return new DurableHost.Configuration(
        defaults.authority(),
        defaults.resultAuthority(),
        defaults.sessionLimits(),
        defaults.maximumPolicy(),
        defaults.maxOwners(),
        defaults.maxSessions(),
        defaults.maxSessionsPerOwner(),
        defaults.files(),
        defaults.objects(),
        defaults.maxJobs(),
        defaults.maxJobsPerOwner(),
        defaults.execution(),
        defaults.scheduler(),
        new DurableHost.RetentionLimits(64, 1),
        defaults.results(),
        defaults.waits(),
        defaults.storageWorkers(),
        defaults.producer());
  }

  @Test
  void anOrphanSweepDuringAdmissionDoesNotReclaimTheInstalledInput() throws Exception {
    byte[] input = DurableServerTest.payload(20_000, 3);
    Park park = new Park();
    try (DurableHost host =
            DurableHost.initialize(
                directory.resolve("race"),
                quietRetention(),
                ReferenceApplications.all(),
                DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true),
                DurableHost.UtcClock.system(true));
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults(),
                park);
        RawDurablePeer peer = new RawDurablePeer(server.address(), pki.client("alice"), 65_536)) {
      peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
      Binding binding = assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(
                  peer.request(), DurableServerTest.operation(1), 0, List.of(1L, 2L), true)));
      peer.sendInput(
          DurableServerTest.header(binding.generation(), 2, WORK, input, "copy/v2", 0),
          input,
          true);
      assertTrue(park.installed.await(30, TimeUnit.SECONDS), "input never installed");

      // The input is installed, its receiver closed, its admission not yet committed: the host's
      // retention timer runs hundreds of orphan pages over the object directory meanwhile.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      while (System.nanoTime() < deadline && host.status().retentionReleased() == 0)
        Thread.sleep(20);
      long releasedDuringPark = host.status().retentionReleased();
      park.release.countDown();

      Message answer = peer.next();
      AdmissionResponse admitted =
          assertInstanceOf(AdmissionResponse.class, answer, "admission after the sweep: " + answer);
      assertEquals(1, assertInstanceOf(Records.Admitted.class, admitted.receipt().outcome()).attempt());
      assertEquals(Records.State.SUCCEEDED, DurableServerTest.awaitTerminal(peer, WORK).state());
      assertEquals(0, releasedDuringPark, "the sweep reclaimed the input under admission");
    }
  }
}
