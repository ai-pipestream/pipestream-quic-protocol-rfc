package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12 ordering clauses that only a boundary hook can pin: the publication-versus-fence
 * commit boundary (12.7), control progress while an application callback is parked (12.7), header
 * validation before payload acceptance (12.5) and the control wait interval including queued
 * storage time (12.11). Every hook observes a boundary the production code actually reached.
 */
@Timeout(120)
class HookPlacementTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(20_000, 60_000, 120_000);
  static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice", "bob"));
    principals = pki.principals(List.of("alice", "bob"));
  }

  /** A hook that parks the host worker at one boundary of one work key until released. */
  static final class Park implements Boundaries {
    final Boundary boundary;
    final CountDownLatch reached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);

    Park(Boundary boundary) {
      this.boundary = boundary;
    }

    @Override
    public void committed(Boundary reachedBoundary, Details details) {
      if (reachedBoundary != boundary || reached.getCount() == 0) return;
      if (details.work() != null && !details.work().equals(WORK)) return;
      reached.countDown();
      try {
        release.await(60, TimeUnit.SECONDS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void sent(Boundary reachedBoundary, Details details) {}

    @Override
    public boolean withhold(Boundary reachedBoundary) {
      return false;
    }
  }

  static DurableHost host(String name, List<DurableHost.Application> applications, int workers)
      throws Exception {
    DurableHost.Configuration defaults =
        DurableHost.Configuration.defaults("issuer-a", "localhost:7443");
    DurableHost.Configuration configuration =
        workers == 0
            ? defaults
            : new DurableHost.Configuration(
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
                defaults.retention(),
                defaults.results(),
                defaults.waits(),
                new DurableHost.WorkerLimits(workers, 3, 3),
                defaults.producer());
    return DurableHost.initialize(
        directory.resolve(name),
        configuration,
        applications,
        DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true),
        DurableHost.UtcClock.system(true));
  }

  static DurableServer server(DurableHost host, Boundaries hooks) throws Exception {
    host.boundaries(hooks);
    return DurableServer.start(
        new InetSocketAddress("127.0.0.1", 0),
        pki.server(principals),
        host,
        DurableOptions.defaults(),
        hooks);
  }

  static RawDurablePeer peer(DurableServer server, String owner) throws Exception {
    RawDurablePeer peer = new RawDurablePeer(server.address(), pki.client(owner), 65_536);
    peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
    return peer;
  }

  static DurableHost.Application parked(CountDownLatch started, CountDownLatch release) {
    return new DurableHost.Application(
        "park/v2",
        Set.of(0),
        DurableHost.RestartSafety.IDEMPOTENT,
        work -> {
          started.countDown();
          assertTrue(release.await(60, TimeUnit.SECONDS), "callback released");
          byte[] buffer = new byte[work.bufferLimit()];
          work.beginOutput(work.input().length(), "application/octet-stream");
          for (int read; (read = work.readInput(buffer, 0, buffer.length)) != -1; )
            work.writeOutput(ByteBuffer.wrap(buffer, 0, read));
          work.finishOutput();
          return DurableHost.Result.succeeded();
        },
        null);
  }

  static byte[] payload(int length) {
    byte[] bytes = new byte[length];
    for (int index = 0; index < length; index++) bytes[index] = (byte) (index * 31);
    return bytes;
  }

  /** Create a session, seal one declared member and admit it under the given application. */
  static Binding admit(RawDurablePeer peer, String application, byte[] bytes) throws Exception {
    Binding binding = assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
    assertInstanceOf(
        DeclarationResponse.class,
        peer.call(new Declare(peer.request(), DurableServerTest.operation(1), 0, List.of(1L), true)));
    peer.sendInput(
        DurableServerTest.header(binding.generation(), 2, WORK, bytes, application, 0), bytes, true);
    assertInstanceOf(AdmissionResponse.class, peer.next());
    return binding;
  }

  static Records.Cancelled cancelFromSecondConnection(DurableServer server, long generation)
      throws Exception {
    try (RawDurablePeer other = peer(server, "alice")) {
      assertInstanceOf(
          Binding.class, other.call(new Attach(other.request(), "issuer-a", "alice", generation)));
      CancelResponse response =
          assertInstanceOf(
              CancelResponse.class,
              other.call(new Cancel(other.request(), DurableServerTest.operation(3), WORK)));
      Records.Cancelled cancelled =
          assertInstanceOf(Records.Cancelled.class, response.receipt().outcome());
      other.call(new Detach(other.request()));
      return cancelled;
    }
  }

  /** Committed boundaries fire for a fresh durable commit only, never for a replay. */
  @Test
  void committedBoundariesFireOnceAcrossReplays() throws Exception {
    java.util.concurrent.ConcurrentHashMap<Boundaries.Boundary, AtomicInteger> counts =
        new java.util.concurrent.ConcurrentHashMap<>();
    Boundaries counting =
        new Boundaries() {
          @Override
          public void committed(Boundary boundary, Details details) {
            counts.computeIfAbsent(boundary, b -> new AtomicInteger()).incrementAndGet();
          }

          @Override
          public void sent(Boundary boundary, Details details) {}

          @Override
          public boolean withhold(Boundary boundary) {
            return false;
          }
        };
    try (DurableHost host = host("replays", ReferenceApplications.all(), 0);
        DurableServer server = server(host, counting);
        RawDurablePeer peer = peer(server, "alice")) {
      // Every mutation twice: the second is a replay that commits nothing. A bound connection
      // refuses a second creation, so the creation replay comes from a fresh connection, as a
      // client reconnecting after a lost reply would send it.
      assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      try (RawDurablePeer again = peer(server, "alice")) {
        assertInstanceOf(Binding.class, again.call(new Create(again.request(), 1, POLICY)));
        assertInstanceOf(Detached.class, again.call(new Detach(again.request())));
      }
      for (int i = 0; i < 2; i++)
        assertInstanceOf(
            DeclarationResponse.class,
            peer.call(
                new Declare(
                    peer.request(), DurableServerTest.operation(1), 0, List.of(1L, 2L), true)));
      for (int i = 0; i < 2; i++)
        assertInstanceOf(
            CancelResponse.class,
            peer.call(new Cancel(peer.request(), DurableServerTest.operation(2), WORK)));
      for (int i = 0; i < 2; i++)
        assertInstanceOf(
            SkipResponse.class,
            peer.call(
                new Skip(
                    peer.request(), DurableServerTest.operation(3), new Records.WorkKey(0, 0, 2))));
      for (int i = 0; i < 2; i++)
        assertInstanceOf(
            CancelScopeResponse.class,
            peer.call(new CancelScope(peer.request(), DurableServerTest.operation(4), 0)));
      assertEquals(1, counts.get(Boundaries.Boundary.SESSION_COMMITTED).get(), counts.toString());
      assertEquals(
          1, counts.get(Boundaries.Boundary.DECLARATION_COMMITTED).get(), counts.toString());
      assertEquals(3, counts.get(Boundaries.Boundary.FENCE_COMMITTED).get(), counts.toString());
      assertInstanceOf(Detached.class, peer.call(new Detach(peer.request())));
    }
  }

  /** S12-221, fence first: a cancel accepted while the callback runs excludes its publication. */
  @Test
  void fenceCommittedBeforePublicationExcludesTheOldAttempt() throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    List<DurableHost.Application> applications = new ArrayList<>(ReferenceApplications.all());
    applications.add(parked(started, release));
    byte[] bytes = payload(3000);
    try (DurableHost host = host("fence-first", applications, 0);
        DurableServer server = server(host, Boundaries.NONE);
        RawDurablePeer peer = peer(server, "alice")) {
      Binding binding = admit(peer, "park/v2", bytes);
      assertTrue(started.await(30, TimeUnit.SECONDS), "callback started");
      // The fence commits while the attempt is still executing: disposition 0, and the accepted
      // fence settles the work at once (DurableMutationTest); the running attempt is excluded.
      Records.Cancelled accepted = cancelFromSecondConnection(server, binding.generation());
      assertEquals(0, accepted.disposition(), accepted.toString());
      assertEquals(Records.State.CANCELLED, accepted.state());
      // The old attempt's publication is refused at the commit boundary: no manifest, CANCELLED.
      release.countDown();
      Records.WorkView view = DurableServerTest.awaitTerminal(peer, WORK);
      assertEquals(Records.State.CANCELLED, view.state(), view.toString());
      assertNull(view.manifest());
      assertEquals(1, view.attempt());
      peer.call(new Detach(peer.request()));
    }
  }

  /** S12-221, publication first: a cancel after the commit reports the terminal result. */
  @Test
  void publicationCommittedBeforeFenceReportsTheTerminalResult() throws Exception {
    Park park = new Park(Boundaries.Boundary.PUBLICATION_COMMITTED);
    byte[] bytes = payload(3000);
    try (DurableHost host = host("publication-first", ReferenceApplications.all(), 0);
        DurableServer server = server(host, park);
        RawDurablePeer peer = peer(server, "alice")) {
      Binding binding = admit(peer, "copy/v2", bytes);
      assertTrue(park.reached.await(30, TimeUnit.SECONDS), "publication committed");
      // The publication is committed and its worker parked; the fence loses deterministically.
      Records.Cancelled late = cancelFromSecondConnection(server, binding.generation());
      assertEquals(1, late.disposition(), late.toString());
      assertEquals(Records.State.SUCCEEDED, late.state());
      park.release.countDown();
      Records.WorkView view = DurableServerTest.awaitTerminal(peer, WORK);
      assertEquals(Records.State.SUCCEEDED, view.state(), view.toString());
      assertNotNull(view.manifest());
      peer.call(new Detach(peer.request()));
    }
  }

  /** S12-208: a parked application callback does not occupy the connection's control reader. */
  @Test
  void controlProgressesWhileACallbackIsParkedOnTheSameConnection() throws Exception {
    Park park = new Park(Boundaries.Boundary.EXECUTION_CLAIMED);
    byte[] bytes = payload(3000);
    try (DurableHost host = host("parked-callback", ReferenceApplications.all(), 0);
        DurableServer server = server(host, park);
        RawDurablePeer peer = peer(server, "alice")) {
      admit(peer, "copy/v2", bytes);
      assertTrue(park.reached.await(30, TimeUnit.SECONDS), "execution claimed");
      // Same connection, callback thread parked: a watch and a sequence request are both served.
      WatchResponse watched =
          assertInstanceOf(WatchResponse.class, peer.call(new Watch(peer.request(), WORK, 0, 0)));
      assertEquals(Records.State.ACTIVE, watched.work().state(), watched.toString());
      assertInstanceOf(Sequence.class, peer.call(new NextSequence(peer.request())));
      assertEquals(1, park.release.getCount(), "the callback was still parked");
      assertEquals(1, host.status().activeJobs());
      park.release.countDown();
      assertEquals(
          Records.State.SUCCEEDED, DurableServerTest.awaitTerminal(peer, WORK).state());
      peer.call(new Detach(peer.request()));
    }
  }

  /** S12-158: an application-label refusal is observed before any payload byte is retained. */
  @Test
  void headerRefusalLeavesTheInputStoreUntouched() throws Exception {
    AtomicReference<InputStore.Usage> atRefusal = new AtomicReference<>();
    AtomicReference<DurableHost> hosted = new AtomicReference<>();
    Boundaries observe =
        new Boundaries() {
          @Override
          public void committed(Boundary boundary, Details details) {}

          @Override
          public void sent(Boundary boundary, Details details) {
            if (boundary == Boundary.REFUSAL_SENT && atRefusal.get() == null)
              atRefusal.set(hosted.get().inputs().usage());
          }

          @Override
          public boolean withhold(Boundary boundary) {
            return false;
          }
        };
    byte[] bytes = payload(4096);
    try (DurableHost host = host("header-refusal", ReferenceApplications.all(), 0);
        DurableServer server = server(host, observe);
        RawDurablePeer peer = peer(server, "alice")) {
      hosted.set(host);
      Binding binding =
          assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(peer.request(), DurableServerTest.operation(1), 0, List.of(1L), true)));
      InputStore.Usage before = host.inputs().usage();
      // Header and complete payload in one write: the label is refused before the bytes count.
      peer.sendInput(
          DurableServerTest.header(binding.generation(), 2, WORK, bytes, "unknown/v2", 0),
          bytes,
          true);
      Refusal refused = assertInstanceOf(Refusal.class, peer.next());
      assertEquals(ProtocolError.Code.APPLICATION_UNSUPPORTED, refused.code(), refused.toString());
      assertNotNull(atRefusal.get(), "the refusal was observed at the transport");
      assertEquals(before, atRefusal.get(), "input store at the moment of the refusal");
      assertEquals(before, host.inputs().usage(), "input store after the refusal");
      assertEquals(
          Records.State.DECLARED,
          assertInstanceOf(WatchResponse.class, peer.call(new Watch(peer.request(), WORK, 0, 0)))
              .work()
              .state());
      peer.call(new Detach(peer.request()));
    }
  }

  /** S12-311: a checkpoint wait queued behind busy storage still expires from its acceptance. */
  @Test
  void checkpointWaitCountsTimeQueuedForStorage() throws Exception {
    AtomicBoolean armed = new AtomicBoolean();
    CountDownLatch paused = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Boundaries stuck =
        new Boundaries() {
          @Override
          public void committed(Boundary boundary, Details details) {
            if (boundary != Boundary.DECLARATION_COMMITTED || !armed.get() || paused.getCount() == 0)
              return;
            paused.countDown();
            try {
              release.await(30, TimeUnit.SECONDS);
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
        };
    long waitMs = 2000;
    long queuedMs = 1500;
    try (DurableHost host = host("queued-wait", ReferenceApplications.all(), 1);
        DurableServer server = server(host, stuck);
        RawDurablePeer alice = peer(server, "alice");
        RawDurablePeer bob = peer(server, "bob")) {
      assertInstanceOf(Binding.class, alice.call(new Create(alice.request(), 1, POLICY)));
      assertInstanceOf(
          DeclarationResponse.class,
          alice.call(
              new Declare(alice.request(), DurableServerTest.operation(1), 0, List.of(1L), true)));
      PageResponse page =
          assertInstanceOf(PageResponse.class, alice.call(new Page(alice.request(), 0, 0, 256)));
      assertTrue(page.sealed());
      // The only storage worker commits bob's declaration and parks inside the hook.
      assertInstanceOf(Binding.class, bob.call(new Create(bob.request(), 1, POLICY)));
      armed.set(true);
      bob.send(new Declare(bob.request(), DurableServerTest.operation(1), 0, List.of(1L), false));
      assertTrue(paused.await(10, TimeUnit.SECONDS), "storage worker parked");
      // Alice's checkpoint is accepted now but queued behind the parked worker.
      long sent = System.nanoTime();
      alice.send(new Checkpoint(alice.request(), 0, page.seal(), waitMs));
      Thread.sleep(queuedMs);
      release.countDown();
      Refusal refused = assertInstanceOf(Refusal.class, alice.next());
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sent);
      assertEquals(ProtocolError.Code.WAIT_TIMEOUT, refused.code(), refused.toString());
      // The interval started at acceptance: it expires about waitMs after the send, not
      // waitMs after the worker finally picked the request up (which would be near 3500 ms).
      assertTrue(
          elapsedMs >= waitMs - 100 && elapsedMs < waitMs + queuedMs - 500,
          "checkpoint wait elapsed after " + elapsedMs + " ms");
      assertInstanceOf(DeclarationResponse.class, bob.next());
      alice.call(new Detach(alice.request()));
      bob.call(new Detach(bob.request()));
    }
  }
}
