package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Lifecycle boundaries: lost acknowledgments after real commits (via the test-only boundary hook),
 * disconnects during transfers and busy waits, and shutdown ordering under a paused application
 * callback. Nothing here forges a commit; hooks only drop replies or pause.
 */
@Timeout(240)
final class DurableLifecycleTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(30_000, 60_000, 120_000);
  static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
  }

  /** Records boundaries and withholds the reply for the configured boundaries, once each. */
  static final class Hooks implements Boundaries {
    final ConcurrentLinkedQueue<String> events = new ConcurrentLinkedQueue<>();
    final Set<Boundary> withhold = java.util.concurrent.ConcurrentHashMap.newKeySet();

    Hooks(Boundary... drop) {
      withhold.addAll(Arrays.asList(drop));
    }

    @Override
    public void committed(Boundary boundary, Details details) {
      events.add("COMMITTED " + boundary + " " + details.operation());
    }

    @Override
    public void sent(Boundary boundary, Details details) {
      events.add("SENT " + boundary + " " + details.operation());
    }

    @Override
    public boolean withhold(Boundary boundary) {
      return withhold.remove(boundary);
    }

    long count(String prefix) {
      return events.stream().filter(e -> e.startsWith(prefix)).count();
    }
  }

  static DurableHost host(Path root, boolean initialize, List<DurableHost.Application> applications)
      throws Exception {
    DurableHost.OwnerPolicy owners = DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true);
    DurableHost.Configuration configuration =
        DurableHost.Configuration.defaults("issuer-a", "localhost:7443");
    return initialize
        ? DurableHost.initialize(
            root, configuration, applications, owners, DurableHost.UtcClock.system(true))
        : DurableHost.open(
            root, configuration, applications, owners, DurableHost.UtcClock.system(true));
  }

  static DurableClient client(DurableServer server, ClientJournal journal) throws Exception {
    DurableClient client =
        DurableClient.connect(
            server.address(), pki.client("alice"), journal, ClientOptions.defaults());
    DurableClientTest.get(client.ready());
    return client;
  }

  static InputSource source(byte[] bytes) throws Exception {
    Path file = Files.createTempFile(directory, "input-", ".bin");
    Files.write(file, bytes);
    return InputSource.file(file, "application/octet-stream", 16L << 20);
  }

  @Test
  void lostAcknowledgmentsAfterRealCommitsRecoverByReplayWithoutNewWork() throws Exception {
    Path root = directory.resolve("lost-ack");
    Path journalFile = directory.resolve("lost-ack-journal.sqlite");
    byte[] bytes = new byte[40_000];
    new java.util.Random(6).nextBytes(bytes);
    Hooks hooks =
        new Hooks(
            Boundaries.Boundary.SESSION_RESPONSE_SENT,
            Boundaries.Boundary.DECLARATION_RESPONSE_SENT,
            Boundaries.Boundary.ADMISSION_RESPONSE_SENT);
    try (DurableHost host = host(root, true, ReferenceApplications.all());
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults(),
                hooks);
        ClientJournal journal =
            ClientJournal.initialize(
                journalFile,
                new ClientJournal.Intent("issuer-a", "alice", 1, POLICY, true),
                ClientJournal.Limits.defaults())) {
      // 1. Creation commits, the reply is dropped: the client sees a connection loss and stays
      // unbound; the same creation sequence replays the same generation on reconnect.
      DurableClient first = client(server, journal);
      ProtocolError lost = DurableClientTest.refusal(first.binding());
      assertEquals(ProtocolError.Code.CONTROL_RESET, lost.code());
      assertTrue(journal.binding().isEmpty());
      assertEquals(1, hooks.count("COMMITTED SESSION_COMMITTED"));
      first.close();
      DurableClient second = client(server, journal);
      Binding binding = DurableClientTest.get(second.binding());
      assertEquals(1, binding.generation());
      // 2. Declaration commits, reply dropped: the operation is unresolved, then replay returns the
      // identical receipt and membership is exactly what was declared once.
      ProtocolError lostDeclare =
          DurableClientTest.refusal(
              second.declare(DurableClientTest.operation(1), 0, List.of(1L), true));
      assertEquals(ProtocolError.Code.CONTROL_RESET, lostDeclare.code());
      assertEquals(1, journal.unresolved(0, 10).size());
      assertEquals(DurableClientTest.operation(1), journal.unresolved(0, 10).get(0).operation());
      second.close();
      DurableClient third = client(server, journal);
      DurableClientTest.get(third.binding());
      Records.OperationReceipt declared =
          DurableClientTest.get(third.lookup(DurableClientTest.operation(1)));
      assertEquals(1, assertInstanceOf(Records.Declared.class, declared.outcome()).acceptedCount());
      assertTrue(journal.unresolved(0, 10).isEmpty());
      assertEquals(
          declared,
          DurableClientTest.get(
              third.declare(DurableClientTest.operation(1), 0, List.of(1L), true)));
      // 3. Admission commits (bytes installed, job funded), reply dropped: NOT_FOUND is never
      // assumed; lookup recovers the receipt and a resend replays without re-admitting.
      InputSource source = source(bytes);
      Records.AdmitParameters parameters =
          new Records.AdmitParameters(
              WORK,
              source.input(),
              "copy/v2",
              0,
              20_000,
              new Records.OutputBudget(1, bytes.length));
      ProtocolError lostAdmit =
          DurableClientTest.refusal(
              third.admit(
                  DurableClientTest.operation(2),
                  parameters,
                  DurableClientTest.operation(1),
                  source));
      assertEquals(ProtocolError.Code.CONTROL_RESET, lostAdmit.code());
      assertEquals(1, hooks.count("COMMITTED ADMISSION_COMMITTED"));
      assertEquals(1, journal.unresolved(0, 10).size());
      third.close();
      DurableClient fourth = client(server, journal);
      DurableClientTest.get(fourth.binding());
      Records.OperationReceipt admitted =
          DurableClientTest.get(fourth.lookup(DurableClientTest.operation(2)));
      assertEquals(1, assertInstanceOf(Records.Admitted.class, admitted.outcome()).attempt());
      InputSource again = source(bytes);
      assertEquals(
          admitted,
          DurableClientTest.get(
              fourth.admit(
                  DurableClientTest.operation(2),
                  parameters,
                  DurableClientTest.operation(1),
                  again)));
      assertEquals(1, hooks.count("COMMITTED ADMISSION_COMMITTED"), "replay never admits twice");
      assertEquals(
          Records.State.SUCCEEDED, DurableClientTest.awaitTerminal(fourth, WORK).view().state());
      assertEquals(1, DurableClientTest.get(fourth.watch(WORK, 0, 0)).view().attempt());
      DurableClientTest.get(fourth.detach());
      fourth.close();
    }
  }

  @Test
  void disconnectDuringInputAndBusyWaitReleasesOnlyConnectionState() throws Exception {
    Path root = directory.resolve("disconnect");
    byte[] bytes = DurableServerTest.payload(600_000, 2);
    try (DurableHost host = host(root, true, ReferenceApplications.all());
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults())) {
      Binding binding;
      try (RawDurablePeer peer =
          new RawDurablePeer(server.address(), pki.client("alice"), 65_536)) {
        peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
        binding = assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
        assertInstanceOf(
            DeclarationResponse.class,
            peer.call(
                new Declare(peer.request(), DurableServerTest.operation(1), 0, List.of(1L), true)));
        // Half the payload, then a busy wait, then an abrupt close with both outstanding.
        peer.sendInput(
            DurableServerTest.header(1, 2, WORK, bytes, "copy/v2", 0),
            Arrays.copyOf(bytes, 200_000),
            false);
        peer.send(new Watch(peer.request(), WORK, 1, 20_000));
        Thread.sleep(200);
        DurableServer.Snapshot busy =
            server.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals(1, busy.inputs());
        assertEquals(1, busy.waits());
      }
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
      DurableServer.Snapshot after;
      do {
        after = server.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
        Thread.sleep(20);
      } while ((after.active() != 0 || after.inputs() != 0 || after.waits() != 0)
          && System.nanoTime() < deadline);
      assertEquals(0, after.active());
      assertEquals(0, after.inputs());
      assertEquals(0, after.waits());
      deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
      while (host.inputs().usage().handles() != 0 && System.nanoTime() < deadline) Thread.sleep(20);
      assertEquals(0, host.inputs().usage().handles(), "abandoned receiver released its handle");
      assertEquals(0, host.status().pendingWaits());
      // The declaration survived; the retransmitted input admits and succeeds on a new connection.
      try (RawDurablePeer peer =
          new RawDurablePeer(server.address(), pki.client("alice"), 65_536)) {
        peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
        assertEquals(
            binding.generation(),
            assertInstanceOf(
                    Binding.class, peer.call(new Attach(peer.request(), "issuer-a", "alice", 1)))
                .generation());
        WatchResponse declared =
            assertInstanceOf(WatchResponse.class, peer.call(new Watch(peer.request(), WORK, 0, 0)));
        assertEquals(Records.State.DECLARED, declared.work().state());
        peer.sendInput(DurableServerTest.header(1, 2, WORK, bytes, "copy/v2", 0), bytes, true);
        assertInstanceOf(AdmissionResponse.class, peer.next());
        assertEquals(Records.State.SUCCEEDED, DurableServerTest.awaitTerminal(peer, WORK).state());
        peer.call(new Detach(peer.request()));
      }
    }
  }

  @Test
  void shutdownWaitsForAPausedCallbackAndLeavesWorkRecoverable() throws Exception {
    Path root = directory.resolve("shutdown");
    Path journalFile = directory.resolve("shutdown-journal.sqlite");
    byte[] bytes = new byte[3000];
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicReference<Exception> callbackFailure = new AtomicReference<>();
    DurableHost.Application slow =
        new DurableHost.Application(
            "slow/v2",
            Set.of(0),
            DurableHost.RestartSafety.IDEMPOTENT,
            work -> {
              started.countDown();
              assertTrue(release.await(60, TimeUnit.SECONDS));
              byte[] buffer = new byte[work.bufferLimit()];
              work.beginOutput(work.input().length(), "application/octet-stream");
              for (int read; (read = work.readInput(buffer, 0, buffer.length)) != -1; )
                work.writeOutput(java.nio.ByteBuffer.wrap(buffer, 0, read));
              work.finishOutput();
              return DurableHost.Result.succeeded();
            },
            null);
    List<DurableHost.Application> applications = new ArrayList<>(ReferenceApplications.all());
    applications.add(slow);
    DurableHost host = host(root, true, applications);
    DurableServer server =
        DurableServer.start(
            new InetSocketAddress("127.0.0.1", 0),
            pki.server(principals),
            host,
            DurableOptions.defaults());
    ClientJournal journal =
        ClientJournal.initialize(
            journalFile,
            new ClientJournal.Intent("issuer-a", "alice", 1, POLICY, true),
            ClientJournal.Limits.defaults());
    DurableClient client = client(server, journal);
    DurableClientTest.get(client.binding());
    DurableClientTest.get(client.declare(DurableClientTest.operation(1), 0, List.of(1L), true));
    InputSource source = source(bytes);
    DurableClientTest.get(
        client.admit(
            DurableClientTest.operation(2),
            new Records.AdmitParameters(
                WORK,
                source.input(),
                "slow/v2",
                0,
                20_000,
                new Records.OutputBudget(1, bytes.length)),
            DurableClientTest.operation(1),
            source));
    assertTrue(started.await(30, TimeUnit.SECONDS), "callback started");
    // Listener close drains the connection-local owners; the busy callback keeps the host open.
    server.close();
    Thread closer =
        new Thread(
            () -> {
              try {
                host.close();
              } catch (Exception failure) {
                callbackFailure.set(failure);
              }
            });
    closer.start();
    closer.join(1500);
    assertTrue(closer.isAlive(), "host.close must wait for the paused callback");
    assertTrue(host.status().activeJobs() >= 1);
    release.countDown();
    closer.join(60_000);
    assertFalse(closer.isAlive(), "host.close returns once the callback returned");
    assertNull(callbackFailure.get());
    client.close();
    journal.close();
    // The publication committed before storage closed; reopening shows SUCCEEDED and the bytes.
    try (DurableHost reopened = host(root, false, applications);
        DurableServer again =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                reopened,
                DurableOptions.defaults());
        ClientJournal reopenedJournal =
            ClientJournal.open(journalFile, ClientJournal.Limits.defaults());
        DurableClient back = client(again, reopenedJournal)) {
      DurableClientTest.get(back.binding());
      assertEquals(
          Records.State.SUCCEEDED, DurableClientTest.awaitTerminal(back, WORK).view().state());
      DurableClientTest.get(back.manifest(WORK, 1));
      DurableClientTest.get(back.select(WORK, 1, 0));
      Path output = directory.resolve("shutdown-output.bin");
      DurableClientTest.get(back.read(WORK, 1, 0, new ResultFiles.Destination(output)));
      assertArrayEquals(bytes, Files.readAllBytes(output));
      DurableClientTest.get(back.detach());
    }
  }

  @Test
  void storageWorkerExhaustionRefusesRequestsInsteadOfBlockingTheLoop() throws Exception {
    Path root = directory.resolve("workers");
    java.util.concurrent.CountDownLatch paused = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    Boundaries stuck =
        new Boundaries() {
          @Override
          public void committed(Boundary boundary, Details details) {
            if (boundary != Boundary.DECLARATION_COMMITTED || paused.getCount() == 0) return;
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
    DurableHost.Configuration defaults =
        DurableHost.Configuration.defaults("issuer-a", "localhost:7443");
    DurableHost.Configuration tight =
        new DurableHost.Configuration(
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
            new DurableHost.WorkerLimits(1, 3, 3),
            defaults.producer());
    DurableHost.OwnerPolicy owners = DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true);
    try (DurableHost host =
            DurableHost.initialize(
                root,
                tight,
                ReferenceApplications.all(),
                owners,
                DurableHost.UtcClock.system(true));
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults(),
                stuck);
        RawDurablePeer peer = new RawDurablePeer(server.address(), pki.client("alice"), 65_536)) {
      peer.negotiate(
          RawDurablePeer.offer(List.of(Messages.DURABLE_WORK, Messages.RESULT_DELIVERY), 1 << 20));
      assertInstanceOf(
          Messages.Binding.class, peer.call(new Messages.Create(peer.request(), 1, POLICY)));
      // The only storage worker commits a declaration and then blocks inside the hook.
      long first = peer.request();
      peer.send(new Messages.Declare(first, DurableClientTest.operation(1), 0, List.of(1L), false));
      assertTrue(paused.await(10, TimeUnit.SECONDS), "worker never reached the paused boundary");
      // Two more requests fill the bounded queue; the next one is refused on the event loop
      // immediately, with the connection intact, instead of stalling behind the stuck worker.
      long second = peer.request();
      long third = peer.request();
      long fourth = peer.request();
      peer.send(
          new Messages.Declare(second, DurableClientTest.operation(2), 0, List.of(2L), false));
      peer.send(new Messages.Declare(third, DurableClientTest.operation(3), 0, List.of(3L), false));
      peer.send(
          new Messages.Declare(fourth, DurableClientTest.operation(4), 0, List.of(4L), false));
      Messages.Refusal refused = assertInstanceOf(Messages.Refusal.class, peer.next());
      assertEquals(new Records.RequestTag(false, fourth), refused.request(), refused.toString());
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, refused.code(), refused.toString());
      assertFalse(peer.closed.isDone(), "a capacity refusal is not a connection failure");
      release.countDown();
      java.util.Set<Long> answered = new java.util.HashSet<>();
      List<Message> replies = new java.util.ArrayList<>();
      for (int i = 0; i < 3; i++) replies.add(peer.next());
      for (Message reply : replies) {
        DeclarationResponse response =
            assertInstanceOf(DeclarationResponse.class, reply, replies::toString);
        answered.add(response.request());
      }
      assertEquals(java.util.Set.of(first, second, third), answered);
      // The fourth declaration is simply retried once capacity exists.
      assertInstanceOf(
          Messages.DeclarationResponse.class,
          peer.call(
              new Messages.Declare(
                  peer.request(), DurableClientTest.operation(4), 0, List.of(4L), true)));
      assertInstanceOf(Messages.Detached.class, peer.call(new Messages.Detach(peer.request())));
    }
  }
}
