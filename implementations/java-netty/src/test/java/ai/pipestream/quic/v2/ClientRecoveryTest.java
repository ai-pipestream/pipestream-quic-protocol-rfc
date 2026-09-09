package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.LauncherInvocation.code;
import static ai.pipestream.quic.v2.LauncherInvocation.hex;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.v2.LauncherInvocation.Run;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12.2.1 client recovery through the shipped launcher: the one-shot mode (recovery is the
 * next invocation over the retained journal) and the explicit {@code --retry-budget} mode. The
 * refusals are real: a saturated storage-worker pool refuses before any commit, and withheld
 * replies after real commits are recovered by replay under the original identity.
 */
@Timeout(240)
final class ClientRecoveryTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(30_000, 60_000, 120_000);

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice", "bob"));
    principals = pki.principals(List.of("alice", "bob"));
  }

  private static Run invoke(Path journal, InetSocketAddress address, String... operation) {
    return LauncherInvocation.invoke(pki, "alice", journal, address, Boundaries.NONE, operation);
  }

  private static long count(String output, String prefix) {
    return output.lines().filter(line -> line.startsWith(prefix)).count();
  }

  private static Path initJournal(String name) throws Exception {
    return LauncherInvocation.initJournal(directory.resolve(name), "alice");
  }

  private static Path file(String name, int length, long seed) throws Exception {
    Path path = directory.resolve(name);
    Files.write(path, DurableServerTest.payload(length, seed));
    return path;
  }

  private static DurableHost.OwnerPolicy owners() {
    return DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true);
  }

  /** One storage worker with a queue of two, so a blocked commit refuses the fourth request. */
  private static DurableHost.Configuration tightWorkers() {
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
        defaults.retention(),
        defaults.results(),
        defaults.waits(),
        new DurableHost.WorkerLimits(1, 3, 3),
        defaults.producer());
  }

  /**
   * Counts admission commits and capacity refusals; optionally parks one declaration commit inside
   * the hook (real worker saturation) and withholds a number of admission replies after their real
   * commits (real reply loss).
   */
  static final class Hooks implements Boundaries {
    /** The other owner's declaration whose commit is parked inside the hook. */
    static final Records.OperationId PARKED = DurableServerTest.operation(101);

    final AtomicInteger admissionCommits = new AtomicInteger();
    final AtomicInteger limitRefusals = new AtomicInteger();
    final AtomicInteger withheld = new AtomicInteger();
    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch installed = new CountDownLatch(1);
    final CountDownLatch releaseInput = new CountDownLatch(1);
    volatile int releaseAfterRefusals = Integer.MAX_VALUE;
    volatile int withholdAdmissionReplies;

    /** When set, this operation's admission parks after its input is installed, before commit. */
    volatile Records.OperationId parkBeforeAdmissionCommit;

    @Override
    public void committed(Boundary boundary, Details details) {
      if (boundary == Boundary.ADMISSION_COMMITTED) admissionCommits.incrementAndGet();
      if (boundary == Boundary.INPUT_INSTALLED
          && details.operation().equals(parkBeforeAdmissionCommit)
          && installed.getCount() > 0) {
        installed.countDown();
        await(releaseInput);
      }
      if (boundary != Boundary.DECLARATION_COMMITTED
          || !PARKED.equals(details.operation())
          || paused.getCount() == 0) return;
      paused.countDown();
      await(release);
    }

    private static void await(CountDownLatch latch) {
      try {
        latch.await(60, TimeUnit.SECONDS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void sent(Boundary boundary, Details details) {
      if (boundary == Boundary.REFUSAL_SENT
          && details.refusal() == ProtocolError.Code.LIMIT_EXCEEDED
          && limitRefusals.incrementAndGet() >= releaseAfterRefusals) release.countDown();
    }

    @Override
    public boolean withhold(Boundary boundary) {
      if (boundary != Boundary.ADMISSION_RESPONSE_SENT
          || withheld.get() >= withholdAdmissionReplies) return false;
      withheld.incrementAndGet();
      return true;
    }
  }

  @Test
  void budgetedInvocationRecoversFromPreAdmissionCapacityRefusals() throws Exception {
    Hooks hooks = new Hooks();
    hooks.releaseAfterRefusals = 2;
    Path journal = initJournal("capacity.sqlite");
    Path input = file("capacity.bin", 40_000, 11);
    try (DurableHost host =
            DurableHost.initialize(
                directory.resolve("capacity"),
                tightWorkers(),
                ReferenceApplications.all(),
                owners(),
                DurableHost.UtcClock.system(true));
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults(),
                hooks);
        RawDurablePeer bob = new RawDurablePeer(server.address(), pki.client("bob"), 65_536)) {
      Run declared =
          invoke(journal, server.address(), "declare", "--operation", hex(1), "--entities", "1");
      assertNull(declared.failure(), declared.output());
      // Another owner parks the only storage worker inside a declaration commit and fills the
      // bounded queue; every request from anyone is now refused LIMIT_EXCEEDED before any commit.
      bob.negotiate(
          RawDurablePeer.offer(List.of(Messages.DURABLE_WORK, Messages.RESULT_DELIVERY), 1 << 20));
      assertInstanceOf(
          Messages.Binding.class, bob.call(new Messages.Create(bob.request(), 1, POLICY)));
      bob.send(new Messages.Declare(bob.request(), Hooks.PARKED, 0, List.of(1L), false));
      assertTrue(
          hooks.paused.await(10, TimeUnit.SECONDS),
          () -> "worker never parked; the other owner received " + bob.messages);
      bob.send(
          new Messages.Declare(
              bob.request(), DurableServerTest.operation(102), 0, List.of(2L), false));
      bob.send(
          new Messages.Declare(
              bob.request(), DurableServerTest.operation(103), 0, List.of(3L), false));
      // The budgeted admission is refused at least twice, each time on a fresh connection, and
      // succeeds once the hook releases the worker: one admission, one receipt, same identity.
      Run admit =
          invoke(
              journal,
              server.address(),
              "admit",
              "--operation",
              hex(2),
              "--declaration",
              hex(1),
              "--work",
              "0:0:1",
              "--input",
              input.toString(),
              "--application",
              "copy/v2",
              "--retry-budget",
              "12",
              "--retry-backoff-ms",
              "100");
      assertNull(admit.failure(), admit.output() + admit.failure());
      assertTrue(count(admit.output(), "RECOVERING") >= 2, admit.output());
      assertTrue(admit.output().contains("code=LIMIT_EXCEEDED"), admit.output());
      assertEquals(1, count(admit.output(), "RECEIPT"), admit.output());
      assertEquals(0, count(admit.output(), "UNRESOLVED"), admit.output());
      assertEquals(1, hooks.admissionCommits.get(), "exactly one admission effect");
      for (int i = 0; i < 3; i++)
        assertInstanceOf(Messages.DeclarationResponse.class, bob.next(), "queued work completes");
      bob.call(new Messages.Detach(bob.request()));
      try (ClientJournal reopened = ClientJournal.open(journal, ClientJournal.Limits.defaults())) {
        Records.OperationReceipt receipt =
            reopened.receipt(DurableServerTest.operation(2)).orElseThrow();
        assertEquals(1, assertInstanceOf(Records.Admitted.class, receipt.outcome()).attempt());
        assertTrue(reopened.unresolved(0, 16).isEmpty());
      }
    }
  }

  @Test
  void lostRepliesAreRecoveredByReplayInBothModesAndBudgetExhaustionStaysUnresolved()
      throws Exception {
    Hooks hooks = new Hooks();
    Path journal = initJournal("replies.sqlite");
    Path input = file("replies.bin", 30_000, 12);
    try (DurableHost host =
            DurableHost.initialize(
                directory.resolve("replies"),
                DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
                ReferenceApplications.all(),
                owners(),
                DurableHost.UtcClock.system(true));
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults(),
                hooks)) {
      assertNull(
          invoke(journal, server.address(), "declare", "--operation", hex(1), "--entities", "1")
              .failure());
      // Every admission reply is withheld after its real commit: a budget of two makes three
      // attempts (commit, replay, replay), then stops with the intent journaled and unresolved.
      hooks.withholdAdmissionReplies = Integer.MAX_VALUE;
      String[] admit = {
        "admit",
        "--operation",
        hex(2),
        "--declaration",
        hex(1),
        "--work",
        "0:0:1",
        "--input",
        input.toString(),
        "--application",
        "copy/v2",
        "--retry-budget",
        "2",
        "--retry-backoff-ms",
        "50"
      };
      Run exhausted = invoke(journal, server.address(), admit);
      assertNotNull(exhausted.failure(), exhausted.output());
      assertEquals(ProtocolError.Code.CONTROL_RESET, code(exhausted.failure()));
      assertEquals(2, count(exhausted.output(), "RECOVERING"), exhausted.output());
      assertTrue(
          exhausted.output().contains("UNRESOLVED attempts=3 last=CONTROL_RESET budget-exhausted"),
          exhausted.output());
      assertEquals(0, count(exhausted.output(), "RECEIPT"), exhausted.output());
      assertEquals(1, hooks.admissionCommits.get(), "replays never commit a second admission");
      try (ClientJournal reopened = ClientJournal.open(journal, ClientJournal.Limits.defaults())) {
        List<ClientJournal.PendingOperation> pending = reopened.unresolved(0, 16);
        assertEquals(1, pending.size());
        assertEquals(DurableServerTest.operation(2), pending.get(0).operation());
        assertTrue(reopened.receipt(DurableServerTest.operation(2)).isEmpty());
      }
      // One-shot mode: the next invocation replays the journaled intent and resolves it once the
      // replies get through; two more replies are withheld first, so the budget mode is also seen.
      hooks.withheld.set(0);
      hooks.withholdAdmissionReplies = 2;
      Run replayed =
          invoke(
              journal,
              server.address(),
              "replay",
              "--operation",
              hex(2),
              "--input",
              input.toString(),
              "--retry-budget",
              "5",
              "--retry-backoff-ms",
              "50");
      assertNull(replayed.failure(), replayed.output() + replayed.failure());
      assertEquals(2, count(replayed.output(), "RECOVERING"), replayed.output());
      assertTrue(replayed.output().contains("code=CONTROL_RESET"), replayed.output());
      assertEquals(1, count(replayed.output(), "RECEIPT"), replayed.output());
      assertEquals(1, hooks.admissionCommits.get(), "still one admission effect");
      Run oneShot = invoke(journal, server.address(), "lookup", "--operation", hex(2));
      assertNull(oneShot.failure(), oneShot.output());
      assertEquals(0, count(oneShot.output(), "RECOVERING"));
      assertEquals(1, count(oneShot.output(), "RECEIPT"), oneShot.output());
      try (ClientJournal reopened = ClientJournal.open(journal, ClientJournal.Limits.defaults())) {
        assertTrue(reopened.unresolved(0, 16).isEmpty());
        assertEquals(
            1,
            assertInstanceOf(
                    Records.Admitted.class,
                    reopened.receipt(DurableServerTest.operation(2)).orElseThrow().outcome())
                .attempt());
      }
      // A refusal that is not a recovery condition stops at once, budget or not: the journal's
      // own CONFLICT for changed parameters under a reused identity.
      Run conflict =
          invoke(
              journal,
              server.address(),
              "declare",
              "--operation",
              hex(1),
              "--entities",
              "1,2",
              "--retry-budget",
              "3");
      assertNotNull(conflict.failure());
      assertEquals(ProtocolError.Code.CONFLICT, code(conflict.failure()));
      assertEquals(0, count(conflict.output(), "RECOVERING"), conflict.output());
      assertTrue(
          conflict.output().contains("UNRESOLVED attempts=1 last=CONFLICT not-recoverable"),
          conflict.output());
      // The selected deadlines are exposed locally, as Section 12.1 asks.
      Run capabilities = invoke(journal, server.address(), "capabilities");
      assertNull(capabilities.failure(), capabilities.output());
      assertTrue(
          capabilities.output().contains("CAPABILITIES offered-idle-ms="), capabilities.output());
      assertTrue(capabilities.output().contains(" selected-lifetime-ms="), capabilities.output());
    }
  }

  @Test
  void lookupDuringAPendingAdmissionReportsAbsenceAndTheOriginalIdentityCommitsOnce()
      throws Exception {
    Hooks hooks = new Hooks();
    Records.OperationId pending = DurableServerTest.operation(2);
    hooks.parkBeforeAdmissionCommit = pending;
    Path journal = initJournal("pending.sqlite");
    Path input = file("pending.bin", 50_000, 13);
    Records.WorkKey work = new Records.WorkKey(0, 0, 1);
    try (DurableHost host =
            DurableHost.initialize(
                directory.resolve("pending"),
                DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
                ReferenceApplications.all(),
                owners(),
                DurableHost.UtcClock.system(true));
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults(),
                hooks)) {
      assertNull(
          invoke(journal, server.address(), "declare", "--operation", hex(1), "--entities", "1")
              .failure());
      Records.OperationReceipt admitted;
      try (ClientJournal opened = ClientJournal.open(journal, ClientJournal.Limits.defaults());
          DurableClient client =
              DurableClient.connect(
                  server.address(), pki.client("alice"), opened, ClientOptions.defaults());
          InputSource source = InputSource.file(input, "application/octet-stream", 16L << 20);
          RawDurablePeer other =
              new RawDurablePeer(server.address(), pki.client("alice"), 65_536)) {
        DurableClientTest.get(client.ready());
        DurableClientTest.get(client.binding());
        // The original admission: journaled, transmitted, its input installed, its commit
        // parked. Nothing about it is visible to anyone yet.
        Records.AdmitParameters parameters =
            new Records.AdmitParameters(
                work, source.input(), "copy/v2", 0, 60_000, new Records.OutputBudget(1, 50_000));
        CompletionStage<Records.OperationReceipt> original =
            client.admit(pending, parameters, DurableServerTest.operation(1), source);
        assertTrue(hooks.installed.await(20, TimeUnit.SECONDS), "input never installed");
        assertEquals(0, hooks.admissionCommits.get());
        // A second connection of the same owner asks for it and is told NOT_FOUND: absence,
        // not proof that the in-flight request will never commit.
        other.negotiate(
            RawDurablePeer.offer(
                List.of(Messages.DURABLE_WORK, Messages.RESULT_DELIVERY), 1 << 20));
        assertInstanceOf(
            Messages.Binding.class,
            other.call(new Messages.Attach(other.request(), "issuer-a", "alice", 1)));
        Messages.Refusal absent =
            assertInstanceOf(
                Messages.Refusal.class,
                other.call(new Messages.LookupOperation(other.request(), pending)));
        assertEquals(ProtocolError.Code.NOT_FOUND, absent.code(), absent.toString());
        // The library client itself refuses to look up an identity it never journaled, without
        // sending anything: a new identity is never inferred from absence.
        ProtocolError local =
            DurableClientTest.refusal(client.lookup(DurableServerTest.operation(9)));
        assertEquals(ProtocolError.Code.NOT_FOUND, local.code());
        assertFalse(local.fromAuthority(), local.toString());
        // The original commits; the other connection now finds the very same receipt.
        hooks.releaseInput.countDown();
        admitted = DurableClientTest.get(original);
        assertEquals(1, assertInstanceOf(Records.Admitted.class, admitted.outcome()).attempt());
        assertEquals(1, hooks.admissionCommits.get());
        Messages.OperationResponse found =
            assertInstanceOf(
                Messages.OperationResponse.class,
                other.call(new Messages.LookupOperation(other.request(), pending)));
        assertEquals(admitted, found.receipt());
        other.call(new Messages.Detach(other.request()));
        DurableClientTest.get(client.detach());
      }
      // The launcher's replay of the same journaled identity is answered from retained state:
      // same receipt, still one admission effect; the work completes once.
      Run replay =
          invoke(
              journal,
              server.address(),
              "replay",
              "--operation",
              hex(2),
              "--input",
              input.toString());
      assertNull(replay.failure(), replay.output() + replay.failure());
      assertEquals(1, count(replay.output(), "RECEIPT"), replay.output());
      assertEquals(1, hooks.admissionCommits.get(), "replay never commits a second admission");
      Run lookup = invoke(journal, server.address(), "lookup", "--operation", hex(2));
      assertNull(lookup.failure(), lookup.output());
      try (ClientJournal reopened = ClientJournal.open(journal, ClientJournal.Limits.defaults())) {
        assertEquals(admitted, reopened.receipt(pending).orElseThrow());
        assertTrue(reopened.unresolved(0, 16).isEmpty());
      }
    }
  }
}
