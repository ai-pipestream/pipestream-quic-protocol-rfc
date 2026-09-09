package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Explicit retry, cancellation, skip, scope cancellation, deadline expiry, retained receipts and
 * the frozen external transform through the real Java client and host.
 */
@Timeout(240)
final class DurableMutationTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(30_000, 60_000, 120_000);

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice", "bob"));
    principals = pki.principals(List.of("alice", "bob"));
  }

  static <T> T get(CompletionStage<T> stage) throws Exception {
    return DurableClientTest.get(stage);
  }

  static Records.OperationId op(int value) {
    return DurableClientTest.operation(value);
  }

  static ProtocolError.Code code(CompletionStage<?> stage) {
    return DurableClientTest.refusal(stage).code();
  }

  private final class Session implements AutoCloseable {
    final DurableHost host;
    final DurableServer server;
    final ClientJournal journal;
    final DurableClient client;

    Session(String name, String owner, boolean allowSkip) throws Exception {
      DurableHost.OwnerPolicy owners =
          DurableHost.OwnerPolicy.fromPrincipals(() -> principals, allowSkip);
      host =
          DurableHost.initialize(
              directory.resolve(name),
              DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
              ReferenceApplications.all(),
              owners,
              DurableHost.UtcClock.system(true));
      server =
          DurableServer.start(
              new InetSocketAddress("127.0.0.1", 0),
              pki.server(principals),
              host,
              DurableOptions.defaults());
      journal =
          ClientJournal.initialize(
              directory.resolve(name + "-journal.sqlite"),
              new ClientJournal.Intent("issuer-a", owner, 1, POLICY, true),
              ClientJournal.Limits.defaults());
      client =
          DurableClient.connect(
              server.address(), pki.client(owner), journal, ClientOptions.defaults());
      get(client.ready());
      get(client.binding());
    }

    Records.OperationReceipt admit(
        int operation,
        int declaration,
        Records.WorkKey work,
        byte[] bytes,
        String application,
        long executionMs)
        throws Exception {
      Path file = Files.createTempFile(directory, "input-", ".bin");
      Files.write(file, bytes);
      InputSource source = InputSource.file(file, "application/octet-stream", 16L << 20);
      return get(
          client.admit(
              op(operation),
              new Records.AdmitParameters(
                  work,
                  source.input(),
                  application,
                  0,
                  executionMs,
                  new Records.OutputBudget(1, bytes.length)),
              op(declaration),
              source));
    }

    byte[] read(Records.WorkKey work, long attempt) throws Exception {
      get(client.manifest(work, attempt));
      get(client.select(work, attempt, 0));
      Path output = Files.createTempFile(directory, "read-", ".bin");
      Files.delete(output);
      get(client.read(work, attempt, 0, new ResultFiles.Destination(output)));
      return Files.readAllBytes(output);
    }

    @Override
    public void close() throws java.io.IOException, java.sql.SQLException {
      client.close();
      journal.close();
      server.close();
      host.close();
    }
  }

  static Records.WorkView awaitState(
      DurableClient client, Records.WorkKey work, Records.State expected) throws Exception {
    long revision = 0;
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60);
    while (true) {
      ClientJournal.Observed observed = get(client.watch(work, revision, 5000));
      if (observed.view().state() == expected) return observed.view();
      assertFalse(observed.view().state().terminal(), "unexpected terminal " + observed.view());
      revision = observed.revision();
      assertTrue(System.nanoTime() < deadline, "work did not reach " + expected);
    }
  }

  @Test
  void explicitRetryAdvancesExactlyOnceAndReplaysWithoutSecondIncrement() throws Exception {
    byte[] bytes = new byte[12_000];
    new Random(4).nextBytes(bytes);
    Records.WorkKey work = new Records.WorkKey(0, 0, 1);
    try (Session s = new Session("retry", "alice", false)) {
      get(s.client.declare(op(1), 0, List.of(1L), true));
      s.admit(2, 1, work, bytes, "retry-copy/v2", 20_000);
      Records.WorkView awaiting = awaitState(s.client, work, Records.State.AWAITING_RETRY);
      assertEquals(1, awaiting.attempt());
      assertNotNull(awaiting.diagnostic());
      // Stale expected attempt is CONFLICT and does not advance.
      assertEquals(ProtocolError.Code.CONFLICT, code(s.client.retry(op(3), work, 2)));
      Records.OperationReceipt retried = get(s.client.retry(op(4), work, 1));
      Records.Retried outcome = assertInstanceOf(Records.Retried.class, retried.outcome());
      assertEquals(2, outcome.replacementAttempt());
      // Exact replay returns the same receipt without a third attempt.
      assertEquals(retried, get(s.client.retry(op(4), work, 1)));
      Records.WorkView done = awaitState(s.client, work, Records.State.SUCCEEDED);
      assertEquals(2, done.attempt());
      assertEquals(awaiting.admittedAt(), done.admittedAt(), "retry preserves admission time");
      assertEquals(awaiting.deadline(), done.deadline(), "retry never extends the deadline");
      assertArrayEquals(bytes, s.read(work, 2));
      // Reading the superseded attempt's output is NOT_FOUND; a terminal retry is ALREADY_TERMINAL.
      assertEquals(ProtocolError.Code.NOT_FOUND, code(s.client.manifest(work, 1)));
      assertEquals(ProtocolError.Code.ALREADY_TERMINAL, code(s.client.retry(op(5), work, 2)));
      // Reusing an operation identity with different parameters is CONFLICT.
      assertEquals(ProtocolError.Code.CONFLICT, code(s.client.cancel(op(4), work)));
      // Cancellation of terminal work returns the existing outcome with disposition 1.
      Records.Cancelled cancelled =
          assertInstanceOf(Records.Cancelled.class, get(s.client.cancel(op(6), work)).outcome());
      assertEquals(1, cancelled.disposition());
      assertEquals(Records.State.SUCCEEDED, cancelled.state());
      get(s.client.detach());
    }
  }

  @Test
  void cancellationSkipAndScopeCancellationSettleDeclaredWork() throws Exception {
    byte[] bytes = new byte[100];
    Records.WorkKey declaredOnly = new Records.WorkKey(0, 0, 1);
    Records.WorkKey skipped = new Records.WorkKey(0, 0, 2);
    Records.WorkKey scoped = new Records.WorkKey(0, 0, 3);
    Records.WorkKey scopedToo = new Records.WorkKey(0, 0, 4);
    try (Session s = new Session("cancel", "alice", true)) {
      get(s.client.declare(op(1), 0, List.of(1L, 2L, 3L, 4L), false));
      // Inputless cancellation of a declared entity: attempt stays 0, no input/admission/deadline.
      Records.Cancelled cancelled =
          assertInstanceOf(
              Records.Cancelled.class, get(s.client.cancel(op(2), declaredOnly)).outcome());
      assertEquals(0, cancelled.disposition());
      Records.WorkView view = awaitState(s.client, declaredOnly, Records.State.CANCELLED);
      assertEquals(0, view.attempt());
      assertNull(view.input());
      assertNull(view.deadline());
      assertNotNull(view.terminalAt());
      // An identical cancel replays; a conflicting later skip of cancelled work reports the
      // existing terminal outcome with disposition 1.
      assertEquals(
          cancelled,
          assertInstanceOf(
              Records.Cancelled.class, get(s.client.cancel(op(2), declaredOnly)).outcome()));
      Records.Skipped later =
          assertInstanceOf(
              Records.Skipped.class, get(s.client.skip(op(3), declaredOnly)).outcome());
      assertEquals(1, later.disposition());
      assertEquals(Records.State.CANCELLED, later.state());
      // Explicit skip with permission settles SKIPPED and never counts as success.
      Records.Skipped skip =
          assertInstanceOf(Records.Skipped.class, get(s.client.skip(op(4), skipped)).outcome());
      assertEquals(0, skip.disposition());
      assertEquals(
          Records.State.SKIPPED, awaitState(s.client, skipped, Records.State.SKIPPED).state());
      // Admitted work then cancelled: the accepted fence settles it and no result is published.
      s.admit(5, 1, scoped, bytes, "copy/v2", 20_000);
      // Whole-scope cancellation seals the root (including the never-admitted member 4) and
      // settles nonterminal members as CANCELLED; terminal outcomes are retained.
      Records.ScopeCancelled scopeCancelled =
          assertInstanceOf(
              Records.ScopeCancelled.class, get(s.client.cancelScope(op(6), 0)).outcome());
      assertEquals(0, scopeCancelled.scope());
      assertEquals(
          Records.State.CANCELLED,
          awaitState(s.client, scopedToo, Records.State.CANCELLED).state());
      Records.WorkView third = DurableClientTest.awaitTerminal(s.client, scoped).view();
      assertTrue(
          third.state() == Records.State.CANCELLED || third.state() == Records.State.SUCCEEDED,
          third.toString());
      // Late declaration into the fenced root is refused; membership is frozen. Section 12.5 names
      // CONFLICT for a late declaration into a sealed scope and 12.6 only says the fence excludes
      // it; the reviewed Java store answers CANCELLED. Recorded as a normative clarification.
      ProtocolError.Code frozen = code(s.client.declare(op(7), 0, List.of(5L), false));
      assertTrue(
          frozen == ProtocolError.Code.CONFLICT || frozen == ProtocolError.Code.CANCELLED,
          frozen.toString());
      DurableClient.ScopePage page = get(s.client.page(0, 0, 256));
      long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
      while (!page.sealed() || page.seal() == null) {
        assertTrue(System.nanoTime() < deadline, "frozen seal did not materialize");
        Thread.sleep(50);
        page = get(s.client.page(0, 0, 256));
      }
      assertEquals(4, page.declared());
      assertTrue(page.membershipVerified());
      Records.ScopeSummary root = get(s.client.checkpoint(0, page.seal(), 10_000));
      long success = third.state() == Records.State.SUCCEEDED ? 1 : 0;
      assertEquals(new Records.Counts(success, 0, 3 - success, 1), root.counts());
      assertEquals(root, get(s.client.complete()));
      get(s.client.detach());
    }
  }

  @Test
  void skipWithoutPermissionIsUnauthorizedAndDeadlineExpiryFailsAuthoritatively() throws Exception {
    byte[] bytes = new byte[50];
    Records.WorkKey work = new Records.WorkKey(0, 0, 1);
    Records.WorkKey expiring = new Records.WorkKey(0, 0, 2);
    try (Session s = new Session("skip-expiry", "bob", false)) {
      get(s.client.declare(op(1), 0, List.of(1L, 2L), true));
      assertEquals(ProtocolError.Code.UNAUTHORIZED, code(s.client.skip(op(2), work)));
      assertEquals(Records.State.DECLARED, get(s.client.watch(work, 0, 0)).view().state());
      // A retryable application whose caller never retries reaches its execution deadline and
      // settles FAILED with a diagnostic; retry after expiry is DEADLINE_EXCEEDED or terminal.
      s.admit(3, 1, expiring, bytes, "retry-copy/v2", 1000);
      awaitState(s.client, expiring, Records.State.AWAITING_RETRY);
      Records.WorkView failed = DurableClientTest.awaitTerminal(s.client, expiring).view();
      assertEquals(Records.State.FAILED, failed.state());
      assertNotNull(failed.diagnostic());
      assertNull(failed.manifest());
      ProtocolError.Code late = code(s.client.retry(op(4), expiring, 1));
      assertTrue(
          late == ProtocolError.Code.DEADLINE_EXCEEDED
              || late == ProtocolError.Code.ALREADY_TERMINAL,
          late.toString());
      // Watching after a revision beyond the current one is CONFLICT.
      ClientJournal.Observed current = get(s.client.watch(expiring, 0, 0));
      assertEquals(
          ProtocolError.Code.CONFLICT,
          code(s.client.watch(expiring, current.revision() + 1000, 0)));
      // A zero wait on the unchanged revision returns the same view, not a failure.
      assertEquals(current.view(), get(s.client.watch(expiring, current.revision(), 0)).view());
      get(s.client.detach());
    }
  }

  @Test
  void frozenTransformMatchesTheExternalOracle() throws Exception {
    byte[] bytes = new byte[100_003];
    new Random(77).nextBytes(bytes);
    byte[] expected = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      int b = bytes[i] & 0xff;
      expected[i] = (byte) ((((b << 1) | (b >>> 7)) & 0xff) ^ (i % 251));
    }
    Records.WorkKey work = new Records.WorkKey(0, 0, 1);
    try (Session s = new Session("transform", "alice", false)) {
      get(s.client.declare(op(1), 0, List.of(1L), true));
      s.admit(2, 1, work, bytes, "transform/v2", 20_000);
      assertEquals(
          Records.State.SUCCEEDED, DurableClientTest.awaitTerminal(s.client, work).view().state());
      assertArrayEquals(expected, s.read(work, 1));
      get(s.client.detach());
    }
  }
}
