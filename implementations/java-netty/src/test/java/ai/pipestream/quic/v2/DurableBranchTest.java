package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Branch workflows through the real Java client and Java host: caller-expanded reassembly,
 * authority-expanded chunking, STRICT failure, empty sealed scopes, zero-output applications and
 * bottom-up coverage including a child-before-parent observation order.
 */
@Timeout(240)
final class DurableBranchTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(30_000, 60_000, 120_000);

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
  }

  static <T> T get(java.util.concurrent.CompletionStage<T> stage) throws Exception {
    return DurableClientTest.get(stage);
  }

  static Records.OperationId op(int value) {
    return DurableClientTest.operation(value);
  }

  private final class Session implements AutoCloseable {
    final DurableHost host;
    final DurableServer server;
    final ClientJournal journal;
    final DurableClient client;

    Session(String name, long sequence) throws Exception {
      this(name, sequence, DurableHost.Configuration.defaults("issuer-a", "localhost:7443"));
    }

    Session(String name, long sequence, DurableHost.Configuration configuration)
        throws Exception {
      DurableHost.OwnerPolicy owners =
          DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true);
      host =
          DurableHost.initialize(
              directory.resolve(name),
              configuration,
              ReferenceApplications.all(),
              owners,
              DurableHost.UtcClock.system(true));
      server =
          DurableServer.start(
              new java.net.InetSocketAddress("127.0.0.1", 0),
              pki.server(principals),
              host,
              DurableOptions.defaults());
      journal =
          ClientJournal.initialize(
              directory.resolve(name + "-journal.sqlite"),
              new ClientJournal.Intent("issuer-a", "alice", sequence, POLICY, true),
              ClientJournal.Limits.defaults());
      client =
          DurableClient.connect(
              server.address(), pki.client("alice"), journal, ClientOptions.defaults());
      get(client.ready());
      get(client.binding());
    }

    Records.OperationReceipt admit(
        int operation,
        int declaration,
        Records.WorkKey work,
        byte[] bytes,
        String application,
        int mode,
        int outputs)
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
                  mode,
                  20_000,
                  new Records.OutputBudget(outputs, outputs == 0 ? 0 : bytes.length)),
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

    /**
     * Bottom-up coverage of one scope: page its whole membership (following {@code more()}, since
     * one page is never completeness evidence), wait for every member, cover each child scope
     * first, then checkpoint over the seal. Members walked for the scope are left in {@link
     * #covered}.
     */
    Records.ScopeSummary coverage(long scope) throws Exception {
      DurableClient.ScopePage page = get(client.page(scope, 0, 256));
      assertTrue(page.sealed(), "scope " + scope + " must be sealed before checkpoint");
      List<Messages.Entry> entries = new ArrayList<>(page.entries());
      while (page.more()) {
        page = get(client.page(scope, entries.getLast().entity(), 256));
        assertTrue(page.sealed(), "scope " + scope + " must stay sealed across pages");
        entries.addAll(page.entries());
      }
      assertEquals(page.declared(), entries.size(), "scope " + scope + " membership incomplete");
      assertTrue(page.membershipVerified(), "scope " + scope + " membership not verified");
      for (Messages.Entry entry : entries) {
        Records.WorkKey member = new Records.WorkKey(scope, page.producer(), entry.entity());
        ClientJournal.Observed observed = DurableClientTest.awaitTerminal(client, member);
        if (observed.view().child() != null) coverage(observed.view().child().scope());
      }
      covered.clear();
      for (Messages.Entry entry : entries) covered.add(entry.entity());
      return get(client.checkpoint(scope, page.seal(), 10_000));
    }

    final List<Long> covered = new ArrayList<>();

    @Override
    public void close() throws java.io.IOException, java.sql.SQLException {
      client.close();
      journal.close();
      server.close();
      host.close();
    }
  }

  @Test
  void callerExpandedChildrenAreReassembledAndCoveredBottomUp() throws Exception {
    byte[] first = new byte[70_000];
    byte[] second = new byte[30_001];
    new Random(1).nextBytes(first);
    new Random(2).nextBytes(second);
    byte[] whole = new byte[first.length + second.length];
    System.arraycopy(first, 0, whole, 0, first.length);
    System.arraycopy(second, 0, whole, first.length, second.length);
    Records.WorkKey parent = new Records.WorkKey(0, 0, 1);
    try (Session s = new Session("caller-expanded", 1)) {
      get(s.client.declare(op(1), 0, List.of(1L), true));
      Records.OperationReceipt admitted = s.admit(2, 1, parent, whole, "reassemble/v2", 1, 1);
      Records.Admitted outcome = assertInstanceOf(Records.Admitted.class, admitted.outcome());
      assertNotNull(outcome.child());
      assertEquals(0, outcome.child().producer());
      long child = outcome.child().scope();
      // The parent waits for its children; watching it reports the allocated child scope.
      ClientJournal.Observed waiting = get(s.client.watch(parent, 0, 0));
      assertEquals(outcome.child(), waiting.view().child());
      assertFalse(waiting.view().state().terminal());
      // Children declared under the child scope by the caller, sealed, then admitted.
      get(s.client.declare(op(3), child, List.of(1L, 2L), true));
      s.admit(4, 3, new Records.WorkKey(child, 0, 1), first, "copy/v2", 0, 1);
      s.admit(5, 3, new Records.WorkKey(child, 0, 2), second, "copy/v2", 0, 1);
      Records.ScopeSummary childSummary = s.coverage(child);
      assertEquals(new Records.Counts(2, 0, 0, 0), childSummary.counts());
      assertEquals(parent, childSummary.parent());
      ClientJournal.Observed done = DurableClientTest.awaitTerminal(s.client, parent);
      assertEquals(Records.State.SUCCEEDED, done.view().state());
      assertArrayEquals(whole, s.read(parent, 1));
      Records.ScopeSummary root = s.coverage(0);
      assertEquals(new Records.Counts(1, 0, 0, 0), root.counts());
      assertEquals(root, get(s.client.complete()));
      // A checkpoint over a foreign seal is refused before it is sent: the journal holds this
      // child's verified membership under its own seal, so the client refuses locally with the
      // same INTEGRITY_ERROR the authority would give (SessionStoreTest); Section 12.8 permits
      // the local refusal (owner decision 2026-09-13).
      assertEquals(
          ProtocolError.Code.INTEGRITY_ERROR,
          DurableClientTest.refusal(s.client.checkpoint(child, root.seal(), 0)).code());
      get(s.client.detach());
    }
  }

  @Test
  void authorityExpandedChunksProduceChildrenAndParentReassembly() throws Exception {
    byte[] whole = new byte[2 * ReferenceApplications.CHUNK + 12_345];
    new Random(9).nextBytes(whole);
    Records.WorkKey parent = new Records.WorkKey(0, 0, 1);
    try (Session s = new Session("authority-expanded", 1)) {
      get(s.client.declare(op(1), 0, List.of(1L), true));
      Records.Admitted outcome =
          assertInstanceOf(
              Records.Admitted.class,
              s.admit(2, 1, parent, whole, "chunk-copy/v2", 2, 1).outcome());
      assertEquals(1, outcome.child().producer());
      long child = outcome.child().scope();
      ClientJournal.Observed done = DurableClientTest.awaitTerminal(s.client, parent);
      assertEquals(
          Records.State.SUCCEEDED, done.view().state(), String.valueOf(done.view().diagnostic()));
      assertArrayEquals(whole, s.read(parent, 1));
      // Child-before-parent observation order: page the producer-1 child scope first.
      DurableClient.ScopePage page = get(s.client.page(child, 0, 256));
      assertEquals(1, page.producer());
      assertEquals(3, page.declared());
      assertTrue(page.membershipVerified());
      assertEquals(parent, page.parent());
      // Children carry the parent's execution duration: a restart during the expansion must not
      // expire them (defect 12).
      for (Messages.Entry entry : page.entries()) {
        Records.WorkKey member = new Records.WorkKey(child, 1, entry.entity());
        var view = get(s.client.watch(member, 0, 0)).view();
        assertEquals(20_000, view.deadline() - view.admittedAt(), String.valueOf(view));
      }
      byte[] chunk = s.read(new Records.WorkKey(child, 1, 2), 1);
      assertArrayEquals(
          Arrays.copyOfRange(whole, ReferenceApplications.CHUNK, 2 * ReferenceApplications.CHUNK),
          chunk);
      // The caller cannot declare or admit into the authority's scope.
      assertEquals(
          ProtocolError.Code.UNAUTHORIZED,
          DurableClientTest.refusal(s.client.declare(op(7), child, List.of(4L), false)).code());
      Records.ScopeSummary childSummary = s.coverage(child);
      assertEquals(new Records.Counts(3, 0, 0, 0), childSummary.counts());
      Records.ScopeSummary root = s.coverage(0);
      assertEquals(new Records.Counts(1, 0, 0, 0), root.counts());
      assertEquals(root, get(s.client.complete()));
      get(s.client.detach());
    }
  }

  @Test
  void strictFailureEmptyScopesAndZeroOutputs() throws Exception {
    Records.WorkKey parent = new Records.WorkKey(0, 0, 1);
    Records.WorkKey consumer = new Records.WorkKey(0, 0, 2);
    Records.WorkKey emptyBranch = new Records.WorkKey(0, 0, 3);
    byte[] whole = new byte[10];
    try (Session s = new Session("strict", 1)) {
      get(s.client.declare(op(1), 0, List.of(1L, 2L, 3L), true));
      long child =
          assertInstanceOf(
                  Records.Admitted.class,
                  s.admit(2, 1, parent, whole, "reassemble/v2", 1, 1).outcome())
              .child()
              .scope();
      get(s.client.declare(op(3), child, List.of(1L), true));
      // The child's output contradicts the parent's committed input: the child succeeds, the
      // STRICT parent reassembly fails, and the failure is authoritative with a diagnostic.
      s.admit(4, 3, new Records.WorkKey(child, 0, 1), new byte[] {1, 2, 3}, "copy/v2", 0, 1);
      assertEquals(
          Records.State.SUCCEEDED,
          DurableClientTest.awaitTerminal(s.client, new Records.WorkKey(child, 0, 1))
              .view()
              .state());
      ClientJournal.Observed failed = DurableClientTest.awaitTerminal(s.client, parent);
      assertEquals(Records.State.FAILED, failed.view().state());
      assertNotNull(failed.view().diagnostic());
      assertNull(failed.view().manifest());
      assertEquals(
          ProtocolError.Code.NOT_READY,
          DurableClientTest.refusal(s.client.manifest(parent, 1)).code());
      // consume/v2 with zero outputs succeeds without a manifest object.
      Records.OperationReceipt consumed = s.admit(5, 1, consumer, whole, "consume/v2", 0, 0);
      assertNotNull(consumed);
      ClientJournal.Observed observed = DurableClientTest.awaitTerminal(s.client, consumer);
      assertEquals(Records.State.SUCCEEDED, observed.view().state());
      assertTrue(observed.view().manifest().outputs().isEmpty());
      // An empty sealed child scope closes with four zero counts; its branch parent then fails
      // reassembly against a nonempty committed input (nothing to reassemble), authoritatively.
      long emptyChild =
          assertInstanceOf(
                  Records.Admitted.class,
                  s.admit(6, 1, emptyBranch, whole, "reassemble/v2", 1, 1).outcome())
              .child()
              .scope();
      get(s.client.declare(op(7), emptyChild, List.of(), true));
      Records.ScopeSummary emptySummary = s.coverage(emptyChild);
      assertEquals(new Records.Counts(0, 0, 0, 0), emptySummary.counts());
      assertEquals(0, emptySummary.declared());
      assertEquals(
          Records.State.FAILED,
          DurableClientTest.awaitTerminal(s.client, emptyBranch).view().state());
      Records.ScopeSummary root = s.coverage(0);
      assertEquals(new Records.Counts(1, 2, 0, 0), root.counts());
      assertEquals(root, get(s.client.complete()));
      get(s.client.detach());
    }
  }

  @Test
  void undeclaredChildAndWrongScopeProducerAreRefusedWithoutLosingDeclarations() throws Exception {
    Records.WorkKey parent = new Records.WorkKey(0, 0, 1);
    byte[] whole = new byte[5];
    try (Session s = new Session("undeclared", 1)) {
      get(s.client.declare(op(1), 0, List.of(1L), true));
      long child =
          assertInstanceOf(
                  Records.Admitted.class,
                  s.admit(2, 1, parent, whole, "reassemble/v2", 1, 1).outcome())
              .child()
              .scope();
      // Input into the child scope before any declaration: the client refuses NOT_READY locally
      // because it holds no covering declaration receipt (S12-150); the authority's own CONFLICT
      // for an undeclared input is proven from a raw peer in DurableServerTest.
      assertEquals(
          ProtocolError.Code.NOT_READY,
          DurableClientTest.refusal(
                  s.client.admit(
                      op(3),
                      new Records.AdmitParameters(
                          new Records.WorkKey(child, 0, 1),
                          new Records.Input(
                              5, DurableServerTest.digest(whole), "application/octet-stream"),
                          "copy/v2",
                          0,
                          20_000,
                          new Records.OutputBudget(1, 5)),
                      op(1),
                      inputSource(whole)))
              .code());
      // Declaring into a scope that does not exist: NOT_FOUND.
      assertEquals(
          ProtocolError.Code.NOT_FOUND,
          DurableClientTest.refusal(s.client.declare(op(4), child + 77, List.of(1L), true)).code());
      // Late declaration into the sealed root: CONFLICT.
      assertEquals(
          ProtocolError.Code.CONFLICT,
          DurableClientTest.refusal(s.client.declare(op(5), 0, List.of(2L), false)).code());
      // The child scope still accepts its producer's declaration and closes normally.
      get(s.client.declare(op(6), child, List.of(1L), true));
      s.admit(7, 6, new Records.WorkKey(child, 0, 1), whole, "copy/v2", 0, 1);
      assertEquals(
          Records.State.SUCCEEDED,
          DurableClientTest.awaitTerminal(s.client, parent).view().state());
      List<Long> members = new ArrayList<>();
      for (Messages.Entry entry : get(s.client.page(0, 0, 256)).entries())
        members.add(entry.entity());
      assertEquals(List.of(1L), members);
      get(s.client.detach());
    }
  }

  /**
   * Section 12.8: the client verifies identity, seal, count partition and known commitments before
   * acknowledging coverage. A root scope of 257 members does not fit one page, so coverage must
   * follow {@code more()} before the journal can verify the membership and the checkpoint can be
   * sent; the seal and the status root the authority returns then equal the ones recomputed here
   * from the members actually walked.
   */
  @Test
  void coverageFollowsMoreAcrossPagesAndRecomputesTheSealAndStatusRoot() throws Exception {
    List<Long> first = new ArrayList<>();
    for (long entity = 1; entity <= 256; entity++) first.add(entity);
    // A 256-member declaration reserves more WAL than the reference file policy allows, so the
    // host gets the file limits DeclarationStoreTest uses for its thousand-member scope.
    DurableHost.Configuration defaults =
        DurableHost.Configuration.defaults("issuer-a", "localhost:7443");
    DurableHost.Configuration roomy =
        new DurableHost.Configuration(
            defaults.authority(),
            defaults.resultAuthority(),
            defaults.sessionLimits(),
            defaults.maximumPolicy(),
            defaults.maxOwners(),
            defaults.maxSessions(),
            defaults.maxSessionsPerOwner(),
            new ai.pipestream.quic.BoundedSqlite.Limits(
                256L << 20, 512L << 20, 64L << 20, 4L << 20),
            defaults.objects(),
            defaults.maxJobs(),
            defaults.maxJobsPerOwner(),
            defaults.execution(),
            defaults.scheduler(),
            defaults.retention(),
            defaults.results(),
            defaults.waits(),
            defaults.storageWorkers(),
            defaults.producer());
    try (Session s = new Session("paged-coverage", 1, roomy)) {
      get(s.client.declare(op(1), 0, first, false));
      get(s.client.declare(op(2), 0, List.of(257L), true));
      DurableClient.ScopePage single = get(s.client.page(0, 0, 256));
      assertTrue(single.sealed());
      assertTrue(single.more());
      assertEquals(256, single.entries().size());
      assertEquals(257, single.declared());
      assertFalse(single.membershipVerified(), "one page of 256 is not the membership");
      assertEquals(
          ProtocolError.Code.NOT_READY,
          DurableClientTest.refusal(s.client.checkpoint(0, single.seal(), 0)).code());

      get(s.client.cancelScope(op(3), 0));
      Records.ScopeSummary root = s.coverage(0);
      List<Long> expected = new ArrayList<>(first);
      expected.add(257L);
      assertEquals(expected, s.covered);
      assertEquals(257, root.declared());
      assertEquals(new Records.Counts(0, 0, 257, 0), root.counts());

      Commitments.Context context =
          new Commitments.Context("issuer-a", "alice", get(s.client.binding()).generation());
      Commitments.Seal seal = new Commitments.Seal(context, 0, 0, null, s.covered.size());
      Commitments.StatusTree tree = new Commitments.StatusTree(0, 0, s.covered.size());
      for (long entity : s.covered) {
        seal.add(entity);
        Records.WorkView view =
            get(s.client.watch(new Records.WorkKey(0, 0, entity), 0, 0)).view();
        assertEquals(Records.State.CANCELLED, view.state());
        tree.add(view, null);
      }
      assertEquals(seal.finish(), root.seal());
      Commitments.Status status = tree.finish();
      assertEquals(status.root(), root.statusRoot());
      assertEquals(status.counts(), root.counts());
      assertEquals(root, get(s.client.complete()));
      get(s.client.detach());
    }
  }

  private InputSource inputSource(byte[] bytes) throws Exception {
    Path file = Files.createTempFile(directory, "input-", ".bin");
    Files.write(file, bytes);
    return InputSource.file(file, "application/octet-stream", 16L << 20);
  }
}
