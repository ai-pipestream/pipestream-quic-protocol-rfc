package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** The Java durable client against the composed Java authority over real QUIC with mTLS. */
@Timeout(180)
final class DurableClientTest {
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

  static <T> T get(CompletionStage<T> stage) throws Exception {
    return stage.toCompletableFuture().get(30, TimeUnit.SECONDS);
  }

  static ProtocolError refusal(CompletionStage<?> stage) {
    Throwable failure = assertThrows(Throwable.class, () -> get(stage));
    while ((failure instanceof ExecutionException || failure instanceof CompletionException)
        && failure.getCause() != null) failure = failure.getCause();
    return assertInstanceOf(ProtocolError.class, failure);
  }

  static DurableHost host(Path root, boolean initialize) throws Exception {
    DurableHost.OwnerPolicy owners = DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true);
    DurableHost.Configuration configuration =
        DurableHost.Configuration.defaults("issuer-a", "localhost:7443");
    return initialize
        ? DurableHost.initialize(
            root,
            configuration,
            ReferenceApplications.all(),
            owners,
            DurableHost.UtcClock.system(true))
        : DurableHost.open(
            root,
            configuration,
            ReferenceApplications.all(),
            owners,
            DurableHost.UtcClock.system(true));
  }

  static DurableServer server(DurableHost host) throws Exception {
    return DurableServer.start(
        new InetSocketAddress("127.0.0.1", 0),
        pki.server(principals),
        host,
        DurableOptions.defaults());
  }

  static Records.OperationId operation(int value) {
    return DurableServerTest.operation(value);
  }

  static Records.AdmitParameters parameters(InputSource source, String application, int mode) {
    return new Records.AdmitParameters(
        WORK,
        source.input(),
        application,
        mode,
        10_000,
        new Records.OutputBudget(1, source.input().length()));
  }

  static ClientJournal.Observed awaitTerminal(DurableClient client, Records.WorkKey work)
      throws Exception {
    long revision = 0;
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (true) {
      ClientJournal.Observed observed = get(client.watch(work, revision, 5000));
      if (observed.view().state().terminal()) return observed;
      revision = observed.revision();
      assertTrue(System.nanoTime() < deadline, "work did not reach a terminal state");
    }
  }

  @Test
  void javaClientCompletesTheWholeSelectedCombinationAgainstJavaAuthority() throws Exception {
    Path root = directory.resolve("java-java");
    Path journalFile = directory.resolve("java-java-journal.sqlite");
    Path input = directory.resolve("java-java-input.bin");
    Path output = directory.resolve("java-java-output.bin");
    byte[] bytes = new byte[400_000];
    new Random(5).nextBytes(bytes);
    Files.write(input, bytes);
    Records.ScopeSummary root0;
    try (DurableHost host = host(root, true);
        DurableServer server = server(host);
        ClientJournal journal =
            ClientJournal.initialize(
                journalFile,
                new ClientJournal.Intent("issuer-a", "alice", 1, POLICY, true),
                ClientJournal.Limits.defaults());
        DurableClient client =
            DurableClient.connect(
                server.address(), pki.client("alice"), journal, ClientOptions.defaults())) {
      Messages.Capabilities selected = get(client.ready());
      assertEquals(List.of(Messages.DURABLE_WORK, Messages.RESULT_DELIVERY), selected.supported());
      assertEquals(1L, get(client.nextSequence()));
      Messages.Binding binding = get(client.binding());
      assertEquals(1, binding.generation());
      assertEquals(binding, journal.binding().orElseThrow());
      assertSame(binding, get(client.binding()), "repeated binding is the retained binding");

      Records.OperationReceipt declared = get(client.declare(operation(1), 0, List.of(1L), true));
      assertEquals(operation(1), declared.operation());
      assertEquals(declared, journal.receipt(operation(1)).orElseThrow());
      // Journaled-before-send: replaying the same declaration returns the retained receipt
      // without another wire round trip, and a changed parameter set is CONFLICT.
      assertEquals(declared, get(client.declare(operation(1), 0, List.of(1L), true)));
      assertEquals(
          ProtocolError.Code.CONFLICT,
          refusal(client.declare(operation(1), 0, List.of(2L), true)).code());

      InputSource source = InputSource.file(input, "application/octet-stream", 16L << 20);
      Records.OperationReceipt admitted =
          get(client.admit(operation(2), parameters(source, "copy/v2", 0), operation(1), source));
      Records.Admitted outcome = assertInstanceOf(Records.Admitted.class, admitted.outcome());
      assertEquals(1, outcome.attempt());
      assertEquals(admitted, journal.receipt(operation(2)).orElseThrow());
      assertTrue(journal.unresolved(0, 256).isEmpty());

      ClientJournal.Observed terminal = awaitTerminal(client, WORK);
      assertEquals(Records.State.SUCCEEDED, terminal.view().state());
      assertEquals(terminal, journal.observedWork(WORK).orElseThrow());
      Records.Manifest manifest = get(client.manifest(WORK, 1));
      assertEquals(terminal.view().manifest(), manifest);
      ClientJournal.Selection selection = get(client.select(WORK, 1, 0));
      assertEquals(bytes.length, selection.output().length());
      // Read requires a saved selection and never overwrites.
      Files.writeString(directory.resolve("occupied.bin"), "x");
      assertEquals(
          ProtocolError.Code.INTERNAL_ERROR,
          refusal(
                  client.read(
                      WORK, 1, 0, new ResultFiles.Destination(directory.resolve("occupied.bin"))))
              .code());
      ResultFiles.Delivered delivered =
          get(client.read(WORK, 1, 0, new ResultFiles.Destination(output)));
      assertFalse(delivered.local());
      assertEquals(bytes.length, delivered.length());
      assertArrayEquals(bytes, Files.readAllBytes(output));
      ResultFiles.Delivered local = ResultFiles.localCopy(output, selection);
      assertTrue(local.local());
      assertEquals(delivered.sha256(), local.sha256());
      try (var listing = Files.list(directory)) {
        assertTrue(
            listing.noneMatch(p -> p.getFileName().toString().startsWith(".pipestream-result-")));
      }

      DurableClient.ScopePage page = get(client.page(0, 0, 256));
      assertTrue(page.sealed());
      assertTrue(page.membershipVerified());
      assertEquals(1, page.entries().size());
      Records.ScopeSummary summary = get(client.checkpoint(0, page.seal(), 5000));
      assertEquals(new Records.Counts(1, 0, 0, 0), summary.counts());
      assertEquals(summary, journal.scope(0).orElseThrow().summary());
      root0 = summary;
      assertEquals(summary, get(client.complete()));
      assertEquals(summary, journal.completedRoot().orElseThrow());
      get(client.detach());
      get(client.closed());
    }

    // Client process "restart": reopen the journal, attach to the retained binding and confirm the
    // retained evidence survives; the authority also restarted meanwhile.
    try (DurableHost host = host(root, false);
        DurableServer server = server(host);
        ClientJournal journal = ClientJournal.open(journalFile, ClientJournal.Limits.defaults());
        DurableClient client =
            DurableClient.connect(
                server.address(), pki.client("alice"), journal, ClientOptions.defaults())) {
      get(client.ready());
      Messages.Binding binding = get(client.binding());
      assertEquals(1, binding.generation());
      assertEquals(Records.State.SUCCEEDED, get(client.watch(WORK, 0, 0)).view().state());
      assertEquals(journal.receipt(operation(2)).orElseThrow(), get(client.lookup(operation(2))));
      assertEquals(root0, get(client.checkpoint(0, root0.seal(), 0)));
      assertEquals(root0, get(client.complete()));
      get(client.detach());
    }
  }

  @Test
  void wrongOwnerCannotAttachAndChangedPolicyIsConflict() throws Exception {
    Path root = directory.resolve("owners");
    try (DurableHost host = host(root, true);
        DurableServer server = server(host)) {
      Path aliceJournal = directory.resolve("owners-alice.sqlite");
      try (ClientJournal journal =
              ClientJournal.initialize(
                  aliceJournal,
                  new ClientJournal.Intent("issuer-a", "alice", 1, POLICY, true),
                  ClientJournal.Limits.defaults());
          DurableClient client =
              DurableClient.connect(
                  server.address(), pki.client("alice"), journal, ClientOptions.defaults())) {
        get(client.ready());
        assertEquals(1, get(client.binding()).generation());
        get(client.detach());
      }
      // Bob presents alice's journal: the authority sees owner bob, refuses the attachment, and the
      // client refuses to bind a contradictory identity.
      try (ClientJournal journal =
              ClientJournal.open(aliceJournal, ClientJournal.Limits.defaults());
          DurableClient client =
              DurableClient.connect(
                  server.address(), pki.client("bob"), journal, ClientOptions.defaults())) {
        get(client.ready());
        ProtocolError denied = refusal(client.binding());
        assertTrue(
            denied.code() == ProtocolError.Code.UNAUTHORIZED
                || denied.code() == ProtocolError.Code.INTEGRITY_ERROR,
            denied.toString());
      }
      // Alice replays creation sequence 1 with a different policy: CONFLICT, no new generation.
      try (ClientJournal journal =
              ClientJournal.initialize(
                  directory.resolve("owners-alice-2.sqlite"),
                  new ClientJournal.Intent(
                      "issuer-a", "alice", 1, new Records.Policy(1000, 2000, 3000), true),
                  ClientJournal.Limits.defaults());
          DurableClient client =
              DurableClient.connect(
                  server.address(), pki.client("alice"), journal, ClientOptions.defaults())) {
        get(client.ready());
        assertEquals(ProtocolError.Code.CONFLICT, refusal(client.binding()).code());
        assertEquals(2L, get(client.nextSequence()));
      }
      // A journal that selected durable work only cannot acquire results on attachment.
      try (ClientJournal journal =
              ClientJournal.initialize(
                  directory.resolve("owners-alice-3.sqlite"),
                  new ClientJournal.Intent("issuer-a", "alice", 2, POLICY, false),
                  ClientJournal.Limits.defaults());
          DurableClient client =
              DurableClient.connect(
                  server.address(), pki.client("alice"), journal, ClientOptions.defaults())) {
        assertEquals(List.of(Messages.DURABLE_WORK), get(client.ready()).supported());
        assertEquals(2, get(client.binding()).generation());
        assertEquals(
            ProtocolError.Code.EXTENSION_UNSUPPORTED, refusal(client.manifest(WORK, 1)).code());
        get(client.detach());
      }
    }
  }
}
