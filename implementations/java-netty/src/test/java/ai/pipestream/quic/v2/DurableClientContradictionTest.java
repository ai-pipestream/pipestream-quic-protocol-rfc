package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Section 12.8: the client verifies identity, seal, count partition and known commitments before
 * acknowledging coverage. A raw authority answers Page and Watch with combinations that contradict
 * each other; the client must refuse INTEGRITY_ERROR, journal nothing from the contradicting
 * answer, and keep the connection.
 */
class DurableClientContradictionTest {
  @org.junit.jupiter.api.io.TempDir static java.nio.file.Path directory;
  static DurableTestPki pki;
  static java.util.Map<Records.Digest, String> principals;

  @org.junit.jupiter.api.BeforeAll
  static void setup() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
  }

  /** One client bound to a fresh raw authority whose control script the test replaces. */
  static final class Session implements AutoCloseable {
    final RawDurableAuthority authority;
    final ClientJournal journal;
    final DurableClient client;

    Session(String name) throws Exception {
      authority =
          new RawDurableAuthority(
              pki.server(principals),
              DurableClientControlDeadlineTest.manifestFor(new byte[1000]),
              DurableClientControlDeadlineTest.POLICY);
      journal =
          ClientJournal.initialize(
              directory.resolve(name + ".sqlite"),
              new ClientJournal.Intent(
                  "issuer-a", "alice", 1, DurableClientControlDeadlineTest.POLICY, true),
              ClientJournal.Limits.defaults());
      client =
          DurableClient.connect(
              authority.address(), pki.client("alice"), journal, ClientOptions.defaults());
      DurableClientTest.get(client.ready());
      DurableClientTest.get(client.binding());
    }

    @Override
    public void close() throws java.io.IOException, java.sql.SQLException {
      client.close();
      journal.close();
      authority.close();
    }
  }
  static final Records.WorkKey PARENT = new Records.WorkKey(0, 0, 1);
  static final Commitments.Context CONTEXT = new Commitments.Context("issuer-a", "alice", 1);
  /** The admitted input the raw authority's manifest names, so views and manifest agree. */
  static final Records.Digest INPUT = inputDigest();

  static Records.Digest inputDigest() {
    try {
      return DurableServerTest.digest(new byte[] {1});
    } catch (Exception failure) {
      throw new IllegalStateException(failure);
    }
  }

  static Records.Digest seal(long scope, int producer, Records.WorkKey parent, long... members) {
    Commitments.Seal seal = new Commitments.Seal(CONTEXT, scope, producer, parent, members.length);
    for (long member : members) seal.add(member);
    return seal.finish();
  }

  static Messages.PageResponse page(
      long request, long scope, int producer, Records.WorkKey parent, long... members) {
    List<Messages.Entry> entries =
        java.util.Arrays.stream(members)
            .mapToObj(member -> new Messages.Entry(member, Records.State.DECLARED))
            .toList();
    return new Messages.PageResponse(
        request, scope, producer, parent, true, seal(scope, producer, parent, members),
        members.length, entries, false);
  }

  /** An admitted branch view of {@code work} owning {@code child}. */
  static Records.WorkView branch(Records.WorkKey work, Records.ChildScope child) {
    return new Records.WorkView(
        work,
        Records.State.ACTIVE,
        1,
        new Records.Input(1000, INPUT, "application/octet-stream"),
        1_000L,
        11_000L,
        null,
        null,
        null,
        child,
        null,
        null);
  }

  static Records.WorkView declared(Records.WorkKey work) {
    return new Records.WorkView(
        work, Records.State.DECLARED, 0, null, null, null, null, null, null, null, null, null);
  }

  static RawDurableAuthority.ControlScript script(
      java.util.function.Function<Messages.Page, Messages.PageResponse> pages,
      java.util.function.Function<Messages.Watch, Records.WorkView> views) {
    return request ->
        switch (request) {
          case Messages.Page p -> pages.apply(p);
          case Messages.Watch w -> {
            Records.WorkView view = views.apply(w);
            yield view == null ? null : new Messages.WatchResponse(w.request(), 1, view);
          }
          default -> null;
        };
  }

  @Test
  @Timeout(60)
  void childScopeContradictingTheParentAdmissionIsIntegrityErrorInBothOrders() throws Exception {
    // Scope 5 claims parent (0,0,1); the parent's view claims child scope 6. One of them lies.
    RawDurableAuthority.ControlScript lying =
        script(
            p -> page(p.request(), p.scope(), 0, PARENT, 1),
            w -> branch(w.work(), new Records.ChildScope(6, 0)));
    // Parent view first, then the child page: the page cross-checks the retained parent view.
    try (var session = new Session("view-then-page")) {
      session.authority.controls = lying;
      DurableClientTest.get(session.client.watch(PARENT, 0, 0));
      ProtocolError refused = DurableClientTest.refusal(session.client.page(5, 0, 256));
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR, refused.code(), refused.toString());
      assertTrue(session.journal.scope(5).isEmpty(), "contradicting page journaled");
      assertTrue(
          session.client.manifest(DurableClientControlDeadlineTest.WORK, 1).toCompletableFuture()
                  .get(10, java.util.concurrent.TimeUnit.SECONDS)
              != null,
          "connection lost after a local refusal");
    }
    // Child page first, then the parent view: the view must cross-check the retained child scope.
    try (var session = new Session("page-then-view")) {
      session.authority.controls = lying;
      assertTrue(DurableClientTest.get(session.client.page(5, 0, 256)).membershipVerified());
      ProtocolError refused = DurableClientTest.refusal(session.client.watch(PARENT, 0, 0));
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR, refused.code(), refused.toString());
      assertTrue(session.journal.observedWork(PARENT).isEmpty(), "contradicting view journaled");
    }
  }

  @Test
  @Timeout(60)
  void memberOutsideTheVerifiedSealedMembershipIsIntegrityError() throws Exception {
    Records.WorkKey stranger = new Records.WorkKey(0, 0, 2);
    Records.WorkKey wrongProducer = new Records.WorkKey(0, 1, 1);
    try (var session = new Session("membership")) {
      session.authority.controls =
          script(p -> page(p.request(), 0, 0, null, 1), w -> declared(w.work()));
      assertTrue(DurableClientTest.get(session.client.page(0, 0, 256)).membershipVerified());
      // Entity 2 is not in the verified sealed membership {1} of scope 0.
      ProtocolError absent = DurableClientTest.refusal(session.client.watch(stranger, 0, 0));
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR, absent.code(), absent.toString());
      assertTrue(session.journal.observedWork(stranger).isEmpty());
      // Producer 1 contradicts the scope's observed producer 0.
      ProtocolError producer = DurableClientTest.refusal(session.client.watch(wrongProducer, 0, 0));
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR, producer.code(), producer.toString());
      // The genuine member is still accepted.
      assertEquals(1, DurableClientTest.get(session.client.watch(PARENT, 0, 0)).revision());
    }
  }
}
