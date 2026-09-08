package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class DurableRequestsTest {
  @TempDir Path directory;

  @Test
  void requestIdsRefusalsBindingAndDirectionAreEnforcedInDecodeOrder() throws Exception {
    SessionStore sessions =
        SessionStore.initialize(directory.resolve("binding.sqlite"), ResultFixture.configuration());
    AtomicLong nanos = new AtomicLong(71);
    DurableRequests wrongFirst =
        new DurableRequests(access("alice"), ResultFixture.SELECTED, nanos::get);
    assertCode(
        ProtocolError.Code.FRAME_ERROR,
        () ->
            wrongFirst.accept(
                new Messages.Create(2, 1, new Records.Policy(10_000, 20_000, 30_000))));
    wrongFirst.close();

    DurableRequests consumed =
        new DurableRequests(access("alice"), ResultFixture.SELECTED, nanos::get);
    DurableRequests.Acceptance refused =
        consumed.accept(new Messages.Watch(1, ResultFixture.WORK, 0, 0));
    assertEquals(ProtocolError.Code.NOT_READY, refused.refusal().code());
    assertNull(refused.ticket());
    assertCode(
        ProtocolError.Code.FRAME_ERROR,
        () -> consumed.accept(new Messages.Watch(1, ResultFixture.WORK, 0, 0)));
    consumed.close();

    DurableRequests requests =
        new DurableRequests(access("alice"), ResultFixture.SELECTED, nanos::get);
    Messages.Create create = new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000));
    DurableRequests.Ticket binding = accepted(requests.accept(create));
    assertEquals(71, binding.acceptedNanos());
    assertEquals(create, binding.request().orElseThrow());
    assertTrue(binding.binding().isEmpty());
    assertTrue(requests.usage().bindingPending());
    assertCode(ProtocolError.Code.NOT_READY, requests::input);
    assertEquals(
        ProtocolError.Code.CONFLICT,
        requests
            .accept(new Messages.Create(2, 2, new Records.Policy(10_000, 20_000, 30_000)))
            .refusal()
            .code());

    Messages.Binding committed = sessions.create(access("alice"), ResultFixture.SELECTED, create);
    Messages.Binding mismatch =
        new Messages.Binding(
            2,
            committed.authority(),
            committed.owner(),
            committed.generation(),
            committed.creationSequence(),
            committed.policy(),
            committed.limits());
    assertCode(ProtocolError.Code.INTEGRITY_ERROR, () -> binding.bind(mismatch));
    binding.bind(committed);
    binding.close();
    assertFalse(requests.usage().bindingPending());
    DurableRequests.Ticket input = requests.input();
    assertEquals(committed, input.binding().orElseThrow());
    input.close();
    DurableRequests.Ticket captured = accepted(requests.accept(new Messages.Page(3, 0, 0, 1)));
    assertEquals(committed, captured.binding().orElseThrow());
    assertCode(ProtocolError.Code.CONFLICT, captured::drained);
    captured.close();

    DurableRequests.Acceptance secondBinding =
        requests.accept(
            new Messages.Attach(
                4, committed.authority(), committed.owner(), committed.generation()));
    assertEquals(ProtocolError.Code.CONFLICT, secondBinding.refusal().code());
    DurableRequests.Ticket detach = accepted(requests.accept(new Messages.Detach(5)));
    assertCode(ProtocolError.Code.FRAME_ERROR, () -> requests.accept(new Messages.Detached(6)));
    detach.close();
    requests.close();
  }

  @Test
  void inputAndResultLimitsShareGlobalPendingButHaveIndependentStreamCaps() throws Exception {
    Messages.Capabilities selected = capabilities(2, 1);
    SessionStore sessions =
        SessionStore.initialize(directory.resolve("limits.sqlite"), ResultFixture.configuration());
    DurableRequests requests = new DurableRequests(access("alice"), selected);
    bindCreated(sessions, requests, selected);

    DurableRequests.Ticket input = requests.input();
    assertTrue(input.request().isEmpty());
    assertEquals(1, requests.usage().inputs());
    assertCode(ProtocolError.Code.LIMIT_EXCEEDED, requests::input);
    DurableRequests.Ticket output =
        accepted(
            requests.accept(
                new Messages.Read(
                    2, ResultFixture.WORK, 1, 0, ResultFixture.digest(new byte[] {1}))));
    assertEquals(new DurableRequests.Usage(2, 1, 1, false, false), requests.usage());
    input.close();
    DurableRequests.Ticket manifest =
        accepted(requests.accept(new Messages.GetManifest(3, ResultFixture.WORK, 1)));
    manifest.close();
    assertEquals(
        ProtocolError.Code.LIMIT_EXCEEDED,
        requests
            .accept(
                new Messages.Read(
                    4, ResultFixture.WORK, 1, 0, ResultFixture.digest(new byte[] {1})))
            .refusal()
            .code());
    DurableRequests.Ticket secondInput = requests.input();
    assertEquals(
        ProtocolError.Code.LIMIT_EXCEEDED,
        requests.accept(new Messages.Page(5, 0, 0, 1)).refusal().code());
    secondInput.close();
    output.close();
    assertEquals(new DurableRequests.Usage(0, 0, 0, false, false), requests.usage());
    requests.close();

    AtomicBoolean denied = new AtomicBoolean();
    DurableRequests authorized =
        new DurableRequests(
            new SessionStore.Access(
                "alice",
                () -> {
                  if (denied.get())
                    throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "revoked");
                }),
            selected);
    bindAttached(sessions, authorized, selected, 1);
    denied.set(true);
    assertEquals(
        ProtocolError.Code.UNAUTHORIZED,
        authorized.accept(new Messages.Page(2, 0, 0, 1)).refusal().code());
    assertEquals(new DurableRequests.Usage(0, 0, 0, false, false), authorized.usage());
    authorized.close();
  }

  @Test
  void retainedOwnersSurviveConnectionCloseAndReleasedTicketsCannotBeRetained() throws Exception {
    SessionStore sessions =
        SessionStore.initialize(directory.resolve("owners.sqlite"), ResultFixture.configuration());
    DurableRequests requests = new DurableRequests(access("alice"), ResultFixture.SELECTED);
    bindCreated(sessions, requests, ResultFixture.SELECTED);
    DurableRequests.Ticket ticket = accepted(requests.accept(new Messages.Page(2, 0, 0, 1)));
    DurableRequests.Ticket worker = ticket.retain();
    ticket.close();
    ticket.close();
    assertCode(ProtocolError.Code.CONFLICT, ticket::retain);
    assertEquals(1, requests.usage().pending());
    requests.close();
    assertEquals(1, requests.usage().pending());
    worker.close();
    assertEquals(0, requests.usage().pending());
  }

  @Test
  void detachWaitsForExistingPhysicalOwnersAndConnectionCloseFailsItsStage() throws Exception {
    SessionStore sessions =
        SessionStore.initialize(directory.resolve("detach.sqlite"), ResultFixture.configuration());
    DurableRequests requests = new DurableRequests(access("alice"), ResultFixture.SELECTED);
    bindCreated(sessions, requests, ResultFixture.SELECTED);
    DurableRequests.Ticket ordinary = accepted(requests.accept(new Messages.Page(2, 0, 0, 1)));
    DurableRequests.Ticket worker = ordinary.retain();
    ordinary.close();
    DurableRequests.Ticket detach = accepted(requests.accept(new Messages.Detach(3)));
    assertFalse(detach.drained().toCompletableFuture().isDone());
    DurableRequests.Acceptance afterDetach =
        requests.accept(new Messages.Watch(4, ResultFixture.WORK, 0, 0));
    assertEquals(ProtocolError.Code.NOT_READY, afterDetach.refusal().code());
    worker.close();
    detach.drained().toCompletableFuture().get(5, TimeUnit.SECONDS);
    detach.close();

    DurableRequests abandoned = new DurableRequests(access("alice"), ResultFixture.SELECTED);
    bindAttached(sessions, abandoned, ResultFixture.SELECTED, 1);
    DurableRequests.Ticket held = accepted(abandoned.accept(new Messages.Page(2, 0, 0, 1)));
    DurableRequests.Ticket pendingDetach = accepted(abandoned.accept(new Messages.Detach(3)));
    abandoned.close();
    assertFailure(ProtocolError.Code.CANCELLED, pendingDetach.drained());
    assertEquals(2, abandoned.usage().pending());
    held.close();
    pendingDetach.close();
  }

  @Test
  void completedCutUsesRealRootAndRemainsExclusiveUntilEveryOwnerReleases() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "complete", new byte[] {1})) {
      fixture.sessions.declare(
          access("alice"),
          ResultFixture.SELECTED,
          fixture.binding.generation(),
          new Messages.Declare(10, ResultFixture.operation(10), 0, List.of(), true));
      Records.Digest seal = seal(fixture, List.of(1L));
      Records.ScopeSummary root = closeRoot(fixture, seal);

      DurableRequests requests = new DurableRequests(access("alice"), ResultFixture.SELECTED);
      bindAttached(
          fixture.sessions, requests, ResultFixture.SELECTED, fixture.binding.generation());
      DurableRequests.Ticket input = requests.input();
      assertEquals(
          ProtocolError.Code.NOT_READY,
          requests
              .accept(new Messages.Complete(2, fixture.binding.generation(), root))
              .refusal()
              .code());
      input.close();
      Messages.Complete complete = new Messages.Complete(3, fixture.binding.generation(), root);
      DurableRequests.Ticket ticket = accepted(requests.accept(complete));
      DurableRequests.Ticket responseOwner = ticket.retain();
      assertEquals(
          new Messages.Completed(3, fixture.binding.generation(), root),
          fixture.sessions.completed(
              access("alice"), ResultFixture.SELECTED, fixture.binding.generation(), complete));
      ticket.close();
      assertTrue(requests.usage().draining());
      assertEquals(1, requests.usage().pending());
      assertEquals(
          ProtocolError.Code.NOT_READY,
          requests.accept(new Messages.Watch(4, ResultFixture.WORK, 0, 0)).refusal().code());

      Records.ScopeSummary altered =
          new Records.ScopeSummary(
              root.scope(),
              root.producer(),
              root.parent(),
              root.seal(),
              root.declared(),
              root.counts(),
              root.statusRoot(),
              root.closedAt() + 1);
      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              fixture.sessions.completed(
                  access("alice"),
                  ResultFixture.SELECTED,
                  fixture.binding.generation(),
                  new Messages.Complete(5, fixture.binding.generation(), altered)));
      responseOwner.close();
      assertEquals(0, requests.usage().pending());
      requests.close();
    }
  }

  private static DurableRequests.Ticket accepted(DurableRequests.Acceptance acceptance) {
    assertNull(acceptance.refusal());
    DurableRequests.Ticket ticket = acceptance.ticket();
    assertNotNull(ticket);
    return ticket;
  }

  private static void bindCreated(
      SessionStore sessions, DurableRequests requests, Messages.Capabilities selected)
      throws Exception {
    Messages.Create create = new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000));
    DurableRequests.Ticket ticket = accepted(requests.accept(create));
    ticket.bind(sessions.create(access("alice"), selected, create));
    ticket.close();
  }

  private static void bindAttached(
      SessionStore sessions,
      DurableRequests requests,
      Messages.Capabilities selected,
      long generation)
      throws Exception {
    Messages.Attach attach = new Messages.Attach(1, "issuer-a", "alice", generation);
    DurableRequests.Ticket ticket = accepted(requests.accept(attach));
    ticket.bind(sessions.attach(access("alice"), selected, attach));
    ticket.close();
  }

  private static Messages.Capabilities capabilities(int pending, int streams) {
    Messages.Capabilities base = ResultFixture.SELECTED;
    return new Messages.Capabilities(
        true,
        base.supported(),
        base.required(),
        base.controlLimit(),
        streams,
        pending,
        base.objectLimit(),
        base.streamIdleMs(),
        base.streamLifetimeMs());
  }

  private static Records.Digest seal(ResultFixture fixture, List<Long> members) {
    Commitments.Seal seal = new Commitments.Seal(fixture.context(), 0, 0, null, members.size());
    for (long member : members) seal.add(member);
    return seal.finish();
  }

  private static Records.ScopeSummary closeRoot(ResultFixture fixture, Records.Digest seal)
      throws Exception {
    ClosureStore.Cursor cursor = new ClosureStore.Cursor();
    for (int count = 0; count < 8; count++) {
      fixture.sessions.reconcileClosures(cursor, 1, ResultFixture.clock(1200));
      var response =
          fixture.sessions.checkpoint(
              access("alice"),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.Checkpoint(20 + count, 0, seal, 0));
      if (response.isPresent()) return response.orElseThrow().summary();
    }
    fail("root did not close within bounded reconciliation");
    throw new AssertionError("unreachable");
  }

  private static SessionStore.Access access(String owner) {
    return new SessionStore.Access(owner, () -> {});
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError failure = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, failure.code(), failure::getMessage);
  }

  private static void assertFailure(
      ProtocolError.Code expected, java.util.concurrent.CompletionStage<?> stage) {
    ExecutionException wrapper =
        assertThrows(
            ExecutionException.class, () -> stage.toCompletableFuture().get(5, TimeUnit.SECONDS));
    ProtocolError failure = assertInstanceOf(ProtocolError.class, wrapper.getCause());
    assertEquals(expected, failure.code(), failure::getMessage);
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
