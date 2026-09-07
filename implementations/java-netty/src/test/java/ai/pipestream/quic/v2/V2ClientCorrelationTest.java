package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.Records.*;
import static ai.pipestream.quic.v2.V2ObjectStreamTest.code;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

final class V2ClientCorrelationTest {
  static Capabilities capabilities(boolean response, int pending, int streams) {
    return new Capabilities(
        response,
        List.of(DURABLE_WORK, RESULT_DELIVERY),
        List.of(DURABLE_WORK, RESULT_DELIVERY),
        4096,
        streams,
        pending,
        1024,
        1000,
        5000);
  }

  static ClientCorrelation client(int pending, int streams) {
    ClientCorrelation state = new ClientCorrelation(capabilities(false, pending, streams));
    assertNull(state.receive(new Wire.Known(capabilities(true, pending, streams))));
    return state;
  }

  static ClientCorrelation.Completion receive(ClientCorrelation state, Message message) {
    return state.receive(new Wire.Known(message));
  }

  static Refusal refusal(long request) {
    return new Refusal(new RequestTag(false, request), ProtocolError.Code.NOT_READY, "not ready");
  }

  static ClientCorrelation.ResultCommitment object() {
    return ClientCorrelation.ResultCommitment.from(V2CommitmentsTest.manifest(), 0);
  }

  static Read read(long request) {
    return new Read(request, V2CommitmentsTest.WORK, 1, 0, V2CommitmentsTest.OUTPUT);
  }

  static ResultHeader header(long request) {
    return new ResultHeader(request, 1, V2CommitmentsTest.WORK, 1, 0, 3, V2CommitmentsTest.OUTPUT);
  }

  @Test
  void negotiationIsFirstAndUniqueAndProfileUseIsExplicit() {
    for (Wire.Frame first :
        List.of(
            new Wire.Ignored(128, 3),
            new Wire.Known(new Detached(1)),
            new Wire.Known(new Detach(1)))) {
      ClientCorrelation state = new ClientCorrelation(capabilities(false, 8, 4));
      code(ProtocolError.Code.FRAME_ERROR, () -> state.receive(first));
      assertThrows(
          ProtocolError.class, () -> state.receive(new Wire.Known(capabilities(true, 8, 4))));
    }
    ClientCorrelation repeated = client(8, 4);
    code(
        ProtocolError.Code.FRAME_ERROR,
        () -> repeated.receive(new Wire.Known(capabilities(true, 8, 4))));
    Capabilities coreOffer =
        new Capabilities(false, List.of(), List.of(), 4096, 1, 1, 0, 1000, 1000);
    ClientCorrelation core = new ClientCorrelation(coreOffer);
    core.receive(new Wire.Known(Capabilities.negotiate(coreOffer, coreOffer)));
    code(ProtocolError.Code.EXTENSION_UNSUPPORTED, () -> core.register(new NextSequence(1), null));
    core.register(new Detach(1), null);
    assertNotNull(receive(core, new Detached(1)));
  }

  @Test
  void requestsAreIncreasingSharedAndBoundedWhileRepliesMayReorder() {
    ClientCorrelation state = client(3, 2);
    NextSequence first = new NextSequence(1);
    GetManifest second = new GetManifest(5, V2CommitmentsTest.WORK, 1);
    Watch third = new Watch(8, V2CommitmentsTest.WORK, 0, 0);
    assertArrayEquals(Wire.encode(first, 4096), state.register(first, null));
    state.register(second, null);
    state.register(third, null);
    code(ProtocolError.Code.LIMIT_EXCEEDED, () -> state.register(new Detach(9), null));
    assertEquals(third, receive(state, refusal(8)).request().control());
    assertEquals(first, receive(state, new Sequence(1, 1)).request().control());
    assertEquals(
        second,
        receive(state, new ManifestResponse(5, V2CommitmentsTest.manifest())).request().control());
    state.register(new Detach(9), null);
    assertEquals(1, state.pendingCount());
    code(ProtocolError.Code.FRAME_ERROR, () -> state.register(new NextSequence(8), null));
    receive(state, new Detached(9));
    assertEquals(0, state.pendingCount());
    code(ProtocolError.Code.FRAME_ERROR, () -> state.register(new NextSequence(9), null));
    ClientCorrelation newConnection = client(1, 1);
    code(ProtocolError.Code.FRAME_ERROR, () -> newConnection.register(new NextSequence(2), null));
    newConnection.register(new NextSequence(1), null);
    receive(newConnection, new Sequence(1, 1));
    newConnection.register(new NextSequence(Long.MAX_VALUE), null);
    receive(newConnection, new Sequence(Long.MAX_VALUE, 1));
    code(
        ProtocolError.Code.FRAME_ERROR,
        () -> newConnection.register(new NextSequence(Long.MAX_VALUE), null));
  }

  @Test
  void wrongKindUnknownDuplicateAndWrongDirectionPoisonOnlyConnectionCorrelation() {
    for (Message invalid :
        List.of(
            new Detached(1),
            new Sequence(2, 1),
            new NextSequence(1),
            new Refusal(
                new RequestTag(true, 2), ProtocolError.Code.NOT_READY, "input not requested"))) {
      ClientCorrelation state = client(2, 2);
      state.register(new NextSequence(1), null);
      code(ProtocolError.Code.FRAME_ERROR, () -> receive(state, invalid));
      assertThrows(ProtocolError.class, () -> receive(state, new Sequence(1, 1)));
      List<ClientCorrelation.Pending> uncertain = state.close();
      assertEquals(1, uncertain.size());
      assertEquals(new NextSequence(1), uncertain.getFirst().control());
      assertTrue(state.close().isEmpty());
    }
    ClientCorrelation duplicate = client(2, 2);
    duplicate.register(new NextSequence(1), null);
    receive(duplicate, new Sequence(1, 1));
    code(ProtocolError.Code.FRAME_ERROR, () -> receive(duplicate, new Sequence(1, 1)));
  }

  @Test
  void everyControlFamilyAcceptsOnlyItsOwnResponseKindOrRefusal() {
    Policy policy = new Policy(1000, 1000, 1000);
    Binding binding =
        new Binding(
            1, "authority-1", "owner-1", 1, 1, policy, new Limits(10, 10, 10, 1024, 1024, 1));
    WorkView declared =
        new WorkView(
            V2CommitmentsTest.WORK,
            State.DECLARED,
            0,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    ScopeSummary root =
        new ScopeSummary(
            0,
            0,
            null,
            V2CommitmentsTest.INPUT,
            0,
            new Counts(0, 0, 0, 0),
            Commitments.emptyStatus(),
            0);
    OperationReceipt declaration =
        new OperationReceipt(
            V2CommitmentsTest.operation(2),
            V2CommitmentsTest.INPUT,
            new Declared(0, 0, 1, 1, V2CommitmentsTest.INPUT));
    OperationReceipt cancelledScope =
        new OperationReceipt(
            V2CommitmentsTest.operation(3), V2CommitmentsTest.INPUT, new ScopeCancelled(0, 0));
    OperationReceipt retry =
        new OperationReceipt(
            V2CommitmentsTest.operation(4),
            V2CommitmentsTest.INPUT,
            new Retried(V2CommitmentsTest.WORK, 1, 2, 0));
    OperationReceipt cancel =
        new OperationReceipt(
            V2CommitmentsTest.operation(5),
            V2CommitmentsTest.INPUT,
            new Cancelled(V2CommitmentsTest.WORK, 0, 0, State.CANCELLING));
    OperationReceipt skip =
        new OperationReceipt(
            V2CommitmentsTest.operation(6),
            V2CommitmentsTest.INPUT,
            new Skipped(V2CommitmentsTest.WORK, 0, 0, State.SKIPPED));
    Message[][] pairs = {
      {new Create(1, 1, policy), binding},
      {new Attach(1, "authority-1", "owner-1", 1), binding},
      {new NextSequence(1), new Sequence(1, 1)},
      {
        new Declare(1, declaration.operation(), 0, List.of(1L), true),
        new DeclarationResponse(1, declaration)
      },
      {
        new Page(1, 0, 0, 1),
        new PageResponse(
            1, 0, 0, null, false, null, 1, List.of(new Entry(1, State.DECLARED)), false)
      },
      {new Checkpoint(1, 0, root.seal(), 0), new CheckpointResponse(1, root)},
      {
        new CancelScope(1, cancelledScope.operation(), 0),
        new CancelScopeResponse(1, cancelledScope)
      },
      {new LookupOperation(1, declaration.operation()), new OperationResponse(1, declaration)},
      {new Watch(1, V2CommitmentsTest.WORK, 0, 0), new WatchResponse(1, 1, declared)},
      {new Retry(1, retry.operation(), V2CommitmentsTest.WORK, 1), new RetryResponse(1, retry)},
      {new Cancel(1, cancel.operation(), V2CommitmentsTest.WORK), new CancelResponse(1, cancel)},
      {new Skip(1, skip.operation(), V2CommitmentsTest.WORK), new SkipResponse(1, skip)},
      {
        new GetManifest(1, V2CommitmentsTest.WORK, 1),
        new ManifestResponse(1, V2CommitmentsTest.manifest())
      },
      {new Complete(1, 1, root), new Completed(1, 1, root)},
      {new Detach(1), new Detached(1)}
    };
    for (Message[] pair : pairs) {
      ClientCorrelation state = client(1, 1);
      state.register(pair[0], null);
      assertEquals(pair[0], receive(state, pair[1]).request().control());
      assertEquals(0, state.pendingCount());
      state = client(1, 1);
      state.register(pair[0], null);
      assertEquals(pair[0], receive(state, refusal(1)).request().control());
      for (Message[] wrong : pairs)
        if (!wrong[1].getClass().equals(pair[1].getClass())) {
          ClientCorrelation mismatch = client(1, 1);
          mismatch.register(pair[0], null);
          code(ProtocolError.Code.FRAME_ERROR, () -> receive(mismatch, wrong[1]));
        }
    }
  }

  @Test
  void inputStreamsUseActualIdsMayArriveOutOfOrderAndRemainPendingUntilReceipt() {
    ClientCorrelation state = client(4, 2);
    InputHeader header = V2CommitmentsTest.admission();
    state.registerInput(10, header);
    state.registerInput(2, header);
    code(ProtocolError.Code.LIMIT_EXCEEDED, () -> state.registerInput(6, header));
    code(ProtocolError.Code.FRAME_ERROR, () -> state.registerInput(10, header));
    code(ProtocolError.Code.FRAME_ERROR, () -> state.registerInput(3, header));
    OperationReceipt receipt =
        new OperationReceipt(
            header.operation(),
            Commitments.operation(V2CommitmentsTest.CONTEXT, 0, header),
            new Admitted(V2CommitmentsTest.WORK, 1, 0, 60000, null));
    assertEquals(
        header,
        receive(state, new AdmissionResponse(new RequestTag(true, 2), receipt)).request().input());
    state.registerInput(6, header);
    assertEquals(
        10,
        receive(
                state,
                new Refusal(
                    new RequestTag(true, 10), ProtocolError.Code.NOT_READY, "missing reservation"))
            .request()
            .tag()
            .id());
    assertEquals(1, state.pendingCount());
    assertEquals(6, state.close().getFirst().tag().id());
  }

  @Test
  void validResultRemainsPendingUntilActualVerifiedFinAndCanBeReadAgain() {
    ClientCorrelation state = client(2, 1);
    state.register(read(1), object());
    state.beginResult(header(1), 0);
    state.resultBytes(1, ByteBuffer.wrap(new byte[] {65}), 0);
    state.resultBytes(1, ByteBuffer.wrap(new byte[] {66, 67}), 1);
    assertEquals(1, state.pendingCount());
    assertEquals(read(1), state.finishResult(1, 2).control());
    assertEquals(0, state.pendingCount());
    state.register(read(2), object());
    state.beginResult(header(2), 3);
    state.abortResult(2);
    state.register(read(3), object());
    state.beginResult(header(3), 4);
    state.resultBytes(3, ByteBuffer.wrap(new byte[] {65, 66, 67}), 5);
    state.finishResult(3, 6);
    assertEquals(0, state.pendingCount());
  }

  @Test
  void eachResultIdentityMismatchIsDeliveryLocalAndPreservesOtherRequests() {
    ResultHeader valid = header(1);
    for (ResultHeader bad :
        List.of(
            new ResultHeader(1, 2, valid.work(), 1, 0, 3, valid.sha256()),
            new ResultHeader(1, 1, new WorkKey(0, 0, 2), 1, 0, 3, valid.sha256()),
            new ResultHeader(1, 1, valid.work(), 2, 0, 3, valid.sha256()),
            new ResultHeader(1, 1, valid.work(), 1, 1, 3, valid.sha256()),
            new ResultHeader(1, 1, valid.work(), 1, 0, 4, valid.sha256()),
            new ResultHeader(1, 1, valid.work(), 1, 0, 3, V2CommitmentsTest.INPUT))) {
      ClientCorrelation state = client(2, 1);
      state.register(read(1), object());
      state.register(new NextSequence(2), null);
      code(ProtocolError.Code.INTEGRITY_ERROR, () -> state.beginResult(bad, 0));
      assertEquals(2, state.pendingCount());
      assertEquals(new NextSequence(2), receive(state, new Sequence(2, 1)).request().control());
      state.abortResult(1);
      assertEquals(0, state.pendingCount());
    }
  }

  @Test
  void unknownNonResultDuplicateHeadersAndSecondControlResponsesAreFatal() {
    ClientCorrelation unknown = client(2, 1);
    code(ProtocolError.Code.FRAME_ERROR, () -> unknown.beginResult(header(1), 0));
    ClientCorrelation wrong = client(2, 1);
    wrong.register(new NextSequence(1), null);
    code(ProtocolError.Code.FRAME_ERROR, () -> wrong.beginResult(header(1), 0));
    assertEquals(1, wrong.close().size());
    for (boolean control : new boolean[] {false, true}) {
      ClientCorrelation state = client(2, 1);
      state.register(read(1), object());
      state.beginResult(header(1), 0);
      code(
          ProtocolError.Code.FRAME_ERROR,
          () -> {
            if (control) receive(state, refusal(1));
            else state.beginResult(header(1), 1);
          });
      assertThrows(ProtocolError.class, () -> state.register(new NextSequence(2), null));
      assertEquals(1, state.close().size());
    }
  }

  @Test
  void activeResultsHaveTheirOwnCeilingAndRejectedStreamCannotReleaseAnotherPermit() {
    ClientCorrelation state = client(3, 1);
    state.register(read(1), object());
    state.register(read(2), object());
    state.beginResult(header(1), 0);
    code(ProtocolError.Code.LIMIT_EXCEEDED, () -> state.beginResult(header(2), 0));
    state.abortResult(2);
    state.register(new NextSequence(3), null);
    receive(state, new Sequence(3, 1));
    state.register(read(4), object());
    code(ProtocolError.Code.LIMIT_EXCEEDED, () -> state.beginResult(header(4), 0));
    state.abortResult(4);
    state.resultBytes(1, ByteBuffer.wrap(new byte[] {65, 66, 67}), 0);
    state.finishResult(1, 0);
    state.register(read(5), object());
    state.beginResult(header(5), 0);
    state.abortResult(5);
    assertEquals(0, state.pendingCount());
  }

  @Test
  void exhaustedTransferSlotsDoNotMaskContradictoryObjectIdentity() {
    ClientCorrelation state = client(2, 1);
    state.register(read(1), object());
    state.beginResult(header(1), 0);
    state.register(read(2), object());
    ResultHeader bad =
        new ResultHeader(2, 2, V2CommitmentsTest.WORK, 1, 0, 3, V2CommitmentsTest.OUTPUT);
    code(ProtocolError.Code.INTEGRITY_ERROR, () -> state.beginResult(bad, 0));
    state.abortResult(2);
    state.register(read(3), object());
    code(ProtocolError.Code.LIMIT_EXCEEDED, () -> state.beginResult(header(3), 0));
    state.abortResult(3);
    state.abortResult(1);
    assertEquals(0, state.pendingCount());
  }

  @Test
  void corruptedFinResetAndDeadlineNeverCompleteWorkOrUnrelatedRequests() {
    ClientCorrelation state = client(2, 1);
    state.register(read(1), object());
    state.register(new NextSequence(2), null);
    state.beginResult(header(1), 0);
    state.resultBytes(1, ByteBuffer.wrap(new byte[] {65, 66, 68}), 1);
    code(ProtocolError.Code.INTEGRITY_ERROR, () -> state.finishResult(1, 2));
    assertEquals(1, state.pendingCount());
    receive(state, new Sequence(2, 1));
    state.register(read(3), object());
    state.beginResult(header(3), 0);
    code(
        ProtocolError.Code.LIMIT_EXCEEDED,
        () -> state.checkResultDeadline(3, 1000 * V2ObjectStreamTest.MS));
    assertEquals(1, state.pendingCount());
    state.abortResult(3);
    assertEquals(0, state.pendingCount());
    state.register(read(4), object());
    assertEquals(read(4), state.close().getFirst().control());
  }
}
