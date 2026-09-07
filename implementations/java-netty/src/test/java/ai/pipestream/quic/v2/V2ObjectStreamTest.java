package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Records.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

final class V2ObjectStreamTest {
  static final long MS = 1_000_000;

  static Messages.Capabilities selected(long size, long idle, long lifetime) {
    return new Messages.Capabilities(
        true,
        List.of(Messages.DURABLE_WORK, Messages.RESULT_DELIVERY),
        List.of(Messages.DURABLE_WORK, Messages.RESULT_DELIVERY),
        4096,
        4,
        8,
        size,
        idle,
        lifetime);
  }

  static void code(ProtocolError.Code expected, org.junit.jupiter.api.function.Executable action) {
    assertEquals(expected, assertThrows(ProtocolError.class, action).code());
  }

  @Test
  void everyHeaderSplitLeavesCoalescedPayloadUnconsumed() {
    for (Value header :
        List.of(
            V2CommitmentsTest.admission(),
            new ResultHeader(1, 1, V2CommitmentsTest.WORK, 1, 0, 3, V2CommitmentsTest.INPUT))) {
      boolean input = header instanceof InputHeader;
      byte[] encoded = Wire.encodeHeader(header);
      for (int cut = 0; cut < encoded.length; cut++) {
        ObjectStream.HeaderReader reader = new ObjectStream.HeaderReader(input, 0, 1000);
        assertNull(reader.feed(ByteBuffer.wrap(encoded, 0, cut), 1));
        ByteBuffer rest =
            ByteBuffer.allocate(encoded.length - cut + 3)
                .put(encoded, cut, encoded.length - cut)
                .put(new byte[] {97, 98, 99})
                .flip();
        assertEquals(header, reader.feed(rest, 999 * MS));
        assertEquals(3, rest.remaining());
        assertEquals(97, rest.get());
        assertEquals(0, reader.bufferedCapacity());
        reader.finish(1000 * MS);
        assertThrows(ProtocolError.class, () -> reader.feed(rest, 1001 * MS));
      }
    }
  }

  @Test
  void headerBoundsAndDeadlinePrecedeAllocationAndCannotBeRenewedByProgress() {
    for (long length : new long[] {0, 4097, 4294967295L}) {
      ObjectStream.HeaderReader reader = new ObjectStream.HeaderReader(true, 0, 1000);
      code(
          ProtocolError.Code.FRAME_ERROR,
          () -> reader.feed(ByteBuffer.allocate(4).putInt((int) length).flip(), 0));
      assertEquals(0, reader.bufferedCapacity());
      assertThrows(ProtocolError.class, () -> reader.finish(1));
    }
    byte[] header = Wire.encodeHeader(V2CommitmentsTest.admission());
    for (int count = 0; count < header.length; count++) {
      ObjectStream.HeaderReader reader = new ObjectStream.HeaderReader(true, 0, 1000);
      reader.feed(ByteBuffer.wrap(header, 0, count), 0);
      code(ProtocolError.Code.FRAME_ERROR, () -> reader.finish(999 * MS));
      assertEquals(0, reader.bufferedCapacity());
    }
    ObjectStream.HeaderReader reader = new ObjectStream.HeaderReader(true, 0, 1000);
    reader.feed(ByteBuffer.wrap(header, 0, 4), 999 * MS);
    code(
        ProtocolError.Code.LIMIT_EXCEEDED,
        () -> reader.feed(ByteBuffer.wrap(header, 4, header.length - 4), 1000 * MS));
    assertEquals(0, reader.bufferedCapacity());
    ObjectStream.HeaderReader silent = new ObjectStream.HeaderReader(false, 0, 1000);
    code(ProtocolError.Code.LIMIT_EXCEEDED, () -> silent.checkDeadline(1000 * MS));
  }

  @Test
  void payloadCountsAreNotVerifiedUntilDigestAndActualFinMatch() {
    ObjectStream.Payload payload =
        new ObjectStream.Payload(3, V2CommitmentsTest.INPUT, selected(3, 1000, 5000), 0);
    ByteBuffer direct = ByteBuffer.allocateDirect(3).put(new byte[] {97, 98, 99}).flip();
    payload.feed(direct, 999 * MS);
    assertFalse(direct.hasRemaining());
    assertEquals(3, payload.consumed());
    assertFalse(payload.verified());
    assertEquals(V2CommitmentsTest.INPUT, payload.finish(1001 * MS));
    assertTrue(payload.verified());
    assertThrows(ProtocolError.class, () -> payload.feed(ByteBuffer.allocate(0), 1002 * MS));
    for (byte[] data :
        List.of(new byte[] {97, 98}, new byte[] {97, 98, 100}, new byte[] {97, 98, 99, 100})) {
      ObjectStream.Payload invalid =
          new ObjectStream.Payload(3, V2CommitmentsTest.INPUT, selected(3, 1000, 5000), 0);
      code(
          ProtocolError.Code.INTEGRITY_ERROR,
          () -> {
            invalid.feed(ByteBuffer.wrap(data), 0);
            invalid.finish(0);
          });
      assertFalse(invalid.verified());
      assertThrows(ProtocolError.class, () -> invalid.finish(0));
    }
    code(
        ProtocolError.Code.LIMIT_EXCEEDED,
        () -> new ObjectStream.Payload(4, V2CommitmentsTest.INPUT, selected(3, 1000, 5000), 0));
  }

  @Test
  void emptyPayloadNeedsItsOwnDigestAndFinAndResetNeverProvesSuccess() {
    Digest empty = new Digest(Commitments.sha256().digest());
    ObjectStream.Payload payload = new ObjectStream.Payload(0, empty, selected(0, 1000, 1000), 0);
    payload.feed(ByteBuffer.allocate(0), 999 * MS);
    assertFalse(payload.verified());
    assertEquals(empty, payload.finish(999 * MS));
    ObjectStream.Payload wrong =
        new ObjectStream.Payload(0, V2CommitmentsTest.INPUT, selected(0, 1000, 1000), 0);
    code(ProtocolError.Code.INTEGRITY_ERROR, () -> wrong.finish(0));
    ObjectStream.Payload reset =
        new ObjectStream.Payload(3, V2CommitmentsTest.INPUT, selected(3, 1000, 1000), 0);
    reset.feed(ByteBuffer.wrap(new byte[] {97, 98, 99}), 0);
    reset.abort();
    assertFalse(reset.verified());
    assertThrows(ProtocolError.class, () -> reset.finish(0));
  }

  @Test
  void progressAndFinAtEitherDeadlineCannotExtendAStream() {
    for (boolean finish : new boolean[] {false, true}) {
      ObjectStream.Payload idle =
          new ObjectStream.Payload(3, V2CommitmentsTest.INPUT, selected(3, 1000, 5000), 0);
      idle.feed(ByteBuffer.wrap(new byte[] {97, 98, 99}), 0);
      code(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () -> {
            if (finish) idle.finish(1000 * MS);
            else idle.feed(ByteBuffer.allocate(0), 1000 * MS);
          });
      ObjectStream.Payload life =
          new ObjectStream.Payload(3, V2CommitmentsTest.INPUT, selected(3, 1000, 2000), 0);
      life.feed(ByteBuffer.wrap(new byte[] {97}), 900 * MS);
      life.feed(ByteBuffer.wrap(new byte[] {98}), 1800 * MS);
      code(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () -> {
            if (finish) life.finish(2000 * MS);
            else life.feed(ByteBuffer.wrap(new byte[] {99}), 2000 * MS);
          });
    }
    ObjectStream.Payload silent =
        new ObjectStream.Payload(
            Long.MAX_VALUE, V2CommitmentsTest.INPUT, selected(Long.MAX_VALUE, 1000, 5000), 0);
    code(ProtocolError.Code.LIMIT_EXCEEDED, () -> silent.checkDeadline(1000 * MS));
    assertFalse(silent.verified());
  }

  @Test
  void monotonicWrapIsValidButBackwardTimeDoesNotCreateMoreLifetime() {
    long start = Long.MAX_VALUE - 2;
    ObjectStream.Payload payload =
        new ObjectStream.Payload(3, V2CommitmentsTest.INPUT, selected(3, 1000, 1000), start);
    payload.feed(ByteBuffer.wrap(new byte[] {97, 98, 99}), start + 4);
    assertEquals(V2CommitmentsTest.INPUT, payload.finish(start + 8));
    ObjectStream.Payload reversed =
        new ObjectStream.Payload(3, V2CommitmentsTest.INPUT, selected(3, 1000, 1000), 10);
    code(ProtocolError.Code.LIMIT_EXCEEDED, () -> reversed.feed(ByteBuffer.allocate(0), 9));
  }
}
