package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

final class RetirementRecordTest {
  private static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);

  @Test
  void boundedEncodingIsDeterministicAndRoundTripsExactEvidence() throws Exception {
    RetirementRecord record = record(context("issuer-a", "alice", 1), 7, root(100), 200, 300);
    byte[] first = record.encode();
    byte[] second = record.encode();
    assertArrayEquals(first, second);
    assertTrue(first.length <= RetirementRecord.CAPACITY);
    assertEquals(record, RetirementRecord.decode(first));
  }

  @Test
  void maximumLabelsAndIntegersRemainWithinTheFixedCapacity() throws Exception {
    String maximum = "a".repeat(128);
    RetirementRecord record =
        record(
            context(maximum, maximum, Long.MAX_VALUE),
            Long.MAX_VALUE,
            maximumRoot(),
            Long.MAX_VALUE,
            Long.MAX_VALUE);
    byte[] encoded = record.encode();
    assertTrue(encoded.length <= RetirementRecord.CAPACITY);
    assertEquals(record, RetirementRecord.decode(encoded));
  }

  @Test
  void constructorRequiresRootIdentityAndOrderedInclusiveTimes() {
    Records.ScopeSummary child =
        new Records.ScopeSummary(
            1, 0, WORK, digest(1), 0, new Records.Counts(0, 0, 0, 0), digest(2), 100);
    assertThrows(
        ProtocolError.class,
        () -> new RetirementRecord(context("issuer", "owner", 1), 1, child, 100, 100));
    assertThrows(
        ProtocolError.class, () -> record(context("issuer", "owner", 1), 1, root(101), 100, 100));
    assertThrows(
        ProtocolError.class, () -> record(context("issuer", "owner", 1), 1, root(100), 101, 100));
    assertThrows(
        ProtocolError.class, () -> record(context("issuer", "owner", 1), 0, root(100), 100, 100));
    assertDoesNotThrow(() -> record(context("issuer", "owner", 1), 1, root(100), 100, 100));
  }

  @Test
  void decoderMapsMalformedTrailingAndOversizedImagesToSqlException() {
    byte[] valid = record(context("issuer", "owner", 1), 1, root(100), 100, 100).encode();
    assertThrows(SQLException.class, () -> RetirementRecord.decode(new byte[] {(byte) 0xff}));
    assertThrows(
        SQLException.class, () -> RetirementRecord.decode(Arrays.copyOf(valid, valid.length + 1)));
    assertThrows(
        SQLException.class, () -> RetirementRecord.decode(new byte[RetirementRecord.CAPACITY + 1]));
  }

  @Test
  void workVerificationAcceptsOnlyTerminalViewsWhosePromisesEndAtTheCutoff() throws Exception {
    RetirementRecord record = record(context("issuer", "owner", 1), 1, root(100), 500, 500);
    record.verifyWork(cancelled(100, 500));

    assertThrows(SQLException.class, () -> record.verifyWork(active()));
    assertThrows(SQLException.class, () -> record.verifyWork(cancelled(100, 501)));
    assertThrows(SQLException.class, () -> record.verifyWork(cancelled(501, 502)));
    assertThrows(SQLException.class, () -> record.verifyWork(successWithOutputUntil(501)));
  }

  private static RetirementRecord record(
      Commitments.Context context,
      long creationSequence,
      Records.ScopeSummary root,
      long cutoff,
      long at) {
    return new RetirementRecord(context, creationSequence, root, cutoff, at);
  }

  private static Records.ScopeSummary root(long closedAt) {
    return new Records.ScopeSummary(
        0, 0, null, digest(1), 0, new Records.Counts(0, 0, 0, 0), digest(2), closedAt);
  }

  private static Records.ScopeSummary maximumRoot() {
    return new Records.ScopeSummary(
        0,
        0,
        null,
        digest(1),
        Long.MAX_VALUE,
        new Records.Counts(Long.MAX_VALUE, 0, 0, 0),
        digest(2),
        Long.MAX_VALUE);
  }

  private static Commitments.Context context(String authority, String owner, long generation) {
    return new Commitments.Context(authority, owner, generation);
  }

  private static Records.WorkView cancelled(long terminalAt, long receiptUntil) {
    return new Records.WorkView(
        WORK,
        Records.State.CANCELLED,
        0,
        null,
        null,
        null,
        terminalAt,
        receiptUntil,
        null,
        null,
        null,
        null);
  }

  private static Records.WorkView active() {
    Records.Input input = new Records.Input(0, digest(3), "application/octet-stream");
    return new Records.WorkView(
        WORK, Records.State.ACTIVE, 1, input, 50L, 1000L, null, null, null, null, null, null);
  }

  private static Records.WorkView successWithOutputUntil(long outputUntil) {
    Records.Input input = new Records.Input(0, digest(3), "application/octet-stream");
    Records.Manifest manifest =
        new Records.Manifest(
            "issuer", "owner", 1, WORK, 1, input.sha256(), 100, outputUntil, List.of());
    return new Records.WorkView(
        WORK,
        Records.State.SUCCEEDED,
        1,
        input,
        50L,
        1000L,
        100L,
        400L,
        outputUntil,
        null,
        manifest,
        null);
  }

  private static Records.Digest digest(int last) {
    byte[] bytes = new byte[32];
    bytes[31] = (byte) last;
    return new Records.Digest(bytes);
  }
}
