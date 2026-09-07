package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.Records.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

final class V2CommitmentsTest {
  static final Commitments.Context CONTEXT = new Commitments.Context("authority-1", "owner-1", 1);
  static final WorkKey WORK = new WorkKey(0, 0, 1);
  static final Digest INPUT =
      new Digest(
          HexFormat.of()
              .parseHex("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"));
  static final Digest OUTPUT =
      new Digest(
          HexFormat.of()
              .parseHex("b5d4045c3f466fa91fe2cc6abe79232a1a57cdf104f7a26e716e0a1e2789df78"));
  static final Input DESCRIPTOR = new Input(3, INPUT, "text/plain");

  static OperationId operation(int id) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) id;
    return new OperationId(bytes);
  }

  static InputHeader admission() {
    return new InputHeader(
        1,
        operation(1),
        new AdmitParameters(
            WORK, DESCRIPTOR, "ascii-uppercase-v1", 0, 60000, new OutputBudget(1, 3)));
  }

  static Manifest manifest() {
    return new Manifest(
        "authority-1",
        "owner-1",
        1,
        WORK,
        1,
        INPUT,
        1000200,
        1120200,
        List.of(
            new Output(
                0,
                3,
                OUTPUT,
                "text/plain",
                new Locator(
                    "pipestream://processor.example:9443/v2/sessions/1/scopes/0/producers/0/entities/1/attempts/1/outputs/0"))));
  }

  static WorkView success() {
    return new WorkView(
        WORK,
        State.SUCCEEDED,
        1,
        DESCRIPTOR,
        1000000L,
        1060000L,
        1000200L,
        1120200L,
        1120200L,
        null,
        manifest(),
        null);
  }

  static Digest seal(
      Commitments.Context context, long scope, int producer, WorkKey parent, long... ids) {
    Commitments.Seal hash = new Commitments.Seal(context, scope, producer, parent, ids.length);
    for (long id : ids) hash.add(id);
    return hash.finish();
  }

  static Digest raw(String domain, byte[]... parts) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    digest.update(domain.getBytes(StandardCharsets.US_ASCII));
    for (byte[] part : parts) digest.update(part);
    return new Digest(digest.digest());
  }

  @TestFactory
  Stream<DynamicTest> frozenCommitmentsComeFromTypedInputs() throws Exception {
    List<String> rows = Files.readAllLines(V2WireTest.VECTORS.resolve("commitments.tsv"));
    assertEquals(13, rows.size());
    return rows.subList(1, rows.size()).stream()
        .map(
            row ->
                DynamicTest.dynamicTest(
                    row.split("\t")[0],
                    () -> {
                      String[] fields = row.split("\t", -1);
                      Digest expected = new Digest(HexFormat.of().parseHex(fields[3]));
                      assertEquals(
                          expected,
                          raw(fields[1], HexFormat.of().parseHex(fields[2])),
                          "frozen preimage digest");
                      Digest actual =
                          switch (fields[0]) {
                            case "one-member-seal" -> seal(CONTEXT, 0, 0, null, 1);
                            case "empty-scope-seal" -> seal(CONTEXT, 0, 0, null);
                            case "admission-operation" ->
                                Commitments.operation(CONTEXT, 0, admission());
                            case "declaration-operation" ->
                                Commitments.operation(
                                    CONTEXT,
                                    0,
                                    new Declare(999, operation(2), 0, List.of(1L), true));
                            case "retry-operation" ->
                                Commitments.operation(
                                    CONTEXT, 0, new Retry(999, operation(3), WORK, 1));
                            case "cancel-operation" ->
                                Commitments.operation(
                                    CONTEXT, 0, new Cancel(999, operation(4), WORK));
                            case "skip-operation" ->
                                Commitments.operation(
                                    CONTEXT, 0, new Skip(999, operation(5), WORK));
                            case "scope-cancel-operation" ->
                                Commitments.operation(
                                    CONTEXT, 0, new CancelScope(999, operation(6), 0));
                            case "result-manifest" -> Commitments.manifest(manifest());
                            case "success-status-leaf" -> Commitments.statusLeaf(success(), null);
                            case "empty-status-root" -> Commitments.emptyStatus();
                            case "two-status-nodes" ->
                                Commitments.statusNode(
                                    Commitments.statusLeaf(success(), null),
                                    Commitments.statusLeaf(success(), null));
                            default ->
                                throw new AssertionError(
                                    "unhandled frozen commitment " + fields[0]);
                          };
                      assertEquals(expected, actual, "independently constructed typed commitment");
                    }));
  }

  @Test
  void operationHashesBindOriginatorAndEveryImmutableFieldButNotConnectionRequest() {
    Cancel cancel = new Cancel(1, operation(4), WORK);
    Digest expected = Commitments.operation(CONTEXT, 0, cancel);
    assertEquals(
        expected,
        Commitments.operation(CONTEXT, 0, new Cancel(Long.MAX_VALUE, operation(4), WORK)));
    for (Commitments.Context other :
        List.of(
            new Commitments.Context("authority-2", "owner-1", 1),
            new Commitments.Context("authority-1", "owner-2", 1),
            new Commitments.Context("authority-1", "owner-1", 2))) {
      assertNotEquals(expected, Commitments.operation(other, 0, cancel));
    }
    assertNotEquals(
        expected,
        Commitments.operation(CONTEXT, 1, cancel),
        "originator is not inferred from target producer");
    for (Message other :
        List.of(
            new Cancel(1, operation(5), WORK),
            new Skip(1, operation(4), WORK),
            new Cancel(1, operation(4), new WorkKey(0, 0, 2)),
            new Cancel(1, operation(4), new WorkKey(1, 0, 1)),
            new Cancel(1, operation(4), new WorkKey(1, 1, 1)),
            new CancelScope(1, operation(4), 0))) {
      assertNotEquals(expected, Commitments.operation(CONTEXT, 0, other));
    }
    assertThrows(ProtocolError.class, () -> Commitments.operation(CONTEXT, 0, new Detach(1)));
    assertThrows(ProtocolError.class, () -> Commitments.operation(CONTEXT, 2, cancel));
    assertThrows(
        ProtocolError.class,
        () ->
            Commitments.operation(
                new Commitments.Context("authority-1", "owner-1", 2), 0, admission()));
    Digest retry = Commitments.operation(CONTEXT, 0, new Retry(1, operation(3), WORK, 1));
    assertNotEquals(retry, Commitments.operation(CONTEXT, 0, new Retry(1, operation(3), WORK, 2)));
    Digest declare =
        Commitments.operation(CONTEXT, 0, new Declare(1, operation(2), 0, List.of(1L), true));
    assertNotEquals(
        declare,
        Commitments.operation(CONTEXT, 0, new Declare(1, operation(2), 0, List.of(1L), false)));
    assertNotEquals(
        declare,
        Commitments.operation(CONTEXT, 0, new Declare(1, operation(2), 0, List.of(1L, 2L), true)));
  }

  @Test
  void sealRejectsMissingExtraRepeatedReorderedAndInvalidMembersWithoutRecovery() throws Exception {
    for (long[] bad : new long[][] {{1, 1}, {2, 1}, {1, 0}, {1, -1}, {1, 2, 3}}) {
      Commitments.Seal hash = new Commitments.Seal(CONTEXT, 0, 0, null, 2);
      assertThrows(
          ProtocolError.class,
          () -> {
            for (long id : bad) hash.add(id);
          });
      assertThrows(ProtocolError.class, hash::finish);
      assertThrows(ProtocolError.class, () -> hash.add(4));
    }
    Commitments.Seal shortHash = new Commitments.Seal(CONTEXT, 0, 0, null, 2);
    shortHash.add(1);
    assertThrows(ProtocolError.class, shortHash::finish);
    assertThrows(ProtocolError.class, () -> shortHash.add(2));
    Commitments.Seal empty = new Commitments.Seal(CONTEXT, 0, 0, null, 0);
    empty.finish();
    assertThrows(ProtocolError.class, empty::finish);
    assertThrows(ProtocolError.class, () -> new Commitments.Seal(CONTEXT, 0, 1, null, 0));
    assertThrows(ProtocolError.class, () -> new Commitments.Seal(CONTEXT, 1, 0, null, 0));
    assertNotEquals(seal(CONTEXT, 1, 0, WORK, 1), seal(CONTEXT, 1, 1, WORK, 1));
    assertNotEquals(seal(CONTEXT, 1, 0, WORK, 1), seal(CONTEXT, 1, 0, new WorkKey(0, 0, 2), 1));
    for (int count : new int[] {0, 1, 23, 24, 255, 256, 1000}) {
      Commitments.Seal incremental = new Commitments.Seal(CONTEXT, 0, 0, null, count);
      Cbor.Writer direct = new Cbor.Writer(65536);
      direct.array(7);
      direct.text("authority-1", 128);
      direct.text("owner-1", 128);
      direct.number(1);
      direct.number(0);
      direct.number(0);
      direct.nil();
      direct.array(count);
      for (int n = 0; n < count; n++) {
        long id = n == count - 1 ? Long.MAX_VALUE : n + 1;
        incremental.add(id);
        direct.number(id);
      }
      assertEquals(raw("pipestream-scope-seal-v2", direct.finish()), incremental.finish());
    }
  }

  static WorkView terminal(long entity) {
    State state = State.from(5 + entity % 4);
    boolean inputless = state == State.CANCELLED || state == State.SKIPPED;
    return new WorkView(
        new WorkKey(0, 0, entity),
        state,
        inputless ? 0 : 1,
        inputless ? null : DESCRIPTOR,
        inputless ? null : 10L,
        inputless ? null : 100L,
        20L,
        120L,
        null,
        null,
        null,
        state == State.FAILED ? new Diagnostic(1, "failed") : null);
  }

  static Digest naiveRoot(List<Digest> leaves) throws Exception {
    if (leaves.isEmpty()) return raw("pipestream-status-empty-v2");
    List<Digest> level = leaves;
    while (level.size() > 1) {
      List<Digest> next = new ArrayList<>();
      for (int n = 0; n < level.size(); n += 2) {
        next.add(
            raw(
                "pipestream-status-node-v2",
                level.get(n).bytes(),
                level.get(Math.min(n + 1, level.size() - 1)).bytes()));
      }
      level = next;
    }
    return level.getFirst();
  }

  @Test
  void streamingStatusFoldMatchesIndependentLevelReductionAcrossOddTreeShapes() throws Exception {
    for (int count = 0; count <= 1025; count++) {
      Commitments.StatusTree tree = new Commitments.StatusTree(0, 0, count);
      List<Digest> leaves = new ArrayList<>();
      long[] counts = new long[4];
      for (long id = 1; id <= count; id++) {
        WorkView view = terminal(id);
        tree.add(view, null);
        leaves.add(Commitments.statusLeaf(view, null));
        counts[view.state().value() - 5]++;
        assertEquals(Long.bitCount(id) * 32, tree.retainedHashBytes());
      }
      Commitments.Status actual = tree.finish();
      assertEquals(naiveRoot(leaves), actual.root(), "count=" + count);
      assertEquals(new Counts(counts[0], counts[1], counts[2], counts[3]), actual.counts());
      assertThrows(ProtocolError.class, tree::finish);
    }
  }

  @Test
  void statusLeavesRequireTerminalViewsAndExactChildPresence() {
    WorkView succeeded = success();
    assertThrows(ProtocolError.class, () -> Commitments.statusLeaf(succeeded, INPUT));
    WorkView branch =
        new WorkView(
            WORK,
            State.SUCCEEDED,
            1,
            DESCRIPTOR,
            10L,
            100L,
            20L,
            120L,
            null,
            new ChildScope(1, 1),
            null,
            null);
    assertThrows(ProtocolError.class, () -> Commitments.statusLeaf(branch, null));
    assertNotEquals(Commitments.statusLeaf(branch, INPUT), Commitments.statusLeaf(branch, OUTPUT));
    WorkView active =
        new WorkView(
            WORK, State.ACTIVE, 1, DESCRIPTOR, 10L, 100L, null, null, null, null, null, null);
    assertThrows(ProtocolError.class, () -> Commitments.statusLeaf(active, null));
    assertThrows(
        ProtocolError.class,
        () ->
            new WorkView(
                WORK,
                State.WAITING_CHILDREN,
                1,
                DESCRIPTOR,
                10L,
                100L,
                null,
                null,
                null,
                null,
                null,
                null));
    for (int mode = 0; mode < 4; mode++) {
      Commitments.StatusTree tree = new Commitments.StatusTree(0, 0, 2);
      tree.add(terminal(2), null);
      if (mode == 0) assertThrows(ProtocolError.class, tree::finish);
      else {
        WorkView invalid = mode == 1 ? terminal(2) : mode == 2 ? terminal(1) : active;
        assertThrows(ProtocolError.class, () -> tree.add(invalid, null));
      }
      assertThrows(ProtocolError.class, () -> tree.add(terminal(3), null));
      assertThrows(ProtocolError.class, tree::finish);
    }
    Commitments.StatusTree empty = new Commitments.StatusTree(0, 0, 0);
    assertThrows(ProtocolError.class, () -> empty.add(terminal(1), null));
    Commitments.StatusTree other = new Commitments.StatusTree(1, 0, 1);
    assertThrows(ProtocolError.class, () -> other.add(terminal(1), null));
  }
}
