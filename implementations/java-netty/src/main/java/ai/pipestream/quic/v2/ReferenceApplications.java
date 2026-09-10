package ai.pipestream.quic.v2;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Reference application contracts shipped with the Java host. Their labels and semantics match the
 * Rust command guide so one scenario can target either authority. They are explicit configured
 * contracts, not a fallback for unknown labels.
 */
public final class ReferenceApplications {
  /** Content type used by every reference output. */
  public static final String OCTET_STREAM = "application/octet-stream";

  /** Chunk size used by authority expansion. */
  public static final int CHUNK = 65_536;

  /** Maximum children produced by authority expansion. */
  public static final int MAX_CHILDREN = 256;

  private ReferenceApplications() {}

  /**
   * Every reference contract.
   *
   * @return copy, consume, retry-copy, reassemble, chunk-copy and transform
   */
  public static List<DurableHost.Application> all() {
    return List.of(copy(), consume(), retryCopy(), reassemble(), chunkCopy(), transform());
  }

  /**
   * {@code transform/v2}, mode 0: the frozen external-workload byte transform, {@code out[i] =
   * rotate_left_8(b, 1) XOR (i mod 251)} over chunk-relative offsets, output length equal to input
   * length. Deterministic and side-effect free, so its restart contract is IDEMPOTENT.
   *
   * @return contract
   */
  public static DurableHost.Application transform() {
    return new DurableHost.Application(
        "transform/v2",
        Set.of(0),
        DurableHost.RestartSafety.IDEMPOTENT,
        work -> {
          byte[] buffer = new byte[work.bufferLimit()];
          work.beginOutput(work.input().length(), OCTET_STREAM);
          long offset = 0;
          for (int read; (read = work.readInput(buffer, 0, buffer.length)) != -1; ) {
            for (int i = 0; i < read; i++) {
              int b = buffer[i] & 0xff;
              int rotated = ((b << 1) | (b >>> 7)) & 0xff;
              buffer[i] = (byte) (rotated ^ (int) ((offset + i) % 251));
            }
            work.writeOutput(ByteBuffer.wrap(buffer, 0, read));
            offset += read;
          }
          work.finishOutput();
          return DurableHost.Result.succeeded();
        },
        null);
  }

  /**
   * {@code copy/v2}, mode 0: one byte-exact streamed output.
   *
   * @return contract
   */
  public static DurableHost.Application copy() {
    return new DurableHost.Application(
        "copy/v2",
        Set.of(0),
        DurableHost.RestartSafety.IDEMPOTENT,
        ReferenceApplications::copy,
        null);
  }

  /**
   * {@code consume/v2}, mode 0: verify and consume the input, produce no output.
   *
   * @return contract
   */
  public static DurableHost.Application consume() {
    return new DurableHost.Application(
        "consume/v2",
        Set.of(0),
        DurableHost.RestartSafety.IDEMPOTENT,
        work -> {
          byte[] buffer = new byte[work.bufferLimit()];
          while (work.readInput(buffer, 0, buffer.length) != -1) {
            work.check();
          }
          return DurableHost.Result.succeeded();
        },
        null);
  }

  /**
   * {@code retry-copy/v2}, mode 0: attempt 1 reports a retryable failure; later attempts copy.
   *
   * @return contract
   */
  public static DurableHost.Application retryCopy() {
    return new DurableHost.Application(
        "retry-copy/v2",
        Set.of(0),
        DurableHost.RestartSafety.IDEMPOTENT,
        work ->
            work.attempt() == 1
                ? DurableHost.Result.retryable(1, "first attempt requests explicit retry")
                : copy(work),
        null);
  }

  /**
   * {@code reassemble/v2}, mode 1: concatenate caller-supplied children's output index zero in
   * increasing entity order; the parent input is the expected complete object.
   *
   * @return contract
   */
  public static DurableHost.Application reassemble() {
    return new DurableHost.Application(
        "reassemble/v2",
        Set.of(1),
        DurableHost.RestartSafety.IDEMPOTENT,
        ReferenceApplications::reassemble,
        null);
  }

  /**
   * {@code chunk-copy/v2}, mode 2: the authority declares and admits producer-1 children of at most
   * 65,536 bytes each (at most 256); children copy and the parent verifies reassembly.
   *
   * @return contract
   */
  public static DurableHost.Application chunkCopy() {
    return new DurableHost.Application(
        "chunk-copy/v2",
        Set.of(0, 2),
        DurableHost.RestartSafety.IDEMPOTENT,
        work -> work.key().producer() == 1 ? copy(work) : reassemble(work),
        ReferenceApplications::chunk);
  }

  private static DurableHost.Result copy(DurableHost.Work work) throws Exception {
    byte[] buffer = new byte[work.bufferLimit()];
    work.beginOutput(work.input().length(), OCTET_STREAM);
    for (int read; (read = work.readInput(buffer, 0, buffer.length)) != -1; ) {
      work.writeOutput(ByteBuffer.wrap(buffer, 0, read));
    }
    work.finishOutput();
    return DurableHost.Result.succeeded();
  }

  private static DurableHost.Result reassemble(DurableHost.Work work) throws Exception {
    byte[] buffer = new byte[work.bufferLimit()];
    MessageDigest digest = Commitments.sha256();
    long total = 0;
    work.beginOutput(work.input().length(), OCTET_STREAM);
    long after = 0;
    boolean more = true;
    while (more) {
      DurableHost.ChildPage page = work.children(after, 256);
      for (Records.WorkKey child : page.members()) {
        work.beginChildOutput(child.entity(), 0);
        for (int read; (read = work.readChildOutput(buffer, 0, buffer.length)) != -1; ) {
          if (total + read > work.input().length())
            return DurableHost.Result.failed(2, "reassembled output exceeds committed length");
          digest.update(buffer, 0, read);
          work.writeOutput(ByteBuffer.wrap(buffer, 0, read));
          total += read;
        }
        work.finishChildOutput();
        after = child.entity();
      }
      more = page.more() && !page.members().isEmpty();
    }
    if (total != work.input().length()
        || !MessageDigest.isEqual(digest.digest(), work.input().sha256().bytes()))
      return DurableHost.Result.failed(3, "reassembled output differs from committed input");
    work.finishOutput();
    return DurableHost.Result.succeeded();
  }

  private static DurableHost.Expansion chunk(DurableHost.Production production) throws Exception {
    long length = production.input().length();
    long count = (length + CHUNK - 1) / CHUNK;
    if (count > MAX_CHILDREN)
      return DurableHost.Expansion.failed(4, "input exceeds the chunk-copy child ceiling");
    List<Long> members = new ArrayList<>();
    for (long i = 1; i <= count; i++) members.add(i);
    production.declare(operation(0, production), members, true);
    byte[] buffer = new byte[Math.min(CHUNK, production.bufferLimit())];
    long scope = production.childScope();
    long consumed = 0;
    for (long i = 1; i <= count; i++) {
      long chunkLength = Math.min(CHUNK, length - (i - 1) * CHUNK);
      // Hash this chunk from the parent input before admitting; the same bytes are streamed again.
      MessageDigest digest = Commitments.sha256();
      long hashed = 0;
      List<byte[]> pieces = new ArrayList<>();
      while (hashed < chunkLength) {
        int want = (int) Math.min(buffer.length, chunkLength - hashed);
        int read = production.readInput(buffer, 0, want);
        if (read == -1)
          return DurableHost.Expansion.failed(5, "parent input shorter than committed");
        digest.update(buffer, 0, read);
        pieces.add(java.util.Arrays.copyOf(buffer, read));
        hashed += read;
      }
      consumed += hashed;
      Records.AdmitParameters parameters =
          new Records.AdmitParameters(
              new Records.WorkKey(scope, 1, i),
              new Records.Input(chunkLength, new Records.Digest(digest.digest()), OCTET_STREAM),
              "chunk-copy/v2",
              0,
              1000,
              new Records.OutputBudget(1, chunkLength));
      Optional<Records.OperationReceipt> retained =
          production.beginInput(operation(i, production), parameters);
      if (retained.isPresent()) continue;
      for (byte[] piece : pieces) production.writeInput(ByteBuffer.wrap(piece));
      production.finishInput();
      production.check();
    }
    if (consumed != length) return DurableHost.Expansion.failed(6, "parent input length differs");
    return DurableHost.Expansion.complete();
  }

  private static Records.OperationId operation(long index, DurableHost.Production production) {
    byte[] bytes = new byte[16];
    bytes[0] = 0x7f;
    long scope = production.childScope();
    for (int i = 0; i < 8; i++) bytes[1 + i] = (byte) (scope >>> (56 - 8 * i));
    for (int i = 0; i < 7; i++) bytes[9 + i] = (byte) (index >>> (48 - 8 * i));
    return new Records.OperationId(bytes);
  }
}
