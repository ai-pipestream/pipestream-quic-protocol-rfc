package ai.pipestream.quic.v2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Test-only event recorder implementing the neutral fixture interface v1 event schema: one bounded
 * UTF-8 TSV record per reached boundary, each line written and synchronized as a unit, with a
 * per-process monotonic sequence and the {@code pid-startnanos} process identity. Never used by
 * shipped launchers.
 */
final class FixtureEvents implements AutoCloseable {
  /** Maximum records per process per run under the interface bound. */
  static final int MAX_EVENTS = 65_536;

  private final FileChannel channel;
  private final String prefix;
  private final Path directory;
  private long sequence;
  private boolean closed;

  private FixtureEvents(FileChannel channel, Path directory, String prefix) {
    this.channel = channel;
    this.directory = directory;
    this.prefix = prefix;
  }

  /**
   * Open (append) the events file for one process.
   *
   * @param file events TSV inside the fixture-owned scenario directory
   * @param runId run identifier
   * @param scenarioId scenario identifier
   * @param role {@code server} or {@code client}
   * @return recorder
   * @throws IOException open failure
   */
  static FixtureEvents open(Path file, String runId, String scenarioId, String role)
      throws IOException {
    Objects.requireNonNull(file);
    Files.createDirectories(file.toAbsolutePath().getParent());
    FileChannel channel =
        FileChannel.open(
            file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    String process =
        ProcessHandle.current().pid()
            + "-"
            + Long.toHexString(
                ProcessHandle.current()
                    .info()
                    .startInstant()
                    .map(i -> i.toEpochMilli() * 1_000_000L)
                    .orElse(System.nanoTime()));
    String prefix =
        "1\t"
            + escape(runId)
            + "\t"
            + escape(scenarioId)
            + "\tjava\t"
            + escape(role)
            + "\t"
            + process
            + "\t";
    return new FixtureEvents(channel, file.toAbsolutePath().getParent(), prefix);
  }

  /**
   * Directory holding the events file; release markers live beside it.
   *
   * @return scenario directory
   */
  Path directory() {
    return directory;
  }

  static String escape(String label) {
    StringBuilder out = new StringBuilder(label.length());
    for (int i = 0; i < label.length(); i++) {
      char c = label.charAt(i);
      switch (c) {
        case '\t' -> out.append("\\t");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\\' -> out.append("\\\\");
        default -> out.append(c);
      }
    }
    return out.toString();
  }

  /**
   * Append one complete record and synchronize it.
   *
   * @param boundary boundary label or empty for a pure observation
   * @param details bounded identity
   * @param artifact optional referenced artifact relative to the scenario directory, or null
   * @throws IOException write failure or bound exceeded
   */
  synchronized void record(String boundary, Boundaries.Details details, Path artifact)
      throws IOException {
    if (closed) return;
    if (sequence >= MAX_EVENTS) throw new IOException("fixture event bound exceeded");
    sequence++;
    StringBuilder line = new StringBuilder(prefix);
    line.append(sequence).append('\t').append(escape(boundary)).append('\t');
    line.append(
            details.operation() == null
                ? ""
                : HexFormat.of().formatHex(details.operation().bytes()))
        .append('\t');
    line.append(
            details.work() == null
                ? ""
                : details.work().scope()
                    + ":"
                    + details.work().producer()
                    + ":"
                    + details.work().entity())
        .append('\t');
    line.append(details.attempt() == 0 ? "" : Long.toString(details.attempt())).append('\t');
    line.append(details.refusal() == null ? "" : Integer.toString(details.refusal().value()))
        .append('\t');
    if (artifact == null) line.append("\t\t");
    else {
      Path absolute = directory.resolve(artifact);
      byte[] bytes = Files.readAllBytes(absolute);
      line.append(escape(directory.relativize(absolute).toString()))
          .append('\t')
          .append(bytes.length)
          .append('\t')
          .append(HexFormat.of().formatHex(digest(bytes)));
    }
    line.append('\n');
    ByteBuffer buffer = ByteBuffer.wrap(line.toString().getBytes(StandardCharsets.UTF_8));
    while (buffer.hasRemaining()) channel.write(buffer);
    channel.force(true);
  }

  private static byte[] digest(byte[] bytes) {
    return Commitments.sha256().digest(bytes);
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) return;
    closed = true;
    channel.close();
  }
}
