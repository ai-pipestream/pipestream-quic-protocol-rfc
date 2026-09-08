package ai.pipestream.quic.v2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Objects;
import java.util.UUID;

/**
 * Owned result file delivery. A destination is written through a bounded staging file in the same
 * directory, synchronized, then linked into place without overwriting. A crashed transfer leaves
 * only a {@code .pipestream-result-*.part} staging file, never a partial destination. A locally
 * retained copy is explicitly distinguished from a newly authorized remote transfer.
 */
public final class ResultFiles {
  private ResultFiles() {}

  /**
   * A verified delivery.
   *
   * @param path installed destination
   * @param length verified length
   * @param sha256 verified digest
   * @param local true for a local-copy verification, false for a fresh authorized transfer
   */
  public record Delivered(Path path, long length, Records.Digest sha256, boolean local) {}

  /** A new destination file; the parent directory must exist and the file must not. */
  public record Destination(Path path) {
    /** Validate the destination. */
    public Destination {
      Objects.requireNonNull(path);
    }
  }

  /** Bounded staging writer for one result transfer. */
  static final class Staging implements AutoCloseable {
    private final Path destination;
    private final Path staging;
    private final FileChannel channel;
    private final MessageDigest digest = Commitments.sha256();
    private long written;
    private boolean installed;

    Staging(Destination destination) throws IOException {
      this.destination = destination.path().toAbsolutePath().normalize();
      Path parent = this.destination.getParent();
      if (parent == null || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS))
        throw new IOException("destination directory does not exist");
      if (Files.exists(this.destination, LinkOption.NOFOLLOW_LINKS))
        throw new IOException("destination already exists; results never overwrite");
      staging = parent.resolve(".pipestream-result-" + UUID.randomUUID() + ".part");
      channel =
          FileChannel.open(
              staging,
              StandardOpenOption.CREATE_NEW,
              StandardOpenOption.WRITE,
              StandardOpenOption.READ);
    }

    void write(ByteBuffer bytes) throws IOException {
      digest.update(bytes.duplicate());
      while (bytes.hasRemaining()) written += channel.write(bytes);
    }

    long written() {
      return written;
    }

    /**
     * Verify and install after transport FIN was validated by the caller.
     *
     * @param expectedLength committed length
     * @param expected committed digest
     * @return installed delivery
     * @throws IOException mismatch or filesystem failure
     */
    Delivered install(long expectedLength, Records.Digest expected) throws IOException {
      Records.Digest actual = new Records.Digest(digest.digest());
      if (written != expectedLength || !actual.equals(expected))
        throw new ProtocolError(
            ProtocolError.Code.INTEGRITY_ERROR, "staged result differs from commitment");
      channel.force(true);
      channel.close();
      Files.createLink(destination, staging);
      Files.delete(staging);
      try (FileChannel directory =
          FileChannel.open(destination.getParent(), StandardOpenOption.READ)) {
        directory.force(true);
      }
      installed = true;
      return new Delivered(destination, written, actual, false);
    }

    @Override
    public void close() throws IOException {
      if (installed) return;
      try {
        channel.close();
      } finally {
        Files.deleteIfExists(staging);
      }
    }
  }

  /**
   * Verify an already delivered local file against a saved selection without any network access.
   * This grants no new remote authorization and renews nothing.
   *
   * @param path local copy
   * @param selection saved manifest selection
   * @return local verification
   * @throws IOException read failure
   */
  public static Delivered localCopy(Path path, ClientJournal.Selection selection)
      throws IOException {
    Records.Output output = selection.output();
    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
      throw new IOException("local copy must be a regular file");
    MessageDigest digest = Commitments.sha256();
    long length = 0;
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      ByteBuffer buffer = ByteBuffer.allocate(65_536);
      for (int read; (read = channel.read(buffer)) > 0; ) {
        buffer.flip();
        digest.update(buffer);
        buffer.clear();
        length += read;
      }
    }
    Records.Digest actual = new Records.Digest(digest.digest());
    if (length != output.length() || !actual.equals(output.sha256()))
      throw new ProtocolError(
          ProtocolError.Code.INTEGRITY_ERROR, "local copy differs from selection");
    return new Delivered(path, length, actual, true);
  }
}
