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

/**
 * One stable input handle: the file is opened once, hashed through that handle, and later streamed
 * again through the same handle. Length and digest are fixed before anything is sent; a file that
 * changes underneath the handle produces an INTEGRITY_ERROR at the authority, never replacement
 * work. Blocking; use off network threads.
 */
public final class InputSource implements AutoCloseable {
  private final FileChannel channel;
  private final Records.Input input;
  private final String contentType;

  private InputSource(FileChannel channel, Records.Input input, String contentType) {
    this.channel = channel;
    this.input = input;
    this.contentType = contentType;
  }

  /**
   * Open and prehash a regular file.
   *
   * @param path regular file, not a symlink final component
   * @param contentType bounded printable content type
   * @param maximumBytes local admission ceiling
   * @return stable handle
   * @throws IOException unreadable, oversized or non-regular file
   */
  public static InputSource file(Path path, String contentType, long maximumBytes)
      throws IOException {
    Objects.requireNonNull(path);
    Checks.label(contentType);
    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
      throw new IOException("input must be a regular file: " + path);
    FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
    try {
      long length = channel.size();
      if (length > maximumBytes) throw new IOException("input exceeds the local object ceiling");
      MessageDigest digest = Commitments.sha256();
      ByteBuffer buffer = ByteBuffer.allocate(65_536);
      long hashed = 0;
      while (hashed < length) {
        buffer.clear();
        int read = channel.read(buffer, hashed);
        if (read <= 0) throw new IOException("input shrank while hashing");
        buffer.flip();
        digest.update(buffer);
        hashed += read;
      }
      return new InputSource(
          channel,
          new Records.Input(length, new Records.Digest(digest.digest()), contentType),
          contentType);
    } catch (IOException | RuntimeException failure) {
      channel.close();
      throw failure;
    }
  }

  /**
   * Immutable input commitment.
   *
   * @return length, digest and content type
   */
  public Records.Input input() {
    return input;
  }

  /**
   * Read one chunk at an absolute offset through the retained handle.
   *
   * @param offset payload offset
   * @param buffer destination, cleared by this call
   * @return bytes read, or -1 past the committed length
   * @throws IOException read failure
   */
  int read(long offset, ByteBuffer buffer) throws IOException {
    if (offset >= input.length()) return -1;
    buffer.clear();
    buffer.limit((int) Math.min(buffer.capacity(), input.length() - offset));
    int read = channel.read(buffer, offset);
    if (read <= 0) throw new IOException("input shrank while streaming");
    buffer.flip();
    return read;
  }

  /**
   * Content type supplied at open.
   *
   * @return label
   */
  public String contentType() {
    return contentType;
  }

  /** Release the handle. */
  @Override
  public void close() throws IOException {
    channel.close();
  }
}
