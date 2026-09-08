package ai.pipestream.quic.v2;

import java.util.List;
import java.util.Objects;

/**
 * Bounded policy for one durable client connection: the Core control budget plus object-stream
 * admission for inputs it sends and results it receives.
 *
 * @param core control-stream and deadline policy; listener-only fields are ignored
 * @param dataStreams concurrent object streams per direction, also the offered {@code stream-limit}
 * @param maxDataStreams lifetime ordinal ceiling of object streams on the connection
 * @param dataSendBytes native send allowance shared by outgoing input streams
 * @param streamWindowBytes fixed per-stream receive window for result streams
 * @param chunkBytes maximum object chunk read from a file or written to the transport
 * @param objectLimit offered maximum input/result object payload
 * @param headerTimeoutMs absolute local deadline for receiving one result header
 */
public record ClientOptions(
    CoreOptions core,
    int dataStreams,
    int maxDataStreams,
    long dataSendBytes,
    int streamWindowBytes,
    int chunkBytes,
    long objectLimit,
    long headerTimeoutMs) {
  /** Validate all transport budgets through the stream owner's own limit checks. */
  public ClientOptions {
    Objects.requireNonNull(core);
    Checks.range(dataStreams, 1, 128);
    Checks.number(objectLimit);
    Checks.range(headerTimeoutMs, 1, 86_400_000);
    transportLimits(
        core, dataStreams, maxDataStreams, dataSendBytes, streamWindowBytes, chunkBytes);
  }

  private static StreamTransport.Limits transportLimits(
      CoreOptions core,
      int dataStreams,
      int maxDataStreams,
      long dataSendBytes,
      int streamWindowBytes,
      int chunkBytes) {
    return new StreamTransport.Limits(
        dataStreams,
        maxDataStreams,
        dataSendBytes,
        core.queuedControlBytes(),
        streamWindowBytes,
        chunkBytes,
        core.controlTimeoutMs());
  }

  StreamTransport.Limits transportLimits() {
    return transportLimits(
        core, dataStreams, maxDataStreams, dataSendBytes, streamWindowBytes, chunkBytes);
  }

  /**
   * The exact client offer for a journaled profile selection. Every selected profile is also
   * required, since resumed work needs it.
   *
   * @param profiles journaled profile list
   * @return client offer
   */
  public Messages.Capabilities offer(List<Integer> profiles) {
    return new Messages.Capabilities(
        false,
        profiles,
        profiles,
        core.controlLimit(),
        dataStreams,
        core.pendingLimit(),
        objectLimit,
        core.streamIdleMs(),
        core.streamLifetimeMs());
  }

  /**
   * Conservative defaults: 16 object streams per direction, 64 KiB chunks, 16 MiB objects.
   *
   * @return explicit default policy
   */
  public static ClientOptions defaults() {
    return new ClientOptions(
        new CoreOptions(
            512 * 1024, 64, 30_000, 300_000, 1, 1, 1 << 20, 262_144, 65_536, 10_000, 30_000),
        16,
        8192,
        4L << 20,
        262_144,
        65_536,
        16L << 20,
        10_000);
  }
}
