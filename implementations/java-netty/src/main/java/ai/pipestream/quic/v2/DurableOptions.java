package ai.pipestream.quic.v2;

import java.util.List;
import java.util.Objects;

/**
 * Bounded deployment policy for one durable V2 listener. Core transport ceilings come from the
 * embedded {@link CoreOptions}; the remaining fields size the per-connection object transport, the
 * advertised profile offer and the shutdown budget. Invalid budgets are rejected, not enlarged.
 *
 * @param core control-stream, connection-count and deadline policy
 * @param dataStreams concurrent object streams admitted per direction per connection (inputs
 *     received, results sent), also the advertised {@code stream-limit}
 * @param maxDataStreams lifetime ordinal ceiling of object streams per connection
 * @param dataSendBytes native send allowance shared by outgoing result streams
 * @param streamWindowBytes fixed per-stream receive window; the connection window is derived
 * @param chunkBytes maximum object chunk read from or written to the transport
 * @param objectLimit advertised maximum input/result object payload
 * @param headerTimeoutMs absolute local deadline for receiving one object header
 * @param requireDurable whether the server offer requires both durable profiles
 * @param shutdownTimeoutMs bounded wait for connection-local owners to drain on close
 */
public record DurableOptions(
    CoreOptions core,
    int dataStreams,
    int maxDataStreams,
    long dataSendBytes,
    int streamWindowBytes,
    int chunkBytes,
    long objectLimit,
    long headerTimeoutMs,
    boolean requireDurable,
    long shutdownTimeoutMs) {
  /** Validate all transport budgets through the stream owner's own limit checks. */
  public DurableOptions {
    Objects.requireNonNull(core);
    Checks.range(dataStreams, 1, 128);
    Checks.number(objectLimit);
    Checks.range(headerTimeoutMs, 1, 86_400_000);
    Checks.range(shutdownTimeoutMs, 1, 300_000);
    transport(core, dataStreams, maxDataStreams, dataSendBytes, streamWindowBytes, chunkBytes);
  }

  private static StreamTransport.Limits transport(
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

  /**
   * Local transport ceilings derived from these options.
   *
   * @return limits
   */
  StreamTransport.Limits transportLimits() {
    return transport(
        core, dataStreams, maxDataStreams, dataSendBytes, streamWindowBytes, chunkBytes);
  }

  /**
   * The exact implemented server offer: both durable profiles supported, required only when
   * configured, and the local object/stream ceilings.
   *
   * @return server capability offer (response false)
   */
  public Messages.Capabilities offer() {
    List<Integer> profiles = List.of(Messages.DURABLE_WORK, Messages.RESULT_DELIVERY);
    return new Messages.Capabilities(
        false,
        profiles,
        requireDurable ? profiles : List.of(),
        core.controlLimit(),
        dataStreams,
        core.pendingLimit(),
        objectLimit,
        core.streamIdleMs(),
        core.streamLifetimeMs());
  }

  /**
   * Conservative library defaults: 16 object streams per direction, 64 KiB chunks, 16 MiB objects.
   *
   * @return explicit default policy
   */
  public static DurableOptions defaults() {
    return new DurableOptions(
        new CoreOptions(
            512 * 1024, 64, 30_000, 300_000, 32, 8, 1 << 20, 262_144, 65_536, 10_000, 30_000),
        16,
        8192,
        4L << 20,
        262_144,
        65_536,
        16L << 20,
        10_000,
        false,
        30_000);
  }
}
