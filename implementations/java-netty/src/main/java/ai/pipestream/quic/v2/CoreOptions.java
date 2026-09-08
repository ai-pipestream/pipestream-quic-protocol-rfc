package ai.pipestream.quic.v2;

import java.util.List;

/**
 * Bounded deployment policy for V2 Core transport. Core has no valid object streams; adding durable
 * objects requires a separate data/control credit budget, not these settings alone.
 *
 * @param controlLimit maximum control body bytes
 * @param pendingLimit maximum pending control responses
 * @param streamIdleMs advertised object idle ceiling, even though Core accepts no objects
 * @param streamLifetimeMs advertised lifetime and absolute detach deadline
 * @param connections global admitted connection ceiling, including handshakes; the listener uses
 *     one additional packet-local transport to emit an overload refusal
 * @param connectionsPerOwner ceiling for each mapped owner and for the anonymous bucket
 * @param queuedControlBytes maximum application bytes queued for Netty control writes
 * @param controlWindowBytes fixed control stream receive window; the connection uses twice this
 *     window for initial credit, replenishment and its maximum window
 * @param readChunkBytes maximum individual application read buffer
 * @param handshakeTimeoutMs local handshake deadline
 * @param controlTimeoutMs local control-frame, idle-control and queued-write deadline
 */
public record CoreOptions(
    int controlLimit,
    int pendingLimit,
    long streamIdleMs,
    long streamLifetimeMs,
    int connections,
    int connectionsPerOwner,
    int queuedControlBytes,
    int controlWindowBytes,
    int readChunkBytes,
    long handshakeTimeoutMs,
    long controlTimeoutMs) {
  /** Validate all deployment ceilings without silently increasing an insufficient budget. */
  public CoreOptions {
    if (controlLimit < 4096
        || controlLimit > 1048576
        || pendingLimit < 1
        || pendingLimit > 1024
        || streamIdleMs < 1000
        || streamIdleMs > 300000
        || streamLifetimeMs < streamIdleMs
        || streamLifetimeMs > 86400000
        || connections < 1
        || connections > 65536
        || connectionsPerOwner < 1
        || connectionsPerOwner > connections
        || queuedControlBytes < controlLimit + 5
        || queuedControlBytes > 16777216
        || controlWindowBytes < 128
        || controlWindowBytes > 1048576
        || readChunkBytes < 128
        || readChunkBytes > 65536
        || handshakeTimeoutMs < 1
        || handshakeTimeoutMs > 300000
        || controlTimeoutMs < 1
        || controlTimeoutMs > 300000)
      throw new IllegalArgumentException("invalid Core transport budget");
    long buffers = (long) connections * (queuedControlBytes + controlLimit + readChunkBytes);
    if (buffers > 128L * 1024 * 1024)
      throw new IllegalArgumentException("Core application buffer policy exceeds 128 MiB");
  }

  /**
   * Build the exact implemented Core offer, with no durable profile or object promise.
   *
   * @return client/server offer with an empty profile inventory
   */
  public Messages.Capabilities offer() {
    return new Messages.Capabilities(
        false,
        List.of(),
        List.of(),
        controlLimit,
        1,
        pendingLimit,
        0,
        streamIdleMs,
        streamLifetimeMs);
  }

  /**
   * Get conservative library defaults. These are count/buffer policies, not total process-memory
   * claims.
   *
   * @return explicit default policy
   */
  public static CoreOptions defaults() {
    return new CoreOptions(16384, 16, 5000, 30000, 64, 8, 131072, 32768, 4096, 10000, 30000);
  }
}
