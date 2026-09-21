package ai.pipestream.quic.v2;

import ai.pipestream.quic.storage.FileObjectStore;
import ai.pipestream.quic.storage.ObjectStore;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.UUID;

/**
 * Subprocess entry point for the SPI output plane's kill/resume row: funds an
 * admission, publishes one output through an {@link InputStore} on the SPI
 * payload plane, and halts the JVM at the named {@link InputStore.Phase}.
 */
public final class SpiOutputKillChild {
  private SpiOutputKillChild() {}

  /**
   * @param args {@code <root> <spiRoot> <phaseName>}
   */
  public static void main(String[] args) throws Exception {
    Path root = Path.of(args[0]);
    Path spiRoot = Path.of(args[1]);
    InputStore.Phase haltAt = InputStore.Phase.valueOf(args[2]);
    UUID authority = UUID.randomUUID();
    ObjectStore backend = FileObjectStore.open(spiRoot);
    InputStore.Probe probe = phase -> {
      if (phase == haltAt) Runtime.getRuntime().halt(7);
    };
    byte[] payload = new byte[2048];
    for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i * 17 + 5);
    byte[] operation = new byte[16];
    operation[15] = 7;
    Commitments.Context context = new Commitments.Context("issuer-a", "alice", 1);
    Records.InputHeader header =
        new Records.InputHeader(
            1,
            new Records.OperationId(operation),
            new Records.AdmitParameters(
                new Records.WorkKey(0, 0, 7),
                new Records.Input(
                    0,
                    new Records.Digest(MessageDigest.getInstance("SHA-256").digest(new byte[0])),
                    "application/octet-stream"),
                "output-kill-row",
                0,
                1000,
                new Records.OutputBudget(1, 1 << 20)));
    try (InputStore store =
        InputStore.initializeForAuthority(
            root,
            new InputStore.Limits(16L << 20, 64, 8 << 20, 16),
            authority,
            backend,
            probe)) {
      publish(store, context, header, authority, payload);
    }
  }

  private static void publish(
      InputStore store,
      Commitments.Context context,
      Records.InputHeader header,
      UUID authority,
      byte[] payload)
      throws Exception {
    store.reserveOutputs(context, header);
    OutputStore.Writer writer =
        store.beginOutput(
            context,
            header,
            new ExecutionStore.Lease(authority, "alice", 1, header.parameters().work(), 1, 1, 2000),
            0,
            payload.length,
            "application/octet-stream",
            1 << 20);
    writer.write(ByteBuffer.wrap(payload));
    writer.finish();
    writer.close();
  }
}
