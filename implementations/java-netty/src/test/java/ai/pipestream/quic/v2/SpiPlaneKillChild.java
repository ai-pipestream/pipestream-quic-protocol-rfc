package ai.pipestream.quic.v2;

import ai.pipestream.quic.storage.FileObjectStore;
import ai.pipestream.quic.storage.ObjectStore;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.UUID;

/**
 * Subprocess entry point for the SPI plane's kill/resume row: installs one
 * input through an {@link InputStore} on the SPI payload plane and halts the
 * JVM at the named {@link InputStore.Phase}, leaving whatever the phase
 * boundary guarantees (and nothing more) durable on disk.
 */
public final class SpiPlaneKillChild {
  private SpiPlaneKillChild() {}

  /**
   * @param args {@code <root> <spiRoot> <phaseName>}
   */
  public static void main(String[] args) throws Exception {
    Path root = Path.of(args[0]);
    Path spiRoot = Path.of(args[1]);
    InputStore.Phase haltAt = InputStore.Phase.valueOf(args[2]);
    ObjectStore backend = FileObjectStore.open(spiRoot);
    InputStore.Probe probe = phase -> {
      if (phase == haltAt) Runtime.getRuntime().halt(7);
    };
    byte[] payload = new byte[4096];
    for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i * 31 + 7);
    byte[] operation = new byte[16];
    operation[15] = 9;
    Records.InputHeader header =
        new Records.InputHeader(
            1,
            new Records.OperationId(operation),
            new Records.AdmitParameters(
                new Records.WorkKey(0, 0, 9),
                new Records.Input(
                    payload.length,
                    new Records.Digest(MessageDigest.getInstance("SHA-256").digest(payload)),
                    "application/octet-stream"),
                "kill-row",
                0,
                1000,
                new Records.OutputBudget(0, 0)));
    Messages.Capabilities selected =
        new Messages.Capabilities(
            true, List.of(Messages.DURABLE_WORK), List.of(), 65536, 4, 16, 1 << 20, 1000, 5000);
    try (InputStore store =
            InputStore.initializeForAuthority(
                root, new InputStore.Limits(16L << 20, 32, 8 << 20, 8), UUID.randomUUID(), backend,
                probe);
        InputStore.Receiver receiver =
            store.begin(new Commitments.Context("issuer-a", "alice", 1), header, selected, 1)) {
      receiver.write(ByteBuffer.wrap(payload), 2);
      receiver.finish(3);
    }
  }
}
