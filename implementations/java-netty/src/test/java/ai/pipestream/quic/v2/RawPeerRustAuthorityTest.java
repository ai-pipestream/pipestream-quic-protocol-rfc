package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.*;

import io.netty.handler.codec.quic.QuicStreamChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The raw Java peer against the Rust authority CLI: the Section 12.1 refused-stream rule, which is
 * PipeStream's own requirement that refused streams advance the peer's cumulative MAX_STREAMS limit
 * (RFC 9000 leaves that policy to implementations). The normative observation is that replacement
 * streams keep opening over several times the initial allowance and a valid transfer then succeeds
 * on the same connection. The stricter reading that the remaining allowance returns to the selected
 * limit after every refusal is fixture evidence of how this authority behaves, not a protocol
 * constant; a batching implementation could pass the first observation and not the second.
 * Rust-side counterpart of {@code
 * DurableWireNegativeTest.sequentialInputsBeyondTheConcurrentLimitReplenishStreamCredit}.
 */
@Tag("sealed-interop")
@Timeout(300)
final class RawPeerRustAuthorityTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Path executable;
  static final Records.Policy RUST_DEFAULT_POLICY =
      new Records.Policy(60_000, 3_600_000, 86_400_000);
  static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);

  @BeforeAll
  static void setup() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    executable =
        Path.of("../rust-quinn/target/release/pipestream-quinn").toAbsolutePath().normalize();
    assertTrue(Files.isExecutable(executable), "sealed interop requires the release Rust CLI");
  }

  @Test
  void refusedInputsReturnStreamCreditOnTheRustAuthority() throws Exception {
    Path principals = directory.resolve("rust-principals.tsv");
    pki.principalMap(principals, List.of("alice"));
    List<String> storage =
        List.of(
            "--state-db",
            directory.resolve("rust-authority.sqlite").toString(),
            "--object-dir",
            directory.resolve("rust-objects").toString(),
            "--authority",
            "issuer-a",
            "--principal-map",
            principals.toString(),
            "--trust-system-clock");
    List<String> initialize =
        new java.util.ArrayList<>(List.of(executable.toString(), "v2", "init-authority"));
    initialize.addAll(storage);
    RustAuthorityProcess.command(directory, initialize);
    byte[] input = DurableServerTest.payload(60_000, 17);
    try (RustAuthorityProcess server =
            new RustAuthorityProcess(executable, pki, directory, storage, "credit");
        RawDurablePeer peer = new RawDurablePeer(server.address(), pki.client("alice"), 65_536)) {
      Capabilities selected =
          peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
      int allowance = selected.streamLimit();
      assertTrue(allowance >= 2 && allowance <= 16, selected.toString());
      assertEquals(allowance, peer.streamCredit(), "initial allowance is the selected limit");
      assertInstanceOf(
          Binding.class, peer.call(new Create(peer.request(), 1, RUST_DEFAULT_POLICY)));
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(
                  peer.request(), DurableServerTest.operation(1), 0, List.of(1L, 2L), true)));
      // Three times the allowance of refused inputs, each one refused with a correlated REFUSAL
      // and each one returning its slot before the next is opened. Even ordinals are truncated
      // with FIN (digest/length contradiction); odd ordinals are reset after a partial payload.
      int refusals = 3 * allowance;
      List<String> observations = new java.util.ArrayList<>();
      observations.add(
          "ordinal\tshape\tstream\tcode\tdetail\tcredit-before-open\tcredit-at-refusal"
              + "\tcredit-after-wait\twait-ms");
      for (int ordinal = 0; ordinal < refusals; ordinal++) {
        Records.InputHeader header =
            DurableServerTest.header(1, 10 + ordinal, WORK, input, "copy/v2", 0);
        long before = peer.streamCredit();
        QuicStreamChannel stream;
        String shape;
        if (ordinal % 2 == 0) {
          shape = "truncated-fin";
          stream = peer.sendInput(header, Arrays.copyOf(input, input.length - 1), true);
        } else {
          shape = "reset-after-partial";
          stream = peer.sendInput(header, Arrays.copyOf(input, 1000), false);
          stream.shutdownOutput(0x204).sync();
        }
        Refusal refused = assertInstanceOf(Refusal.class, peer.next());
        assertEquals(new Records.RequestTag(true, stream.streamId()), refused.request());
        assertEquals(ProtocolError.Code.INTEGRITY_ERROR, refused.code(), refused.toString());
        assertFalse(refused.detail().isBlank(), "the local bound or check is named");
        long atRefusal = peer.streamCredit();
        long started = System.nanoTime();
        peer.awaitStreamCredit(allowance);
        observations.add(
            ordinal
                + "\t"
                + shape
                + "\t"
                + stream.streamId()
                + "\t"
                + refused.code()
                + "\t"
                + refused.detail()
                + "\t"
                + before
                + "\t"
                + atRefusal
                + "\t"
                + peer.streamCredit()
                + "\t"
                + (System.nanoTime() - started) / 1_000_000);
      }
      Files.write(Path.of("target", "rust-stream-credit-observations.tsv"), observations);
      assertFalse(peer.closed.isDone(), "refused inputs never end the connection");
      // The replacement transfer on the same connection admits and completes.
      QuicStreamChannel valid =
          peer.sendInput(DurableServerTest.header(1, 2, WORK, input, "copy/v2", 0), input, true);
      AdmissionResponse admitted = assertInstanceOf(AdmissionResponse.class, peer.next());
      assertEquals(new Records.RequestTag(true, valid.streamId()), admitted.request());
      assertEquals(Records.State.SUCCEEDED, DurableServerTest.awaitTerminal(peer, WORK).state());
      peer.awaitStreamCredit(allowance);
      assertInstanceOf(Detached.class, peer.call(new Detach(peer.request())));
    }
  }
}
