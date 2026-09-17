package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.handler.codec.quic.QuicStreamChannel;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12.1 credit reservation under packet loss and reordering (S12-046): refused, reset and
 * finished input streams return their slots as peer-observable credit, replacement streams admit,
 * more transfers than the initial allowance complete without a stream-limit failure, and the
 * initial allowance is the selected limit, all measured through a datagram relay that drops and
 * reorders packets in both directions. The relay counts what it actually dropped and reordered,
 * so the test cannot pass with the injection idle. The listener's transport delivers MAX_STREAMS
 * updates in batches (the credit reading returns to the allowance every few collected streams,
 * never one by one), which the observations file records; the clause requires the reservation to
 * hold, not a per-stream update. Diagnostic knobs: {@code -Dlossy.drop} (percent), {@code
 * -Dlossy.hold} (hold every nth datagram behind its successor) and {@code -Dlossy.direct=true}
 * (bypass the relay; the injection assertions then fail by design). A refusal at the negotiated
 * idle bound instead of by the reset or the length check is tolerated and counted per shape: it
 * is what the listener does when the loss delays the reset or the FIN past that bound.
 */
@Timeout(180)
class LossyTransportCreditTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(20_000, 60_000, 120_000);
  static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
  }

  /** Poll the peer-observable credit for up to the given time; return the last reading. */
  static long creditWithin(RawDurablePeer peer, long target, long millis) throws Exception {
    long deadline = System.nanoTime() + millis * 1_000_000;
    long credit = peer.streamCredit();
    while (credit < target && System.nanoTime() < deadline) {
      Thread.sleep(20);
      credit = peer.streamCredit();
    }
    return credit;
  }

  @Test
  void creditReservationHoldsUnderDatagramLossAndReordering() throws Exception {
    Path root = directory.resolve("lossy");
    DurableHost host =
        DurableHost.initialize(
            root,
            DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
            ReferenceApplications.all(),
            DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true),
            DurableHost.UtcClock.system(true));
    byte[] input = DurableServerTest.payload(60_000, 17);
    List<String> observations = new ArrayList<>();
    observations.add(
        "ordinal\tshape\tstream\tresponse\tresponse-ms\tcredit-before-open"
            + "\tcredit-at-response\tcredit-after-wait\twait-ms");
    try (host;
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults());
        DatagramRelay relay =
            new DatagramRelay(
                server.address(),
                20260912L,
                Integer.getInteger("lossy.drop", 8),
                Integer.getInteger("lossy.hold", 7));
        RawDurablePeer peer =
            new RawDurablePeer(
                Boolean.getBoolean("lossy.direct") ? server.address() : relay.address(),
                pki.client("alice"),
                65_536)) {
      // The shortest permitted idle bound: a reset that loss delays past it is refused at the
      // bound, inside the peer's response window, and the count of those refusals is recorded.
      Capabilities selected =
          peer.negotiate(
              new Capabilities(
                  false,
                  List.of(DURABLE_WORK, RESULT_DELIVERY),
                  List.of(),
                  1 << 20,
                  16,
                  64,
                  1 << 20,
                  // Idle bound short enough that loss-delayed resets and FINs are refused at it, long
                  // enough that an undisturbed admitted transfer is never refused on a loaded host.
                  3000,
                  30_000));
      int allowance = selected.streamLimit();
      assertTrue(allowance >= 2 && allowance <= 16, selected.toString());
      // The initially permitted streams are the selected limit, observed as transport credit.
      assertEquals(allowance, peer.streamCredit(), "initial allowance is the selected limit");
      assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      int rounds = 2 * allowance;
      List<Long> members = new ArrayList<>();
      for (long entity = 1; entity <= rounds + 1; entity++) members.add(entity);
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(peer.request(), DurableServerTest.operation(1), 0, members, true)));
      long minimumBeforeOpen = Long.MAX_VALUE;
      int deadlineRefusals = 0;
      int deadlineFinRefusals = 0;
      // Twice the allowance of transfers: refused (truncated FIN), reset after a partial payload,
      // and admitted, in rotation. Every one must hand its slot back as credit through the relay.
      for (int ordinal = 0; ordinal < rounds; ordinal++) {
        Records.WorkKey work = new Records.WorkKey(0, 0, ordinal + 1);
        Records.InputHeader header =
            DurableServerTest.header(1, 10 + ordinal, work, input, "copy/v2", 0);
        long before = peer.streamCredit();
        QuicStreamChannel stream;
        String shape;
        switch (ordinal % 3) {
          case 0 -> {
            shape = "truncated-fin";
            stream = peer.sendInput(header, Arrays.copyOf(input, input.length - 1), true);
          }
          case 1 -> {
            shape = "reset-after-partial";
            stream = peer.sendInput(header, Arrays.copyOf(input, 1000), false);
            stream.shutdownOutput(0x204).sync();
          }
          default -> {
            shape = "admitted";
            stream = peer.sendInput(header, input, true);
          }
        }
        long opened = System.nanoTime();
        Message response;
        try {
          response = peer.next();
        } catch (AssertionError silent) {
          observations.add(
              ordinal + "\t" + shape + "\t" + stream.streamId() + "\tno response in 10 s\tcredit="
                  + peer.streamCredit() + "\tdropped=" + relay.dropped.get() + "\treordered="
                  + relay.reordered.get());
          Files.write(Path.of("target", "lossy-credit-observations.tsv"), observations);
          throw silent;
        }
        long responseMs = (System.nanoTime() - opened) / 1_000_000;
        if (ordinal % 3 == 2) {
          String seen = String.valueOf(response);
          AdmissionResponse admitted =
              assertInstanceOf(
                  AdmissionResponse.class, response, "admitted shape answered " + seen);
          assertEquals(new Records.RequestTag(true, stream.streamId()), admitted.request());
          assertEquals(
              Records.State.SUCCEEDED, DurableServerTest.awaitTerminal(peer, work).state());
        } else {
          Refusal refused = assertInstanceOf(Refusal.class, response);
          assertEquals(new Records.RequestTag(true, stream.streamId()), refused.request());
          // A reset or FIN that the loss delays past the negotiated idle bound is refused at that
          // bound (LIMIT_EXCEEDED "input receive deadline") instead of by the reset or the length
          // check; either way the refusal is correlated and the slot must return. The counts per
          // shape are recorded (observation O-1 in the handoff).
          boolean deadline =
              refused.code() == ProtocolError.Code.LIMIT_EXCEEDED
                  && refused.detail().equals("input receive deadline");
          if (deadline && ordinal % 3 == 1) deadlineRefusals++;
          else if (deadline) deadlineFinRefusals++;
          else assertEquals(ProtocolError.Code.INTEGRITY_ERROR, refused.code(), refused.toString());
        }
        long atResponse = peer.streamCredit();
        long started = System.nanoTime();
        long reached = creditWithin(peer, allowance, 300);
        minimumBeforeOpen = Math.min(minimumBeforeOpen, before);
        observations.add(
            ordinal
                + "\t"
                + shape
                + "\t"
                + stream.streamId()
                + "\t"
                + (response instanceof Refusal refusal ? "Refusal " + refusal.code() : "Admitted")
                + "\t"
                + responseMs
                + "\t"
                + before
                + "\t"
                + atResponse
                + "\t"
                + reached
                + "\t"
                + (System.nanoTime() - started) / 1_000_000);
      }
      assertFalse(peer.closed.isDone(), "refused and reset inputs never end the connection");
      // A replacement transfer for the last member admits and completes after all of the above.
      Records.WorkKey last = new Records.WorkKey(0, 0, rounds + 1);
      QuicStreamChannel valid =
          peer.sendInput(
              DurableServerTest.header(1, 10 + rounds, last, input, "copy/v2", 0), input, true);
      AdmissionResponse admitted = assertInstanceOf(AdmissionResponse.class, peer.next());
      assertEquals(new Records.RequestTag(true, valid.streamId()), admitted.request());
      assertEquals(Records.State.SUCCEEDED, DurableServerTest.awaitTerminal(peer, last).state());
      // The reservation held: 2n+1 transfers opened on an allowance of n without a stream-limit
      // failure, and the credit left is at least half the allowance (updates are batched).
      long finalCredit = creditWithin(peer, allowance, 2000);
      observations.add("final\tcredit=" + finalCredit + "\ttransfers=" + (rounds + 1));
      assertTrue(rounds + 1 > allowance, "more transfers than the allowance");
      assertTrue(finalCredit >= allowance / 2, "final credit " + finalCredit + " of " + allowance);
      assertInstanceOf(Detached.class, peer.call(new Detach(peer.request())));
      // Batched updates never let the peer approach exhaustion: at least half the allowance was
      // always available before a transfer opened, over more transfers than the allowance.
      assertTrue(
          minimumBeforeOpen >= allowance / 2,
          "credit before open fell to " + minimumBeforeOpen + " of " + allowance);
      observations.add(
          "relay\tdeadline-refusals-reset="
              + deadlineRefusals
              + "\tdeadline-refusals-fin="
              + deadlineFinRefusals
              + "\tforwarded="
              + relay.forwarded.get()
              + "\tdropped="
              + relay.dropped.get()
              + "\treordered="
              + relay.reordered.get());
      Files.write(Path.of("target", "lossy-credit-observations.tsv"), observations);
      // The injection was real: datagrams were dropped and reordered in the measured window.
      assertTrue(relay.dropped.get() >= 20, "dropped " + relay.dropped.get());
      assertTrue(relay.reordered.get() >= 20, "reordered " + relay.reordered.get());
    }
  }
}
