package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12.4 storage bounds under sustained read load: the authority's write-ahead log must
 * stay inside its funded bound while readers overlap continuously, so a long pipelined session
 * is refused only for the work it declares, never for the log the readers keep alive. SQLite
 * restarts its log only when no reader holds it; without an explicit checkpoint a permanently
 * polled authority grows the log to its cap and refuses every further write with
 * LIMIT_EXCEEDED "SQLite file capacity exhausted" (Meta C16a xlarge64 mixed cell).
 */
@Timeout(180)
class SessionLogGrowthTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(20_000, 60_000, 120_000);
  static final long WAL_BYTES = 256L << 20;
  static final long BOUND = 8L << 20;
  static final int UNITS = 100;

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
  }

  static RawDurablePeer connect(DurableServer server) throws Exception {
    RawDurablePeer peer = new RawDurablePeer(server.address(), pki.client("alice"), 65_536);
    peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
    return peer;
  }

  @Test
  void continuousReadersDoNotLetTheLogReachItsFundedBound() throws Exception {
    Path root = directory.resolve("growth");
    DurableHost host =
        DurableHost.initialize(
            root,
            V2Main.configuration(
                Map.of(
                    "authority", "issuer-a",
                    "result-authority", "localhost:7443",
                    "db-mib", "1024",
                    "wal-mib", "256")),
            ReferenceApplications.all(),
            DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true),
            DurableHost.UtcClock.system(true));
    DurableServer server =
        DurableServer.start(
            new InetSocketAddress("127.0.0.1", 0),
            pki.server(principals),
            host,
            DurableOptions.defaults());
    Path log = root.resolve(DurableHost.DATABASE + "-wal");
    AtomicBoolean stop = new AtomicBoolean();
    AtomicReference<Throwable> readerFailure = new AtomicReference<>();
    List<Thread> readers = new ArrayList<>();
    try (RawDurablePeer writer = connect(server)) {
      Binding binding =
          assertInstanceOf(Binding.class, writer.call(new Create(writer.request(), 1, POLICY)));
      // Six readers page the scope without pause: at every instant some reader holds the log.
      for (int i = 0; i < 6; i++) {
        Thread reader =
            new Thread(
                () -> {
                  try (RawDurablePeer peer = connect(server)) {
                    assertInstanceOf(
                        Binding.class,
                        peer.call(
                            new Attach(peer.request(), "issuer-a", "alice", binding.generation())));
                    while (!stop.get()) {
                      assertInstanceOf(
                          PageResponse.class, peer.call(new Page(peer.request(), 0, 0, 256)));
                    }
                  } catch (Throwable failure) {
                    readerFailure.compareAndSet(null, failure);
                  }
                },
                "reader-" + i);
        reader.start();
        readers.add(reader);
      }
      // One unsealed batch of members; every unit below admits and completes one of them.
      assertInstanceOf(
          DeclarationResponse.class,
          writer.call(
              new Declare(
                  writer.request(),
                  DurableServerTest.operation(1),
                  0,
                  LongStream.rangeClosed(1, UNITS).boxed().toList(),
                  false)));
      long largest = 0;
      int units = 0;
      byte[] input = new byte[4096];
      for (int unit = 1; unit <= UNITS && readerFailure.get() == null; unit++) {
        Records.WorkKey work = new Records.WorkKey(0, 0, unit);
        input[0] = (byte) unit;
        writer.sendInput(
            DurableServerTest.header(binding.generation(), 1000 + unit, work, input, "copy/v2", 0),
            input,
            true);
        Message answer = writer.next();
        if (answer instanceof Refusal refusal)
          fail(
              "unit "
                  + unit
                  + " refused "
                  + refusal.code()
                  + " "
                  + refusal.detail()
                  + " with the log at "
                  + largest
                  + " bytes");
        assertInstanceOf(AdmissionResponse.class, answer);
        assertEquals(
            Records.State.SUCCEEDED, DurableServerTest.awaitTerminal(writer, work).state());
        units++;
        if (Files.exists(log)) largest = Math.max(largest, Files.size(log));
      }
      stop.set(true);
      for (Thread reader : readers) reader.join(10_000);
      if (readerFailure.get() != null) throw new AssertionError("reader failed", readerFailure.get());
      assertTrue(
          largest < BOUND,
          "the log grew to " + largest + " bytes over " + units + " units under continuous readers");
      assertTrue(host.status().logRestarts() > 0, "the retention service never restarted the log");
    } finally {
      stop.set(true);
      server.close();
      host.close();
    }
  }
}
