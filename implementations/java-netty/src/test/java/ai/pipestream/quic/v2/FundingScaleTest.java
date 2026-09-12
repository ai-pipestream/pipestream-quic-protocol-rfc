package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.pipestream.quic.BoundedSqlite;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12.4 storage funding at scale (defect D15): the launcher's {@code --wal-mib} funds the
 * log, but the log is usable only as far as its shared-memory index reaches, and the reference
 * 512 KiB sidecar indexes about 257 MiB. A larger funded log therefore did nothing, and a session
 * that declares and admits a few hundred units hit "SQLite file capacity exhausted" at the same
 * count whatever the flag said. The launcher now scales the sidecar with the funded log.
 */
@Timeout(120)
class FundingScaleTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(20_000, 60_000, 120_000);

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
  }

  static DurableHost.Configuration funded(String walMib) {
    return V2Main.configuration(
        Map.of(
            "authority",
            "issuer-a",
            "result-authority",
            "localhost:7443",
            "db-mib",
            "1024",
            "wal-mib",
            walMib));
  }

  @Test
  void theSharedMemoryIndexScalesWithTheFundedLog() {
    long page = 4096;
    // The reference bound and everything up to what 512 KiB indexes are unchanged.
    assertEquals(512L << 10, BoundedSqlite.Limits.sharedMemoryFor(64L << 20, page));
    assertEquals(512L << 10, BoundedSqlite.Limits.sharedMemoryFor(256L << 20, page));
    assertEquals(512L << 10, funded("64").files().sharedMemoryBytes());
    assertEquals(64L << 20, FixedRecords.usable(funded("64").files(), page));
    // Beyond it the sidecar grows so the whole funded log is usable, up to the 16 MiB ceiling.
    BoundedSqlite.Limits large = funded("1024").files();
    assertTrue(large.sharedMemoryBytes() > (512L << 10), String.valueOf(large));
    assertEquals(0, large.sharedMemoryBytes() % 65536);
    assertEquals(1024L << 20, FixedRecords.usable(large, page), String.valueOf(large));
    assertEquals(4096L << 20, FixedRecords.usable(funded("4096").files(), page));
    assertEquals(16L << 20, BoundedSqlite.Limits.sharedMemoryFor(16384L << 20, page));
    assertTrue(FixedRecords.usable(funded("16384").files(), page) > (8L << 30));
  }

  static int declareUntilRefused(DurableHost.Configuration configuration, String name, int batches)
      throws Exception {
    try (DurableHost host =
            DurableHost.initialize(
                directory.resolve(name),
                configuration,
                ReferenceApplications.all(),
                DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true),
                DurableHost.UtcClock.system(true));
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults());
        RawDurablePeer peer = new RawDurablePeer(server.address(), pki.client("alice"), 65_536)) {
      peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
      assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      int accepted = 0;
      for (int batch = 0; batch < batches; batch++) {
        List<Long> ids = new ArrayList<>();
        for (long id = 1; id <= 100; id++) ids.add(batch * 100L + id);
        Message response =
            peer.call(
                new Declare(
                    peer.request(),
                    DurableServerTest.operation(1 + batch),
                    0,
                    ids,
                    batch == batches - 1));
        if (response instanceof Refusal refused) {
          // Either face of the same bound: the reservation check before the write, or the
          // native ceiling refusing the write itself.
          assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, refused.code(), refused.toString());
          assertTrue(
              refused.detail().equals("SQLite file capacity exhausted")
                  || refused.detail().equals("fixed-record completion capacity exhausted"),
              refused.toString());
          return accepted;
        }
        assertInstanceOf(DeclarationResponse.class, response);
        accepted += 100;
      }
      assertInstanceOf(Detached.class, peer.call(new Detach(peer.request())));
      return accepted;
    }
  }

  @Test
  void aFundedLogAdmitsDeclarationsBeyondTheOldSidecarCap() throws Exception {
    // Twenty batches of a hundred: past what a 512 KiB sidecar could index in any case.
    int capped = declareUntilRefused(funded("256"), "capped", 20);
    assertTrue(capped < 2000, "the reference cap should have refused: " + capped);
    int funded = declareUntilRefused(funded("2048"), "funded", 20);
    assertEquals(2000, funded, "the funded log should have taken every batch");
    assertTrue(funded > capped);
  }
}
