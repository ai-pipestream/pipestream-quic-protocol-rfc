package ai.pipestream.quic.v2;


import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12.1 per-owner connection ceiling as the durable client sees it. The listener judges
 * the ceiling after authentication, so it cannot use the transport's CONNECTION_REFUSED; it
 * closes the surplus connection with an application close naming LIMIT_EXCEEDED before selecting
 * capabilities, and the client reports exactly that code to its caller (the Rust client now does
 * the same; the durable-index-build example met the Rust side as a bare "connection lost"). The
 * slot is free again once its holder leaves.
 */
class DurableClientCeilingTest {
  static final Records.Policy POLICY = DurableClientTest.POLICY;

  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;

  @BeforeAll
  static void setup() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
  }

  static DurableOptions oneConnectionPerOwner() {
    DurableOptions defaults = DurableOptions.defaults();
    CoreOptions core = defaults.core();
    return new DurableOptions(
        new CoreOptions(
            core.controlLimit(),
            core.pendingLimit(),
            core.streamIdleMs(),
            core.streamLifetimeMs(),
            core.connections(),
            1,
            core.queuedControlBytes(),
            core.controlWindowBytes(),
            core.readChunkBytes(),
            core.handshakeTimeoutMs(),
            core.controlTimeoutMs()),
        defaults.dataStreams(),
        defaults.maxDataStreams(),
        defaults.dataSendBytes(),
        defaults.streamWindowBytes(),
        defaults.chunkBytes(),
        defaults.objectLimit(),
        defaults.headerTimeoutMs(),
        defaults.requireDurable(),
        defaults.shutdownTimeoutMs());
  }

  static ClientJournal journal(String name) throws Exception {
    return ClientJournal.initialize(
        directory.resolve(name + ".sqlite"),
        new ClientJournal.Intent("issuer-a", "alice", 1, POLICY, true),
        ClientJournal.Limits.defaults());
  }

  @Test
  @Timeout(60)
  void aSurplusConnectionForTheOwnerIsRefusedLimitExceededAndTheSlotFreesWhenTheHolderLeaves()
      throws Exception {
    try (DurableHost host =
            DurableAuthorizationTest.host(directory.resolve("ceiling"), true, principals);
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                oneConnectionPerOwner())) {
      ClientJournal first = journal("holder");
      DurableClient holder =
          DurableClient.connect(
              server.address(), pki.client("alice"), first, ClientOptions.defaults());
      DurableClientTest.get(holder.ready());

      ClientJournal second = journal("surplus");
      DurableClient surplus =
          DurableClient.connect(
              server.address(), pki.client("alice"), second, ClientOptions.defaults());
      ProtocolError refused = DurableClientTest.refusal(surplus.ready());
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, refused.code(), refused.toString());
      assertTrue(refused.getMessage().contains("peer closed connection"), refused.toString());
      surplus.close();
      second.close();

      // The holder leaves; the same owner connects again once the listener has retired the slot.
      holder.close();
      first.close();
      ClientJournal third = journal("again");
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
      while (true) {
        DurableClient again =
            DurableClient.connect(
                server.address(), pki.client("alice"), third, ClientOptions.defaults());
        try {
          Capabilities selected = DurableClientTest.get(again.ready());
          assertTrue(selected.supported().contains(DURABLE_WORK), selected.toString());
          again.close();
          break;
        } catch (Exception stillHeld) {
          again.close();
          if (System.nanoTime() > deadline) fail("slot never freed: " + stillHeld);
          Thread.sleep(100);
        }
      }
      third.close();
    }
  }
}
