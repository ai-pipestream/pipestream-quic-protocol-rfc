package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.incubator.codec.quic.QuicStreamType;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

final class SealedServerTest {
  private static final UUID PRODUCER = new UUID(1, 2);
  private static final String SESSION = "java-listener";
  private static final SealedWork.EntityKey ROOT = new SealedWork.EntityKey(0, 1);
  @TempDir Path directory;

  @Test @Tag("sealed-interop") void rustPublicProducerRunsRecursiveReplayAndRefusalScenariosAgainstJava() throws Exception {
    Path certs = certificates(); var calls = new AtomicInteger();
    try (var fixture = new Fixture(certs, (context, input) -> {
      calls.incrementAndGet();
      if (context.operation() == SealedExecutor.Operation.PROCESS && "dehydrate".equals(context.header().metadata().get("pipestream.action"))) {
        input.transferTo(java.io.OutputStream.nullOutputStream()); return new SealedExecutor.Decision(6, null);
      }
      return complete(input);
    })) {
      Path binary = Path.of("../rust-quinn/target/release/pipestream-quinn").toAbsolutePath().normalize();
      assertTrue(Files.isExecutable(binary), "Build the Rust public scenario client before running interop tests");
      Path log = directory.resolve("rust-producer.log");
      Process process = new ProcessBuilder(binary.toString(), "sealed-scenario", "--connect", "127.0.0.1:" + fixture.server.address().getPort(),
          "--ca", certs.resolve("ca.crt").toString(), "--session-id", "rust-to-java")
          .redirectErrorStream(true).redirectOutput(log.toFile()).start();
      try {
        assertTrue(process.waitFor(40, TimeUnit.SECONDS), () -> "Rust scenario timed out: " + log);
        assertEquals(0, process.exitValue(), () -> { try { return Files.readString(log); } catch (Exception failure) { return failure.toString(); } });
        assertTrue(Files.readString(log).contains("SEALED_OK rust-to-java")); assertEquals(9, calls.get());
      } finally { if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
    }
  }

  @Test void publicProducerCompletesMultipleRootsNestedScopesAndOutOfOrderChunks() throws Exception {
    Path certs = certificates(); var calls = new AtomicInteger();
    try (var fixture = new Fixture(certs, (context, input) -> {
      calls.incrementAndGet();
      if (context.operation() == SealedExecutor.Operation.PROCESS && "dehydrate".equals(context.header().metadata().get("action"))) {
        input.transferTo(java.io.OutputStream.nullOutputStream()); return new SealedExecutor.Decision(6, null);
      }
      return complete(input);
    }); var client = connect(fixture.server, certs)) {
      client.declare(declaration(0, null, 0, List.of(1L, 2L), null));
      assertEquals(6, send(client, ROOT, null, "dehydrate", "root").getLast().state());
      send(client, new SealedWork.EntityKey(0, 2), null, "complete", "other-root");
      var branch = new SealedWork.EntityKey(7, 10);
      client.declare(declaration(7, ROOT, 0, List.of(10L, 20L), List.of(10L, 20L)));
      send(client, branch, ROOT, "dehydrate", "branch");
      send(client, new SealedWork.EntityKey(7, 20), ROOT, "complete", "sibling");
      assertFalse(client.barrier(7).released());
      client.declare(declaration(9, branch, 0, List.of(1L, 2L), List.of(1L, 2L)));
      send(client, new SealedWork.EntityKey(9, 1), branch, "complete", "leaf");
      var chunked = new SealedWork.EntityKey(9, 2);
      var first = chunk(chunked, branch, 0, 0, "abc");
      var second = chunk(chunked, branch, 1, 3, "def");
      assertEquals(3, client.sendChunks(List.of(second, first)).getLast().state());
      assertEquals(BigInteger.TWO, client.closeScope(9).succeeded());
      assertTrue(client.barrier(9).released());
      assertEquals(BigInteger.TWO, client.closeScope(7).succeeded());
      client.declare(declaration(0, null, 1, List.of(), List.of(1L, 2L)));
      var cut = checkpoint("root", SealedCbor.MAX_UINT, 2, null, 5000);
      assertEquals(cut.acknowledgement(), client.checkpoint(cut));
      client.goaway(2); assertEquals(8, calls.get());
    }
  }

  @Test void durableProducerRestoresNestedOutcomesAndRequiresAFreshRootCheckpointAfterRestart() throws Exception {
    Path certs = certificates(); var calls = new AtomicInteger();
    var durable = SealedClient.Durability.at(directory.resolve("producer.db"));
    SealedExecutor.Processor processor = (context, input) -> {
      calls.incrementAndGet();
      if (context.operation() == SealedExecutor.Operation.PROCESS && "dehydrate".equals(context.header().metadata().get("action"))) {
        input.transferTo(java.io.OutputStream.nullOutputStream()); return new SealedExecutor.Decision(6, null);
      }
      return complete(input);
    };
    var cut = checkpoint("durable-root", SealedCbor.MAX_UINT, 2, null, 5000);
    try (var fixture = new Fixture(certs, processor); var client = durable(fixture.server, certs, durable)) {
      client.declare(declaration(0, null, 0, List.of(1L, 2L), null));
      send(client, ROOT, null, "dehydrate", "root");
      send(client, new SealedWork.EntityKey(0, 2), null, "complete", "second-root");
      client.declare(declaration(7, ROOT, 0, List.of(1L, 2L), List.of(1L, 2L)));
      send(client, new SealedWork.EntityKey(7, 1), ROOT, "complete", "child");
      var chunked = new SealedWork.EntityKey(7, 2);
      client.sendChunks(List.of(chunk(chunked, ROOT, 1, 3, "def"), chunk(chunked, ROOT, 0, 0, "abc")));
      client.closeScope(7);
      client.declare(declaration(0, null, 1, List.of(), List.of(1L, 2L)));
      assertEquals(cut.acknowledgement(), client.checkpoint(cut));
      assertTrue(client.unresolvedInputs().isEmpty()); assertTrue(client.scopesAwaitingClosure().isEmpty());
    }
    assertEquals(5, calls.get());
    try (var fixture = new Fixture(certs, processor)) {
      try (var client = durable(fixture.server, certs, durable)) {
        assertEquals(3, client.observedStatus(ROOT).orElseThrow().state());
        assertEquals(3, client.observedStatus(new SealedWork.EntityKey(7, 2)).orElseThrow().state());
        assertTrue(client.declarationsAwaitingAcknowledgement().isEmpty());
        assertTrue(client.checkpointsAwaitingAcknowledgement().isEmpty());
        assertEquals(5, assertThrows(ProtocolException.class, () -> client.goaway(2)).errorCode());
      }
      try (var client = durable(fixture.server, certs, durable)) {
        assertEquals(cut.acknowledgement(), client.checkpoint(cut)); client.goaway(2);
      }
      assertTrue(assertThrows(java.io.IOException.class, () -> durable(fixture.server, certs, durable)).getMessage().contains("acknowledged shutdown"));
    }
    assertEquals(5, calls.get(), "reconnection must not execute the old payloads again");
  }

  @Test void durableCheckpointIntentSurvivesTimeoutAndALaterSeal() throws Exception {
    Path certs = certificates(); var calls = new AtomicInteger();
    var durable = SealedClient.Durability.at(directory.resolve("producer.db"));
    var cut = checkpoint("before-seal", BigInteger.ONE.shiftLeft(63), 1, null, 100);
    try (var fixture = new Fixture(certs, (context, input) -> { calls.incrementAndGet(); return complete(input); })) {
      try (var client = durable(fixture.server, certs, durable)) {
        client.declare(declaration(0, null, 0, List.of(1L), null)); send(client, ROOT, null, "complete", "input");
        assertEquals(14, assertThrows(ProtocolException.class, () -> client.checkpoint(cut)).errorCode());
        assertEquals(List.of(cut), client.checkpointsAwaitingAcknowledgement());
      }
      try (var client = durable(fixture.server, certs, durable)) {
        var changed = checkpoint("before-seal", cut.sequence(), 1, 0L, 100);
        assertEquals(List.of(cut), client.checkpointsAwaitingAcknowledgement());
        assertEquals(5, assertThrows(ProtocolException.class, () -> client.checkpoint(changed)).errorCode());
      }
      try (var client = durable(fixture.server, certs, durable)) {
        client.declare(declaration(0, null, 1, List.of(), List.of(1L)));
        assertEquals(cut.acknowledgement(), client.checkpoint(cut));
        assertTrue(client.checkpointsAwaitingAcknowledgement().isEmpty());
      }
      // The ACK is stored in the earlier intent row, before the later seal row.
      try (var client = durable(fixture.server, certs, durable)) {
        assertEquals(cut.acknowledgement(), client.checkpoint(cut)); client.goaway(1);
      }
    }
    assertEquals(1, calls.get());
  }

  @Test void durableUncertainInputIsVisibleAndNeverBlindlyResent() throws Exception {
    Path certs = certificates(), file = directory.resolve("body.bin"); Files.writeString(file, "input");
    var durable = SealedClient.Durability.at(directory.resolve("producer.db"));
    var calls = new AtomicInteger(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
    var header = new SealedTransport.Header(ROOT, null, 0, "text/plain", null, null, Map.of(), null);
    try (var fixture = new Fixture(certs, (context, input) -> {
      calls.incrementAndGet();
      if (context.identity().entity().equals(ROOT)) {
        assertEquals(BigInteger.valueOf(5), context.header().payloadLength()); assertNotNull(context.header().checksum());
        entered.countDown(); if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("test callback not released");
      }
      return complete(input);
    })) {
      try {
        try (var client = SealedClient.connectDurable(fixture.server.address(), certs.resolve("ca.crt"), "localhost",
            SealedTransport.Limits.defaults(), Duration.ofMillis(1500), durable)) {
          client.declare(declaration(0, null, 0, List.of(1L, 2L), List.of(1L, 2L)));
          assertThrows(java.util.concurrent.TimeoutException.class, () -> client.send(header, file));
          assertEquals(0, entered.getCount()); assertEquals(List.of(ROOT), client.unresolvedInputs());
          assertEquals(2, client.observedStatus(ROOT).orElseThrow().state());
        }
        release.countDown();
        try (var client = durable(fixture.server, certs, durable)) {
          assertEquals(List.of(ROOT), client.unresolvedInputs());
          send(client, new SealedWork.EntityKey(0, 2), null, "complete", "independent");
          assertEquals(5, assertThrows(ProtocolException.class, () -> client.send(header, file)).errorCode());
        }
        try (var client = durable(fixture.server, certs, durable)) {
          assertEquals(List.of(ROOT), client.unresolvedInputs());
          assertEquals(2, client.observedStatus(ROOT).orElseThrow().state());
          assertEquals(3, client.observedStatus(new SealedWork.EntityKey(0, 2)).orElseThrow().state());
        }
      } finally { release.countDown(); }
    }
    assertEquals(2, calls.get());
  }

  @Test void durableCapacityRefusalPrecedesPayloadAdmissionAndPeerBindingCannotChange() throws Exception {
    Path certs = certificates(); var calls = new AtomicInteger();
    var durable = new SealedClient.Durability(directory.resolve("producer.db"), 1, 8192, SealedSessionStore.FileLimits.defaults());
    try (var fixture = new Fixture(certs, (context, input) -> { calls.incrementAndGet(); return complete(input); })) {
      try (var client = durable(fixture.server, certs, durable)) {
        client.declare(declaration(0, null, 0, List.of(1L), List.of(1L)));
        assertEquals(6, assertThrows(ProtocolException.class, () -> send(client, ROOT, null, "complete", "input")).errorCode());
        assertTrue(client.unresolvedInputs().isEmpty()); assertTrue(client.observedStatus(ROOT).isEmpty());
      }
      Path changed = directory.resolve("same-ca-different-bytes.pem"); Files.writeString(changed, Files.readString(certs.resolve("ca.crt")) + "\n");
      assertThrows(java.sql.SQLException.class, () -> SealedClient.connectDurable(fixture.server.address(), changed, "localhost",
          SealedTransport.Limits.defaults(), Duration.ofSeconds(5), durable));
      try (var client = durable(fixture.server, certs, durable)) { assertTrue(client.declarationsAwaitingAcknowledgement().isEmpty()); }
    }
    assertEquals(0, calls.get());
  }

  @Test void sealedClientsRejectWrongCertificateNamesBeforeAttaching() throws Exception {
    Path certs = certificates();
    try (var fixture = new Fixture(certs, (context, input) -> complete(input))) {
      for (boolean durable : List.of(false, true)) {
        var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> {
          try (var client = durable
              ? SealedClient.connectDurable(fixture.server.address(), certs.resolve("ca.crt"), "not-localhost.example",
                  SealedTransport.Limits.defaults(), Duration.ofSeconds(3), SealedClient.Durability.at(directory.resolve("wrong-peer.db")))
              : SealedClient.connect(fixture.server.address(), certs.resolve("ca.crt"), "not-localhost.example",
                  SealedTransport.Limits.defaults(), Duration.ofSeconds(3))) {
            assertNotNull(client.limits());
          }
        }, "a CA-valid certificate for another DNS name must be rejected");
        assertInstanceOf(javax.net.ssl.SSLPeerUnverifiedException.class, failure.getCause());
      }
    }
  }

  @Test void layerZeroRejectsWrongPeerBeforeSendingCapabilities() throws Exception {
    Path certs = certificates(), body = directory.resolve("body.bin"); Files.writeString(body, "input");
    var frames = new AtomicInteger();
    try (var server = new SealedTestPeer.ScriptServer(certs, frame -> {
      frames.incrementAndGet(); return List.of(Wire.encodeCapabilities(Wire.Capabilities.defaults()));
    })) {
      var failure = assertThrows(java.util.concurrent.ExecutionException.class, () ->
          PipeStreamClient.send(server.address(), certs.resolve("ca.crt"), "wrong.example", 1, body, "text/plain"));
      assertInstanceOf(javax.net.ssl.SSLPeerUnverifiedException.class, failure.getCause());
      assertEquals(0, frames.get());
    }
  }

  @Test void sealedClientDoesNotFallBackToCertificateCommonName() throws Exception {
    Path certs = certificates();
    Files.writeString(certs.resolve("extensions"), "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\n");
    command(certs, "openssl", "x509", "-req", "-in", "server.csr", "-CA", "ca.crt", "-CAkey", "ca.key", "-CAcreateserial",
        "-out", "server.crt", "-days", "2", "-extfile", "extensions");
    try (var fixture = new Fixture(certs, (context, input) -> complete(input))) {
      var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> connect(fixture.server, certs));
      assertInstanceOf(javax.net.ssl.SSLPeerUnverifiedException.class, failure.getCause());
    }
  }

  @Test void sealedClientChecksDnsWildcardsAndIpSubjectAlternativeNamesOverQuic() throws Exception {
    Path certs = certificates();
    Files.writeString(certs.resolve("extensions"), "subjectAltName=DNS:*.example.com,IP:127.0.0.1,IP:::1,DNS:127.0.0.2\n"
        + "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\n");
    command(certs, "openssl", "x509", "-req", "-in", "server.csr", "-CA", "ca.crt", "-CAkey", "ca.key", "-CAcreateserial",
        "-out", "server.crt", "-days", "2", "-extfile", "extensions");
    try (var fixture = new Fixture(certs, (context, input) -> complete(input))) {
      for (String name : List.of("node.example.com", "127.0.0.1", "::1")) {
        try (var client = SealedClient.connect(fixture.server.address(), certs.resolve("ca.crt"), name,
            SealedTransport.Limits.defaults(), Duration.ofSeconds(3))) { assertNotNull(client.limits()); }
      }
      for (String name : List.of("example.com", "two.node.example.com", "127.0.0.2", "::2", "localhost")) {
        var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> {
          try (var client = SealedClient.connect(fixture.server.address(), certs.resolve("ca.crt"), name,
              SealedTransport.Limits.defaults(), Duration.ofSeconds(3))) { assertNotNull(client.limits()); }
        }, name);
        assertInstanceOf(javax.net.ssl.SSLPeerUnverifiedException.class, failure.getCause(), name);
      }
    }
  }

  @Test void durableScopeWaitRestoresRehydratingStateAndReplaysClosure() throws Exception {
    Path certs = certificates(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
    var durable = SealedClient.Durability.at(directory.resolve("producer.db")); var calls = new AtomicInteger();
    try (var fixture = new Fixture(certs, (context, input) -> {
      calls.incrementAndGet();
      if (context.operation() == SealedExecutor.Operation.PROCESS && context.identity().entity().equals(ROOT)) {
        input.transferTo(java.io.OutputStream.nullOutputStream()); return new SealedExecutor.Decision(6, null);
      }
      if (context.operation() == SealedExecutor.Operation.REHYDRATE) {
        entered.countDown(); if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("test rehydration not released");
      }
      return complete(input);
    })) {
      try {
        try (var client = SealedClient.connectDurable(fixture.server.address(), certs.resolve("ca.crt"), "localhost",
            SealedTransport.Limits.defaults(), Duration.ofSeconds(1), durable)) {
          client.declare(declaration(0, null, 0, List.of(1L), List.of(1L)));
          send(client, ROOT, null, "dehydrate", "root");
          client.declare(declaration(7, ROOT, 0, List.of(1L), List.of(1L)));
          send(client, new SealedWork.EntityKey(7, 1), ROOT, "complete", "child");
          assertThrows(java.util.concurrent.TimeoutException.class, () -> client.closeScope(7));
          assertEquals(0, entered.getCount()); assertEquals(7, client.observedStatus(ROOT).orElseThrow().state());
        }
        try (var client = SealedClient.connectDurable(fixture.server.address(), certs.resolve("ca.crt"), "localhost",
            SealedTransport.Limits.defaults(), Duration.ofSeconds(1), durable)) {
          assertEquals(List.of(7L), client.scopesAwaitingClosure());
          assertEquals(7, client.observedStatus(ROOT).orElseThrow().state());
          assertThrows(java.util.concurrent.TimeoutException.class, () -> client.closeScope(7));
        }
        release.countDown();
        try (var client = durable(fixture.server, certs, durable)) {
          assertEquals(7, client.observedStatus(ROOT).orElseThrow().state());
          assertEquals(BigInteger.ONE, client.closeScope(7).succeeded());
          assertTrue(client.scopesAwaitingClosure().isEmpty());
          client.checkpoint(checkpoint("rehydrated", BigInteger.ONE, 1, null, 5000)); client.goaway(1);
        }
      } finally { release.countDown(); }
    }
    assertEquals(3, calls.get());
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"declared", "processed", "closed"})
  void durableProducerRecoversAfterAbruptProcessExit(String phase) throws Exception {
    Path certs = certificates(), body = directory.resolve("body.bin"); Files.writeString(body, "input");
    var durable = SealedClient.Durability.at(directory.resolve("producer.db")); var calls = new AtomicInteger();
    try (var fixture = new Fixture(certs, (context, input) -> {
      calls.incrementAndGet();
      if (context.operation() == SealedExecutor.Operation.PROCESS && "dehydrate".equals(context.header().metadata().get("action"))) {
        input.transferTo(java.io.OutputStream.nullOutputStream()); return new SealedExecutor.Decision(6, null);
      }
      return complete(input);
    })) {
      Path log = directory.resolve("producer-process.log");
      Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
          "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"), DurableProbe.class.getName(),
          Integer.toString(fixture.server.address().getPort()), certs.resolve("ca.crt").toString(), durable.database().toString(), body.toString(), phase)
          .redirectErrorStream(true).redirectOutput(log.toFile()).start();
      try {
        assertTrue(process.waitFor(30, TimeUnit.SECONDS), () -> "producer process timed out: " + log);
        assertEquals(73, process.exitValue(), () -> { try { return Files.readString(log); } catch (Exception failure) { return failure.toString(); } });
      } finally { if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
      try (var client = durable(fixture.server, certs, durable)) {
        assertTrue(client.declarationsAwaitingAcknowledgement().isEmpty());
        if (phase.equals("declared")) DurableProbe.process(client, body);
        if (!phase.equals("closed")) client.closeScope(7);
        assertEquals(3, client.observedStatus(ROOT).orElseThrow().state());
        var cut = checkpoint("process-exit", SealedCbor.MAX_UINT, 2, null, 5000);
        assertEquals(cut.acknowledgement(), client.checkpoint(cut)); client.goaway(2);
      }
    }
    assertEquals(5, calls.get(), "reconnect must not re-execute previously observed work");
  }

  public static final class DurableProbe {
    private DurableProbe() {}
    public static void main(String[] args) throws Exception {
      var durable = SealedClient.Durability.at(Path.of(args[2]));
      try (var client = SealedClient.connectDurable(new InetSocketAddress("127.0.0.1", Integer.parseInt(args[0])), Path.of(args[1]), "localhost",
          SealedTransport.Limits.defaults(), Duration.ofSeconds(10), durable)) {
        client.declare(declaration(0, null, 0, List.of(1L, 2L), List.of(1L, 2L)));
        if (!args[4].equals("declared")) process(client, Path.of(args[3]));
        if (args[4].equals("closed")) {
          client.closeScope(7); client.checkpoint(checkpoint("process-exit", SealedCbor.MAX_UINT, 2, null, 5000));
        }
        Runtime.getRuntime().halt(73);
      }
    }
    private static void process(SealedClient client, Path body) throws Exception {
      for (long id : List.of(1L, 2L)) {
        client.send(new SealedTransport.Header(new SealedWork.EntityKey(0, id), null, 0, "text/plain", null, null,
            Map.of("action", id == 1 ? "dehydrate" : "complete"), null), body);
      }
      client.declare(declaration(7, ROOT, 0, List.of(1L, 2L), List.of(1L, 2L)));
      for (long id : List.of(1L, 2L)) client.send(new SealedTransport.Header(new SealedWork.EntityKey(7, id), ROOT, 0, "text/plain", null, null, Map.of(), null), body);
    }
  }

  @Test void pendingCheckpointDoesNotBlockPayloadsOrIndependentCompletionAndCannotOvertakeStatuses() throws Exception {
    Path certs = certificates(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
    try (var fixture = new Fixture(certs, (context, input) -> {
      if (context.identity().entity().equals(ROOT)) { entered.countDown(); if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("test callback not released"); }
      return complete(input);
    }); var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      try {
        declare(raw, declaration(0, null, 0, List.of(1L, 2L), List.of(1L, 2L)));
        var cut = checkpoint("pending", BigInteger.ONE, 2, 0L, 5000);
        raw.send(SealedTransport.checkpoint(cut));
        payload(raw, ROOT, null, "first", true);
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        payload(raw, new SealedWork.EntityKey(0, 2), null, "second", true);
        int processing = 0;
        while (true) {
          var frame = raw.response(); assertEquals(Wire.FRAME_STATUS, frame.type());
          var status = SealedTransport.status(frame.payload());
          if (status.state() == 2) processing++;
          if (status.state() == 3) { assertEquals(2, status.entityId()); break; }
        }
        assertEquals(2, processing);
        release.countDown();
        assertEquals(3, SealedTransport.status(raw.response().payload()).state());
        var response = raw.response(); assertEquals(Wire.FRAME_CHECKPOINT, response.type());
        assertEquals(cut.acknowledgement(), SealedTransport.checkpoint(response.payload()));
      } finally { release.countDown(); }
    }
  }

  @Test void reclaimedInputStaysPendingUntilMatchingPayloadReturnsOverQuic() throws Exception {
    Path certs = certificates(), payloadRoot = directory.resolve("payloads");
    var sessions = SealedSessionStore.open(directory.resolve("sessions.sqlite3"));
    var root = declaration(0, null, 0, List.of(1L), List.of(1L));
    sessions.declare(root, 7, 1024);
    byte[] bytes = "retained input".getBytes(StandardCharsets.UTF_8);
    var header = new SealedTransport.Header(ROOT, null, 0, "text/plain", BigInteger.valueOf(bytes.length),
        SealedWork.sha256().digest(bytes), Map.of(), null);
    try (var payloads = SealedPayloadStore.open(payloadRoot, SealedPayloadStore.Limits.defaults())) {
      payloads.bind(sessions);
      try (var receiver = payloads.begin(new SealedPayloadStore.Identity(SESSION, PRODUCER, ROOT), header)) {
        receiver.write(bytes, 0, bytes.length);
        try (var receipt = receiver.finish()) { payloads.install(List.of(receipt)); }
      }
    }
    assertEquals(1, SealedPayloadStore.reconcile(payloadRoot, SealedPayloadStore.Limits.defaults(), sessions).payloadsReclaimed());
    var calls = new AtomicInteger();
    try (var fixture = new Fixture(certs, (context, input) -> { calls.incrementAndGet(); return complete(input); })) {
      var cut = checkpoint("reclaimed", BigInteger.ONE, 1, null, 150);
      try (var missing = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
        declare(missing, root); missing.send(SealedTransport.checkpoint(cut));
        assertEquals(14L, missing.closeCode.get(2, TimeUnit.SECONDS));
      }
      try (var changed = connect(fixture.server, certs)) {
        changed.declare(root);
        byte[] replacement = bytes.clone(); replacement[0] ^= 1;
        Path file = directory.resolve("changed.bin"); Files.write(file, replacement);
        var badHeader = new SealedTransport.Header(ROOT, null, 0, "text/plain", BigInteger.valueOf(bytes.length),
            SealedWork.sha256().digest(replacement), Map.of(), null);
        assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, () -> changed.send(badHeader, file)).errorCode());
      }
      assertEquals(0, calls.get()); assertEquals(0, fixture.sessions.jobUsage().processingJobs());
      assertFalse(fixture.sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
      try (var client = connect(fixture.server, certs)) {
        client.declare(root); Path file = directory.resolve("original.bin"); Files.write(file, bytes);
        assertEquals(3, client.send(header, file).getLast().state());
        assertEquals(cut.acknowledgement(), client.checkpoint(cut)); client.goaway(1);
      }
      assertEquals(1, calls.get());
    }
    assertEquals(1, SealedPayloadStore.reconcile(payloadRoot, SealedPayloadStore.Limits.defaults(), sessions).admittedPayloads());
  }

  @Test void heldSqliteCannotStopCheckpointDeadlineOrImmediateProtocolRefusal() throws Exception {
    Path certs = certificates();
    try (var fixture = new Fixture(certs, (context, input) -> complete(input));
        var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs);
        var second = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      var root = declaration(0, null, 0, List.of(1L), List.of(1L)); declare(raw, root); declare(second, root);
      try (var sql = DriverManager.getConnection("jdbc:sqlite:" + fixture.database); var statement = sql.createStatement()) {
        statement.execute("BEGIN IMMEDIATE");
        try {
          long start = System.nanoTime();
          raw.send(SealedTransport.checkpoint(checkpoint("deadline", BigInteger.ZERO, 1, null, 150)));
          assertEquals(14L, raw.closeCode.get(2, TimeUnit.SECONDS));
          assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2));
          second.send(SealedTransport.checkpoint(checkpoint("queued", BigInteger.ZERO, 1, null, 5000)));
          second.send(SealedTransport.capabilities(SealedTransport.Limits.defaults()));
          assertEquals(Wire.ERROR_FRAME, second.closeCode.get(2, TimeUnit.SECONDS));
        } finally { statement.execute("ROLLBACK"); }
      }
      assertFalse(fixture.sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
    }
  }

  @Test void reservedRehydrationCommitsOverQuicWhileOrdinaryProcessingQueueIsFull() throws Exception {
    Path certs = certificates(); var blocked = new CountDownLatch(2); var release = new CountDownLatch(1);
    try (var fixture = new Fixture(certs, (context, input) -> {
      if (context.operation() == SealedExecutor.Operation.PROCESS && context.identity().entity().scopeId() == 0) {
        if (context.identity().entity().equals(ROOT)) {
          input.transferTo(java.io.OutputStream.nullOutputStream()); return new SealedExecutor.Decision(6, null);
        }
        blocked.countDown();
        if (!release.await(20, TimeUnit.SECONDS)) throw new IllegalStateException("test callback not released");
      }
      return complete(input);
    }); var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      try {
        var roots = java.util.stream.LongStream.rangeClosed(1, 33).boxed().toList();
        declare(raw, declaration(0, null, 0, roots, roots));
        payload(raw, ROOT, null, "parent", true);
        assertEquals(2, SealedTransport.status(raw.response().payload()).state());
        assertEquals(6, SealedTransport.status(raw.response().payload()).state());
        declare(raw, declaration(7, ROOT, 0, List.of(1L), List.of(1L)));
        payload(raw, new SealedWork.EntityKey(7, 1), ROOT, "child", true);
        assertEquals(2, SealedTransport.status(raw.response().payload()).state());
        assertEquals(3, SealedTransport.status(raw.response().payload()).state());
        for (int id = 2; id <= 33; id++) {
          payload(raw, new SealedWork.EntityKey(0, id), null, "queued", true);
          var response = raw.response(); assertEquals(Wire.FRAME_STATUS, response.type());
          var status = SealedTransport.status(response.payload()); assertEquals(2, status.state()); assertEquals(id, status.entityId());
        }
        assertTrue(blocked.await(5, TimeUnit.SECONDS));
        assertEquals(32, fixture.sessions.jobUsage().processingJobs());
        var summary = SealedScope.summarize(7, List.of(new SealedScope.Terminal(1, 3)));
        raw.send(SealedScope.encode(summary));
        var closure = raw.response(); assertEquals(SealedScope.FRAME, closure.type());
        assertEquals(summary, SealedScope.decode(Wire.encodeControl(closure.type(), closure.payload())));
        assertEquals(7, SealedTransport.status(raw.response().payload()).state());
        assertEquals(1, fixture.sessions.jobUsage().rehydrationJobs());
        assertEquals(0, fixture.sessions.jobUsage().waitingParents());
        release.countDown();
        for (int completed = 0; completed < 33; completed++) {
          var status = SealedTransport.status(raw.response().payload()); assertEquals(3, status.state());
        }
        var cut = checkpoint("reserved", BigInteger.ONE, 33, null, 5000);
        raw.send(SealedTransport.checkpoint(cut));
        assertEquals(cut.acknowledgement(), SealedTransport.checkpoint(raw.response().payload()));
        assertEquals(0, fixture.sessions.jobUsage().processingJobs());
        assertEquals(0, fixture.sessions.jobUsage().rehydrationJobs());
      } finally { release.countDown(); }
    }
  }

  @Test void walCapacityRefusesOverQuicWithoutLosingDeclaredWorkAndReplayResumesAfterCheckpoint() throws Exception {
    Path certs = certificates(), database = directory.resolve("sessions.sqlite3");
    var policy = new SealedSessionStore.FileLimits(4L << 20, 65536, 2L << 20, 65536);
    SealedSessionStore.open(database, policy);
    var accepted = new java.util.ArrayList<SealedWork.Declaration>();
    var root = declaration(0, null, 0, List.of(1L), null);
    accepted.add(root);
    SealedWork.Declaration refused = null;
    try (var fixture = new Fixture(certs, (context, input) -> complete(input))) {
      var files = SealedSqliteFiles.open(database, policy);
      try (var client = connect(fixture.server, certs); var reader = files.connect(); var statement = reader.createStatement()) {
        client.declare(root);
        statement.execute("BEGIN");
        try {
          try (var rows = statement.executeQuery("SELECT count(*) FROM ps_java_entities")) {
            assertTrue(rows.next()); assertEquals(1, rows.getLong(1));
          }
          for (int batch = 1; batch < 128; batch++) {
            long first = (batch - 1) * 8L + 2;
            var request = declaration(0, null, batch, java.util.stream.LongStream.range(first, first + 8).boxed().toList(), null);
            try { assertEquals(request.acknowledgement(), client.declare(request)); accepted.add(request); }
            catch (ProtocolException full) {
              assertEquals(Wire.ERROR_LIMIT_EXCEEDED, full.errorCode()); refused = request; break;
            }
          }
          assertNotNull(refused, "real WAL exhaustion must reach the public QUIC client");
          long committed = accepted.stream().mapToLong(batch -> batch.entityIds().size()).sum();
          assertEquals(committed, fixture.sessions.declared(SESSION, PRODUCER, 0).size());
          assertFalse(fixture.sessions.checkpointReady(SESSION, PRODUCER, 0, committed));
          assertEquals(0, fixture.payloads.usage().retainedFiles());
          assertTrue(fixture.sessions.fileUsage().walBytes() <= policy.walBytes());
          try (var rows = statement.executeQuery("SELECT count(*) FROM ps_java_entities")) {
            assertTrue(rows.next()); assertEquals(1, rows.getLong(1), "held snapshot must not advance");
          }
        } finally { statement.execute("ROLLBACK"); }
        try (var checkpoint = statement.executeQuery("PRAGMA wal_checkpoint(TRUNCATE)")) {
          assertTrue(checkpoint.next()); assertEquals(0, checkpoint.getInt(1));
        }
        try (var rows = statement.executeQuery("SELECT count(*) FROM ps_java_jobs")) {
          assertTrue(rows.next()); assertEquals(0, rows.getLong(1));
        }
      }
      try (var replay = connect(fixture.server, certs)) {
        for (var request : accepted) assertEquals(request.acknowledgement(), replay.declare(request));
        assertEquals(refused.acknowledgement(), replay.declare(refused));
      }
      assertTrue(fixture.server.failure().isEmpty());
    }
    assertEquals(policy, SealedSessionStore.open(database).fileLimits());
    assertEquals(refused.acknowledgement(), SealedSessionStore.open(database).declare(refused, 7, 16384));
  }

  @Test void resetStreamNeverCompletesDeclaredWorkAndCheckpointAckReplaysAfterRestart() throws Exception {
    Path certs = certificates(); var calls = new AtomicInteger();
    SealedExecutor.Processor processor = (context, input) -> { calls.incrementAndGet(); return complete(input); };
    var root = declaration(0, null, 0, List.of(1L), List.of(1L));
    var cut = checkpoint("retained", BigInteger.ONE.shiftLeft(63), 1, 0L, 5000);
    try (var fixture = new Fixture(certs, processor); var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      declare(raw, root);
      var stream = payload(raw, ROOT, null, "unfinished", false);
      // close() sends FIN in Netty; the error-code overload sends RESET_STREAM.
      stream.shutdownOutput((int) Wire.ERROR_ENTITY_INVALID).sync();
      assertEquals(Wire.ERROR_FRAME, raw.closeCode.get(5, TimeUnit.SECONDS));
      assertEquals(0, fixture.sessions.describe(SESSION, PRODUCER, ROOT).state());
      assertFalse(fixture.sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
    }
    assertEquals(0, calls.get());
    try (var fixture = new Fixture(certs, processor); var client = connect(fixture.server, certs)) {
      client.declare(root); send(client, ROOT, null, "complete", "finished");
      assertEquals(cut.acknowledgement(), client.checkpoint(cut));
    }
    try (var fixture = new Fixture(certs, processor); var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      declare(raw, root); raw.send(SealedTransport.checkpoint(cut));
      assertEquals(cut.acknowledgement(), SealedTransport.checkpoint(raw.response().payload()));
      var changed = checkpoint("changed", cut.sequence(), 1, 0L, 5000);
      raw.send(SealedTransport.checkpoint(changed));
      assertEquals(Wire.ERROR_ENTITY_INVALID, raw.closeCode.get(5, TimeUnit.SECONDS));
    }
    assertEquals(1, calls.get());
  }

  @Test void gracefulStreamCloseDeliversFinAndCompletesTheFullPayload() throws Exception {
    Path certs = certificates(); var calls = new AtomicInteger();
    try (var fixture = new Fixture(certs, (context, input) -> { calls.incrementAndGet(); return complete(input); });
        var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      declare(raw, declaration(0, null, 0, List.of(1L), List.of(1L)));
      payload(raw, ROOT, null, "finished", false).close().sync();
      assertEquals(2, SealedTransport.status(raw.response().payload()).state());
      assertEquals(3, SealedTransport.status(raw.response().payload()).state());
      var cut = checkpoint("fin", BigInteger.ONE, 1, 0L, 5000);
      raw.send(SealedTransport.checkpoint(cut));
      assertEquals(cut.acknowledgement(), SealedTransport.checkpoint(raw.response().payload()));
      assertEquals(1, calls.get());
    }
  }

  @Test void forgedScopeSummaryRollsBackClosureAndCorrectReplayPropagatesStrictFailure() throws Exception {
    Path certs = certificates(); var rehydrations = new AtomicInteger();
    try (var fixture = new Fixture(certs, (context, input) -> {
      if (context.operation() == SealedExecutor.Operation.REHYDRATE) rehydrations.incrementAndGet();
      input.transferTo(java.io.OutputStream.nullOutputStream());
      return new SealedExecutor.Decision(context.identity().entity().scopeId() == 0 ? 6 : 4, null);
    })) {
      var root = declaration(0, null, 0, List.of(1L), List.of(1L));
      var child = declaration(7, ROOT, 0, List.of(1L), List.of(1L));
      try (var client = connect(fixture.server, certs)) {
        client.declare(root); send(client, ROOT, null, "dehydrate", "root");
        client.declare(child); assertEquals(4, send(client, new SealedWork.EntityKey(7, 1), ROOT, "failed", "child").getLast().state());
      }
      try (var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
        declare(raw, root);
        var forged = SealedScope.summarize(7, List.of(new SealedScope.Terminal(1, 3)));
        raw.send(SealedScope.encode(forged)); assertEquals(4L, raw.closeCode.get(5, TimeUnit.SECONDS));
        assertFalse(fixture.sessions.ancestry(SESSION, PRODUCER, 7).getFirst().closed());
        assertEquals(6, fixture.sessions.describe(SESSION, PRODUCER, ROOT).state());
      }
      try (var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
        declare(raw, root);
        var correct = SealedScope.summarize(7, List.of(new SealedScope.Terminal(1, 4)));
        raw.send(SealedScope.encode(correct)); assertEquals(SealedScope.FRAME, raw.response().type());
        assertEquals(4, SealedTransport.status(raw.response().payload()).state());
        var cut = checkpoint("strict-failure", BigInteger.ONE, 1, null, 5000);
        raw.send(SealedTransport.checkpoint(cut)); assertEquals(cut.acknowledgement(), SealedTransport.checkpoint(raw.response().payload()));
      }
      assertEquals(0, rehydrations.get());
    }
  }

  @Test void duplicateCheckpointDoesNotExtendDeadlineAndStorageBacklogHasNamedBound() throws Exception {
    Path certs = certificates();
    try (var fixture = new Fixture(certs, (context, input) -> complete(input));
        var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs);
        var flooded = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      var root = declaration(0, null, 0, List.of(1L), List.of(1L)); declare(raw, root); declare(flooded, root);
      try (var sql = DriverManager.getConnection("jdbc:sqlite:" + fixture.database); var statement = sql.createStatement()) {
        statement.execute("BEGIN IMMEDIATE");
        try {
          var request = SealedTransport.checkpoint(checkpoint("duplicate", BigInteger.ONE, 1, null, 500));
          raw.send(request); Thread.sleep(300); raw.send(request);
          // Extending on the duplicate would leave another 500 ms, not this 300 ms budget.
          assertEquals(14L, raw.closeCode.get(300, TimeUnit.MILLISECONDS));
          for (int index = 0; index < 34; index++) flooded.send(SealedWork.encode(root));
          assertEquals(6L, flooded.closeCode.get(2, TimeUnit.SECONDS));
          fixture.server.close(); assertFalse(fixture.server.isTerminated(), "started SQLite operations retain physical ownership");
        } finally { statement.execute("ROLLBACK"); }
      }
    }
  }

  @Test void unobservedDeclarationAndCheckpointAcksReplayAfterServerRestart() throws Exception {
    Path certs = certificates(); var root = declaration(0, null, 0, List.of(1L), List.of(1L));
    var cut = checkpoint("dropped-ack", BigInteger.ONE, 1, null, 5000); var calls = new AtomicInteger();
    SealedExecutor.Processor processor = (context, input) -> { calls.incrementAndGet(); return complete(input); };
    try (var fixture = new Fixture(certs, processor); var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      raw.send(SealedWork.encode(root)); awaitRows(fixture.database, "SELECT count(*) FROM ps_java_batches", 1);
      // No producer ledger observes the response before this connection and listener stop.
    }
    try (var fixture = new Fixture(certs, processor); var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      declare(raw, root); payload(raw, ROOT, null, "retained", true);
      assertEquals(2, SealedTransport.status(raw.response().payload()).state());
      assertEquals(3, SealedTransport.status(raw.response().payload()).state());
      raw.send(SealedTransport.checkpoint(cut)); awaitRows(fixture.database, "SELECT count(*) FROM ps_java_checkpoints WHERE acknowledged=1", 1);
    }
    try (var fixture = new Fixture(certs, processor); var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      declare(raw, root); raw.send(SealedTransport.checkpoint(cut));
      assertEquals(cut.acknowledgement(), SealedTransport.checkpoint(raw.response().payload()));
    }
    assertEquals(1, calls.get());
  }

  private static void awaitRows(Path database, String query, int expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (true) {
      try (var sql = DriverManager.getConnection("jdbc:sqlite:" + database); var statement = sql.createStatement(); var rows = statement.executeQuery(query)) {
        assertTrue(rows.next()); if (rows.getInt(1) == expected) return;
      }
      assertTrue(System.nanoTime() < deadline, query); Thread.sleep(5);
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void realQuicTransfersAndProcesses32MiBUnderA24MiBJavaHeap(boolean durable) throws Exception {
    Path certs = certificates(), log = directory.resolve("heap-gate.log");
    Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(), "-Xmx24m",
        "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"), SealedServerTest.class.getName(),
        directory.toString(), certs.toString(), Boolean.toString(durable)).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(process.waitFor(45, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue(), () -> { try { return Files.readString(log); } catch (Exception failure) { return failure.toString(); } });
    } finally { if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
  }

  public static void main(String[] args) throws Exception {
    var test = new SealedServerTest(); test.directory = Path.of(args[0]); Path certs = Path.of(args[1]);
    Path input = test.directory.resolve("large.bin"); long length = 32L << 20; var hash = SealedWork.sha256();
    try (var output = Files.newOutputStream(input)) {
      byte[] buffer = new byte[8192];
      for (long offset = 0; offset < length; offset += buffer.length) { buffer[0] = (byte) (offset / buffer.length); output.write(buffer); hash.update(buffer); }
    }
    byte[] digest = hash.digest(); boolean durable = Boolean.parseBoolean(args[2]); var calls = new AtomicInteger();
    var journal = SealedClient.Durability.at(test.directory.resolve("producer.db"));
    try (var fixture = test.new Fixture(certs, (context, source) -> { calls.incrementAndGet(); return complete(source); })) {
      try (var client = durable
          ? SealedClient.connectDurable(fixture.server.address(), certs.resolve("ca.crt"), "localhost", SealedTransport.Limits.defaults(), Duration.ofSeconds(30), journal)
          : SealedClient.connect(fixture.server.address(), certs.resolve("ca.crt"), "localhost", SealedTransport.Limits.defaults(), Duration.ofSeconds(30))) {
        client.declare(declaration(0, null, 0, List.of(1L), List.of(1L)));
        var header = new SealedTransport.Header(ROOT, null, 0, null, durable ? null : BigInteger.valueOf(length), durable ? null : digest, Map.of(), null);
        assertEquals(3, client.send(header, input).getLast().state());
        client.checkpoint(checkpoint("heap", BigInteger.ONE, 1, null, 5000));
        if (!durable) client.goaway(1);
      }
      if (durable) {
        try (var client = durable(fixture.server, certs, journal)) {
          assertEquals(3, client.observedStatus(ROOT).orElseThrow().state());
          assertTrue(client.unresolvedInputs().isEmpty());
          client.checkpoint(checkpoint("heap", BigInteger.ONE, 1, null, 5000)); client.goaway(1);
        }
      }
      var stored = fixture.payloads.find(new SealedPayloadStore.Identity(SESSION, PRODUCER, ROOT)).orElseThrow();
      assertEquals(length, stored.length()); assertArrayEquals(digest, stored.digest());
      assertEquals(0, fixture.payloads.usage().temporaryBytes()); assertEquals(0, fixture.payloads.usage().activeHandles());
      assertEquals(1, calls.get());
    }
  }

  private final class Fixture implements AutoCloseable {
    final Path database = directory.resolve("sessions.sqlite3");
    final SealedSessionStore sessions;
    final SealedPayloadStore payloads;
    final SealedServer server;
    Fixture(Path certs, SealedExecutor.Processor processor) throws Exception {
      sessions = SealedSessionStore.open(database);
      payloads = SealedPayloadStore.open(directory.resolve("payloads"), SealedPayloadStore.Limits.defaults());
      try {
        server = SealedServer.start(new InetSocketAddress("127.0.0.1", 0), certs.resolve("server.crt"), certs.resolve("server.key"),
            sessions, payloads, processor, SealedTransport.Limits.defaults(), new SealedExecutor.Limits(4, 2, Duration.ofSeconds(30)));
      } catch (Exception failure) { payloads.close(); throw failure; }
    }
    @Override public void close() throws java.io.IOException {
      server.close(); long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (!server.isTerminated() && System.nanoTime() < deadline) {
        try { Thread.sleep(10); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new java.io.IOException("interrupted during test shutdown", interrupted); }
      }
      assertTrue(server.isTerminated(), "physical server work must stop before closing payload storage"); payloads.close();
    }
  }

  private static SealedExecutor.Decision complete(InputStream input) throws Exception {
    var hash = SealedWork.sha256(); byte[] buffer = new byte[8192]; int count;
    while ((count = input.read(buffer)) >= 0) hash.update(buffer, 0, count);
    return new SealedExecutor.Decision(3, hash.digest());
  }
  private static SealedClient connect(SealedServer server, Path certs) throws Exception {
    return SealedClient.connect(server.address(), certs.resolve("ca.crt"), "localhost", SealedTransport.Limits.defaults(), Duration.ofSeconds(10));
  }
  private static SealedClient durable(SealedServer server, Path certs, SealedClient.Durability durability) throws Exception {
    return SealedClient.connectDurable(server.address(), certs.resolve("ca.crt"), "localhost", SealedTransport.Limits.defaults(), Duration.ofSeconds(10), durability);
  }
  private static SealedWork.Declaration declaration(long scope, SealedWork.EntityKey parent, long sequence, List<Long> ids, List<Long> whole) throws Exception {
    return new SealedWork.Declaration(SESSION, PRODUCER, scope, parent, BigInteger.valueOf(sequence), ids,
        whole == null ? 0 : SealedWork.SEAL, whole == null ? null : SealedWork.sealDigest(SESSION, PRODUCER, scope, parent, whole));
  }
  private static SealedTransport.Checkpoint checkpoint(String id, BigInteger sequence, long last, Long scope, long timeout) {
    return new SealedTransport.Checkpoint(id, sequence, last, scope, 0, BigInteger.valueOf(timeout));
  }
  private static void declare(SealedTestPeer.RawClient raw, SealedWork.Declaration request) throws Exception {
    raw.send(SealedWork.encode(request)); SealedWork.requireAcknowledgement(request, SealedWork.decodePayload(raw.response().payload()));
  }
  private List<Wire.Status> send(SealedClient client, SealedWork.EntityKey key, SealedWork.EntityKey parent, String action, String text) throws Exception {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8); Path file = Files.createTempFile(directory, "entity", ".bin"); Files.write(file, bytes);
    return client.send(new SealedTransport.Header(key, parent, 0, "text/plain", BigInteger.valueOf(bytes.length), SealedWork.sha256().digest(bytes), Map.of("action", action), null), file);
  }
  private SealedClient.FileChunk chunk(SealedWork.EntityKey key, SealedWork.EntityKey parent, int index, int offset, String text) throws Exception {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8); Path file = Files.createTempFile(directory, "chunk", ".bin"); Files.write(file, bytes);
    return new SealedClient.FileChunk(new SealedTransport.Header(key, parent, 0, "text/plain", BigInteger.valueOf(bytes.length), SealedWork.sha256().digest(bytes), Map.of(),
        new SealedTransport.Chunk(BigInteger.TWO, BigInteger.valueOf(index), BigInteger.valueOf(offset))), file);
  }
  private static QuicStreamChannel payload(SealedTestPeer.RawClient raw, SealedWork.EntityKey key, SealedWork.EntityKey parent, String text, boolean fin) throws Exception {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    var stream = raw.connection.createStream(QuicStreamType.UNIDIRECTIONAL, new ChannelInboundHandlerAdapter()).get(5, TimeUnit.SECONDS);
    var header = new SealedTransport.Header(key, parent, 0, "text/plain", BigInteger.valueOf(bytes.length), SealedWork.sha256().digest(bytes), Map.of(), null);
    stream.writeAndFlush(Unpooled.wrappedBuffer(SealedTransport.header(header))).get(5, TimeUnit.SECONDS);
    stream.writeAndFlush(Unpooled.wrappedBuffer(bytes)).get(5, TimeUnit.SECONDS);
    if (fin) stream.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(QuicStreamChannel.SHUTDOWN_OUTPUT).get(5, TimeUnit.SECONDS);
    return stream;
  }
  private Path certificates() throws Exception {
    Path certs = Files.createDirectory(directory.resolve("certs"));
    command(certs, "openssl", "req", "-x509", "-newkey", "rsa:2048", "-noenc", "-keyout", "ca.key", "-out", "ca.crt",
        "-days", "2", "-subj", "/CN=Java-Listener-Test-CA", "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign");
    command(certs, "openssl", "req", "-new", "-newkey", "rsa:2048", "-noenc", "-keyout", "server.key", "-out", "server.csr", "-subj", "/CN=localhost");
    Files.writeString(certs.resolve("extensions"), "subjectAltName=DNS:localhost\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\n");
    command(certs, "openssl", "x509", "-req", "-in", "server.csr", "-CA", "ca.crt", "-CAkey", "ca.key", "-CAcreateserial",
        "-out", "server.crt", "-days", "2", "-extfile", "extensions"); return certs;
  }
  private static void command(Path directory, String... args) throws Exception {
    Path log = Files.createTempFile(directory, "openssl", ".log");
    Process process = new ProcessBuilder(args).directory(directory.toFile()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try { assertTrue(process.waitFor(10, TimeUnit.SECONDS)); assertEquals(0, process.exitValue(), () -> log.toString()); }
    finally { if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
  }
}
