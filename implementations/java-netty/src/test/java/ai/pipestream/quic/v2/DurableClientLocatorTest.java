package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12.7: locators grant no access; the client never dereferences a caller-supplied URL,
 * follows it, or sends credentials to the authority it names (S12-280), and takes its endpoint
 * and credentials from configuration, never from an untrusted URI (S12-283). A raw authority
 * serves a manifest whose only output locator names a different endpoint on which the test
 * listens; the client's manifest, selection and read all stay on the configured connection and
 * the named endpoint never receives a packet.
 */
@Timeout(60)
class DurableClientLocatorTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(20_000, 60_000, 120_000);
  static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);

  @BeforeAll
  static void setup() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
  }

  static Records.Manifest manifest(byte[] payload, String endpoint) throws Exception {
    return new Records.Manifest(
        "issuer-a",
        "alice",
        1,
        WORK,
        1,
        DurableServerTest.digest(new byte[] {1}),
        1_000,
        2_000_000_000_000L,
        List.of(
            new Records.Output(
                0,
                payload.length,
                DurableServerTest.digest(payload),
                "application/octet-stream",
                new Locator(
                    "pipestream://"
                        + endpoint
                        + "/v2/sessions/1/scopes/0/producers/0/entities/1/attempts/1/outputs/0"))));
  }

  @Test
  void aLocatorNamingAnotherEndpointIsNeverDereferencedOrSentCredentials() throws Exception {
    byte[] payload = new byte[20_000];
    new Random(7).nextBytes(payload);
    Records.Digest digest = DurableServerTest.digest(payload);
    InetAddress loopback = InetAddress.getByName("127.0.0.1");
    // The endpoint the locator names: a UDP socket (QUIC) and a TCP socket on the same port.
    try (DatagramSocket udp = new DatagramSocket(new InetSocketAddress(loopback, 0));
        ServerSocket tcp = new ServerSocket(udp.getLocalPort(), 1, loopback)) {
      udp.setSoTimeout(1_000);
      tcp.setSoTimeout(1_000);
      String endpoint = "127.0.0.1:" + udp.getLocalPort();
      Records.Manifest served = manifest(payload, endpoint);
      try (RawDurableAuthority authority =
              new RawDurableAuthority(pki.server(principals), served, POLICY);
          ClientJournal journal =
              ClientJournal.initialize(
                  directory.resolve("locator.sqlite"),
                  new ClientJournal.Intent("issuer-a", "alice", 1, POLICY, true),
                  ClientJournal.Limits.defaults());
          DurableClient client =
              DurableClient.connect(
                  authority.address(), pki.client("alice"), journal, ClientOptions.defaults())) {
        authority.script =
            (read, stream) -> {
              RawDurableAuthority.write(
                  stream,
                  RawDurableAuthority.header(
                      new Records.ResultHeader(
                          read.request(), 1, WORK, 1, 0, payload.length, digest)));
              RawDurableAuthority.write(stream, payload);
              RawDurableAuthority.fin(stream);
            };
        DurableClientTest.get(client.ready());
        DurableClientTest.get(client.binding());
        // The manifest is accepted as evidence; the locator is retained, not resolved.
        Records.Manifest observed = DurableClientTest.get(client.manifest(WORK, 1));
        assertEquals(served, observed);
        assertEquals(
            endpoint,
            observed.outputs().get(0).locator().value().substring("pipestream://".length(), 
                "pipestream://".length() + endpoint.length()));
        DurableClientTest.get(client.select(WORK, 1, 0));
        // The read goes to the configured authority, which is the only endpoint that delivers.
        Path out = Files.createDirectories(directory.resolve("locator-out")).resolve("out.bin");
        ResultFiles.Delivered delivered =
            DurableClientTest.get(client.read(WORK, 1, 0, new ResultFiles.Destination(out)));
        assertArrayEquals(payload, Files.readAllBytes(delivered.path()));
      }
      // Nothing reached the endpoint the locator named: no QUIC datagram, no TCP connection.
      DatagramPacket packet = new DatagramPacket(new byte[2048], 2048);
      assertThrows(SocketTimeoutException.class, () -> udp.receive(packet), "datagram received");
      assertThrows(SocketTimeoutException.class, tcp::accept, "TCP connection received");
    }
  }
}
