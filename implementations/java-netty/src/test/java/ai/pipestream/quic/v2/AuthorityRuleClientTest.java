package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * Section 12.7 client obligations observed at a raw authority: a bare locator is not recovery
 * evidence and the client makes no discovery call for it (S12-284), and bytes already delivered
 * are not recalled by a later revocation at the authority (S12-285).
 */
@Timeout(60)
class AuthorityRuleClientTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(30_000, 60_000, 120_000);
  static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);
  static byte[] payload;
  static Records.Digest digest;
  static Records.Manifest manifest;

  @BeforeAll
  static void setup() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
    payload = new byte[40_000];
    new Random(5).nextBytes(payload);
    digest = DurableServerTest.digest(payload);
    manifest =
        new Records.Manifest(
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
                    digest,
                    "application/octet-stream",
                    new Locator(
                        "pipestream://localhost:7443/v2/sessions/1/scopes/0/producers/0"
                            + "/entities/1/attempts/1/outputs/0"))));
  }

  static final class Session implements AutoCloseable {
    final RawDurableAuthority authority;
    final ClientJournal journal;
    final DurableClient client;
    final Path outputs;

    Session(String name) throws Exception {
      authority = new RawDurableAuthority(pki.server(principals), manifest, POLICY);
      journal =
          ClientJournal.initialize(
              directory.resolve(name + ".sqlite"),
              new ClientJournal.Intent("issuer-a", "alice", 1, POLICY, true),
              ClientJournal.Limits.defaults());
      client =
          DurableClient.connect(
              authority.address(), pki.client("alice"), journal, ClientOptions.defaults());
      DurableClientTest.get(client.ready());
      DurableClientTest.get(client.binding());
      outputs = Files.createDirectories(directory.resolve(name + "-outputs"));
    }

    ResultFiles.Destination destination(String file) {
      return new ResultFiles.Destination(outputs.resolve(file));
    }

    long sent(Class<?> type) {
      return authority.received.stream().filter(type::isInstance).count();
    }

    @Override
    public void close() throws java.io.IOException, java.sql.SQLException {
      client.close();
      journal.close();
      authority.close();
    }
  }

  @Test
  void aBareLocatorIsNotRecoveryEvidenceAndTriggersNoDiscoveryCall() throws Exception {
    try (Session session = new Session("bare-locator")) {
      // The client holds the locator string (it is in the manifest it could fetch) but has no
      // saved selection: the read is refused locally and nothing is asked of the authority.
      ProtocolError refused =
          DurableClientTest.refusal(session.client.read(WORK, 1, 0, session.destination("x.bin")));
      assertEquals(ProtocolError.Code.NOT_FOUND, refused.code(), refused.toString());
      assertEquals(0, session.sent(Messages.Read.class), session.authority.received.toString());
      assertEquals(
          0, session.sent(Messages.GetManifest.class), session.authority.received.toString());
      assertTrue(Files.notExists(session.outputs.resolve("x.bin")));
      DurableClientTest.get(session.client.detach());
    }
  }

  @Test
  void deliveredBytesSurviveALaterRevocationAtTheAuthority() throws Exception {
    try (Session session = new Session("revoked-after")) {
      DurableClientTest.get(session.client.manifest(WORK, 1));
      DurableClientTest.get(session.client.select(WORK, 1, 0));
      session.authority.script =
          (read, stream) -> {
            RawDurableAuthority.write(
                stream,
                RawDurableAuthority.header(
                    new Records.ResultHeader(
                        read.request(), 1, WORK, 1, 0, payload.length, digest)));
            RawDurableAuthority.write(stream, payload);
            RawDurableAuthority.fin(stream);
          };
      Path delivered =
          DurableClientTest.get(session.client.read(WORK, 1, 0, session.destination("a.bin")))
              .path();
      assertArrayEquals(payload, Files.readAllBytes(delivered));
      // The authority now answers everything as a revoked session.
      session.authority.controls =
          request ->
              new Messages.Refusal(
                  new Records.RequestTag(false, ClientCorrelation.requestId(request)),
                  ProtocolError.Code.UNAUTHORIZED,
                  "revoked");
      ProtocolError refused =
          DurableClientTest.refusal(session.client.read(WORK, 1, 0, session.destination("b.bin")));
      assertEquals(ProtocolError.Code.UNAUTHORIZED, refused.code(), refused.toString());
      assertArrayEquals(payload, Files.readAllBytes(delivered), "delivered bytes were recalled");
      assertTrue(Files.notExists(session.outputs.resolve("b.bin")));
    }
  }
}
