package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.LauncherInvocation.hex;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.v2.LauncherInvocation.Run;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12.2.1 UNAUTHORIZED and CLOCK_UNSAFE recovery through the launcher (CR06). A temporary
 * withdrawal of the owner's authorization (the deployment's principal policy no longer authorizes
 * the mapped owner, while its credential still maps) denies session-scoped requests until it is
 * restored, and is distinct from durable revocation of the session, which no restoration undoes. An
 * untrusted or regressed authority clock refuses time-issuing operations while evidence reads keep
 * working under current authorization. Neither code is retried by the client's budget.
 */
@Timeout(240)
final class AuthorizationClockRecoveryTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
  }

  /** A deployment clock whose trust and offset the test controls. */
  static final class Clock implements DurableHost.UtcClock {
    volatile boolean trusted = true;
    volatile long behindMillis;

    @Override
    public Sample sample() {
      return new Sample(System.currentTimeMillis() - behindMillis, trusted);
    }
  }

  static final class ServerHooks implements Boundaries {
    final AtomicInteger admissions = new AtomicInteger();

    @Override
    public void committed(Boundary boundary, Details details) {
      if (boundary == Boundary.ADMISSION_COMMITTED) admissions.incrementAndGet();
    }

    @Override
    public void sent(Boundary boundary, Details details) {}

    @Override
    public boolean withhold(Boundary boundary) {
      return false;
    }
  }

  private static Run invoke(Path journal, InetSocketAddress address, String... operation) {
    return LauncherInvocation.invoke(pki, "alice", journal, address, Boundaries.NONE, operation);
  }

  private static String[] admit(int operation, int entity, Path input, String... extra) {
    List<String> args =
        new java.util.ArrayList<>(
            List.of(
                "admit",
                "--operation",
                hex(operation),
                "--declaration",
                hex(1),
                "--work",
                "0:0:" + entity,
                "--input",
                input.toString(),
                "--application",
                "copy/v2"));
    args.addAll(List.of(extra));
    return args.toArray(new String[0]);
  }

  @Test
  void temporaryWithdrawalUnsafeClockAndDurableRevocationAreDistinctAndNeverRetried()
      throws Exception {
    Map<Records.Digest, String> live = new HashMap<>(principals);
    Clock clock = new Clock();
    ServerHooks hooks = new ServerHooks();
    Path journal = LauncherInvocation.initJournal(directory.resolve("journal.sqlite"), "alice");
    Path input = directory.resolve("input.bin");
    Files.write(input, DurableServerTest.payload(20_000, 31));
    Path output = directory.resolve("output.bin");
    try (DurableHost host =
            DurableHost.initialize(
                directory.resolve("authority"),
                DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
                ReferenceApplications.all(),
                DurableHost.OwnerPolicy.fromPrincipals(() -> live, true),
                clock);
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults(),
                hooks)) {
      InetSocketAddress address = server.address();
      assertNull(
          invoke(journal, address, "declare", "--operation", hex(1), "--entities", "1,2,3,4")
              .failure());
      assertNull(invoke(journal, address, admit(2, 1, input)).failure());
      Run terminal = invoke(journal, address, "watch", "--work", "0:0:1", "--wait-ms", "5000");
      for (int i = 0; i < 20 && !terminal.output().contains("state=5"); i++)
        terminal = invoke(journal, address, "watch", "--work", "0:0:1", "--wait-ms", "5000");
      assertTrue(terminal.output().contains("state=5"), terminal.output());
      assertNull(
          invoke(journal, address, "manifest", "--work", "0:0:1", "--attempt", "1").failure());
      assertEquals(1, hooks.admissions.get());

      // (a) Temporary withdrawal: the owner policy stops authorizing alice while her credential
      // still maps. The session-scoped mutation is refused UNAUTHORIZED by the authority; the
      // budget does not retry it; nothing commits; the journaled intent survives.
      live.remove(pki.fingerprint("alice"));
      Run denied = invoke(journal, address, admit(3, 2, input, "--retry-budget", "3"));
      assertNotNull(denied.failure(), denied.output());
      assertEquals(ProtocolError.Code.UNAUTHORIZED, denied.code(), denied.output());
      assertTrue(denied.error().fromAuthority(), denied.error().toString());
      assertEquals(0, denied.count("RECOVERING"), denied.output());
      assertTrue(
          denied.output().contains("UNRESOLVED attempts=1 last=UNAUTHORIZED not-recoverable"),
          denied.output());
      assertEquals(1, hooks.admissions.get(), "a denied admission has no effect");
      // Reconnecting with a cached receipt or another identity grants nothing either.
      Run lookup = invoke(journal, address, "lookup", "--operation", hex(2), "--retry-budget", "2");
      assertNotNull(lookup.failure(), lookup.output());
      assertEquals(ProtocolError.Code.UNAUTHORIZED, lookup.code(), lookup.output());
      assertEquals(0, lookup.count("RECOVERING"), lookup.output());
      // Legitimate restoration: the same journaled identity now commits, once.
      live.put(pki.fingerprint("alice"), "alice");
      Run restored = invoke(journal, address, admit(3, 2, input));
      assertNull(restored.failure(), restored.output() + restored.failure());
      assertEquals(1, restored.count("RECEIPT"), restored.output());
      assertEquals(2, hooks.admissions.get());
      try (ClientJournal reopened = ClientJournal.open(journal, ClientJournal.Limits.defaults())) {
        assertEquals(
            1,
            assertInstanceOf(
                    Records.Admitted.class,
                    reopened.receipt(DurableServerTest.operation(3)).orElseThrow().outcome())
                .attempt());
        assertTrue(reopened.unresolved(0, 16).isEmpty());
      }

      // (b) Unsafe clock: an untrusted deployment clock refuses time-issuing operations with
      // CLOCK_UNSAFE (never retried by the budget), while evidence reads under current
      // authorization keep working; a regressed clock is the same condition. Trust restored,
      // the same journaled admission commits once.
      clock.trusted = false;
      Run unsafe = invoke(journal, address, admit(4, 3, input, "--retry-budget", "3"));
      assertNotNull(unsafe.failure(), unsafe.output());
      assertEquals(ProtocolError.Code.CLOCK_UNSAFE, unsafe.code(), unsafe.output());
      assertTrue(unsafe.error().fromAuthority(), unsafe.error().toString());
      assertEquals(0, unsafe.count("RECOVERING"), unsafe.output());
      assertTrue(unsafe.output().contains("last=CLOCK_UNSAFE not-recoverable"), unsafe.output());
      assertEquals(2, hooks.admissions.get());
      Run read =
          invoke(journal, address, "select", "--work", "0:0:1", "--attempt", "1", "--index", "0");
      assertNull(
          read.failure(), "selection is an evidence read: " + read.output() + read.failure());
      Run delivered =
          invoke(
              journal,
              address,
              "read",
              "--work",
              "0:0:1",
              "--attempt",
              "1",
              "--index",
              "0",
              "--output",
              output.toString());
      // Result delivery evaluates output retention against UTC, so under Section 12.9 the
      // authority refuses it too, without touching the retained output; retained receipts and
      // work views stay readable.
      assertNotNull(delivered.failure(), delivered.output());
      assertEquals(ProtocolError.Code.CLOCK_UNSAFE, delivered.code(), delivered.output());
      assertTrue(delivered.error().fromAuthority());
      assertFalse(Files.exists(output), "no partial or unverified output is left behind");
      assertNull(invoke(journal, address, "lookup", "--operation", hex(2)).failure());
      assertNull(invoke(journal, address, "watch", "--work", "0:0:1").failure());
      assertNull(
          invoke(journal, address, "manifest", "--work", "0:0:1", "--attempt", "1").failure());
      clock.trusted = true;
      clock.behindMillis = 3_600_000;
      Run regressed = invoke(journal, address, admit(4, 3, input));
      assertNotNull(regressed.failure(), regressed.output());
      assertEquals(ProtocolError.Code.CLOCK_UNSAFE, regressed.code(), regressed.output());
      clock.behindMillis = 0;
      Run trusted = invoke(journal, address, admit(4, 3, input));
      assertNull(trusted.failure(), trusted.output() + trusted.failure());
      assertEquals(3, hooks.admissions.get());
      Run reread =
          invoke(
              journal,
              address,
              "read",
              "--work",
              "0:0:1",
              "--attempt",
              "1",
              "--index",
              "0",
              "--output",
              output.toString());
      assertNull(reread.failure(), reread.output() + reread.failure());
      assertArrayEquals(Files.readAllBytes(input), Files.readAllBytes(output));

      // (c) Durable revocation of the session is not a temporary condition: with the owner
      // fully authorized and the clock trusted, every session-scoped request stays UNAUTHORIZED
      // and no restoration applies.
      host.revoke(1);
      Run revoked =
          invoke(journal, address, "lookup", "--operation", hex(2), "--retry-budget", "2");
      assertNotNull(revoked.failure(), revoked.output());
      assertEquals(ProtocolError.Code.UNAUTHORIZED, revoked.code(), revoked.output());
      assertEquals(0, revoked.count("RECOVERING"), revoked.output());
      Run afterRevoke = invoke(journal, address, admit(5, 4, input));
      assertNotNull(afterRevoke.failure(), afterRevoke.output());
      assertEquals(ProtocolError.Code.UNAUTHORIZED, afterRevoke.code(), afterRevoke.output());
      assertEquals(3, hooks.admissions.get());
    }
  }
}
