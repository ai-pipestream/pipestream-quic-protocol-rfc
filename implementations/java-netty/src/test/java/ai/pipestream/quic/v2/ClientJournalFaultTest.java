package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.LauncherInvocation.hex;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.v2.LauncherInvocation.Run;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12.2.1 local journal failures (CR10) with real I/O errors, not process death: the journal
 * runs in rollback-journal mode, so making its directory read-only makes SQLite's next write
 * transaction fail exactly as a full or unwritable disk would. The fault is injected before intent
 * transmission, while the received receipt is being saved, and while a verified output selection is
 * being saved; each is surfaced, never reported as durable success, and resolved by the next
 * invocation once the journal is writable again.
 */
@Timeout(240)
final class ClientJournalFaultTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
    assumeWritablePermissionsMatter();
  }

  /** As root every directory is writable; the fault could not be injected. */
  private static void assumeWritablePermissionsMatter() {
    org.junit.jupiter.api.Assumptions.assumeFalse(
        "root".equals(System.getProperty("user.name")), "journal faults need a non-root user");
  }

  static void writable(Path dir, boolean writable) {
    try {
      Files.setPosixFilePermissions(
          dir, PosixFilePermissions.fromString(writable ? "rwxr-xr-x" : "r-xr-xr-x"));
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }

  /** One observed client boundary with the operation it named, if any. */
  record Event(Boundaries.Boundary boundary, Records.OperationId operation) {}

  /** Records client boundaries and makes the journal directory read-only at one of them. */
  static final class ClientHooks implements Boundaries {
    final ConcurrentLinkedQueue<Event> events = new ConcurrentLinkedQueue<>();
    final Path journalDir;
    volatile Boundary faultAt;
    volatile Records.OperationId faultOperation;

    ClientHooks(Path journalDir) {
      this.journalDir = journalDir;
    }

    private void observe(Boundary boundary, Details details) {
      events.add(new Event(boundary, details.operation()));
      if (boundary == faultAt
          && faultOperation != null
          && faultOperation.equals(details.operation())) writable(journalDir, false);
    }

    boolean saw(Boundary boundary, Records.OperationId operation) {
      return events.stream()
          .anyMatch(e -> e.boundary() == boundary && operation.equals(e.operation()));
    }

    boolean saw(Boundary boundary) {
      return events.stream().anyMatch(e -> e.boundary() == boundary);
    }

    @Override
    public void committed(Boundary boundary, Details details) {
      observe(boundary, details);
    }

    @Override
    public void sent(Boundary boundary, Details details) {
      observe(boundary, details);
    }

    @Override
    public boolean withhold(Boundary boundary) {
      return false;
    }
  }

  /** Counts authority commits so single-effect claims are checked at the authority. */
  static final class ServerHooks implements Boundaries {
    final AtomicInteger declarations = new AtomicInteger();
    final AtomicInteger admissions = new AtomicInteger();

    @Override
    public void committed(Boundary boundary, Details details) {
      if (boundary == Boundary.DECLARATION_COMMITTED) declarations.incrementAndGet();
      if (boundary == Boundary.ADMISSION_COMMITTED) admissions.incrementAndGet();
    }

    @Override
    public void sent(Boundary boundary, Details details) {}

    @Override
    public boolean withhold(Boundary boundary) {
      return false;
    }
  }

  private static Run invoke(
      Path journal, InetSocketAddress address, Boundaries hooks, String... operation) {
    return LauncherInvocation.invoke(pki, "alice", journal, address, hooks, operation);
  }

  @Test
  void journalFaultsAreSurfacedAtEveryPersistencePointAndResolvedByTheNextInvocation()
      throws Exception {
    Path journalDir = Files.createDirectories(directory.resolve("journal-dir"));
    Path journal = LauncherInvocation.initJournal(journalDir.resolve("journal.sqlite"), "alice");
    Path input = directory.resolve("input.bin");
    Files.write(input, DurableServerTest.payload(30_000, 21));
    Path output = directory.resolve("output.bin");
    ServerHooks server = new ServerHooks();
    ClientHooks client = new ClientHooks(journalDir);
    try (DurableHost host =
            DurableHost.initialize(
                directory.resolve("authority"),
                DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
                ReferenceApplications.all(),
                DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true),
                DurableHost.UtcClock.system(true));
        DurableServer listener =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults(),
                server)) {
      InetSocketAddress address = listener.address();
      String[] admit = {
        "admit",
        "--operation",
        hex(2),
        "--declaration",
        hex(1),
        "--work",
        "0:0:1",
        "--input",
        input.toString(),
        "--application",
        "copy/v2"
      };
      try {
        assertNull(
            invoke(journal, address, client, "declare", "--operation", hex(1), "--entities", "1")
                .failure());
        // (a) Before transmission: the journal cannot record the intent, so nothing is sent and
        // the failure is local INTERNAL_ERROR, which no budget retries.
        writable(journalDir, false);
        client.events.clear();
        Run unsent =
            invoke(
                journal,
                address,
                client,
                "admit",
                "--operation",
                hex(2),
                "--declaration",
                hex(1),
                "--work",
                "0:0:1",
                "--input",
                input.toString(),
                "--application",
                "copy/v2",
                "--retry-budget",
                "3");
        assertNotNull(unsent.failure(), unsent.output());
        assertEquals(ProtocolError.Code.INTERNAL_ERROR, unsent.code(), unsent.output());
        assertFalse(unsent.error().fromAuthority(), unsent.error().toString());
        assertTrue(
            unsent.output().contains("UNRESOLVED attempts=1 last=INTERNAL_ERROR not-recoverable"),
            unsent.output());
        // The session attach was sent (it is not a mutation); the admission intent was neither
        // journaled nor transmitted.
        Records.OperationId admission = DurableServerTest.operation(2);
        assertFalse(
            client.saw(Boundaries.Boundary.INTENT_JOURNALED, admission), client.events.toString());
        assertFalse(
            client.saw(Boundaries.Boundary.REQUEST_SENT, admission), client.events.toString());
        assertEquals(0, server.admissions.get(), "nothing reached the authority");
        writable(journalDir, true);
        try (ClientJournal reopened =
            ClientJournal.open(journal, ClientJournal.Limits.defaults())) {
          assertTrue(reopened.operation(DurableServerTest.operation(2)).isEmpty());
        }
        // (b) While saving the received receipt: the intent was journaled and sent, the
        // authority committed, the receipt was validated, but it cannot be saved. No success is
        // reported; the intent stays unresolved; the next invocation resolves the same identity
        // from retained state without a second commit.
        client.events.clear();
        client.faultOperation = DurableServerTest.operation(3);
        client.faultAt = Boundaries.Boundary.INTENT_JOURNALED;
        Run unsaved =
            invoke(journal, address, client, "declare", "--operation", hex(3), "--entities", "2");
        client.faultAt = null;
        assertNotNull(unsaved.failure(), unsaved.output());
        assertEquals(ProtocolError.Code.INTERNAL_ERROR, unsaved.code(), unsaved.output());
        assertFalse(unsaved.error().fromAuthority());
        assertEquals(0, unsaved.count("RECEIPT"), unsaved.output());
        Records.OperationId declaration = DurableServerTest.operation(3);
        String seen = client.events.toString();
        assertTrue(client.saw(Boundaries.Boundary.INTENT_JOURNALED, declaration), seen);
        assertTrue(client.saw(Boundaries.Boundary.REQUEST_SENT, declaration), seen);
        assertTrue(client.saw(Boundaries.Boundary.RECEIPT_VALIDATED), seen);
        assertFalse(client.saw(Boundaries.Boundary.RECEIPT_JOURNALED), seen);
        assertEquals(2, server.declarations.get(), "the authority committed the declaration");
        writable(journalDir, true);
        try (ClientJournal reopened =
            ClientJournal.open(journal, ClientJournal.Limits.defaults())) {
          assertTrue(reopened.receipt(DurableServerTest.operation(3)).isEmpty());
          assertEquals(1, reopened.unresolved(0, 16).size());
        }
        Run replayed = invoke(journal, address, client, "replay", "--operation", hex(3));
        assertNull(replayed.failure(), replayed.output() + replayed.failure());
        assertEquals(1, replayed.count("RECEIPT"), replayed.output());
        // One effect: the scope holds exactly the two declared entities. (The authority reports
        // DECLARATION_COMMITTED for the replay's idempotent transaction as well, so that boundary
        // counts transactions, not effects, for declarations.)
        Run page = invoke(journal, address, client, "page", "--scope", "0");
        assertNull(page.failure(), page.output());
        assertEquals(2, page.output().split("entity=", -1).length - 1, page.output());
        try (ClientJournal reopened =
            ClientJournal.open(journal, ClientJournal.Limits.defaults())) {
          assertTrue(reopened.receipt(DurableServerTest.operation(3)).isPresent());
          assertTrue(reopened.unresolved(0, 16).isEmpty());
        }
        // (c) While saving a verified output selection: the manifest is retained, the selection
        // is validated against it, but cannot be journaled; the read is refused locally until
        // the journal is writable again, and then the verified output installs normally.
        assertNull(invoke(journal, address, client, admit).failure());
        Run terminal =
            invoke(journal, address, client, "watch", "--work", "0:0:1", "--wait-ms", "5000");
        for (int i = 0; i < 20 && !terminal.output().contains("state=5"); i++)
          terminal =
              invoke(journal, address, client, "watch", "--work", "0:0:1", "--wait-ms", "5000");
        assertTrue(terminal.output().contains("state=5"), terminal.output());
        assertNull(
            invoke(journal, address, client, "manifest", "--work", "0:0:1", "--attempt", "1")
                .failure());
        writable(journalDir, false);
        Run unselected =
            invoke(
                journal,
                address,
                client,
                "select",
                "--work",
                "0:0:1",
                "--attempt",
                "1",
                "--index",
                "0");
        assertNotNull(unselected.failure(), unselected.output());
        assertEquals(ProtocolError.Code.INTERNAL_ERROR, unselected.code(), unselected.output());
        assertFalse(unselected.error().fromAuthority());
        writable(journalDir, true);
        assertNull(
            invoke(
                    journal,
                    address,
                    client,
                    "select",
                    "--work",
                    "0:0:1",
                    "--attempt",
                    "1",
                    "--index",
                    "0")
                .failure());
        Run read =
            invoke(
                journal,
                address,
                client,
                "read",
                "--work",
                "0:0:1",
                "--attempt",
                "1",
                "--index",
                "0",
                "--output",
                output.toString());
        assertNull(read.failure(), read.output() + read.failure());
        assertTrue(read.output().contains("VERIFIED"), read.output());
        assertArrayEquals(Files.readAllBytes(input), Files.readAllBytes(output));
        assertEquals(1, server.admissions.get());
      } finally {
        writable(journalDir, true);
      }
    }
  }
}
