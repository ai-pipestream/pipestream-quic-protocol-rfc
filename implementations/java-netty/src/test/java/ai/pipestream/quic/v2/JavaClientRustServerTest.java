package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** The Java durable client against the existing Rust authority CLI, including a Rust restart. */
@Tag("sealed-interop")
@Timeout(300)
final class JavaClientRustServerTest {
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
  void javaClientCompletesAgainstRustAuthorityAndRecoversAcrossRustRestart() throws Exception {
    Path state = directory.resolve("rust-authority.sqlite");
    Path objects = directory.resolve("rust-objects");
    Path principals = directory.resolve("rust-principals.tsv");
    pki.principalMap(principals, List.of("alice"));
    List<String> storage =
        List.of(
            "--state-db",
            state.toString(),
            "--object-dir",
            objects.toString(),
            "--authority",
            "issuer-a",
            "--principal-map",
            principals.toString(),
            "--trust-system-clock");
    List<String> initialize =
        new ArrayList<>(List.of(executable.toString(), "v2", "init-authority"));
    initialize.addAll(storage);
    RustAuthorityProcess.command(directory, initialize);

    Path journalFile = directory.resolve("java-client-journal.sqlite");
    Path input = directory.resolve("input.bin");
    Path output = directory.resolve("output.bin");
    byte[] bytes = new byte[250_000];
    new Random(21).nextBytes(bytes);
    Files.write(input, bytes);
    Records.ScopeSummary root;
    try (RustAuthorityProcess server =
            new RustAuthorityProcess(executable, pki, directory, storage, "first");
        ClientJournal journal =
            ClientJournal.initialize(
                journalFile,
                new ClientJournal.Intent("issuer-a", "alice", 1, RUST_DEFAULT_POLICY, true),
                ClientJournal.Limits.defaults());
        DurableClient client =
            DurableClient.connect(
                server.address(), pki.client("alice"), journal, ClientOptions.defaults())) {
      assertEquals(
          List.of(Messages.DURABLE_WORK, Messages.RESULT_DELIVERY),
          DurableClientTest.get(client.ready()).supported());
      assertEquals(1L, DurableClientTest.get(client.nextSequence()));
      Messages.Binding binding = DurableClientTest.get(client.binding());
      assertEquals("issuer-a", binding.authority());
      assertEquals(1, binding.generation());
      DurableClientTest.get(client.declare(DurableClientTest.operation(1), 0, List.of(1L), true));
      InputSource source = InputSource.file(input, "application/octet-stream", 16L << 20);
      Records.OperationReceipt admitted =
          DurableClientTest.get(
              client.admit(
                  DurableClientTest.operation(2),
                  DurableClientTest.parameters(source, "copy/v2", 0),
                  DurableClientTest.operation(1),
                  source));
      assertEquals(1, assertInstanceOf(Records.Admitted.class, admitted.outcome()).attempt());
      ClientJournal.Observed terminal = DurableClientTest.awaitTerminal(client, WORK);
      assertEquals(Records.State.SUCCEEDED, terminal.view().state());
      Records.Manifest manifest = DurableClientTest.get(client.manifest(WORK, 1));
      assertEquals(terminal.view().manifest(), manifest);
      DurableClientTest.get(client.select(WORK, 1, 0));
      ResultFiles.Delivered delivered =
          DurableClientTest.get(client.read(WORK, 1, 0, new ResultFiles.Destination(output)));
      assertEquals(bytes.length, delivered.length());
      assertArrayEquals(bytes, Files.readAllBytes(output));
      DurableClient.ScopePage page = DurableClientTest.get(client.page(0, 0, 256));
      assertTrue(page.membershipVerified());
      root = DurableClientTest.get(client.checkpoint(0, page.seal(), 5000));
      assertEquals(new Records.Counts(1, 0, 0, 0), root.counts());
      assertEquals(root, DurableClientTest.get(client.complete()));
      DurableClientTest.get(client.detach());
      DurableClientTest.get(client.closed());
    }

    // Rust authority restart with the same roots; the Java client reopens its journal, attaches,
    // finds the same terminal evidence and replays the admission by lookup and by exact resend.
    try (RustAuthorityProcess server =
            new RustAuthorityProcess(executable, pki, directory, storage, "second");
        ClientJournal journal = ClientJournal.open(journalFile, ClientJournal.Limits.defaults());
        DurableClient client =
            DurableClient.connect(
                server.address(), pki.client("alice"), journal, ClientOptions.defaults())) {
      DurableClientTest.get(client.ready());
      assertEquals(1, DurableClientTest.get(client.binding()).generation());
      assertEquals(
          Records.State.SUCCEEDED, DurableClientTest.get(client.watch(WORK, 0, 0)).view().state());
      assertEquals(
          journal.receipt(DurableClientTest.operation(2)).orElseThrow(),
          DurableClientTest.get(client.lookup(DurableClientTest.operation(2))));
      InputSource source = InputSource.file(input, "application/octet-stream", 16L << 20);
      assertEquals(
          journal.receipt(DurableClientTest.operation(2)).orElseThrow(),
          DurableClientTest.get(
              client.admit(
                  DurableClientTest.operation(2),
                  DurableClientTest.parameters(source, "copy/v2", 0),
                  DurableClientTest.operation(1),
                  source)));
      assertEquals(root, DurableClientTest.get(client.checkpoint(0, root.seal(), 0)));
      assertEquals(
          ProtocolError.Code.CONFLICT,
          DurableClientTest.refusal(
                  client.declare(DurableClientTest.operation(1), 0, List.of(1L, 2L), true))
              .code());
      DurableClientTest.get(client.detach());
    }
  }
}
