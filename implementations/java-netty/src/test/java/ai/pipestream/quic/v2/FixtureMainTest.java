package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The test-only fixture launcher as real processes: interface-v1 event records for both roles and
 * the three schedule actions a subject applies itself (drop-reply, kill, pause), each observed
 * through the durable state the shipped launcher then recovers.
 */
@Timeout(600)
final class FixtureMainTest {
  static final int BOUNDARY = 7;
  static final int REFUSAL = 11;
  static final int ROLE = 4;

  @TempDir static Path directory;
  static DurableTestPki pki;
  static String classpath;
  static String javaBinary;
  static Path principals;
  static Path input;
  static byte[] bytes;

  @BeforeAll
  static void setup() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    classpath = System.getProperty("java.class.path");
    javaBinary = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    principals = directory.resolve("principals.tsv");
    pki.principalMap(principals, List.of("alice"));
    input = directory.resolve("input.bin");
    bytes = new byte[70_000];
    new Random(11).nextBytes(bytes);
    Files.write(input, bytes);
  }

  record Run(int exit, String output) {}

  private static Process spawn(String mainClass, List<String> args, Path log) throws IOException {
    List<String> command =
        new ArrayList<>(
            List.of(javaBinary, "--enable-native-access=ALL-UNNAMED", "-cp", classpath, mainClass));
    command.addAll(args);
    return new ProcessBuilder(command)
        .directory(directory.toFile())
        .redirectErrorStream(true)
        .redirectOutput(log.toFile())
        .start();
  }

  private static Run java(String mainClass, List<String> args, long timeoutSeconds)
      throws Exception {
    Path log = Files.createTempFile(directory, "fixture-", ".log");
    Process process = spawn(mainClass, args, log);
    try {
      assertTrue(process.waitFor(timeoutSeconds, TimeUnit.SECONDS), () -> "timeout: " + args);
      return new Run(process.exitValue(), Files.readString(log));
    } finally {
      if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
    }
  }

  private static Run shipped(String... args) throws Exception {
    Run run = java("ai.pipestream.quic.v2.V2Main", List.of(args), 120);
    assertEquals(0, run.exit(), () -> String.join(" ", args) + "\n" + run.output());
    return run;
  }

  private static String bounded(Path log) {
    try {
      String text = Files.exists(log) ? Files.readString(log) : "";
      return text.substring(Math.max(0, text.length() - 8192));
    } catch (Exception failure) {
      return failure.toString();
    }
  }

  private static List<String> fixture(Path events, Path schedule, String run, String scenario) {
    List<String> args = new ArrayList<>(List.of("--fixture-events", events.toString()));
    if (schedule != null) args.addAll(List.of("--fixture-schedule", schedule.toString()));
    args.addAll(List.of("--fixture-run", run, "--fixture-scenario", scenario));
    return args;
  }

  /** A {@code FixtureMain serve} process with readiness marker. */
  static final class FixtureServer implements AutoCloseable {
    final Process process;
    final Path log;
    final InetSocketAddress address;

    FixtureServer(Path root, String tag, List<String> fixtureArgs) throws Exception {
      Path ready = directory.resolve("ready-" + tag);
      log = directory.resolve("server-" + tag + ".log");
      List<String> args =
          new ArrayList<>(
              List.of(
                  "serve",
                  "--root",
                  root.toString(),
                  "--authority",
                  "issuer-a",
                  "--result-authority",
                  "localhost:7443",
                  "--bind",
                  "127.0.0.1:0",
                  "--cert",
                  pki.path("server.crt").toString(),
                  "--key",
                  pki.path("server.key").toString(),
                  "--client-ca",
                  pki.path("ca.crt").toString(),
                  "--principal-map",
                  principals.toString(),
                  "--trust-system-clock",
                  "--ready-file",
                  ready.toString()));
      args.addAll(fixtureArgs);
      process = spawn("ai.pipestream.quic.v2.FixtureMain", args, log);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      InetSocketAddress bound = null;
      while (System.nanoTime() < deadline) {
        if (!process.isAlive()) fail("fixture server exited before readiness: " + bounded(log));
        if (Files.isRegularFile(ready)) {
          String[] parts = Files.readString(ready).trim().split(":");
          if (parts.length == 2) {
            bound = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
            break;
          }
        }
        Thread.sleep(20);
      }
      assertNotNull(bound, () -> "fixture server readiness timeout: " + bounded(log));
      address = bound;
    }

    int awaitExit(long seconds) throws InterruptedException {
      assertTrue(
          process.waitFor(seconds, TimeUnit.SECONDS), () -> "server still alive: " + bounded(log));
      return process.exitValue();
    }

    @Override
    public void close() {
      if (!process.isAlive()) return;
      process.destroy();
      boolean exited;
      try {
        exited = process.waitFor(60, TimeUnit.SECONDS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new AssertionError(interrupted);
      }
      assertTrue(exited, () -> "no drain after SIGTERM: " + bounded(log));
      assertEquals(0, process.exitValue(), () -> bounded(log));
      assertTrue(bounded(log).contains("DRAINED"), () -> bounded(log));
    }
  }

  private static List<String> client(Path journal, InetSocketAddress address, String... operation) {
    List<String> args =
        new ArrayList<>(
            List.of(
                "client",
                "--journal",
                journal.toString(),
                "--authority",
                "issuer-a",
                "--owner",
                "alice",
                "--creation-sequence",
                "1",
                "--connect",
                address.getHostString() + ":" + address.getPort(),
                "--server-name",
                "localhost",
                "--ca",
                pki.path("ca.crt").toString(),
                "--cert",
                pki.path("alice.crt").toString(),
                "--key",
                pki.path("alice.key").toString()));
    args.addAll(Arrays.asList(operation));
    return args;
  }

  private static Run fixtureClient(
      Path journal,
      InetSocketAddress address,
      Path events,
      String run,
      String scenario,
      String... op)
      throws Exception {
    List<String> args = client(journal, address, op);
    args.addAll(fixture(events, null, run, scenario));
    return java("ai.pipestream.quic.v2.FixtureMain", args, 120);
  }

  private static String hexOperation(int value) {
    return String.format("%032x", value);
  }

  private static int state(String watch) {
    Matcher matcher =
        Pattern.compile("WORK revision=(\\d+) state=(\\d+) attempt=(\\d+)").matcher(watch);
    assertTrue(matcher.find(), watch);
    return Integer.parseInt(matcher.group(2));
  }

  private static List<String[]> records(Path events) throws IOException {
    List<String[]> rows = new ArrayList<>();
    if (!Files.exists(events)) return rows;
    for (String line : Files.readAllLines(events, StandardCharsets.UTF_8)) {
      if (line.isEmpty()) continue;
      String[] columns = line.split("\t", -1);
      assertEquals(15, columns.length, line);
      assertEquals("1", columns[0], line);
      assertEquals("java", columns[3], line);
      rows.add(columns);
    }
    return rows;
  }

  private static List<String> boundaries(Path events) throws IOException {
    List<String> labels = new ArrayList<>();
    for (String[] row : records(events)) labels.add(row[BOUNDARY]);
    return labels;
  }

  private static void awaitBoundary(Path events, String boundary, long seconds) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
    while (System.nanoTime() < deadline) {
      if (boundaries(events).contains(boundary)) return;
      Thread.sleep(25);
    }
    fail("boundary " + boundary + " never recorded; have " + boundaries(events));
  }

  private static Path authority(String name) throws Exception {
    Path root = directory.resolve(name);
    shipped(
        "init-authority",
        "--root",
        root.toString(),
        "--authority",
        "issuer-a",
        "--result-authority",
        "localhost:7443");
    return root;
  }

  private static Path journal(String name, InetSocketAddress address) throws Exception {
    Path journal = directory.resolve(name + ".sqlite");
    List<String> args =
        new ArrayList<>(
            List.of(
                "init-client",
                "--journal",
                journal.toString(),
                "--authority",
                "issuer-a",
                "--owner",
                "alice",
                "--creation-sequence",
                "1"));
    Run run = java("ai.pipestream.quic.v2.V2Main", args, 120);
    assertEquals(0, run.exit(), run.output());
    return journal;
  }

  @Test
  void dropReplyWithholdsTheCommittedDeclarationReplyAndTheRetryReplaysIt() throws Exception {
    Path root = authority("drop");
    Path serverEvents = directory.resolve("drop-fixture/server-events.tsv");
    Path clientEvents = directory.resolve("drop-fixture/client-events.tsv");
    Path schedule = directory.resolve("drop-schedule.tsv");
    Files.writeString(
        schedule, "1\trun-drop\tlost-ack\tserver\tDECLARATION_COMMITTED\tdrop-reply\t0\t5000\n");
    try (FixtureServer server =
        new FixtureServer(root, "drop", fixture(serverEvents, schedule, "run-drop", "lost-ack"))) {
      Path journal = journal("drop", server.address);
      String[] declare = {"declare", "--operation", hexOperation(1), "--entities", "1", "--seal"};
      Run lost =
          fixtureClient(journal, server.address, clientEvents, "run-drop", "lost-ack", declare);
      assertNotEquals(
          0, lost.exit(), "the withheld reply must surface as a failure:\n" + lost.output());
      List<String[]> afterLoss = records(serverEvents);
      List<String> lossBoundaries = boundaries(serverEvents);
      int committed = lossBoundaries.indexOf("DECLARATION_COMMITTED");
      assertTrue(committed >= 0, lossBoundaries::toString);
      assertFalse(lossBoundaries.contains("DECLARATION_RESPONSE_SENT"));
      assertFalse(lossBoundaries.contains("REFUSAL_SENT"), "no refusal frame was written");
      long observations =
          afterLoss.stream()
              .filter(
                  row ->
                      row[BOUNDARY].isEmpty()
                          && row[REFUSAL].equals(
                              Integer.toString(ProtocolError.Code.CONTROL_RESET.value())))
              .count();
      assertEquals(1, observations, "exactly one withheld-reply observation");
      List<String> clientFirst = boundaries(clientEvents);
      assertTrue(clientFirst.contains("INTENT_JOURNALED"), clientFirst.toString());
      assertTrue(clientFirst.contains("REQUEST_SENT"), clientFirst.toString());
      assertFalse(clientFirst.contains("RECEIPT_JOURNALED"), clientFirst.toString());

      Run replayed =
          fixtureClient(journal, server.address, clientEvents, "run-drop", "lost-ack", declare);
      assertEquals(0, replayed.exit(), replayed.output());
      assertTrue(replayed.output().contains("RECEIPT"), replayed.output());
      List<String> serverAll = boundaries(serverEvents);
      assertEquals(1, serverAll.stream().filter("DECLARATION_RESPONSE_SENT"::equals).count());
      assertTrue(serverAll.lastIndexOf("DECLARATION_RESPONSE_SENT") > committed);
      List<String> clientAll = boundaries(clientEvents);
      assertTrue(
          clientAll.lastIndexOf("RECEIPT_JOURNALED") > clientAll.lastIndexOf("RECEIPT_VALIDATED")
              && clientAll.lastIndexOf("RECEIPT_VALIDATED")
                  > clientAll.lastIndexOf("INTENT_JOURNALED"),
          clientAll.toString());
      for (String[] row : records(clientEvents)) assertEquals("client", row[ROLE]);
      for (String[] row : records(serverEvents)) assertEquals("server", row[ROLE]);
      shipped(client(journal, server.address, "detach").toArray(String[]::new));
    }
    assertTrue(boundaries(serverEvents).contains("SHUTDOWN_DRAINED"));
  }

  @Test
  void killAfterAdmissionCommitLeavesAReplayableAdmissionAndRuntimeBoundariesFollow()
      throws Exception {
    Path root = authority("kill");
    Path serverEvents = directory.resolve("kill-fixture/server-events.tsv");
    Path clientEvents = directory.resolve("kill-fixture/client-events.tsv");
    Path schedule = directory.resolve("kill-schedule.tsv");
    Files.writeString(schedule, "1\trun-kill\tdeath\tserver\tADMISSION_COMMITTED\tkill\t0\t0\n");
    Path journal;
    FixtureServer first =
        new FixtureServer(root, "kill-1", fixture(serverEvents, schedule, "run-kill", "death"));
    try {
      journal = journal("kill", first.address);
      Run declared =
          fixtureClient(
              journal,
              first.address,
              clientEvents,
              "run-kill",
              "death",
              "declare",
              "--operation",
              hexOperation(1),
              "--entities",
              "1",
              "--seal");
      assertEquals(0, declared.exit(), declared.output());
      Run admitted =
          fixtureClient(
              journal,
              first.address,
              clientEvents,
              "run-kill",
              "death",
              "admit",
              "--operation",
              hexOperation(2),
              "--declaration",
              hexOperation(1),
              "--work",
              "0:0:1",
              "--input",
              input.toString(),
              "--application",
              "copy/v2");
      assertNotEquals(0, admitted.exit(), "the server died before replying:\n" + admitted.output());
      assertEquals(137, first.awaitExit(30));
    } finally {
      if (first.process.isAlive()) first.process.destroyForcibly();
    }
    List<String> died = boundaries(serverEvents);
    assertTrue(died.contains("INPUT_INSTALLED"), died.toString());
    assertEquals("ADMISSION_COMMITTED", died.get(died.size() - 1), died.toString());
    assertFalse(died.contains("ADMISSION_RESPONSE_SENT"));

    try (FixtureServer second =
        new FixtureServer(root, "kill-2", fixture(serverEvents, null, "run-kill", "death"))) {
      Run replay =
          fixtureClient(
              journal,
              second.address,
              clientEvents,
              "run-kill",
              "death",
              "replay",
              "--operation",
              hexOperation(2),
              "--input",
              input.toString());
      assertEquals(0, replay.exit(), replay.output());
      assertTrue(replay.output().contains("RECEIPT"), replay.output());
      int state = -1;
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
      while (state != 5 && System.nanoTime() < deadline) {
        Run watch =
            fixtureClient(
                journal,
                second.address,
                clientEvents,
                "run-kill",
                "death",
                "watch",
                "--work",
                "0:0:1",
                "--wait-ms",
                "3000");
        assertEquals(0, watch.exit(), watch.output());
        state = state(watch.output());
        assertTrue(state < 6, watch.output());
      }
      assertEquals(5, state);
      awaitBoundary(serverEvents, "CLOSURE_COMMITTED", 30);
      List<String> recovered = boundaries(serverEvents);
      int replayed = recovered.lastIndexOf("ADMISSION_RESPONSE_SENT");
      assertTrue(replayed > recovered.indexOf("ADMISSION_COMMITTED"), recovered.toString());
      assertEquals(
          1,
          recovered.stream().filter("ADMISSION_COMMITTED"::equals).count(),
          "no second admission commit");
      int claimed = recovered.indexOf("EXECUTION_CLAIMED");
      int installed = recovered.indexOf("OUTPUT_INSTALLED");
      int published = recovered.indexOf("PUBLICATION_COMMITTED");
      assertTrue(
          claimed >= 0 && installed > claimed && published > installed, recovered.toString());
      List<String> clientAll = boundaries(clientEvents);
      assertTrue(clientAll.contains("OBSERVATION_JOURNALED"), clientAll.toString());
      assertEquals(
          1,
          clientAll.stream().filter("RECEIPT_JOURNALED"::equals).count() - 1,
          "declaration and admission receipts each journaled exactly once: " + clientAll);
      shipped(client(journal, second.address, "detach").toArray(String[]::new));
    }
  }

  @Test
  void pauseHoldsTheCommittedDeclarationUntilTheReleaseMarkerAppears() throws Exception {
    Path root = authority("pause");
    Path serverEvents = directory.resolve("pause-fixture/server-events.tsv");
    Path clientEvents = directory.resolve("pause-fixture/client-events.tsv");
    Path schedule = directory.resolve("pause-schedule.tsv");
    Files.writeString(
        schedule, "1\trun-pause\thold\tserver\tDECLARATION_COMMITTED\tpause\t0\t60000\n");
    try (FixtureServer server =
        new FixtureServer(root, "pause", fixture(serverEvents, schedule, "run-pause", "hold"))) {
      Path journal = journal("pause", server.address);
      List<String> args =
          client(
              journal,
              server.address,
              "declare",
              "--operation",
              hexOperation(1),
              "--entities",
              "1",
              "--seal");
      args.addAll(fixture(clientEvents, null, "run-pause", "hold"));
      Path log = directory.resolve("pause-client.log");
      Process declaring = spawn("ai.pipestream.quic.v2.FixtureMain", args, log);
      try {
        awaitBoundary(serverEvents, "DECLARATION_COMMITTED", 30);
        assertFalse(declaring.waitFor(500, TimeUnit.MILLISECONDS), "client must still be waiting");
        assertFalse(boundaries(serverEvents).contains("DECLARATION_RESPONSE_SENT"));
        Files.writeString(serverEvents.resolveSibling("release-server-DECLARATION_COMMITTED"), "");
        assertTrue(declaring.waitFor(60, TimeUnit.SECONDS), () -> bounded(log));
        assertEquals(0, declaring.exitValue(), () -> bounded(log));
        assertTrue(bounded(log).contains("RECEIPT"), () -> bounded(log));
      } finally {
        if (declaring.isAlive()) declaring.destroyForcibly();
      }
      List<String> serverAll = boundaries(serverEvents);
      assertTrue(
          serverAll.indexOf("DECLARATION_RESPONSE_SENT")
              > serverAll.indexOf("DECLARATION_COMMITTED"));
      shipped(client(journal, server.address, "detach").toArray(String[]::new));
    }
  }

  @Test
  void scheduleParserRejectsRowsTheSubjectCannotHonour() throws Exception {
    Path schedule = directory.resolve("bad-schedule.tsv");
    Files.writeString(schedule, "1\tr\ts\tserver\tEXECUTION_CLAIMED\tdrop-reply\t0\t0\n");
    assertThrows(IOException.class, () -> FixtureMain.schedule(schedule, "r", "s", "server"));
    Files.writeString(schedule, "1\tr\ts\tserver\tSESSION_COMMITTED\tstop\t0\t0\n");
    assertThrows(IOException.class, () -> FixtureMain.schedule(schedule, "r", "s", "server"));
    Files.writeString(schedule, "1\tr\tother\tserver\tSESSION_COMMITTED\tkill\t0\t0\n");
    assertThrows(IOException.class, () -> FixtureMain.schedule(schedule, "r", "s", "server"));
    Files.writeString(schedule, "1\tr\ts\tserver\tNOT_A_BOUNDARY\tkill\t0\t0\n");
    assertThrows(IOException.class, () -> FixtureMain.schedule(schedule, "r", "s", "server"));
    Files.writeString(
        schedule,
        "version\trun_id\tscenario_id\ttarget\tboundary\taction\tseed\tdeadline_ms\n"
            + "1\tr\ts\tclient\tINTENT_JOURNALED\tkill\t0\t0\n"
            + "1\tr\ts\tserver\tADMISSION_COMMITTED\tdrop-reply\t0\t100\n");
    assertEquals(1, FixtureMain.schedule(schedule, "r", "s", "server").size());
    assertEquals(1, FixtureMain.schedule(schedule, "r", "s", "client").size());
  }

  @Test
  void clientDeathAfterJournalBoundariesIsRecoveredByTheJournalAlone() throws Exception {
    Path root = authority("client-kill");
    Path serverEvents = directory.resolve("client-kill-fixture/server-events.tsv");
    Path clientEvents = directory.resolve("client-kill-fixture/client-events.tsv");
    Path intentSchedule = directory.resolve("client-kill-intent.tsv");
    Path receiptSchedule = directory.resolve("client-kill-receipt.tsv");
    Files.writeString(
        intentSchedule, "1\trun-ck\tclient-death\tclient\tINTENT_JOURNALED\tkill\t0\t0\n");
    Files.writeString(
        receiptSchedule, "1\trun-ck\tclient-death\tclient\tRECEIPT_VALIDATED\tkill\t0\t0\n");
    try (FixtureServer server =
        new FixtureServer(
            root, "client-kill", fixture(serverEvents, null, "run-ck", "client-death"))) {
      Path journal = journal("client-kill", server.address);
      String[] declare = {"declare", "--operation", hexOperation(1), "--entities", "1", "--seal"};

      // Death right after the intent commit: the request never left; the journal replays it.
      List<String> args = client(journal, server.address, declare);
      args.addAll(fixture(clientEvents, intentSchedule, "run-ck", "client-death"));
      Run beforeSend = java("ai.pipestream.quic.v2.FixtureMain", args, 120);
      assertEquals(137, beforeSend.exit(), beforeSend.output());
      List<String> afterIntent = boundaries(clientEvents);
      assertEquals(
          "INTENT_JOURNALED", afterIntent.get(afterIntent.size() - 1), afterIntent.toString());
      assertFalse(boundaries(serverEvents).contains("DECLARATION_COMMITTED"));

      // Death after validation but before the receipt commit: the server's committed declaration
      // is replayed to the reopened journal, which then commits the receipt exactly once.
      args = client(journal, server.address, declare);
      args.addAll(fixture(clientEvents, receiptSchedule, "run-ck", "client-death"));
      Run beforeReceipt = java("ai.pipestream.quic.v2.FixtureMain", args, 120);
      assertEquals(137, beforeReceipt.exit(), beforeReceipt.output());
      List<String> afterValidation = boundaries(clientEvents);
      assertEquals(
          "RECEIPT_VALIDATED",
          afterValidation.get(afterValidation.size() - 1),
          afterValidation.toString());
      assertFalse(afterValidation.contains("RECEIPT_JOURNALED"));
      assertEquals(
          1, boundaries(serverEvents).stream().filter("DECLARATION_COMMITTED"::equals).count());

      Run recovered =
          fixtureClient(journal, server.address, clientEvents, "run-ck", "client-death", declare);
      assertEquals(0, recovered.exit(), recovered.output());
      assertTrue(recovered.output().contains("RECEIPT"), recovered.output());
      List<String> clientAll = boundaries(clientEvents);
      assertEquals(
          1, clientAll.stream().filter("RECEIPT_JOURNALED"::equals).count(), clientAll.toString());
      Run again =
          fixtureClient(journal, server.address, clientEvents, "run-ck", "client-death", declare);
      assertEquals(0, again.exit(), again.output());
      assertEquals(
          1,
          boundaries(clientEvents).stream().filter("RECEIPT_JOURNALED"::equals).count(),
          "a retained receipt is answered from the journal without a request");
      assertEquals(
          clientAll.stream().filter("REQUEST_SENT"::equals).count() + 2,
          boundaries(clientEvents).stream().filter("REQUEST_SENT"::equals).count(),
          "only the session create and detach requests leave for a retained receipt");
      shipped(client(journal, server.address, "detach").toArray(String[]::new));
    }
  }
}
