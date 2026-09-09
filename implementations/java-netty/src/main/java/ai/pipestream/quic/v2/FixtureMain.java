package ai.pipestream.quic.v2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Test-only fixture entry point for the neutral failure driver. It runs the same {@code serve} and
 * {@code client} commands as {@link V2Main} with boundary hooks that record interface-v1 event
 * records and apply a frozen fault schedule: {@code pause} holds a committed boundary until a
 * release marker appears, {@code drop-reply} (or {@code disconnect} at a committed reply boundary)
 * withholds the committed reply and resets the connection, {@code kill} (or {@code exit}) halts the
 * process right after the commit. Hooks cannot forge commits, receipts, callbacks or results. This
 * class is not reachable from {@link V2Main} and is not a shipped launcher.
 */
public final class FixtureMain {
  private static final String USAGE =
      "FixtureMain supports: serve <V2Main serve options> | client <V2Main client options>"
          + " <operation>, each followed by --fixture-events FILE --fixture-schedule FILE"
          + " --fixture-run ID --fixture-scenario ID [--fixture-target NAME]";

  private FixtureMain() {}

  /**
   * One schedule row.
   *
   * @param target fixture target
   * @param boundary armed boundary
   * @param action action name
   * @param seed row seed
   * @param deadlineMs pause deadline in milliseconds
   */
  record Row(
      String target, Boundaries.Boundary boundary, String action, long seed, long deadlineMs) {}

  /**
   * Run {@code serve} or {@code client} with fixture hooks.
   *
   * @param arguments {@code serve} plus the V2Main serve options, or {@code client} plus the V2Main
   *     client options and operation, followed by {@code --fixture-events FILE --fixture-schedule
   *     FILE --fixture-run ID --fixture-scenario ID [--fixture-target NAME]}
   */
  public static void main(String[] arguments) {
    try {
      boolean server = arguments.length > 0 && "serve".equals(arguments[0]);
      if (!server && (arguments.length == 0 || !"client".equals(arguments[0])))
        throw new IllegalArgumentException(USAGE);
      String role = server ? "server" : "client";
      Map<String, String> options = V2Main.options(arguments, 1);
      String run = V2Main.required(options, "fixture-run");
      String scenario = V2Main.required(options, "fixture-scenario");
      String target = options.getOrDefault("fixture-target", role);
      Path events = V2Main.requiredPath(options, "fixture-events");
      List<Row> rows =
          options.containsKey("fixture-schedule")
              ? schedule(V2Main.requiredPath(options, "fixture-schedule"), run, scenario, target)
              : List.of();
      try (FixtureEvents recorder = FixtureEvents.open(events, run, scenario, role);
          Hooks hooks = new Hooks(recorder, rows, target)) {
        if (server) V2Main.serve(options, hooks);
        else if (!ClientCommands.run("client", arguments, hooks))
          throw new IllegalArgumentException(USAGE);
      }
    } catch (Exception failure) {
      System.err.println(failure.getMessage() == null ? failure.toString() : failure.getMessage());
      System.exit(1);
    }
  }

  /**
   * Parse a version-1 schedule, keeping rows for one run/scenario/target.
   *
   * @param file schedule TSV
   * @param run run identifier
   * @param scenario scenario identifier
   * @param target fixture target
   * @return applicable rows in file order
   * @throws IOException unreadable or malformed schedule
   */
  static List<Row> schedule(Path file, String run, String scenario, String target)
      throws IOException {
    List<Row> rows = new ArrayList<>();
    List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
    for (int i = 0; i < lines.size(); i++) {
      String line = lines.get(i);
      if (line.isEmpty()) continue;
      String[] columns = line.split("\t", -1);
      if (columns.length != 8)
        throw new IOException("schedule row " + (i + 1) + " must have 8 columns");
      if (columns[0].equals("version")) continue;
      if (!columns[0].equals("1"))
        throw new IOException("schedule row " + (i + 1) + " has unknown version");
      if (!columns[1].equals(run) || !columns[2].equals(scenario))
        throw new IOException("schedule row " + (i + 1) + " names another run or scenario");
      if (!columns[3].equals(target)) continue;
      Boundaries.Boundary boundary;
      try {
        boundary = Boundaries.Boundary.valueOf(columns[4]);
      } catch (IllegalArgumentException unknown) {
        throw new IOException(
            "schedule row " + (i + 1) + " names an unknown boundary " + columns[4]);
      }
      String action = columns[5];
      switch (action) {
        case "pause", "release", "drop-reply", "disconnect", "kill", "exit" -> {}
        case "stop", "restart", "clock-set" ->
            throw new IOException(
                "schedule action " + action + " is driven by the fixture, not the subject");
        default ->
            throw new IOException("schedule row " + (i + 1) + " has unknown action " + action);
      }
      if ((action.equals("drop-reply") || action.equals("disconnect"))
          && !REPLY.containsKey(boundary))
        throw new IOException(
            "schedule row "
                + (i + 1)
                + " withholds a reply at "
                + boundary
                + ", which has no pending reply");
      rows.add(
          new Row(
              target, boundary, action, Long.parseLong(columns[6]), Long.parseLong(columns[7])));
    }
    return rows;
  }

  private static final Map<Boundaries.Boundary, Boundaries.Boundary> REPLY =
      new EnumMap<>(Boundaries.Boundary.class);

  static {
    REPLY.put(Boundaries.Boundary.SESSION_COMMITTED, Boundaries.Boundary.SESSION_RESPONSE_SENT);
    REPLY.put(
        Boundaries.Boundary.DECLARATION_COMMITTED, Boundaries.Boundary.DECLARATION_RESPONSE_SENT);
    REPLY.put(Boundaries.Boundary.ADMISSION_COMMITTED, Boundaries.Boundary.ADMISSION_RESPONSE_SENT);
  }

  /**
   * Subject hook for either role: records every boundary, applies pause/drop-reply/kill rows once
   * each. Withhold applies only to server reply boundaries; a client never withholds anything.
   */
  static final class Hooks implements Boundaries, AutoCloseable {
    private final FixtureEvents recorder;
    private final List<Row> pending;
    private final String target;
    private final ExecutorService writer =
        Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon().name("pipestream-v2-fixture-events").factory());

    /**
     * Create hooks.
     *
     * @param recorder event recorder
     * @param rows applicable schedule rows
     * @param target fixture target
     */
    Hooks(FixtureEvents recorder, List<Row> rows, String target) {
      this.recorder = Objects.requireNonNull(recorder);
      this.pending = new ArrayList<>(rows);
      this.target = target;
    }

    private synchronized Row take(Boundary boundary, String... actions) {
      for (int i = 0; i < pending.size(); i++) {
        Row row = pending.get(i);
        if (row.boundary() != boundary) continue;
        for (String action : actions) if (row.action().equals(action)) return pending.remove(i);
      }
      return null;
    }

    private void write(String boundary, Details details) {
      try {
        recorder.record(boundary, details, null);
      } catch (Exception failure) {
        System.err.println("fixture event write failed: " + failure.getMessage());
        Runtime.getRuntime().halt(3);
      }
    }

    private void observe(Details details) {
      try {
        writer.execute(() -> write("", details));
      } catch (RuntimeException stopped) {
        // Recorder closed during shutdown.
      }
    }

    @Override
    public void committed(Boundary boundary, Details details) {
      write(boundary.name(), details);
      Row kill = take(boundary, "kill", "exit");
      if (kill != null) {
        // The commit is durable; no reply follows. Process death only, not power loss.
        Runtime.getRuntime().halt(137);
      }
      Row pause = take(boundary, "pause");
      if (pause != null) hold(boundary, pause.deadlineMs());
    }

    private void hold(Boundary boundary, long deadlineMs) {
      Path release = recorder.directory().resolve("release-" + target + "-" + boundary.name());
      long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(1, deadlineMs));
      while (!Files.exists(release) && System.nanoTime() < deadline) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    @Override
    public void sent(Boundary boundary, Details details) {
      try {
        writer.execute(() -> write(boundary.name(), details));
      } catch (RuntimeException stopped) {
        // Recorder closed during shutdown.
      }
    }

    @Override
    public boolean withhold(Boundary boundary) {
      for (Map.Entry<Boundary, Boundary> entry : REPLY.entrySet()) {
        if (entry.getValue() != boundary) continue;
        Row row = take(entry.getKey(), "drop-reply", "disconnect");
        if (row != null) {
          // No refusal frame is written: the reply is withheld and the connection is reset. Record
          // a pure observation (empty boundary) so the trace never claims a frame that never left.
          observe(Details.NONE.refusal(ProtocolError.Code.CONTROL_RESET));
          return true;
        }
      }
      return false;
    }

    @Override
    public void close() throws IOException {
      writer.shutdown();
      try {
        writer.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
      recorder.close();
    }
  }
}
