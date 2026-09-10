package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * One {@code V2Main client} invocation run in this JVM through {@link ClientCommands} with its
 * stdout captured: the launcher's process contract (journal arguments, connection arguments, one
 * operation, diagnostic output lines) without a process boundary, so tests can hold server-side and
 * client-side boundary hooks around it.
 */
final class LauncherInvocation {
  /** The launcher's default session policy ({@code --execution-ms} and retention defaults). */
  static final Records.Policy LAUNCHER_POLICY = new Records.Policy(60_000, 3_600_000, 86_400_000);

  private LauncherInvocation() {}

  /** Captured stdout and the failure, if any. */
  record Run(String output, Exception failure) {
    long count(String prefix) {
      return output.lines().filter(line -> line.startsWith(prefix)).count();
    }

    ProtocolError.Code code() {
      return LauncherInvocation.code(failure);
    }

    ProtocolError error() {
      Throwable cause = failure;
      while (cause != null && !(cause instanceof ProtocolError)) cause = cause.getCause();
      assertNotNull(cause, String.valueOf(failure));
      return (ProtocolError) cause;
    }
  }

  static Run invoke(
      DurableTestPki pki,
      String owner,
      Path journal,
      InetSocketAddress address,
      Boundaries hooks,
      String... operation) {
    List<String> args =
        new ArrayList<>(
            List.of(
                "client",
                "--journal",
                journal.toString(),
                "--authority",
                "issuer-a",
                "--owner",
                owner,
                "--creation-sequence",
                "1",
                "--connect",
                address.getHostString() + ":" + address.getPort(),
                "--server-name",
                "localhost",
                "--ca",
                pki.path("ca.crt").toString(),
                "--cert",
                pki.path(owner + ".crt").toString(),
                "--key",
                pki.path(owner + ".key").toString()));
    args.addAll(Arrays.asList(operation));
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    PrintStream original = System.out;
    Exception failure = null;
    System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
    try {
      assertTrue(ClientCommands.run("client", args.toArray(new String[0]), hooks));
    } catch (Exception thrown) {
      failure = thrown;
    } finally {
      System.setOut(original);
    }
    return new Run(captured.toString(StandardCharsets.UTF_8), failure);
  }

  static ProtocolError.Code code(Exception failure) {
    Throwable cause = failure;
    while (cause != null && !(cause instanceof ProtocolError)) cause = cause.getCause();
    assertNotNull(cause, String.valueOf(failure));
    return ((ProtocolError) cause).code();
  }

  static String hex(int operation) {
    return String.format("%032x", operation);
  }

  static Path initJournal(Path journal, String owner) throws Exception {
    ClientJournal.initialize(
            journal,
            new ClientJournal.Intent("issuer-a", owner, 1, LAUNCHER_POLICY, true),
            ClientJournal.Limits.defaults())
        .close();
    return journal;
  }
}
