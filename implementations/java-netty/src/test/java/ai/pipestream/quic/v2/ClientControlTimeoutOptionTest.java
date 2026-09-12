package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The client launcher's {@code --control-timeout-ms}: a driver that bounds one operation at
 * thirty seconds cannot distinguish a dead authority from the client's default thirty-second
 * control deadline, so the launcher lets the caller shorten it. Against an authority that never
 * answers the request, the launcher fails LIMIT_EXCEEDED "control response deadline" after the
 * shortened deadline and exits, well inside the default.
 */
@Timeout(60)
class ClientControlTimeoutOptionTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
  }

  @Test
  void aShortenedControlDeadlineBoundsAnUnansweredRequest() throws Exception {
    try (RawDurableAuthority authority =
        new RawDurableAuthority(
            pki.server(principals),
            DurableClientControlDeadlineTest.manifestFor(new byte[100]),
            LauncherInvocation.LAUNCHER_POLICY)) {
      // Session creation is answered; the watch that follows is never answered.
      authority.withholdControls = true;
      Path journal = LauncherInvocation.initJournal(directory.resolve("short.sqlite"), "alice");
      long started = System.nanoTime();
      LauncherInvocation.Run run =
          LauncherInvocation.invoke(
              pki,
              "alice",
              journal,
              authority.address(),
              Boundaries.NONE,
              "--control-timeout-ms",
              "1500",
              "watch",
              "--work",
              "0:0:1",
              "--wait-ms",
              "0");
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
      assertNotNull(run.failure(), run.output());
      ProtocolError error = run.error();
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, error.code(), error.toString());
      assertTrue(error.getMessage().endsWith("control response deadline"), error.toString());
      assertTrue(
          elapsedMs >= 1_300 && elapsedMs < 10_000, "bounded by the option: " + elapsedMs + " ms");
    }
  }
}
