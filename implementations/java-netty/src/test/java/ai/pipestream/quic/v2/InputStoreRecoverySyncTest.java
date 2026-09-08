package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class InputStoreRecoverySyncTest {
  private static final byte[] INPUT = {9, 8, 7};
  private static final byte[] OUTPUT = {1, 2, 3, 4};

  @TempDir Path directory;

  @Test
  void failedFinalRecoverySyncReleasesOwnershipBeforeExactEmptyReopen() throws Exception {
    Path database;
    Path inputsPath;
    try (ResultFixture fixture =
        new ResultFixture(directory, "released-before-recovery", INPUT, OUTPUT)) {
      database = fixture.database;
      inputsPath = fixture.inputsPath;
      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimInput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(1200)));
      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimOutput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(21_200)));
      assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
    }

    AtomicInteger reached = new AtomicInteger();
    IOException interrupted =
        assertThrows(
            IOException.class,
            () -> {
              try (InputStore ignored =
                  InputStore.open(
                      inputsPath,
                      ResultFixture.INPUT_LIMITS,
                      phase -> {
                        if (phase == InputStore.Phase.RECOVERY_RELEASES_SYNCED) {
                          reached.incrementAndGet();
                          throw new IOException("stop after recovery release syncs");
                        }
                      })) {
                ignored.usage();
                fail("recovery sync probe was not reached");
              }
            });
    assertEquals("stop after recovery release syncs", interrupted.getMessage());
    assertEquals(1, reached.get());

    SessionStore sessions = SessionStore.open(database, ResultFixture.configuration());
    try (InputStore inputs = InputStore.open(inputsPath, ResultFixture.INPUT_LIMITS)) {
      sessions.verifyInputs(inputs);
      assertEquals(new InputStore.Usage(0, 0, 0), inputs.usage());
    }
  }
}
