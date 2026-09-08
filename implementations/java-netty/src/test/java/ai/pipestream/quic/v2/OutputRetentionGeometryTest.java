package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class OutputRetentionGeometryTest {
  private static final byte[] INPUT = {9};
  private static final byte[] OUTPUT = {1, 2, 3, 4};
  private static final byte[] SECOND = {5, 6};
  private static final long OUTPUT_BUDGET = 32;
  private static final long WRITER_LIMIT = 8;
  private static final long RELEASE_AT = 21_200;

  @TempDir Path directory;

  @Test
  void publishedWriterMayUseAProperSubsetOfTheAdmittedJobObjectLimit() throws Exception {
    try (Fixture fixture = new Fixture(directory, "subset")) {
      AdmissionStore.StoredJob before = fixture.job();
      assertEquals(OUTPUT_BUDGET, before.record().objectLimit());
      assertTrue(WRITER_LIMIT < before.record().objectLimit());
      assertEquals(Records.State.SUCCEEDED, fixture.current().state());
      assertEquals(
          ResultFixture.digest(OUTPUT), fixture.current().manifest().outputs().get(0).sha256());
      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimOutput(
              1, ResultFixture.WORK, fixture.inputs, ResultFixture.clock(RELEASE_AT)));
      assertTrue(fixture.inputs.find(fixture.context(), fixture.header).isPresent());
      assertTrue(fixture.inputs.findReservation(fixture.context(), fixture.header).isEmpty());
      assertEquals(
          ResultFixture.digest(OUTPUT), fixture.current().manifest().outputs().get(0).sha256());
      assertEquals(RELEASE_AT, fixture.job().record().outputReleaseAt());
      assertFalse(fixture.job().record().outputsLive());
    }
  }

  @Test
  void corruptionAfterCommittedIntentCannotRefundTheOutputReservation() throws Exception {
    try (Fixture fixture = new Fixture(directory, "corrupt")) {
      InputStore.Usage charged = fixture.inputs.usage();
      IOException interrupted =
          assertThrows(
              IOException.class,
              () ->
                  fixture.sessions.reclaimOutput(
                      1,
                      ResultFixture.WORK,
                      fixture.inputs,
                      ResultFixture.clock(RELEASE_AT),
                      phase -> {
                        if (phase == RetentionStore.Phase.OUTPUT_INTENT_COMMITTED)
                          throw new IOException("stop after output intent");
                      }));
      assertEquals("stop after output intent", interrupted.getMessage());
      assertEquals(RELEASE_AT, fixture.job().record().outputReleaseAt());
      assertTrue(fixture.job().record().outputsLive());
      assertEquals(charged, fixture.inputs.usage());

      Path output = onlyOutput(fixture.inputsPath);
      try (FileChannel channel =
          FileChannel.open(output, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
        assertTrue(channel.size() > 0);
        ByteBuffer original = ByteBuffer.allocate(1);
        channel.position(channel.size() - 1);
        assertEquals(1, channel.read(original));
        original.flip();
        byte changed = (byte) (original.get() ^ 1);
        channel.position(channel.size() - 1);
        assertEquals(1, channel.write(ByteBuffer.wrap(new byte[] {changed})));
        channel.force(true);
      }
      assertThrows(
          IOException.class,
          () ->
              fixture.sessions.reclaimOutput(
                  1, ResultFixture.WORK, fixture.inputs, ResultFixture.clock(RELEASE_AT)));
      assertEquals(charged, fixture.inputs.usage());
      assertTrue(fixture.inputs.findReservation(fixture.context(), fixture.header).isPresent());
      assertEquals(RELEASE_AT, fixture.job().record().outputReleaseAt());
      assertTrue(fixture.job().record().outputsLive());
      Records.WorkView retained = fixture.current();
      assertEquals(Records.State.SUCCEEDED, retained.state());
      assertEquals(ResultFixture.digest(OUTPUT), retained.manifest().outputs().get(0).sha256());
      assertTrue(fixture.inputs.find(fixture.context(), fixture.header).isPresent());
    }
  }

  @Test
  void partialMultiOutputDeletionRetainsFundingAndResumesRemainingSlotAfterReopen()
      throws Exception {
    try (Fixture fixture = new Fixture(directory, "partial-set", 2)) {
      InputStore.Usage charged = fixture.inputs.usage();
      assertEquals(2, fixture.current().manifest().outputs().size());
      AtomicInteger removed = new AtomicInteger();
      fixture.inputs.close();
      fixture.inputs =
          InputStore.open(
              fixture.inputsPath,
              ResultFixture.INPUT_LIMITS,
              phase -> {
                if (phase == InputStore.Phase.OUTPUT_RETENTION_INSTALLED_REMOVED
                    && removed.incrementAndGet() == 1)
                  throw new IOException("stop after first installed output");
              });
      fixture.sessions.verifyInputs(fixture.inputs);
      IOException interrupted =
          assertThrows(
              IOException.class,
              () ->
                  fixture.sessions.reclaimOutput(
                      1, ResultFixture.WORK, fixture.inputs, ResultFixture.clock(RELEASE_AT)));
      assertEquals("stop after first installed output", interrupted.getMessage());
      assertEquals(1, removed.get());
      assertEquals(charged, fixture.inputs.usage());
      assertEquals(RELEASE_AT, fixture.job().record().outputReleaseAt());
      assertTrue(fixture.job().record().outputsLive());

      fixture.reopenInputs();
      assertEquals(charged, fixture.inputs.usage());
      assertTrue(fixture.inputs.findReservation(fixture.context(), fixture.header).isPresent());
      Records.Manifest retained = fixture.current().manifest();
      assertNotNull(retained);
      assertEquals(ResultFixture.digest(OUTPUT), retained.outputs().get(0).sha256());
      assertEquals(ResultFixture.digest(SECOND), retained.outputs().get(1).sha256());
      assertTrue(
          fixture.inputs.findOutput(fixture.context(), fixture.header, fixture.lease, 0).isEmpty());
      OutputStore.Stored surviving =
          fixture
              .inputs
              .findOutput(fixture.context(), fixture.header, fixture.lease, 1)
              .orElseThrow();
      assertEquals(ResultFixture.digest(SECOND), surviving.sha256());
      try (var stream = surviving.openStream()) {
        assertArrayEquals(SECOND, stream.readAllBytes());
      }
      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimOutput(
              1, ResultFixture.WORK, fixture.inputs, ResultFixture.clock(RELEASE_AT)));
      assertEquals(Files.size(onlyInput(fixture.inputsPath)), fixture.inputs.usage().bytes());
      assertEquals(1, fixture.inputs.usage().files());
      assertTrue(fixture.inputs.findReservation(fixture.context(), fixture.header).isEmpty());
      assertEquals(retained, fixture.current().manifest());
      assertFalse(fixture.job().record().outputsLive());
    }
  }

  private static Path onlyOutput(Path root) throws Exception {
    try (var entries = Files.newDirectoryStream(root.resolve("outputs"), "*.output")) {
      var iterator = entries.iterator();
      assertTrue(iterator.hasNext());
      Path result = iterator.next();
      assertFalse(iterator.hasNext());
      return result;
    }
  }

  private static Path onlyInput(Path root) throws Exception {
    try (var entries = Files.newDirectoryStream(root.resolve("objects"), "*.input")) {
      var iterator = entries.iterator();
      assertTrue(iterator.hasNext());
      Path result = iterator.next();
      assertFalse(iterator.hasNext());
      return result;
    }
  }

  private static final class Fixture implements AutoCloseable {
    final Path database;
    final Path inputsPath;
    final Messages.Binding binding;
    final Records.InputHeader header;
    final ExecutionStore.Lease lease;
    final SessionStore sessions;
    InputStore inputs;

    Fixture(Path directory, String name) throws Exception {
      this(directory, name, 1);
    }

    Fixture(Path directory, String name, int outputCount) throws Exception {
      database = directory.resolve(name + ".sqlite");
      inputsPath = directory.resolve(name + "-inputs");
      sessions = SessionStore.initialize(database, ResultFixture.configuration());
      binding =
          sessions.create(
              ResultFixture.sessionAccess("alice"),
              ResultFixture.SELECTED,
              new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
      assertEquals(1, binding.generation());
      sessions.declare(
          ResultFixture.sessionAccess("alice"),
          ResultFixture.SELECTED,
          1,
          new Messages.Declare(2, ResultFixture.operation(1), 0, java.util.List.of(1L), false));
      inputs =
          InputStore.initializeForAuthority(
              inputsPath, ResultFixture.INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
      header =
          new Records.InputHeader(
              1,
              ResultFixture.operation(2),
              new Records.AdmitParameters(
                  ResultFixture.WORK,
                  new Records.Input(
                      INPUT.length, ResultFixture.digest(INPUT), "application/octet-stream"),
                  "copy",
                  0,
                  1000,
                  new Records.OutputBudget(outputCount, OUTPUT_BUDGET * outputCount)));
      try (InputStore.Receiver receiver =
          inputs.begin(context(), header, ResultFixture.SELECTED, 1)) {
        receiver.write(ByteBuffer.wrap(INPUT), 2);
        receiver.finish(3);
      }
      sessions.admit(
          ResultFixture.sessionAccess("alice"),
          ResultFixture.SELECTED,
          1,
          inputs,
          header,
          3,
          ResultFixture.clock(1000),
          ResultFixture.ALLOW_EXECUTION);
      lease =
          sessions.claimExecution(
              ResultFixture.executionAccess("alice"),
              1,
              ResultFixture.WORK,
              inputs,
              500,
              ResultFixture.clock(1100),
              ResultFixture.ALLOW_EXECUTION);
      try (OutputStore.Writer writer =
          inputs.beginOutput(
              context(),
              header,
              lease,
              0,
              OUTPUT.length,
              "application/octet-stream",
              WRITER_LIMIT)) {
        writer.write(ByteBuffer.wrap(OUTPUT));
        writer.finish();
      }
      if (outputCount == 2) {
        try (OutputStore.Writer writer =
            inputs.beginOutput(
                context(),
                header,
                lease,
                1,
                SECOND.length,
                "application/octet-stream",
                WRITER_LIMIT)) {
          writer.write(ByteBuffer.wrap(SECOND));
          writer.finish();
        }
      }
      sessions.succeedExecution(
          ResultFixture.executionAccess("alice"),
          lease,
          inputs,
          outputCount,
          ResultFixture.ENDPOINT,
          ResultFixture.clock(1200),
          ResultFixture.ALLOW_EXECUTION);
    }

    void reopenInputs() throws Exception {
      inputs.close();
      inputs = InputStore.open(inputsPath, ResultFixture.INPUT_LIMITS);
      sessions.verifyInputs(inputs);
    }

    Commitments.Context context() {
      return new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
    }

    Records.WorkView current() throws Exception {
      return sessions
          .snapshot(
              ResultFixture.sessionAccess("alice"),
              ResultFixture.SELECTED,
              1,
              new Messages.Watch(90, ResultFixture.WORK, 0, 0))
          .work();
    }

    AdmissionStore.StoredJob job() throws Exception {
      try (var connection =
          BoundedSqlite.open(database, ResultFixture.configuration().files()).connect()) {
        return AdmissionStore.job(connection, binding, ResultFixture.WORK);
      }
    }

    @Override
    public void close() throws IOException {
      inputs.close();
    }
  }
}
