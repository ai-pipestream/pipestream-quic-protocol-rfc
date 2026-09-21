package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.storage.MongoDocker;
import ai.pipestream.quic.storage.MongoObjectStore;
import ai.pipestream.quic.storage.ObjectStore;
import ai.pipestream.quic.storage.ScopePath;
import com.mongodb.client.MongoClients;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Shared-database race proof, mirroring the Rust lane's
 * {@code spi_mongo_shared_database_installations_do_not_sweep_each_other}:
 * the conformance matrix points many authority installations at one service
 * database, and the open-time orphan sweep must retire only scopes under this
 * installation's plane tag. Installation B opening its store runs the sweep
 * while installation A's payloads carry no sidecar B can see; A's entities
 * must survive, and A's later reopen must leave B's payloads alone. See
 * {@link MongoDocker} for the fixture lifecycle and the docker requirement.
 */
final class InputStoreSpiPlaneSharedDatabaseTest {
  private static final InputStore.Limits LIMITS = new InputStore.Limits(16L << 20, 32, 8 << 20, 8);
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65536, 4, 16, 1 << 20, 1000, 5000);
  private static final Commitments.Context CONTEXT =
      new Commitments.Context("issuer-a", "alice", 1);

  private static MongoDocker mongo;

  @TempDir Path directory;

  @BeforeAll static void startMongo() {
    mongo = MongoDocker.shared();
  }

  @AfterAll static void stopMongo() {
    MongoDocker.releaseShared();
  }

  @Test void installationsSharingOneDatabaseDoNotSweepEachOther() throws Exception {
    String database = "shared-" + UUID.randomUUID().toString().replace("-", "");
    byte[] payloadA = pattern(23, 5);
    byte[] payloadB = pattern(41, 9);
    Records.InputHeader headerA = header(1, payloadA, "shared-a");
    Records.InputHeader headerB = header(2, payloadB, "shared-b");
    Path rootA = directory.resolve("a");
    Path rootB = directory.resolve("b");

    // Installation A commits its payload, then releases the shared database.
    ScopePath scopeA;
    try (ObjectStore backendA = MongoObjectStore.open(mongo.connectionString(), database);
        InputStore storeA =
            InputStore.initializeForAuthority(rootA, LIMITS, UUID.randomUUID(), backendA)) {
      install(storeA, headerA, payloadA);
      scopeA = storeA.spiScopeFor(only(rootA.resolve("objects"), "*.input"));
    }

    // Installation B opens against the same database, so its orphan sweep
    // runs over A's still-referenced scopes; then B commits its own payload.
    ScopePath scopeB;
    try (ObjectStore backendB = MongoObjectStore.open(mongo.connectionString(), database);
        InputStore storeB =
            InputStore.initializeForAuthority(rootB, LIMITS, UUID.randomUUID(), backendB)) {
      assertEquals(
          1,
          entityCount(database),
          "B's open-time sweep leaves A's entity alone");
      install(storeB, headerB, payloadB);
      scopeB = storeB.spiScopeFor(only(rootB.resolve("objects"), "*.input"));
    }
    assertNotEquals(
        scopeA.components().get(0),
        scopeB.components().get(0),
        "installations derive different plane tags from their store identities");

    // A reopens: its sweep must leave B's payload alone, and B's earlier open
    // already proved the same for A's.
    try (ObjectStore backendA = MongoObjectStore.open(mongo.connectionString(), database);
        InputStore storeA = InputStore.open(rootA, LIMITS, backendA, null)) {
      assertArrayEquals(payloadA, read(storeA, headerA), "A reads its payload after B's sweep");
    }
    try (ObjectStore backendB = MongoObjectStore.open(mongo.connectionString(), database);
        InputStore storeB = InputStore.open(rootB, LIMITS, backendB, null)) {
      assertArrayEquals(payloadB, read(storeB, headerB), "B reads its payload after A's sweep");
    }
    assertEquals(2, entityCount(database), "both installations' scopes survive every sweep");
  }

  private static long entityCount(String database) {
    try (var client = MongoClients.create(mongo.connectionString())) {
      return client.getDatabase(database).getCollection("objects").countDocuments();
    }
  }

  private static void install(InputStore store, Records.InputHeader header, byte[] payload)
      throws IOException {
    try (InputStore.Receiver receiver = store.begin(CONTEXT, header, SELECTED, 1)) {
      receiver.write(ByteBuffer.wrap(payload), 2);
      assertArrayEquals(payload, read(receiver.finish(3)));
    }
  }

  private static byte[] read(InputStore store, Records.InputHeader header) throws IOException {
    return read(store.find(CONTEXT, header).orElseThrow());
  }

  private static byte[] read(InputStore.Stored stored) throws IOException {
    try (InputStream input = stored.openStream()) {
      return input.readAllBytes();
    }
  }

  private static Records.InputHeader header(int id, byte[] payload, String application)
      throws Exception {
    byte[] operation = new byte[16];
    operation[15] = (byte) id;
    return new Records.InputHeader(
        1,
        new Records.OperationId(operation),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, id),
            new Records.Input(
                payload.length,
                new Records.Digest(MessageDigest.getInstance("SHA-256").digest(payload)),
                "application/octet-stream"),
            application,
            0,
            1000,
            new Records.OutputBudget(0, 0)));
  }

  private static byte[] pattern(int length, int seed) {
    byte[] bytes = new byte[length];
    for (int index = 0; index < length; index++) bytes[index] = (byte) (seed + index * 31);
    return bytes;
  }

  private static Path only(Path directory, String glob) throws IOException {
    List<Path> matches = new ArrayList<>();
    try (var entries = Files.newDirectoryStream(directory, glob)) {
      entries.forEach(matches::add);
    }
    assertEquals(1, matches.size(), "expected exactly one " + glob + " in " + directory);
    return matches.get(0);
  }
}
