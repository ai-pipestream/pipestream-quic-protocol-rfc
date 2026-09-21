package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * S3-backend specifics on a real MinIO container: key layout, seal object
 * format, container stop/start persistence, second-opener behavior, and key
 * prefix isolation. See {@link MinioDocker} for the fixture lifecycle and the
 * docker requirement.
 */
final class S3BackendTest {
  private static MinioDocker minio;

  @BeforeAll static void startMinio() {
    minio = MinioDocker.shared();
  }

  @AfterAll static void stopMinio() {
    MinioDocker.releaseShared();
  }

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static byte[] digest(byte[] bytes) {
    return StoreLayout.sha256().digest(bytes);
  }

  private static ObjectStore openStore(String prefix) throws StorageException {
    return S3ObjectStore.open(minio.endpoint(), "us-east-1", minio.accessKey(), minio.secretKey(),
        true, minio.bucket(), prefix);
  }

  @Test void keyLayoutFollowsSharedLayoutAndSealFormat() throws Exception {
    var scope = ScopePath.of(0x01020304, 0x05060708);
    byte[] payload = bytes("s3-layout");
    String prefix = "layout-" + java.util.UUID.randomUUID() + "/";
    try (ObjectStore store = openStore(prefix)) {
      store.put(scope, 0x01020304, payload, digest(payload));
      store.sealScope(scope, digest(payload), 1);
    }
    String expectedEntity = prefix + "01020304/05060708/" + StoreLayout.fanoutBucket(0x01020304)
        + "/" + StoreLayout.entityName(0x01020304);
    String expectedSeal = prefix + "01020304/05060708/" + S3ObjectStore.SEAL_OBJECT_NAME;
    try (var s3 = minio.newClient()) {
      var entity = s3.headObject(request -> request.bucket(minio.bucket()).key(expectedEntity));
      assertArrayEquals(digest(payload),
          java.util.HexFormat.of().parseHex(entity.metadata().get(S3ObjectStore.DIGEST_METADATA_KEY)));
      byte[] seal;
      try (var stream = s3.getObject(request -> request.bucket(minio.bucket()).key(expectedSeal))) {
        seal = stream.readAllBytes();
      }
      assertEquals(StoreLayout.DIGEST_BYTES + 8, seal.length);
      assertArrayEquals(digest(payload), java.util.Arrays.copyOf(seal, StoreLayout.DIGEST_BYTES));
      assertEquals(1, ByteBuffer.wrap(seal, StoreLayout.DIGEST_BYTES, 8).getLong());
    }
  }

  @Test void contentSurvivesContainerStopAndStart() throws Exception {
    var scope = ScopePath.root(0x1111);
    byte[] payload = bytes("s3-survives-restart");
    String prefix = "restart-" + java.util.UUID.randomUUID() + "/";
    try (ObjectStore store = openStore(prefix)) {
      store.put(scope, 7, payload, digest(payload));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(7, digest(payload)))), 1);
      minio.stopServer();
    }
    minio.startServer();
    try (ObjectStore reopened = openStore(prefix)) {
      ObjectStore.SealRecord seal = reopened.verifyScope(scope);
      assertArrayEquals(StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(7, digest(payload)))), seal.digest());
      try (var stream = reopened.openRead(scope, 7)) {
        assertArrayEquals(payload, stream.readAllBytes());
      }
    }
  }

  @Test void secondOpenerIsRefusedUntilClose() throws Exception {
    String prefix = "second-" + java.util.UUID.randomUUID() + "/";
    ObjectStore first = openStore(prefix);
    assertStorage(StorageException.Kind.FAILED, () -> openStore(prefix));
    first.close();
    try (ObjectStore second = openStore(prefix)) {
      assertEquals(List.of(), second.list(ScopePath.root(0x2222)));
    }
  }

  @Test void keyPrefixesIsolateStores() throws Exception {
    var scope = ScopePath.root(0x3333);
    byte[] fromA = bytes("store-a"), fromB = bytes("store-b");
    try (ObjectStore a = openStore("iso-a-" + java.util.UUID.randomUUID() + "/");
         ObjectStore b = openStore("iso-b-" + java.util.UUID.randomUUID() + "/")) {
      a.put(scope, 1, fromA, digest(fromA));
      assertEquals(List.of(), b.list(scope), "other prefix sees nothing");
      assertStorage(StorageException.Kind.NOT_FOUND, () -> b.openRead(scope, 1));
      // Same scope and entity id under another prefix is an independent object.
      b.put(scope, 1, fromB, digest(fromB));
      try (var stream = a.openRead(scope, 1)) {
        assertArrayEquals(fromA, stream.readAllBytes());
      }
      try (var stream = b.openRead(scope, 1)) {
        assertArrayEquals(fromB, stream.readAllBytes());
      }
    }
  }

  @Test void listScopesSkipsForeignNamespacesSharingThePrefix() throws Exception {
    String prefix = "shared-" + java.util.UUID.randomUUID() + "/";
    var scope = ScopePath.root(0x4444);
    byte[] payload = bytes("ours");
    try (ObjectStore store = openStore(prefix)) {
      store.put(scope, 1, payload, digest(payload));
      // Another lane's S3 layout (its own objects/ subtree) shares our bucket
      // and prefix, as certification deployments do; enumeration must not
      // treat its namespace as corruption.
      try (var raw = minio.newClient()) {
        raw.putObject(
            request -> request.bucket(minio.bucket()).key(prefix + "objects/00000040/43/00000000"),
            software.amazon.awssdk.core.sync.RequestBody.fromBytes(bytes("foreign")));
      }
      assertEquals(List.of(scope), store.listScopes(), "foreign subtree is skipped, ours is found");
      try (var stream = store.openRead(scope, 1)) {
        assertArrayEquals(payload, stream.readAllBytes());
      }
    }
  }
}
