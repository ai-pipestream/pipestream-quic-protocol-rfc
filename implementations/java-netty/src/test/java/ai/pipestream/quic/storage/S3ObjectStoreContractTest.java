package ai.pipestream.quic.storage;

import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Certifies {@link S3ObjectStore} against the object-store contract on a real
 * MinIO container (see {@link MinioDocker} for the fixture lifecycle and the
 * docker requirement). Every test gets a fresh store under a unique key
 * prefix, so isolation needs no cleanup.
 */
final class S3ObjectStoreContractTest extends ObjectStoreContractTest {
  private static MinioDocker minio;

  private String prefix;

  @BeforeAll static void startMinio() {
    minio = MinioDocker.shared();
  }

  @AfterAll static void stopMinio() {
    MinioDocker.releaseShared();
  }

  @Override protected ObjectStore openStore() throws StorageException {
    prefix = "contract-" + UUID.randomUUID() + "/";
    return S3ObjectStore.open(minio.endpoint(), "us-east-1", minio.accessKey(), minio.secretKey(),
        true, minio.bucket(), prefix);
  }

  @Override protected boolean supportsRecordTamper() {
    return true;
  }

  @Override protected void tamperRecord(ScopePath scope, long entityId) throws Exception {
    String key = prefix + scope.render() + "/" + StoreLayout.fanoutBucket(entityId)
        + "/" + StoreLayout.entityName(entityId);
    try (var s3 = minio.newClient()) {
      var head = s3.headObject(request -> request.bucket(minio.bucket()).key(key));
      String recorded = head.metadata().get(S3ObjectStore.DIGEST_METADATA_KEY);
      byte[] payload;
      try (var stream = s3.getObject(request -> request.bucket(minio.bucket()).key(key))) {
        payload = stream.readAllBytes();
      }
      payload[payload.length - 1] ^= 1;
      s3.putObject(request -> request.bucket(minio.bucket()).key(key)
          .metadata(java.util.Map.of(S3ObjectStore.DIGEST_METADATA_KEY, recorded)),
          software.amazon.awssdk.core.sync.RequestBody.fromBytes(payload));
    }
  }

  @Override protected void tamperSealCount(ScopePath scope) throws Exception {
    String key = prefix + scope.render() + "/" + S3ObjectStore.SEAL_OBJECT_NAME;
    try (var s3 = minio.newClient()) {
      byte[] raw;
      try (var stream = s3.getObject(request -> request.bucket(minio.bucket()).key(key))) {
        raw = stream.readAllBytes();
      }
      long count = java.nio.ByteBuffer.wrap(raw, StoreLayout.DIGEST_BYTES, 8).getLong();
      java.nio.ByteBuffer.wrap(raw, StoreLayout.DIGEST_BYTES, 8).putLong(count + 1);
      s3.putObject(request -> request.bucket(minio.bucket()).key(key),
          software.amazon.awssdk.core.sync.RequestBody.fromBytes(raw));
    }
  }
}
