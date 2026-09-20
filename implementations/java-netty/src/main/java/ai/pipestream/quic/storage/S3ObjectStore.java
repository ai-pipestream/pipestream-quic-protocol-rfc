package ai.pipestream.quic.storage;

import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Durable S3-compatible {@link ObjectStore} (object role only; per the storage
 * design note S3 has no journal role). The shared logical layout maps to keys:
 * the scope path renders as a key prefix, entities live at
 * {@code <prefix><scope>/<bucket>/<8-hex entity id>} with the 2-hex fanout
 * bucket, and one seal object per scope at {@code <prefix><scope>/scope.seal}
 * holds {@code [32B digest][8B count]} exactly like the filesystem backend.
 * Each entity object records its content digest as S3 object metadata, which
 * {@link #verifyScope} recomputes against the payload bytes.
 *
 * <p>Semantics follow what S3 guarantees. A {@code PutObject} that returns
 * success is durable (HTTP 200 from the service; MinIO or multi-node durability
 * is deployment configuration outside this backend). Single-object writes are
 * atomic, and conditional creation ({@code If-None-Match: *}) makes
 * {@link #put} single-writer per key: a conflicting concurrent put fails the
 * precondition, reads the winner back, and reports Corrupt. {@code sealScope}
 * is a single-object put; a seal that lands while a put is mid-flight is
 * detectable afterwards as a verify count mismatch, which is the documented
 * S3 limit of atomicity. Listing and verification rely on S3's read-after-write
 * consistency. The bucket must exist and must not use object versioning with
 * retention holds; administration is deployment configuration and {@code open}
 * does not create buckets.
 *
 * <p>The AWS SDK v2 {@link S3Client} is thread-safe, so no additional locking
 * is needed. One live store per endpoint, bucket, and prefix may exist in a
 * JVM, mirroring the other backends' single-writer convention. The client is
 * blocking, matching the synchronous SPI.
 */
public final class S3ObjectStore implements ObjectStore {
  /** Name of the per-scope seal object, matching the filesystem backend. */
  public static final String SEAL_OBJECT_NAME = "scope.seal";
  /** S3 object metadata key holding the put-time SHA-256 of the payload, as lowercase hex. */
  public static final String DIGEST_METADATA_KEY = "pipestream-digest";
  private static final String SEAL_FORMAT = "seal object holds [32B digest][8B count]";
  private static final int DELETE_BATCH = 1000;
  private static final Set<String> OPEN_IDENTITIES = ConcurrentHashMap.newKeySet();

  private final String identity;
  private final S3Client s3;
  private final String bucket;
  private final String prefix;
  private volatile boolean closed;

  private S3ObjectStore(String identity, S3Client s3, String bucket, String prefix) {
    this.identity = identity;
    this.s3 = s3;
    this.bucket = bucket;
    this.prefix = prefix;
  }

  /**
   * Opens an exclusively owned object store, creating the blocking S3 client
   * from the supplied endpoint configuration. For MinIO, pass the container
   * endpoint (for example {@code http://127.0.0.1:9000}), any region, the
   * root credentials, and {@code pathStyleAccess = true}.
   *
   * @param endpoint service endpoint, or null for the AWS default of the region
   * @param region signing region, for example {@code us-east-1}
   * @param accessKey service access key
   * @param secretKey service secret key
   * @param pathStyleAccess true for path-style addressing (MinIO and most S3-compatible stores)
   * @param bucket existing bucket that this store exclusively owns
   * @param keyPrefix key prefix of this store, empty or ending in '/'; leading '/' is stripped
   * @return exclusive store handle
   * @throws StorageException {@code FAILED} for invalid configuration or a second live store on the same identity
   */
  public static S3ObjectStore open(URI endpoint, String region, String accessKey, String secretKey,
      boolean pathStyleAccess, String bucket, String keyPrefix) throws StorageException {
    Objects.requireNonNull(region, "region");
    Objects.requireNonNull(accessKey, "accessKey");
    Objects.requireNonNull(secretKey, "secretKey");
    Objects.requireNonNull(bucket, "bucket");
    Objects.requireNonNull(keyPrefix, "keyPrefix");
    if (bucket.isEmpty()) throw new IllegalArgumentException("bucket must be non-empty");
    String normalized = keyPrefix.startsWith("/") ? keyPrefix.substring(1) : keyPrefix;
    if (!normalized.isEmpty() && !normalized.endsWith("/")) normalized = normalized + "/";
    String identity = "s3\n" + endpoint + "\n" + bucket + "\n" + normalized;
    if (!OPEN_IDENTITIES.add(identity)) {
      throw StorageException.failed("s3 object store already has a writer for this identity", null);
    }
    try {
      S3Client s3 = S3Client.builder()
          .endpointOverride(endpoint)
          .region(Region.of(region))
          .credentialsProvider(StaticCredentialsProvider.create(
              AwsBasicCredentials.create(accessKey, secretKey)))
          .serviceConfiguration(S3Configuration.builder()
              .pathStyleAccessEnabled(pathStyleAccess).build())
          .build();
      return new S3ObjectStore(identity, s3, bucket, normalized);
    } catch (RuntimeException failure) {
      OPEN_IDENTITIES.remove(identity);
      throw StorageException.failed("s3 client creation failed", failure);
    }
  }

  @Override public void put(ScopePath scope, long entityId, byte[] bytes, byte[] expectedDigest)
      throws StorageException {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(bytes, "bytes");
    validateEntityId(entityId);
    byte[] expected = validateDigest(expectedDigest);
    checkOpen();
    if (exists(sealKey(scope))) {
      throw StorageException.alreadySealed("scope " + scope.render() + " is sealed");
    }
    byte[] actual = StoreLayout.sha256().digest(bytes);
    if (!MessageDigest.isEqual(actual, expected)) {
      throw StorageException.corrupt("put digest mismatch for entity " + entityId);
    }
    try {
      PutObjectRequest request = PutObjectRequest.builder()
          .bucket(bucket).key(entityKey(scope, entityId))
          .contentLength((long) bytes.length)
          .metadata(java.util.Map.of(DIGEST_METADATA_KEY, HexFormat.of().formatHex(expected)))
          .ifNoneMatch("*")
          .build();
      s3.putObject(request, RequestBody.fromBytes(bytes));
    } catch (S3Exception failure) {
      if (failure.statusCode() == 412) {
        // A concurrent put won the key; the winner decides no-op vs Corrupt.
        byte[] stored = readEntityBytes(scope, entityId);
        if (Arrays.equals(stored, bytes)) return;
        throw StorageException.corrupt("conflicting content for entity " + entityId);
      }
      throw mapS3Exception(failure, "put");
    } catch (SdkClientException failure) {
      throw StorageException.transientFailure("put failed", failure);
    }
  }

  @Override public InputStream openRead(ScopePath scope, long entityId) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    validateEntityId(entityId);
    checkOpen();
    try {
      ResponseInputStream<GetObjectResponse> stream =
          s3.getObject(GetObjectRequest.builder().bucket(bucket).key(entityKey(scope, entityId)).build());
      return stream;
    } catch (S3Exception failure) {
      throw mapS3Exception(failure, "openRead");
    } catch (SdkClientException failure) {
      throw StorageException.transientFailure("openRead failed", failure);
    }
  }

  @Override public void sealScope(ScopePath scope, byte[] digest, long entityCount) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    byte[] recorded = validateDigest(digest);
    if (entityCount < 0) throw new IllegalArgumentException("entity count must be non-negative");
    checkOpen();
    byte[] existing = readSealBytes(scope);
    if (existing != null) {
      Seal existingSeal = parseSeal(scope, existing);
      if (existingSeal.entityCount == entityCount && Arrays.equals(existingSeal.digest, recorded)) return;
      throw StorageException.corrupt("scope " + scope.render() + " already carries a different seal");
    }
    byte[] body = new byte[StoreLayout.DIGEST_BYTES + 8];
    System.arraycopy(recorded, 0, body, 0, StoreLayout.DIGEST_BYTES);
    ByteBufferHelper.putLong(body, StoreLayout.DIGEST_BYTES, entityCount);
    try {
      s3.putObject(PutObjectRequest.builder().bucket(bucket).key(sealKey(scope))
          .contentLength((long) body.length).build(), RequestBody.fromBytes(body));
    } catch (S3Exception failure) {
      throw mapS3Exception(failure, "sealScope");
    } catch (SdkClientException failure) {
      throw StorageException.transientFailure("sealScope failed", failure);
    }
  }

  @Override public SealRecord verifyScope(ScopePath scope) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    checkOpen();
    byte[] raw = readSealBytes(scope);
    if (raw == null) throw StorageException.notFound("scope " + scope.render() + " has no seal record");
    Seal seal = parseSeal(scope, raw);
    long live = 0;
    for (String key : listEntityKeys(scope)) {
      verifyEntity(scope, key);
      live++;
    }
    if (live != seal.entityCount) {
      throw StorageException.corrupt("scope " + scope.render() + " entity count differs from its seal");
    }
    return new SealRecord(seal.digest, seal.entityCount);
  }

  @Override public List<Long> list(ScopePath scope) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    checkOpen();
    Set<Long> ids = new TreeSet<>();
    for (String key : listEntityKeys(scope)) {
      ids.add(entityIdFromKey(scope, key));
    }
    return List.copyOf(ids);
  }

  @Override public void retire(ScopePath scope, long retentionCursor) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    if (retentionCursor < 0) throw new IllegalArgumentException("retention cursor must be non-negative");
    checkOpen();
    boolean sealed = exists(sealKey(scope));
    List<String> keys = listEntityKeys(scope);
    if (sealed) {
      List<String> all = new ArrayList<>(keys);
      all.add(sealKey(scope));
      deleteBatch(all);
      return;
    }
    if (keys.isEmpty()) {
      throw StorageException.notFound("scope " + scope.render() + " holds nothing to retire");
    }
    List<String> past = new ArrayList<>();
    for (String key : keys) {
      if (entityIdFromKey(scope, key) <= retentionCursor) past.add(key);
    }
    deleteBatch(past);
  }

  /**
   * Marks the store closed and releases the client and the single-writer
   * identity. Idempotent; a failed close still releases the identity.
   *
   * @throws StorageException {@code FAILED} if the client close fails
   */
  @Override public void close() throws StorageException {
    if (closed) return;
    closed = true;
    try {
      s3.close();
    } catch (RuntimeException failure) {
      throw StorageException.failed("s3 client close failed", failure);
    } finally {
      OPEN_IDENTITIES.remove(identity);
    }
  }

  private record Seal(byte[] digest, long entityCount) {}

  private void verifyEntity(ScopePath scope, String key) throws StorageException {
    try {
      ResponseInputStream<GetObjectResponse> stream =
          s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
      try (stream) {
        byte[] payload = stream.readAllBytes();
        String recorded = stream.response().metadata().get(DIGEST_METADATA_KEY);
        if (recorded == null) {
          throw StorageException.corrupt("entity " + key + " carries no recorded digest");
        }
        byte[] recordedDigest = HexFormat.of().parseHex(recorded);
        if (!MessageDigest.isEqual(StoreLayout.sha256().digest(payload), recordedDigest)) {
          throw StorageException.corrupt("entity under scope " + scope.render() + " fails its recorded digest");
        }
      }
    } catch (S3Exception failure) {
      throw mapS3Exception(failure, "verifyScope");
    } catch (java.io.IOException failure) {
      throw StorageException.transientFailure("verifyScope read failed", failure);
    } catch (SdkClientException failure) {
      throw StorageException.transientFailure("verifyScope failed", failure);
    }
  }

  /** Returns the entity payload bytes, or null when the object is absent. */
  private byte[] readEntityBytes(ScopePath scope, long entityId) throws StorageException {
    try {
      ResponseInputStream<GetObjectResponse> stream =
          s3.getObject(GetObjectRequest.builder().bucket(bucket).key(entityKey(scope, entityId)).build());
      try (stream) {
        return stream.readAllBytes();
      }
    } catch (S3Exception failure) {
      if (failure.statusCode() == 404) {
        // The winner's object vanished between the precondition failure and
        // the read-back; treat as our own write having landed nowhere.
        throw StorageException.transientFailure("put conflict read-back raced a delete", failure);
      }
      throw mapS3Exception(failure, "read-back");
    } catch (java.io.IOException failure) {
      throw StorageException.transientFailure("read-back failed", failure);
    } catch (SdkClientException failure) {
      throw StorageException.transientFailure("read-back failed", failure);
    }
  }

  /** Returns the raw seal object bytes, or null when the seal is absent. */
  private byte[] readSealBytes(ScopePath scope) throws StorageException {
    try {
      ResponseInputStream<GetObjectResponse> stream =
          s3.getObject(GetObjectRequest.builder().bucket(bucket).key(sealKey(scope)).build());
      try (stream) {
        return stream.readAllBytes();
      }
    } catch (S3Exception failure) {
      if (failure.statusCode() == 404) return null;
      throw mapS3Exception(failure, "seal read");
    } catch (java.io.IOException failure) {
      throw StorageException.transientFailure("seal read failed", failure);
    } catch (SdkClientException failure) {
      throw StorageException.transientFailure("seal read failed", failure);
    }
  }

  private boolean exists(String key) throws StorageException {
    try {
      s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
      return true;
    } catch (S3Exception failure) {
      if (failure.statusCode() == 404) return false;
      throw mapS3Exception(failure, "head");
    } catch (SdkClientException failure) {
      throw StorageException.transientFailure("head failed", failure);
    }
  }

  /** Lists every entity key directly under one scope, paginated; folder markers are skipped. */
  private List<String> listEntityKeys(ScopePath scope) throws StorageException {
    String scopePrefix = scopePrefix(scope);
    List<String> keys = new ArrayList<>();
    String continuation = null;
    try {
      do {
        ListObjectsV2Request.Builder request = ListObjectsV2Request.builder()
            .bucket(bucket).prefix(scopePrefix).maxKeys(1000);
        if (continuation != null) request.continuationToken(continuation);
        ListObjectsV2Response response = s3.listObjectsV2(request.build());
        for (S3Object object : response.contents()) {
          String remainder = object.key().substring(scopePrefix.length());
          if (remainder.endsWith("/")) continue; // console folder markers
          if (remainder.equals(SEAL_OBJECT_NAME)) continue; // the seal object, verified separately
          // Keys under an 8-hex first component belong to child scopes.
          if (remainder.matches("[0-9a-f]{8}/.*")) continue;
          if (!remainder.matches("[0-9a-f]{2}/[0-9a-f]{8}")) {
            throw StorageException.corrupt("unexpected key " + object.key() + " under scope " + scope.render());
          }
          keys.add(object.key());
        }
        continuation = response.isTruncated() ? response.nextContinuationToken() : null;
      } while (continuation != null);
      return keys;
    } catch (S3Exception failure) {
      throw mapS3Exception(failure, "list");
    } catch (SdkClientException failure) {
      throw StorageException.transientFailure("list failed", failure);
    }
  }

  private void deleteBatch(List<String> keys) throws StorageException {
    try {
      for (int offset = 0; offset < keys.size(); offset += DELETE_BATCH) {
        List<ObjectIdentifier> objects = new ArrayList<>();
        for (String key : keys.subList(offset, Math.min(offset + DELETE_BATCH, keys.size()))) {
          objects.add(ObjectIdentifier.builder().key(key).build());
        }
        s3.deleteObjects(DeleteObjectsRequest.builder().bucket(bucket)
            .delete(Delete.builder().objects(objects).build()).build());
      }
    } catch (S3Exception failure) {
      throw mapS3Exception(failure, "retire");
    } catch (SdkClientException failure) {
      throw StorageException.transientFailure("retire failed", failure);
    }
  }

  private long entityIdFromKey(ScopePath scope, String key) {
    return Long.parseLong(key.substring(scopePrefix(scope).length() + 3), 16);
  }

  private String scopePrefix(ScopePath scope) {
    return prefix + scope.render() + "/";
  }

  private String entityKey(ScopePath scope, long entityId) {
    return scopePrefix(scope) + StoreLayout.fanoutBucket(entityId) + "/" + StoreLayout.entityName(entityId);
  }

  private String sealKey(ScopePath scope) {
    return scopePrefix(scope) + SEAL_OBJECT_NAME;
  }

  private Seal parseSeal(ScopePath scope, byte[] raw) throws StorageException {
    if (raw.length != StoreLayout.DIGEST_BYTES + 8) {
      throw StorageException.corrupt("seal object of scope " + scope.render() + " has wrong length");
    }
    return new Seal(Arrays.copyOf(raw, StoreLayout.DIGEST_BYTES),
        ByteBufferHelper.getLong(raw, StoreLayout.DIGEST_BYTES));
  }

  private StorageException mapS3Exception(S3Exception failure, String operation) {
    int status = failure.statusCode();
    if (status == 404) return StorageException.notFound(operation + " found no object");
    if (status == 429 || status == 408 || status >= 500) {
      return StorageException.transientFailure(operation + " failed", failure);
    }
    return StorageException.failed(operation + " failed", failure);
  }

  private void checkOpen() throws StorageException {
    if (closed) throw StorageException.failed("object store is closed", null);
  }

  private static void validateEntityId(long entityId) {
    if (entityId < 0 || entityId > StoreLayout.MAX_ENTITY_ID) {
      throw new IllegalArgumentException("entity identifier is not unsigned 32-bit");
    }
  }

  private static byte[] validateDigest(byte[] digest) {
    Objects.requireNonNull(digest, "digest");
    if (digest.length != StoreLayout.DIGEST_BYTES) {
      throw new IllegalArgumentException("digest must be " + StoreLayout.DIGEST_BYTES + " octets");
    }
    return digest;
  }

  /** Big-endian long encoding over byte arrays, shared by the seal object format. */
  private static final class ByteBufferHelper {
    private ByteBufferHelper() {}

    static void putLong(byte[] target, int offset, long value) {
      java.nio.ByteBuffer.wrap(target, offset, 8).putLong(value);
    }

    static long getLong(byte[] source, int offset) {
      return java.nio.ByteBuffer.wrap(source, offset, 8).getLong();
    }
  }
}
