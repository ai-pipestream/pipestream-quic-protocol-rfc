package ai.pipestream.quic.storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Durable filesystem {@link ObjectStore} and the reference default backend.
 * The scope tree maps to directories, entities shard into 2-hex-character
 * fanout buckets within their leaf, and one seal record holding the scope
 * digest and entity count lives beside the objects. Every mutation writes to a
 * scratch file under the configured root, flushes it to stable storage, moves
 * it into place, and flushes the containing directories before returning
 * success, so success implies survival of a process kill. Scratch is never
 * placed in an ambient temp directory and is swept on open and close. One
 * cooperating process owns a store root.
 */
public final class FileObjectStore implements ObjectStore {
  /** Name of the per-scope seal record inside its leaf directory. */
  public static final String SEAL_FILE_NAME = "scope.seal";
  /** Entity file layout: 8-octet big-endian payload length, 32-octet digest, payload. */
  static final int ENTITY_HEADER_BYTES = 8 + StoreLayout.DIGEST_BYTES;
  /** Seal record layout: 32-octet digest, 8-octet big-endian entity count. */
  static final int SEAL_BYTES = StoreLayout.DIGEST_BYTES + 8;
  private static final String OBJECTS_DIRECTORY = "objects";
  private static final String SCRATCH_PREFIX = "write-";
  private static final String SCRATCH_SUFFIX = ".tmp";
  private static final Set<Path> OPEN_ROOTS = ConcurrentHashMap.newKeySet();

  private record Seal(byte[] digest, long entityCount) {}

  private final Path root;
  private final Path objects;
  private final FileChannel lockChannel;
  private final FileLock lock;
  private final Object mutationLock = new Object();
  private final Set<Path> scratchPaths = ConcurrentHashMap.newKeySet();
  private volatile boolean closed;

  private FileObjectStore(Path root, Path objects, FileChannel lockChannel, FileLock lock) {
    this.root = root;
    this.objects = objects;
    this.lockChannel = lockChannel;
    this.lock = lock;
  }

  /**
   * Opens (creating if necessary) an exclusively owned filesystem store under
   * {@code root}. Abandoned scratch files from a killed predecessor are
   * removed before the store accepts operations.
   *
   * @param root configured store root; all files live under it
   * @return exclusive store handle
   * @throws StorageException {@code TRANSIENT} for filesystem failure, {@code FAILED} for a second writer
   */
  public static FileObjectStore open(Path root) throws StorageException {
    Objects.requireNonNull(root, "root");
    Path requested = root.toAbsolutePath().normalize();
    if (Files.exists(requested, LinkOption.NOFOLLOW_LINKS)
        && !Files.isDirectory(requested, LinkOption.NOFOLLOW_LINKS)) {
      throw StorageException.failed("object store root is not a real directory", null);
    }
    try {
      Files.createDirectories(requested);
      Path base = requested.toRealPath();
      Path objects = base.resolve(OBJECTS_DIRECTORY);
      createDirectoriesDurable(objects);
      // Closing a second channel can release another channel's OS lock in this JVM.
      boolean registered = false;
      synchronized (OPEN_ROOTS) {
        if (!OPEN_ROOTS.add(objects)) throw StorageException.failed("object store already has a writer", null);
        registered = true;
      }
      FileChannel channel = null;
      FileLock held = null;
      try {
        channel = FileChannel.open(objects.resolve("writer.lock"), StandardOpenOption.CREATE,
            StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
        try {
          held = channel.tryLock();
        } catch (OverlappingFileLockException failure) {
          throw StorageException.failed("object store already has a writer", failure);
        }
        if (held == null) throw StorageException.failed("object store already has a writer", null);
        var store = new FileObjectStore(base, objects, channel, held);
        store.sweepScratch(objects);
        return store;
      } catch (StorageException | RuntimeException failure) {
        if (held != null) try { held.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
        if (channel != null) try { channel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
        if (registered) synchronized (OPEN_ROOTS) { OPEN_ROOTS.remove(objects); }
        throw failure;
      }
    } catch (IOException failure) {
      throw StorageException.transientFailure("object store open failed", failure);
    }
  }

  @Override public void put(ScopePath scope, long entityId, byte[] bytes, byte[] expectedDigest)
      throws StorageException {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(bytes, "bytes");
    validateEntityId(entityId);
    byte[] expected = validateDigest(expectedDigest);
    checkOpen();
    synchronized (mutationLock) {
      checkOpen();
      Path scopeDir = scopeDirectory(scope);
      if (Files.isRegularFile(scopeDir.resolve(SEAL_FILE_NAME), LinkOption.NOFOLLOW_LINKS)) {
        throw StorageException.alreadySealed("scope " + scope.render() + " is sealed");
      }
      byte[] actual = StoreLayout.sha256().digest(bytes);
      if (!MessageDigest.isEqual(actual, expected)) {
        throw StorageException.corrupt("put digest mismatch for entity " + entityId);
      }
      Path target = entityPath(scopeDir, entityId);
      if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
        byte[] existing = readEntity(target);
        if (Arrays.equals(existing, bytes)) return;
        throw StorageException.corrupt("conflicting content for entity " + entityId);
      }
      Path leaf = target.getParent();
      try {
        createDirectoriesDurable(leaf);
      } catch (IOException failure) {
        throw StorageException.transientFailure("scope directory creation failed", failure);
      }
      Path scratch = leaf.resolve(SCRATCH_PREFIX + UUID.randomUUID() + SCRATCH_SUFFIX);
      scratchPaths.add(scratch);
      try {
        writeEntity(scratch, bytes, expected);
        moveIntoPlace(scratch, target);
      } catch (StorageException | RuntimeException failure) {
        try {
          Files.deleteIfExists(scratch);
        } catch (IOException cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      } finally {
        scratchPaths.remove(scratch);
      }
    }
  }

  @Override public InputStream openRead(ScopePath scope, long entityId) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    validateEntityId(entityId);
    checkOpen();
    Path target = entityPath(scopeDirectory(scope), entityId);
    if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
      throw StorageException.notFound("entity " + entityId + " is absent from scope " + scope.render());
    }
    try {
      FileChannel channel = FileChannel.open(target, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
      if (channel.size() < ENTITY_HEADER_BYTES) {
        channel.close();
        throw StorageException.corrupt("entity " + entityId + " is truncated");
      }
      channel.position(ENTITY_HEADER_BYTES);
      return Channels.newInputStream(channel);
    } catch (IOException failure) {
      throw StorageException.transientFailure("entity open failed", failure);
    }
  }

  @Override public void sealScope(ScopePath scope, byte[] digest, long entityCount) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    byte[] recorded = validateDigest(digest);
    if (entityCount < 0) throw new IllegalArgumentException("entity count must be non-negative");
    checkOpen();
    synchronized (mutationLock) {
      checkOpen();
      Path scopeDir = scopeDirectory(scope);
      Seal existing = readSeal(scopeDir);
      if (existing != null) {
        if (existing.entityCount == entityCount && Arrays.equals(existing.digest, recorded)) return;
        throw StorageException.corrupt("scope " + scope.render() + " already carries a different seal");
      }
      try {
        createDirectoriesDurable(scopeDir);
      } catch (IOException failure) {
        throw StorageException.transientFailure("scope directory creation failed", failure);
      }
      Path scratch = scopeDir.resolve(SCRATCH_PREFIX + UUID.randomUUID() + SCRATCH_SUFFIX);
      scratchPaths.add(scratch);
      try {
        ByteBuffer body = ByteBuffer.allocate(SEAL_BYTES).put(recorded).putLong(entityCount);
        writeFullyAndSync(scratch, body.array());
        moveIntoPlace(scratch, scopeDir.resolve(SEAL_FILE_NAME));
      } catch (StorageException | RuntimeException failure) {
        try {
          Files.deleteIfExists(scratch);
        } catch (IOException cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      } finally {
        scratchPaths.remove(scratch);
      }
    }
  }

  @Override public SealRecord verifyScope(ScopePath scope) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    checkOpen();
    synchronized (mutationLock) {
      checkOpen();
      Path scopeDir = scopeDirectory(scope);
      Seal seal = readSeal(scopeDir);
      if (seal == null) {
        throw StorageException.notFound("scope " + scope.render() + " has no seal record");
      }
      long live = 0;
      for (Long entityId : listEntityIds(scopeDir, true)) {
        readEntityDigest(scopeDir, entityId);
        live++;
      }
      if (live != seal.entityCount) {
        throw StorageException.corrupt("scope " + scope.render() + " entity count differs from its seal");
      }
      return new SealRecord(seal.digest, seal.entityCount);
    }
  }

  @Override public List<Long> list(ScopePath scope) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    checkOpen();
    synchronized (mutationLock) {
      checkOpen();
      Path scopeDir = scopeDirectory(scope);
      if (!Files.isDirectory(scopeDir, LinkOption.NOFOLLOW_LINKS)) return List.of();
      return listEntityIds(scopeDir, false);
    }
  }

  @Override public List<ScopePath> listScopes() throws StorageException {
    checkOpen();
    synchronized (mutationLock) {
      checkOpen();
      if (!Files.isDirectory(objects, LinkOption.NOFOLLOW_LINKS)) return List.of();
      try {
        List<ScopePath> scopes = new ArrayList<>();
        listScopes(objects, new ArrayList<>(), scopes);
        scopes.sort(java.util.Comparator.comparing(ScopePath::render));
        return List.copyOf(scopes);
      } catch (IOException failure) {
        throw StorageException.transientFailure("scope listing failed", failure);
      }
    }
  }

  /** Walks the scope tree; a directory is a scope when it holds a seal or at least one entity. */
  private void listScopes(Path directory, List<Long> components, List<ScopePath> out)
      throws IOException, StorageException {
    boolean hasState =
        Files.isRegularFile(directory.resolve(SEAL_FILE_NAME), LinkOption.NOFOLLOW_LINKS);
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
      for (Path entry : entries) {
        String name = entry.getFileName().toString();
        if (name.equals(SEAL_FILE_NAME) || isScratch(name)) continue;
        if (!Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS)) {
          // The backend root holds its own lock file; scope content is directories only.
          if (components.isEmpty()) continue;
          throw StorageException.corrupt("unexpected entry " + name + " in " + directory);
        }
        if (name.matches("[0-9a-f]{8}")) {
          List<Long> child = new ArrayList<>(components);
          child.add(Long.parseUnsignedLong(name, 16));
          listScopes(entry, child, out);
          continue;
        }
        if (!name.matches("[0-9a-f]{2}")) {
          throw StorageException.corrupt("unexpected entry " + name + " in " + directory);
        }
        try (DirectoryStream<Path> entities = Files.newDirectoryStream(entry)) {
          for (Path entity : entities) {
            String entityName = entity.getFileName().toString();
            if (isScratch(entityName)) continue;
            if (!entityName.matches("[0-9a-f]{8}")
                || !Files.isRegularFile(entity, LinkOption.NOFOLLOW_LINKS)) {
              throw StorageException.corrupt("unexpected entry " + entityName + " in " + directory);
            }
            hasState = true;
          }
        }
      }
    }
    if (hasState && !components.isEmpty()) {
      long[] path = new long[components.size()];
      for (int i = 0; i < path.length; i++) path[i] = components.get(i);
      out.add(ScopePath.of(path));
    }
  }

  @Override public void retire(ScopePath scope, long retentionCursor) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    if (retentionCursor < 0) throw new IllegalArgumentException("retention cursor must be non-negative");
    checkOpen();
    synchronized (mutationLock) {
      checkOpen();
      Path scopeDir = scopeDirectory(scope);
      if (!Files.isDirectory(scopeDir, LinkOption.NOFOLLOW_LINKS)) {
        throw StorageException.notFound("scope " + scope.render() + " holds nothing to retire");
      }
      Seal seal = readSeal(scopeDir);
      try {
        if (seal != null) {
          // Pinned decision 5: retirement is scope-local. Only this scope's
          // own entities and seal record go; child scopes are independent
          // containers and survive (a recursive delete would destroy them).
          retireScopeEntities(scopeDir);
          Files.delete(scopeDir.resolve(SEAL_FILE_NAME));
          syncDirectory(root.resolve("objects"));
          return;
        }
        List<Long> ids = listEntityIds(scopeDir, false);
        if (ids.isEmpty()) {
          throw StorageException.notFound("scope " + scope.render() + " holds nothing to retire");
        }
        for (long entityId : ids) {
          if (entityId > retentionCursor) continue;
          Path target = entityPath(scopeDir, entityId);
          Files.delete(target);
          syncDirectory(target.getParent());
          removeBucketIfEmpty(target.getParent());
        }
      } catch (IOException failure) {
        throw StorageException.transientFailure("scope retirement failed", failure);
      }
    }
  }

  /**
   * Releases the exclusive lock and removes any scratch this handle still
   * tracks. Reads already opened keep their file descriptors.
   *
   * @throws StorageException {@code FAILED} if scratch cleanup fails
   */
  @Override public void close() throws StorageException {
    if (closed) return;
    closed = true;
    StorageException cleanup = null;
    for (Path scratch : List.copyOf(scratchPaths)) {
      try {
        Files.deleteIfExists(scratch);
        scratchPaths.remove(scratch);
      } catch (IOException failure) {
        if (cleanup == null) cleanup = StorageException.failed("scratch cleanup failed", failure);
      }
    }
    try {
      lock.release();
    } catch (IOException failure) {
      if (cleanup == null) cleanup = StorageException.failed("lock release failed", failure);
    } finally {
      try {
        lockChannel.close();
      } catch (IOException failure) {
        if (cleanup == null) cleanup = StorageException.failed("lock close failed", failure);
      } finally {
        synchronized (OPEN_ROOTS) { OPEN_ROOTS.remove(objects); }
      }
    }
    if (cleanup != null) throw cleanup;
  }

  private Path scopeDirectory(ScopePath scope) {
    Path directory = objects;
    for (long component : scope.components()) {
      directory = directory.resolve(StoreLayout.scopeComponentName(component));
    }
    return directory;
  }

  private Path entityPath(Path scopeDir, long entityId) {
    return scopeDir.resolve(StoreLayout.fanoutBucket(entityId)).resolve(StoreLayout.entityName(entityId));
  }

  private List<Long> listEntityIds(Path scopeDir, boolean verifyDigests) throws StorageException {
    List<Long> ids = new ArrayList<>();
    try (DirectoryStream<Path> buckets = Files.newDirectoryStream(scopeDir)) {
      for (Path bucket : buckets) {
        String bucketName = bucket.getFileName().toString();
        if (bucketName.equals(SEAL_FILE_NAME) || isScratch(bucketName)) continue;
        // 8-hex directories are child scopes (they hold their own entities);
        // an 8-hex regular file directly at this level is a shape violation:
        // entities live inside their fanout bucket, never beside one.
        if (bucketName.matches("[0-9a-f]{8}")) {
          if (Files.isDirectory(bucket, LinkOption.NOFOLLOW_LINKS)) continue;
          throw StorageException.corrupt(
              "entity file " + bucketName + " sits outside its fanout bucket in scope " + scopeDir);
        }
        if (!bucketName.matches("[0-9a-f]{2}")
            || !Files.isDirectory(bucket, LinkOption.NOFOLLOW_LINKS)) {
          throw StorageException.corrupt("unexpected entry " + bucketName + " in scope " + scopeDir);
        }
        try (DirectoryStream<Path> entities = Files.newDirectoryStream(bucket)) {
          for (Path entity : entities) {
            String name = entity.getFileName().toString();
            if (isScratch(name)) continue;
            if (!name.matches("[0-9a-f]{8}") || !Files.isRegularFile(entity, LinkOption.NOFOLLOW_LINKS)) {
              throw StorageException.corrupt("unexpected entry " + name + " in scope " + scopeDir);
            }
            long entityId = Long.parseLong(name, 16);
            ids.add(entityId);
            if (verifyDigests) readEntityDigest(scopeDir, entityId);
          }
        }
      }
    } catch (IOException failure) {
      throw StorageException.transientFailure("scope listing failed", failure);
    }
    ids.sort(null);
    return ids;
  }

  private byte[] readEntityDigest(Path scopeDir, long entityId) throws StorageException {
    Path target = entityPath(scopeDir, entityId);
    byte[] payload = readEntity(target);
    byte[] recorded = readRecordedDigest(target);
    if (!MessageDigest.isEqual(StoreLayout.sha256().digest(payload), recorded)) {
      throw StorageException.corrupt("entity " + entityId + " fails its recorded digest");
    }
    return recorded;
  }

  private byte[] readRecordedDigest(Path target) throws StorageException {
    try (FileChannel channel = FileChannel.open(target, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
      if (channel.size() < ENTITY_HEADER_BYTES) {
        throw StorageException.corrupt("entity file " + target.getFileName() + " is truncated");
      }
      ByteBuffer header = ByteBuffer.allocate(ENTITY_HEADER_BYTES);
      readExactly(channel, header, target);
      return Arrays.copyOfRange(header.array(), 8, ENTITY_HEADER_BYTES);
    } catch (IOException failure) {
      throw StorageException.transientFailure("entity read failed", failure);
    }
  }

  private byte[] readEntity(Path target) throws StorageException {
    try (FileChannel channel = FileChannel.open(target, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
      if (channel.size() < ENTITY_HEADER_BYTES) {
        throw StorageException.corrupt("entity file " + target.getFileName() + " is truncated");
      }
      ByteBuffer header = ByteBuffer.allocate(ENTITY_HEADER_BYTES);
      readExactly(channel, header, target);
      long length = header.getLong(0);
      byte[] digest = Arrays.copyOfRange(header.array(), 8, ENTITY_HEADER_BYTES);
      if (length != channel.size() - ENTITY_HEADER_BYTES) {
        throw StorageException.corrupt("entity file " + target.getFileName() + " length differs from its header");
      }
      byte[] payload = new byte[(int) length];
      ByteBuffer body = ByteBuffer.wrap(payload);
      readExactly(channel, body, target);
      if (!MessageDigest.isEqual(StoreLayout.sha256().digest(payload), digest)) {
        throw StorageException.corrupt("entity file " + target.getFileName() + " fails its recorded digest");
      }
      return payload;
    } catch (IOException failure) {
      throw StorageException.transientFailure("entity read failed", failure);
    }
  }

  private Seal readSeal(Path scopeDir) throws StorageException {
    Path seal = scopeDir.resolve(SEAL_FILE_NAME);
    if (!Files.exists(seal, LinkOption.NOFOLLOW_LINKS)) return null;
    try (FileChannel channel = FileChannel.open(seal, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
      if (channel.size() != SEAL_BYTES) {
        throw StorageException.corrupt("seal record of scope " + scopeDir + " has wrong length");
      }
      ByteBuffer body = ByteBuffer.allocate(SEAL_BYTES);
      readExactly(channel, body, seal);
      return new Seal(Arrays.copyOfRange(body.array(), 0, StoreLayout.DIGEST_BYTES),
          body.getLong(StoreLayout.DIGEST_BYTES));
    } catch (IOException failure) {
      throw StorageException.transientFailure("seal read failed", failure);
    }
  }

  private void writeEntity(Path scratch, byte[] bytes, byte[] digest) throws StorageException {
    ByteBuffer header = ByteBuffer.allocate(ENTITY_HEADER_BYTES).putLong(bytes.length).put(digest);
    writeFullyAndSync(scratch, header.array(), bytes);
  }

  private void writeFullyAndSync(Path target, byte[]... parts) throws StorageException {
    try (FileChannel channel = FileChannel.open(target, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      for (byte[] part : parts) {
        ByteBuffer remaining = ByteBuffer.wrap(part);
        while (remaining.hasRemaining()) channel.write(remaining);
      }
      channel.force(true);
    } catch (IOException failure) {
      throw StorageException.transientFailure("scratch write failed", failure);
    }
  }

  private void moveIntoPlace(Path scratch, Path target) throws StorageException {
    try {
      try {
        Files.move(scratch, target, StandardCopyOption.ATOMIC_MOVE);
      } catch (java.nio.file.AtomicMoveNotSupportedException fallback) {
        Files.move(scratch, target);
      }
      syncDirectory(target.getParent());
    } catch (IOException failure) {
      throw StorageException.transientFailure("scratch publication failed", failure);
    }
  }

  private void sweepScratch(Path directory) throws StorageException {
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
      for (Path entry : entries) {
        if (Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS)) {
          sweepScratch(entry);
        } else if (isScratch(entry.getFileName().toString())) {
          Files.delete(entry);
          syncDirectory(directory);
        }
      }
    } catch (IOException failure) {
      throw StorageException.transientFailure("scratch sweep failed", failure);
    }
  }

  /** Deletes every entity in one scope's own fanout buckets, leaving child scopes untouched. */
  private void retireScopeEntities(Path scopeDir) throws IOException {
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(scopeDir)) {
      for (Path entry : entries) {
        String name = entry.getFileName().toString();
        if (!name.matches("[0-9a-f]{2}") || !Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS)) {
          continue; // child scopes, the seal record, and foreign names all survive
        }
        try (DirectoryStream<Path> entities = Files.newDirectoryStream(entry)) {
          for (Path entity : entities) Files.delete(entity);
        }
        Files.delete(entry);
      }
    }
  }

  private void removeBucketIfEmpty(Path bucket) throws IOException {
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(bucket)) {
      if (entries.iterator().hasNext()) return;
    }
    Files.delete(bucket);
    syncDirectory(bucket.getParent());
  }

  private static boolean isScratch(String name) {
    return name.startsWith(SCRATCH_PREFIX) && name.endsWith(SCRATCH_SUFFIX);
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

  private static void readExactly(FileChannel channel, ByteBuffer bytes, Path source) throws IOException {
    while (bytes.hasRemaining()) {
      if (channel.read(bytes) == -1) throw new IOException("truncated file " + source);
    }
  }

  /** Creates {@code directory} and every missing ancestor, flushing each new level to stable storage. */
  private static void createDirectoriesDurable(Path directory) throws IOException {
    List<Path> missing = new ArrayList<>();
    Path cursor = directory;
    while (cursor != null && !Files.isDirectory(cursor, LinkOption.NOFOLLOW_LINKS)) {
      missing.add(cursor);
      cursor = cursor.getParent();
    }
    if (cursor == null) throw new IOException("no existing ancestor of " + directory);
    for (int i = missing.size() - 1; i >= 0; i--) {
      Path created = missing.get(i);
      Files.createDirectory(created);
      syncDirectory(created.getParent());
      syncDirectory(created);
    }
  }

  /** Flushes one directory so entry creation and removal inside it survive a process kill. */
  private static void syncDirectory(Path directory) throws IOException {
    if (directory == null) return;
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }
}
