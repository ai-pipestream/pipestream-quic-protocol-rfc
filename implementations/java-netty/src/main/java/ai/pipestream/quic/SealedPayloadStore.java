package ai.pipestream.quic;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Blocking, file-backed receive and immutable payload storage for the Java sealed server.
 * Calls belong outside Netty event loops. A directory lock permits one cooperative
 * writer on a local filesystem. Identities are labels, not authorization.
 * Installing bytes does not admit an entity or acknowledge application completion.
 * Explicit offline reconciliation may discard unadmitted bodies while retaining
 * their immutable headers and digests for checked retransmission.
 */
public final class SealedPayloadStore implements AutoCloseable {
  private static final int BLOCK = 8192;
  private static final int MAX_ACTIVE = 128;
  private static final int MAX_METADATA = Wire.MAX_ENTITY_HEADER + 2048;
  private static final byte[] MAGIC = new byte[] {'P', 'S', 'J', 'P', 'A', 'Y', '0', '1'};
  private static final Set<String> ROOT_NAMES = Set.of("writer.lock", "policy.cbor", "session-store.bin", "spool", "objects");
  private static final Set<Path> OPEN_ROOTS = new HashSet<>();
  private final Path root;
  private final Limits limits;
  private final UUID storeId;
  private final FileChannel lockChannel;
  private final FileLock lock;
  private final Object publication = new Object();
  private long temporaryBytes, retainedBytes;
  private int temporaryFiles, retainedFiles, active;
  private boolean closed;

  /**
   * Persistent file-length and file-count policy. Filesystem metadata is not included.
   * @param temporaryBytes aggregate live and abandoned spool octets
   * @param temporaryFiles aggregate spool files, including empty files
   * @param retainedBytes aggregate object and staging octets, including headers
   * @param retainedFiles aggregate object and staging files
   * @param entityBytes maximum reassembled payload size
   * @param chunks maximum chunks per entity
   */
  public record Limits(long temporaryBytes, int temporaryFiles, long retainedBytes,
      int retainedFiles, long entityBytes, int chunks) {
    /** Validates positive, representable local bounds. */
    public Limits {
      if (temporaryBytes < 1 || temporaryFiles < 1 || retainedBytes < 1 || retainedFiles < 2
          || entityBytes < 1 || entityBytes > Long.MAX_VALUE / 4 || chunks < 1 || chunks > 65_536) {
        throw new IllegalArgumentException("invalid payload limits");
      }
    }
    /** Returns conservative local defaults.
     * @return persistent defaults for a new store
     */
    public static Limits defaults() { return new Limits(256L << 20, 4096, 512L << 20, 8192, 64L << 20, 1024); }
  }

  /**
   * Durable payload identity; no field is interpreted as a filesystem path.
   * @param session bounded session label
   * @param producer nonzero producer label
   * @param entity scope-qualified entity identifier
   */
  public record Identity(String session, UUID producer, SealedWork.EntityKey entity) {
    /** Requires non-null identity members; protocol validation occurs before I/O. */
    public Identity { Objects.requireNonNull(session); Objects.requireNonNull(producer); Objects.requireNonNull(entity); }
  }

  /**
   * Current conservative charges. In-progress publication reserves both link names.
   * @param temporaryBytes charged spool octets
   * @param temporaryFiles charged spool files
   * @param retainedBytes charged retained/staging octets
   * @param retainedFiles charged retained/staging files
   * @param activeHandles receivers, receipts, readers, and active store operations
   */
  public record Usage(long temporaryBytes, int temporaryFiles, long retainedBytes, int retainedFiles, int activeHandles) {}

  /**
   * Completed offline reclamation, measured as logical file lengths, not filesystem blocks.
   * Declared identities, payload commitments and all admitted inputs remain retained.
   * @param admittedPayloads verified payloads retained for managed jobs, including terminal jobs
   * @param temporaryFilesRemoved abandoned receive files removed
   * @param stagingFilesRemoved abandoned installation names removed
   * @param payloadsReclaimed unadmitted payload objects converted to commitment-only records
   * @param commitmentsRetained commitment-only records left for immutable retransmission
   * @param temporaryBytesReleased reduction in charged temporary file lengths
   * @param retainedBytesReleased reduction in charged retained and staging file lengths
   */
  public record Reconciliation(long admittedPayloads, long temporaryFilesRemoved, long stagingFilesRemoved,
      long payloadsReclaimed, long commitmentsRetained, long temporaryBytesReleased, long retainedBytesReleased) {}

  /**
   * Explicitly reclaims abandoned files from a closed, previously paired managed store.
   * Obtains exclusive payload ownership and holds the database writer lock throughout
   * audit and reclamation. All admitted inputs and immutable records are checked before
   * deletion begins. No lifecycle state changes, admission or completion are inferred.
   * Unadmitted bodies become commitment-only records; identical retransmission can restore
   * their bytes. Completed and refused jobs retain their original payloads.
   * Filesystem failure can leave partial reclamation, which a later explicit call resumes.
   * This blocking operation must not run on a network event loop.
   * @param directory dedicated, closed payload directory
   * @param limits exact retained payload policy
   * @param sessions database previously paired with this directory
   * @return counters after all requested changes and directory synchronization complete
   * @throws IOException for an open store, lock failure or filesystem failure
   * @throws SQLException for database failure
   * @throws ProtocolException for a mismatched/unbound pair, caller-managed admission,
   *     missing or corrupt admitted input, invalid records or capacity refusal
   */
  public static Reconciliation reconcile(Path directory, Limits limits, SealedSessionStore sessions)
      throws IOException, SQLException, ProtocolException {
    return reconcile(directory, limits, sessions, null);
  }

  // Package-local fault boundaries exercise the actual filesystem sequence in
  // subprocess tests. The public API never accepts an application callback.
  enum ReconcilePhase { AUDITED, STAGING_REMOVED, COMMITMENTS_PUBLISHED, BODY_TRUNCATED }
  @FunctionalInterface interface ReconciliationProbe { void reached(ReconcilePhase phase) throws IOException; }

  static Reconciliation reconcile(Path directory, Limits limits, SealedSessionStore sessions, ReconciliationProbe probe)
      throws IOException, SQLException, ProtocolException {
    Objects.requireNonNull(sessions);
    if (!Files.isRegularFile(directory.resolve("policy.cbor"), LinkOption.NOFOLLOW_LINKS)) {
      throw Wire.integrity("payload maintenance requires an existing store policy");
    }
    try (var payloads = open(directory, limits)) {
      var binding = payloads.retainedBinding();
      if (binding == null) throw Wire.entity("payload maintenance requires a previously paired managed store");
      return sessions.withPayloadMaintenance(binding, connection -> payloads.reclaim(connection, probe));
    }
  }

  private Reconciliation reclaim(java.sql.Connection connection, ReconciliationProbe probe) throws IOException, SQLException, ProtocolException {
    Usage before = usage();
    List<Path> spools = snapshot(root.resolve("spool"), limits.temporaryFiles);
    List<Path> objects = snapshot(root.resolve("objects"), limits.retainedFiles);
    Set<Path> references = new HashSet<>();
    Set<Path> admitted = new HashSet<>();
    SealedJobs.visitRetainedInputs(connection, input -> {
      Metadata expected = new Metadata(input.identity(), input.header(), input.length(), input.digest());
      Path path = objectPath(input.identity());
      verify(path, expected);
      admitted.add(path);
    });
    // Audit every immutable object before deleting even an unrelated spool.
    for (Path path : objects) {
      String name = path.getFileName().toString();
      if (name.endsWith(".pay")) verify(path, readMetadata(path));
      else if (name.endsWith(".commit")) references.add(path);
    }
    if (probe != null) probe.reached(ReconcilePhase.AUDITED);
    for (Path path : spools) Files.delete(path);
    syncDirectory(root.resolve("spool"));
    long staging = 0;
    for (Path path : objects) {
      if (path.getFileName().toString().endsWith(".tmp")) { Files.delete(path); staging++; }
    }
    syncDirectory(root.resolve("objects"));
    if (probe != null) probe.reached(ReconcilePhase.STAGING_REMOVED);
    long reclaimed = 0, commitments = 0;
    for (Path path : objects) {
      if (!path.getFileName().toString().endsWith(".pay") || admitted.contains(path)) continue;
      Metadata metadata = readMetadata(path);
      Path reference = commitmentPath(metadata.identity);
      Metadata retained = commitment(metadata.identity);
      if (retained == null) Files.move(path, reference, StandardCopyOption.ATOMIC_MOVE);
      else {
        requireCommitment(metadata, retained);
        Files.delete(path);
      }
      // Persist the commitment name before discarding any bytes it binds.
      syncDirectory(path.getParent());
      references.add(reference);
      reclaimed++;
    }
    if (probe != null) probe.reached(ReconcilePhase.COMMITMENTS_PUBLISHED);
    for (Path path : references) {
      boolean redundant;
      try (var channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE,
          LinkOption.NOFOLLOW_LINKS)) {
        Metadata metadata = readMetadata(channel, true);
        redundant = admitted.contains(objectPath(metadata.identity));
        if (!redundant) {
          channel.truncate(channel.position()); channel.force(true); commitments++;
          if (probe != null) probe.reached(ReconcilePhase.BODY_TRUNCATED);
        }
      }
      // A restored admitted payload and its job retain the same commitment.
      if (redundant) Files.delete(path);
    }
    syncDirectory(root.resolve("objects"));
    temporaryBytes = 0; retainedBytes = 0; temporaryFiles = 0; retainedFiles = 0;
    scan("spool", false); scan("objects", true);
    Usage after = usage();
    return new Reconciliation(admitted.size(), spools.size(), staging, reclaimed, commitments,
        before.temporaryBytes - after.temporaryBytes, before.retainedBytes - after.retainedBytes);
  }

  private static List<Path> snapshot(Path directory, int maximum) throws IOException, ProtocolException {
    List<Path> paths = new ArrayList<>();
    try (var entries = Files.newDirectoryStream(directory)) {
      for (Path path : entries) {
        if (paths.size() >= maximum) throw Wire.integrity("payload maintenance snapshot exceeds file policy");
        paths.add(path);
      }
    }
    return paths;
  }

  private SealedPayloadStore(Path root, Limits limits, UUID storeId, FileChannel channel, FileLock lock) {
    this.root = root; this.limits = limits; this.storeId = storeId; this.lockChannel = channel; this.lock = lock;
  }

  /**
   * Opens an exclusively owned store, refusing foreign layouts or changed policy.
   * Startup counts abandoned files without deleting or admitting them.
   * @param directory dedicated payload directory, not the session database
   * @param limits exact persistent policy
   * @return exclusive store handle
   * @throws IOException for filesystem failure or another open writer
   * @throws ProtocolException for corrupt layout, changed policy, or exhausted quota
   */
  public static SealedPayloadStore open(Path directory, Limits limits) throws IOException, ProtocolException {
    Objects.requireNonNull(limits);
    Path requested = directory.toAbsolutePath().normalize();
    Files.createDirectories(requested);
    if (!Files.isDirectory(requested, LinkOption.NOFOLLOW_LINKS)) throw Wire.integrity("payload root is not a real directory");
    Path root = requested.toRealPath();
    inspectRoot(root);
    // Closing a second channel can release another channel's OS lock in this JVM.
    synchronized (OPEN_ROOTS) {
      if (!OPEN_ROOTS.add(root)) throw new IOException("payload store already has a writer");
    }
    FileChannel channel = null;
    FileLock lock = null;
    try {
      channel = FileChannel.open(root.resolve("writer.lock"), StandardOpenOption.CREATE,
          StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
      try { lock = channel.tryLock(); }
      catch (OverlappingFileLockException failure) { throw new IOException("payload store already has a writer", failure); }
      if (lock == null) throw new IOException("payload store already has a writer");
      inspectRoot(root);
      UUID storeId;
      Path policyPath = root.resolve("policy.cbor");
      if (!Files.exists(policyPath, LinkOption.NOFOLLOW_LINKS)) {
        try (var entries = Files.newDirectoryStream(root)) {
          for (Path entry : entries) if (!entry.getFileName().toString().equals("writer.lock")) throw Wire.integrity("incomplete or foreign payload layout; no conversion performed");
        }
        storeId = UUID.randomUUID();
        try (FileChannel output = FileChannel.open(policyPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
          write(output, ByteBuffer.wrap(policy(limits, storeId))); output.force(true);
        }
        Files.createDirectory(root.resolve("spool")); Files.createDirectory(root.resolve("objects"));
        syncDirectory(root); syncDirectory(root.getParent());
      } else {
        byte[] encoded = readBounded(policyPath, 4096);
        var fields = SealedCbor.decode(encoded, 4096);
        if (!"pipestream-java-payload-v3".equals(fields.get("format"))) throw Wire.integrity("payload policy or format differs; no conversion performed");
        ByteBuffer identity = ByteBuffer.wrap(SealedWork.bytes(fields, "store-id", 16));
        storeId = new UUID(identity.getLong(), identity.getLong());
        if (storeId.equals(SealedStoreBinding.UNBOUND) || !Arrays.equals(policy(limits, storeId), encoded)) {
          throw Wire.integrity("payload policy or identity differs; no conversion performed");
        }
      }
      SealedPayloadStore store = new SealedPayloadStore(root, limits, storeId, channel, lock);
      store.retainedBinding();
      store.scan("spool", false); store.scan("objects", true);
      return store;
    } catch (IOException | ProtocolException | RuntimeException failure) {
      try { if (lock != null) lock.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
      try { if (channel != null) channel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
      if (channel == null || !channel.isOpen()) synchronized (OPEN_ROOTS) { OPEN_ROOTS.remove(root); }
      throw failure;
    }
  }

  /** Returns conservative current accounting.
   * @return immutable usage snapshot
   */
  public synchronized Usage usage() { return new Usage(temporaryBytes, temporaryFiles, retainedBytes, retainedFiles, active); }

  /** Binds before any managed admission; interruption may leave only the file half. */
  void bind(SealedSessionStore sessions) throws IOException, SQLException, ProtocolException {
    pin();
    try {
      synchronized (publication) {
        var current = sessions.binding();
        var expected = new SealedStoreBinding(current.database(), storeId);
        if (!current.payloads().equals(SealedStoreBinding.UNBOUND) && !current.equals(expected)) {
          throw Wire.integrity("Java database belongs to a different payload store");
        }
        var retained = retainedBinding();
        if (retained != null && !retained.equals(expected)) throw Wire.integrity("payload store belongs to a different Java database");
        if (current.equals(expected)) {
          if (retained == null) throw Wire.integrity("bound payload store is missing its database claim");
          return;
        }
        if (retained == null) {
          // The durable file claim precedes the atomic database claim. Neither
          // half admits work; an identical retry can finish an interrupted bind.
          try (var output = FileChannel.open(root.resolve("session-store.bin"), StandardOpenOption.CREATE_NEW,
              StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            write(output, ByteBuffer.wrap(expected.encode())); output.force(true);
          }
        } else {
          // A prior attempt may have written every byte but failed its force.
          // Re-reading the image alone is not durable installation evidence.
          try (var output = FileChannel.open(root.resolve("session-store.bin"), StandardOpenOption.WRITE,
              LinkOption.NOFOLLOW_LINKS)) { output.force(true); }
        }
        syncDirectory(root);
        sessions.bindPayloads(expected);
      }
    } finally { unpin(); }
  }

  private SealedStoreBinding retainedBinding() throws IOException, ProtocolException {
    Path path = root.resolve("session-store.bin");
    if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return null;
    var binding = SealedStoreBinding.decode(readBounded(path, SealedStoreBinding.BYTES));
    if (!binding.payloads().equals(storeId)) throw Wire.integrity("payload binding differs from store identity");
    return binding;
  }

  /**
   * Starts one bounded receive stream after header validation, without admission.
   * @param identity session and producer inherited from negotiation/declaration
   * @param header validated entity or chunk header
   * @return receiver whose close cancels unfinished reception
   * @throws IOException for closed store or file creation failure
   * @throws ProtocolException for invalid identity/header or capacity exhaustion
   */
  public Receiver begin(Identity identity, SealedTransport.Header header) throws IOException, ProtocolException {
    validate(identity, header);
    if (header.payloadLength() != null && header.payloadLength().compareTo(BigInteger.valueOf(limits.entityBytes)) > 0) throw Wire.limit("declared payload exceeds entity limit");
    if (header.chunk() != null && header.chunk().total().compareTo(BigInteger.valueOf(limits.chunks)) > 0) throw Wire.limit("declared chunk count exceeds local limit");
    pin();
    Credit credit = null;
    Path path = root.resolve("spool").resolve(UUID.randomUUID() + ".tmp");
    try {
      credit = reserve(false, 0, 1);
      FileChannel output = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
      return new Receiver(identity, header, path, output, credit);
    } catch (IOException | ProtocolException | RuntimeException failure) {
      if (credit != null) credit.release();
      unpin(); throw failure;
    }
  }

  /** One incremental receive stream. Methods serialize access to this stream, not the store. */
  public final class Receiver implements AutoCloseable {
    private final Identity identity;
    private final SealedTransport.Header header;
    private final Path path;
    private final FileChannel output;
    private final Credit credit;
    private final MessageDigest digest = SealedWork.sha256();
    private long length;
    private boolean done;
    private Receiver(Identity identity, SealedTransport.Header header, Path path, FileChannel output, Credit credit) {
      this.identity = identity; this.header = header; this.path = path; this.output = output; this.credit = credit;
    }

    /**
     * Writes incrementally, charging capacity before each at-most-8-KiB file write.
     * @param bytes caller-owned bytes, not retained by this method
     * @param offset first byte
     * @param count number of bytes
     * @throws IOException for file failure or a finished receiver
     * @throws ProtocolException for capacity or declared-length overrun
     */
    public synchronized void write(byte[] bytes, int offset, int count) throws IOException, ProtocolException {
      Objects.checkFromIndexSize(offset, count, bytes.length);
      if (done) throw new IOException("receiver is finished");
      try {
        if (count > limits.entityBytes - length) throw Wire.limit("received payload exceeds entity limit");
        if (header.payloadLength() != null && BigInteger.valueOf(length).add(BigInteger.valueOf(count)).compareTo(header.payloadLength()) > 0) throw Wire.entity("received payload exceeds declared length");
        while (count > 0) {
          int n = Math.min(count, BLOCK); credit.grow(n);
          SealedPayloadStore.write(output, ByteBuffer.wrap(bytes, offset, n));
          digest.update(bytes, offset, n); length += n; offset += n; count -= n;
        }
      } catch (IOException | ProtocolException | RuntimeException failure) {
        try { close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
        throw failure;
      }
    }

    /**
     * Validates FIN length/checksum, syncs the spool, and transfers its capacity to a receipt.
     * @return owned receipt; caller must close it after installation or cancellation
     * @throws IOException for file failure or repeated FIN
     * @throws ProtocolException for payload integrity failure
     */
    public synchronized Received finish() throws IOException, ProtocolException {
      if (done) throw new IOException("receiver is finished");
      try {
        byte[] hash = digest.digest();
        if (header.payloadLength() != null && !header.payloadLength().equals(BigInteger.valueOf(length))) throw Wire.integrity("FIN payload length differs");
        if (header.checksum() != null && !MessageDigest.isEqual(hash, header.checksum())) throw Wire.integrity("FIN payload checksum differs");
        output.force(true); output.close(); done = true;
        return new Received(identity, header, path, length, hash, credit);
      } catch (IOException | ProtocolException | RuntimeException failure) {
        try { close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
        throw failure;
      }
    }

    /** Cancels unfinished reception; credit is released only after successful deletion.
     * @throws IOException if cleanup fails
     */
    @Override public synchronized void close() throws IOException {
      if (done) return;
      done = true;
      try { output.close(); }
      finally { try { discard(path, credit); } finally { unpin(); } }
    }
  }

  /** Validated spool ownership. Installation pins it against concurrent cancellation. */
  public final class Received implements AutoCloseable {
    private final Identity identity;
    private final SealedTransport.Header header;
    private final Path path;
    private final long length;
    private final byte[] digest;
    private final Credit credit;
    private int readers;
    private boolean ownerClosed, cleaned;
    private Received(Identity identity, SealedTransport.Header header, Path path, long length, byte[] digest, Credit credit) {
      this.identity = identity; this.header = header; this.path = path; this.length = length; this.digest = digest.clone(); this.credit = credit;
    }
    /** Returns the measured stream payload length.
     * @return validated octets
     */
    public long length() { return length; }
    /** Returns the measured stream SHA-256.
     * @return defensive digest copy
     */
    public byte[] digest() { return digest.clone(); }
    private SealedPayloadStore store() { return SealedPayloadStore.this; }
    private synchronized void acquire() throws IOException {
      if (ownerClosed) throw new IOException("received payload is closed");
      readers++;
    }
    private synchronized void release() throws IOException { readers--; cleanup(); }
    private void cleanup() throws IOException {
      if (ownerClosed && readers == 0 && !cleaned) {
        cleaned = true;
        try { discard(path, credit); } finally { unpin(); }
      }
    }
    /** Releases ownership; active installation retains the file and its credit.
     * @throws IOException if final spool cleanup fails
     */
    @Override public synchronized void close() throws IOException { ownerClosed = true; cleanup(); }
  }

  /**
   * Installs a complete entity or chunk set without overwriting an existing identity.
   * A staging file is synced, hard-linked without replacement, and its directory
   * synced before success. Both names are reserved before any retained-file writes.
   * When restoring a reclaimed body, its retained commitment is checked first and
   * staging is atomically renamed instead; one additional payload name is reserved.
   * @param received complete set of receipts from this store, in any arrival order
   * @return immutable retained input, not an admission or execution receipt
   * @throws IOException for file/lock failure or unavailable receipt
   * @throws ProtocolException for incomplete chunks, changed input, or quota exhaustion
   */
  public Stored install(List<Received> received) throws IOException, ProtocolException {
    if (received.isEmpty() || received.size() > limits.chunks) throw Wire.limit("invalid received chunk count");
    List<Received> inputs = List.copyOf(received);
    pin();
    List<Received> acquired = new ArrayList<>();
    Throwable primaryFailure = null;
    try {
      for (Received input : inputs) {
        if (input.store() != this) throw Wire.entity("receipt belongs to another payload store");
        input.acquire(); acquired.add(input);
      }
      inputs = ordered(inputs);
      Received first = inputs.getFirst();
      long length = 0;
      for (Received input : inputs) {
        if (input.length > limits.entityBytes - length) throw Wire.limit("assembled payload exceeds entity limit");
        length += input.length;
      }
      SealedTransport.Header header = assembled(first.header, length, new byte[32]);
      Metadata provisional = new Metadata(first.identity, header, length, new byte[32]);
      byte[] placeholder = encode(provisional);
      long objectBytes = Math.addExact(12L + 32 + placeholder.length, length);
      Path target = objectPath(first.identity);
      boolean restoring;
      synchronized (publication) {
        Metadata commitment = commitment(first.identity);
        restoring = commitment != null;
        if (commitment != null) {
          byte[] digest = hashInputs(inputs, null);
          var incoming = new Metadata(first.identity, assembled(first.header, length, digest), length, digest);
          if (!Arrays.equals(encode(incoming), encode(commitment))) throw Wire.entity("reclaimed payload bytes or commitments changed");
        }
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
          Metadata existing = readMetadata(target);
          requireCommitment(existing, commitment);
          if (!existing.identity.equals(first.identity) || existing.length != length
              || !sameApplicationHeader(existing.header, first.header)) throw Wire.entity("retained payload identity or header changed");
          byte[] digest = hashInputs(inputs, null);
          SealedTransport.Header actual = assembled(first.header, length, digest);
          if (!Arrays.equals(encode(new Metadata(first.identity, actual, length, digest)), encode(existing))) throw Wire.entity("retained payload bytes or commitments changed");
          verify(target, existing); syncDirectory(target.getParent());
          return new Stored(target, existing);
        }
      }
      // A retained commitment protects replay identity while restoration atomically
      // renames its staging file. No second payload link is needed on that path.
      Credit credit = reserve(true, Math.multiplyExact(objectBytes, restoring ? 1 : 2), restoring ? 1 : 2);
      Path staging = root.resolve("objects").resolve("install-" + UUID.randomUUID() + ".tmp");
      boolean published = false, moved = false, publicationAttempted = false;
      try {
        Metadata actual;
        try (FileChannel output = FileChannel.open(staging, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
          write(output, ByteBuffer.wrap(MAGIC)); write(output, ByteBuffer.allocate(4).putInt(placeholder.length).flip());
          write(output, ByteBuffer.wrap(placeholder)); write(output, ByteBuffer.wrap(new byte[32]));
          byte[] digest = hashInputs(inputs, output);
          actual = new Metadata(first.identity, assembled(first.header, length, digest), length, digest);
          byte[] encoded = encode(actual);
          if (encoded.length != placeholder.length) throw Wire.integrity("retained header size changed during installation");
          output.position(12); write(output, ByteBuffer.wrap(encoded)); write(output, ByteBuffer.wrap(SealedWork.sha256().digest(encoded)));
          if (output.size() != objectBytes) throw Wire.integrity("retained object geometry changed");
          output.force(true);
        }
        synchronized (publication) {
          requireCommitment(actual, commitment(first.identity));
          if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            Metadata existing = readMetadata(target);
            if (!Arrays.equals(encode(existing), encode(actual))) throw Wire.entity("concurrent installation changed retained input");
            verify(target, existing); syncDirectory(target.getParent());
          } else {
            if (restoring) {
              publicationAttempted = true;
              Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE); moved = true;
            } else Files.createLink(target, staging);
            published = true;
            syncDirectory(target.getParent());
          }
        }
        if (!moved) Files.delete(staging);
        syncDirectory(staging.getParent());
        credit.keep(published ? objectBytes : 0, published ? 1 : 0);
        return new Stored(target, actual);
      } catch (IOException | ProtocolException | RuntimeException failure) {
        if (restoring) {
          // An uncertain atomic move owns at most one payload name. Keep its
          // entire allowance until reopen rather than guessing that it vanished.
          boolean keep = publicationAttempted;
          try { Files.deleteIfExists(staging); syncDirectory(staging.getParent()); }
          catch (IOException cleanup) { keep = true; failure.addSuppressed(cleanup); }
          credit.keep(keep ? objectBytes : 0, keep ? 1 : 0);
          throw failure;
        }
        long keptBytes = published ? objectBytes : 0; int keptFiles = published ? 1 : 0;
        try { Files.deleteIfExists(staging); syncDirectory(staging.getParent()); }
        catch (IOException cleanup) { keptBytes += objectBytes; keptFiles++; failure.addSuppressed(cleanup); }
        credit.keep(keptBytes, keptFiles);
        throw failure;
      }
    } catch (IOException | ProtocolException | RuntimeException failure) {
      primaryFailure = failure; throw failure;
    } finally {
      IOException failure = null;
      for (Received input : acquired) {
        try { input.release(); } catch (IOException cleanup) { if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup); }
      }
      unpin();
      if (failure != null) {
        if (primaryFailure != null) primaryFailure.addSuppressed(failure);
        else throw failure;
      }
    }
  }

  /**
   * Loads an immutable input and verifies its bounded metadata and complete payload.
   * @param identity expected identity, never authorization
   * @return retained input, or empty if no object was installed or its unadmitted body was reclaimed
   * @throws IOException for closed store or filesystem failure
   * @throws ProtocolException for corruption or invalid identity
   */
  public Optional<Stored> find(Identity identity) throws IOException, ProtocolException {
    validateIdentity(identity); pin();
    try {
      Path path = objectPath(identity);
      Metadata commitment = commitment(identity);
      if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return Optional.empty();
      Metadata metadata = readMetadata(path);
      requireCommitment(metadata, commitment);
      if (!metadata.identity.equals(identity)) throw Wire.integrity("retained payload identity differs");
      verify(path, metadata); return Optional.of(new Stored(path, metadata));
    } finally { unpin(); }
  }

  /** Retained input metadata. No method removes an installed payload or recycles its identity. */
  public final class Stored {
    private final Path path;
    private final Metadata metadata;
    private Stored(Path path, Metadata metadata) { this.path = path; this.metadata = metadata; }
    boolean belongsTo(SealedPayloadStore store) { return SealedPayloadStore.this == store; }

    <T> T withAdmission(SealedSessionStore sessions, SealedSessionStore.FundedOperation<T> operation)
        throws IOException, SQLException, ProtocolException {
      pin();
      try {
        verify(path, metadata);
        bind(sessions);
        return sessions.fundedTransaction(operation);
      } finally { unpin(); }
    }
    /** Returns the durable scoped identity.
     * @return immutable identity
     */
    public Identity identity() { return metadata.identity; }
    /** Returns the application header; assembled chunks have combined length/checksum and no chunk-info.
     * @return immutable application header
     */
    public SealedTransport.Header header() { return metadata.header; }
    /** Returns measured payload length.
     * @return payload octets, excluding retained metadata
     */
    public long length() { return metadata.length; }
    /** Returns the payload commitment.
     * @return defensive SHA-256 copy
     */
    public byte[] digest() { return metadata.digest.clone(); }
    /**
     * Revalidates the same opened file before returning its positioned, file-backed reader.
     * @return reader whose close releases the store handle, not the retained data
     * @throws IOException for filesystem failure or closed store
     * @throws ProtocolException for changed header or payload
     */
    public InputStream openStream() throws IOException, ProtocolException {
      pin(); FileChannel channel = null;
      try {
        channel = readChannel(path);
        Metadata current = readMetadata(channel);
        if (!Arrays.equals(encode(current), encode(metadata))) throw Wire.integrity("retained input metadata changed");
        requireCommitment(current, commitment(current.identity));
        long offset = channel.position(); verifyBody(channel, metadata); channel.position(offset);
        return new FilterInputStream(Channels.newInputStream(channel)) {
          private boolean readerClosed;
          @Override public synchronized void close() throws IOException {
            if (readerClosed) return;
            readerClosed = true;
            try { super.close(); } finally { unpin(); }
          }
        };
      } catch (IOException | ProtocolException | RuntimeException failure) {
        if (channel != null) try { channel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
        unpin(); throw failure;
      }
    }
  }

  /** Refuses to release the writer lock while receivers, receipts, reads, or installations remain active.
   * @throws IOException for active handles or lock close failure; an active store remains open
   */
  @Override public synchronized void close() throws IOException {
    if (closed) return;
    if (active != 0) throw new IOException("payload store still has active handles");
    closed = true;
    try { lock.release(); }
    finally {
      try { lockChannel.close(); }
      finally { if (!lockChannel.isOpen()) synchronized (OPEN_ROOTS) { OPEN_ROOTS.remove(root); } }
    }
  }

  private List<Received> ordered(List<Received> inputs) throws ProtocolException {
    Received first = inputs.getFirst();
    if (first.header.chunk() == null) {
      if (inputs.size() != 1) throw Wire.entity("unchunked entity has multiple streams");
      return inputs;
    }
    List<Received> sorted = new ArrayList<>(inputs);
    var indexes = new HashSet<BigInteger>();
    var offsets = new HashSet<BigInteger>();
    for (Received input : inputs) {
      if (!input.identity.equals(first.identity) || !sameApplicationHeader(input.header, first.header)
          || input.header.chunk() == null || !input.header.chunk().total().equals(BigInteger.valueOf(inputs.size()))
          || !indexes.add(input.header.chunk().index()) || !offsets.add(input.header.chunk().offset())) throw Wire.entity("inconsistent chunk identity, total, index, or duplicated range");
    }
    sorted.sort(Comparator.comparing(input -> input.header.chunk().offset()));
    BigInteger next = BigInteger.ZERO;
    for (Received input : sorted) {
      if (!input.header.chunk().offset().equals(next)) throw Wire.entity("chunk ranges overlap or have gaps");
      next = next.add(BigInteger.valueOf(input.length));
    }
    return sorted;
  }

  private byte[] hashInputs(List<Received> inputs, FileChannel output) throws IOException, ProtocolException {
    MessageDigest combined = SealedWork.sha256(); byte[] buffer = new byte[BLOCK];
    for (Received input : inputs) {
      MessageDigest part = SealedWork.sha256(); long count = 0;
      try (FileChannel channel = readChannel(input.path)) {
        if (channel.size() != input.length) throw Wire.integrity("spooled chunk length changed");
        int n;
        while ((n = channel.read(ByteBuffer.wrap(buffer))) != -1) {
          count += n;
          if (count > input.length) throw Wire.integrity("spooled chunk grew");
          part.update(buffer, 0, n); combined.update(buffer, 0, n);
          if (output != null) write(output, ByteBuffer.wrap(buffer, 0, n));
        }
      }
      if (count != input.length || !MessageDigest.isEqual(part.digest(), input.digest)) throw Wire.integrity("spooled chunk checksum changed");
    }
    return combined.digest();
  }

  private static boolean sameApplicationHeader(SealedTransport.Header left, SealedTransport.Header right) {
    return left.key().equals(right.key()) && Objects.equals(left.parent(), right.parent())
        && left.layer() == right.layer() && Objects.equals(left.contentType(), right.contentType())
        && left.metadata().equals(right.metadata());
  }

  private static SealedTransport.Header assembled(SealedTransport.Header header, long length, byte[] digest) {
    if (header.chunk() == null) return header;
    return new SealedTransport.Header(header.key(), header.parent(), header.layer(), header.contentType(),
        BigInteger.valueOf(length), digest, header.metadata(), null);
  }

  private record Metadata(Identity identity, SealedTransport.Header header, long length, byte[] digest) {}
  private static byte[] encode(Metadata metadata) throws ProtocolException {
    return SealedCbor.encode(Map.of("session", metadata.identity.session, "producer", SealedWork.producerBytes(metadata.identity.producer),
        "header", SealedTransport.header(metadata.header), "length", metadata.length, "digest", metadata.digest), MAX_METADATA);
  }
  private Metadata readMetadata(Path path) throws IOException, ProtocolException {
    try (FileChannel channel = readChannel(path)) { return readMetadata(channel); }
  }
  private Metadata readMetadata(FileChannel channel) throws IOException, ProtocolException {
    return readMetadata(channel, false);
  }
  private Metadata readMetadata(FileChannel channel, boolean commitment) throws IOException, ProtocolException {
    ByteBuffer prefix = ByteBuffer.allocate(12); readExactly(channel, prefix); prefix.flip();
    byte[] magic = new byte[8]; prefix.get(magic); int length = prefix.getInt();
    if (!Arrays.equals(magic, MAGIC) || length < 1 || length > MAX_METADATA) throw Wire.integrity("unsupported retained payload format");
    ByteBuffer encoded = ByteBuffer.allocate(length); readExactly(channel, encoded);
    ByteBuffer checksum = ByteBuffer.allocate(32); readExactly(channel, checksum);
    if (!MessageDigest.isEqual(SealedWork.sha256().digest(encoded.array()), checksum.array())) throw Wire.integrity("retained metadata checksum differs");
    var fields = SealedCbor.decode(encoded.array(), MAX_METADATA);
    SealedWork.only(fields, "session", "producer", "header", "length", "digest");
    byte[] producer = SealedWork.bytes(fields, "producer", 16);
    if (!(fields.get("header") instanceof byte[] headerBytes) || headerBytes.length < 4
        || ByteBuffer.wrap(headerBytes).getInt() != headerBytes.length - 4) throw Wire.integrity("retained header geometry differs");
    SealedTransport.Header header = SealedTransport.header(Arrays.copyOfRange(headerBytes, 4, headerBytes.length));
    if (header.chunk() != null) throw Wire.integrity("retained input still has chunk-info");
    ByteBuffer id = ByteBuffer.wrap(producer);
    Identity identity = new Identity(SealedWork.text(fields, "session"), new UUID(id.getLong(), id.getLong()), header.key());
    validate(identity, header);
    long bytes = SealedWork.bounded(fields, "length", limits.entityBytes);
    byte[] digest = SealedWork.bytes(fields, "digest", 32);
    long body = channel.size() - channel.position();
    if ((commitment ? body < 0 || body > bytes : body != bytes) || (header.payloadLength() != null && !header.payloadLength().equals(BigInteger.valueOf(bytes)))
        || (header.checksum() != null && !MessageDigest.isEqual(header.checksum(), digest))) throw Wire.integrity("retained payload geometry or commitments differ");
    return new Metadata(identity, header, bytes, digest);
  }

  private Metadata commitment(Identity identity) throws IOException, ProtocolException {
    Path path = commitmentPath(identity);
    if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return null;
    try (var channel = readChannel(path)) {
      Metadata metadata = readMetadata(channel, true);
      if (!metadata.identity.equals(identity)) throw Wire.integrity("retained commitment identity differs");
      return metadata;
    }
  }

  private static void requireCommitment(Metadata payload, Metadata commitment) throws ProtocolException {
    if (commitment != null && !Arrays.equals(encode(payload), encode(commitment))) {
      throw Wire.integrity("retained payload differs from its reclaimed commitment");
    }
  }
  private void verify(Path path, Metadata metadata) throws IOException, ProtocolException {
    try (FileChannel channel = readChannel(path)) {
      Metadata actual = readMetadata(channel);
      if (!Arrays.equals(encode(actual), encode(metadata))) throw Wire.integrity("retained metadata changed during verification");
      requireCommitment(actual, commitment(actual.identity));
      verifyBody(channel, actual);
    }
  }
  private static void verifyBody(FileChannel channel, Metadata metadata) throws IOException, ProtocolException {
    MessageDigest digest = SealedWork.sha256(); ByteBuffer buffer = ByteBuffer.allocate(BLOCK); long count = 0;
    while (channel.read(buffer) != -1) {
      count += buffer.position();
      if (count > metadata.length) throw Wire.integrity("retained payload grew");
      buffer.flip(); digest.update(buffer); buffer.clear();
    }
    if (count != metadata.length || !MessageDigest.isEqual(digest.digest(), metadata.digest)) throw Wire.integrity("retained payload checksum differs");
  }

  private synchronized void pin() throws IOException, ProtocolException {
    if (closed) throw new IOException("payload store is closed");
    if (active >= MAX_ACTIVE) throw Wire.limit("payload operation and handle capacity exhausted");
    active = Math.incrementExact(active);
  }
  private synchronized void unpin() { active--; }
  private synchronized Credit reserve(boolean retained, long bytes, int files) throws ProtocolException {
    if (retained) {
      if (bytes > limits.retainedBytes - retainedBytes || files > limits.retainedFiles - retainedFiles) throw Wire.limit("retained payload capacity exhausted");
      retainedBytes += bytes; retainedFiles += files;
    } else {
      if (bytes > limits.temporaryBytes - temporaryBytes || files > limits.temporaryFiles - temporaryFiles) throw Wire.limit("temporary payload capacity exhausted");
      temporaryBytes += bytes; temporaryFiles += files;
    }
    return new Credit(retained, bytes, files);
  }
  private final class Credit {
    final boolean retained;
    long bytes; int files;
    Credit(boolean retained, long bytes, int files) { this.retained = retained; this.bytes = bytes; this.files = files; }
    void grow(int count) throws ProtocolException {
      synchronized (SealedPayloadStore.this) {
        if (count > limits.temporaryBytes - temporaryBytes) throw Wire.limit("temporary payload byte capacity exhausted");
        temporaryBytes += count; bytes += count;
      }
    }
    void keep(long keptBytes, int keptFiles) {
      synchronized (SealedPayloadStore.this) {
        if (keptBytes < 0 || keptBytes > bytes || keptFiles < 0 || keptFiles > files) throw new IllegalStateException("invalid payload credit release");
        if (retained) { retainedBytes -= bytes - keptBytes; retainedFiles -= files - keptFiles; }
        else { temporaryBytes -= bytes - keptBytes; temporaryFiles -= files - keptFiles; }
        bytes = keptBytes; files = keptFiles;
      }
    }
    void release() { keep(0, 0); }
  }

  private void scan(String name, boolean retained) throws IOException, ProtocolException {
    Path directory = root.resolve(name);
    if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) throw Wire.integrity("payload subdirectory is absent or not a directory");
    try (var entries = Files.newDirectoryStream(directory)) {
      for (Path path : entries) {
        String file = path.getFileName().toString();
        boolean object = retained && file.matches("[0-9a-f]{64}\\.pay");
        boolean commitment = retained && file.matches("[0-9a-f]{64}\\.commit");
        boolean temporary = file.matches((retained ? "install-" : "") + "[0-9a-f-]{36}\\.tmp");
        if ((!object && !commitment && !temporary) || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) throw Wire.integrity("unexpected payload-store entry");
        reserve(retained, Files.size(path), 1);
        if (object) {
          Metadata metadata = readMetadata(path);
          if (!objectPath(metadata.identity).equals(path)) throw Wire.integrity("payload name does not bind its identity");
          requireCommitment(metadata, commitment(metadata.identity));
        } else if (commitment) {
          try (var channel = readChannel(path)) {
            Metadata metadata = readMetadata(channel, true);
            if (!commitmentPath(metadata.identity).equals(path)) throw Wire.integrity("commitment name does not bind its identity");
          }
        }
      }
    }
  }
  private Path objectPath(Identity identity) throws ProtocolException {
    byte[] key = SealedCbor.encode(Map.of("session", identity.session, "producer", SealedWork.producerBytes(identity.producer),
        "scope", identity.entity.scopeId(), "entity", identity.entity.entityId()), 1024);
    return root.resolve("objects").resolve(HexFormat.of().formatHex(SealedWork.sha256().digest(key)) + ".pay");
  }
  private Path commitmentPath(Identity identity) throws ProtocolException {
    Path object = objectPath(identity);
    String name = object.getFileName().toString();
    return object.resolveSibling(name.substring(0, name.length() - 4) + ".commit");
  }
  private static void validate(Identity identity, SealedTransport.Header header) throws ProtocolException {
    validateIdentity(identity); SealedTransport.header(header);
    if (!identity.entity.equals(header.key())) throw Wire.entity("payload identity differs from header");
    String explicit = header.metadata().get("pipestream.session-id");
    if (explicit != null && !explicit.equals(identity.session)) throw Wire.entity("payload session metadata differs");
  }
  private static void validateIdentity(Identity identity) throws ProtocolException {
    if (!SealedWork.validSessionId(identity.session) || identity.producer.equals(new UUID(0, 0))
        || identity.entity.scopeId() < 0 || identity.entity.scopeId() > 0xffff_ffffL
        || identity.entity.entityId() < 1 || identity.entity.entityId() > Wire.MAX_ENTITY_ID) throw Wire.entity("invalid durable payload identity");
  }
  private static byte[] policy(Limits limits, UUID identity) throws ProtocolException {
    return SealedCbor.encode(Map.of("format", "pipestream-java-payload-v3", "store-id", SealedWork.producerBytes(identity), "temporary-bytes", limits.temporaryBytes,
        "temporary-files", limits.temporaryFiles, "retained-bytes", limits.retainedBytes, "retained-files", limits.retainedFiles,
        "entity-bytes", limits.entityBytes, "chunks", limits.chunks), 4096);
  }
  private static void inspectRoot(Path root) throws IOException, ProtocolException {
    try (var entries = Files.newDirectoryStream(root)) {
      for (Path entry : entries) if (!ROOT_NAMES.contains(entry.getFileName().toString()) || Files.isSymbolicLink(entry)) throw Wire.integrity("foreign payload directory or symbolic link");
    }
  }
  private static FileChannel readChannel(Path path) throws IOException, ProtocolException {
    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) throw Wire.integrity("payload file is missing or not a regular file");
    return FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
  }
  private static byte[] readBounded(Path path, int maximum) throws IOException, ProtocolException {
    try (FileChannel channel = readChannel(path)) {
      if (channel.size() > maximum) throw Wire.integrity("metadata file exceeds bound");
      ByteBuffer buffer = ByteBuffer.allocate((int) channel.size()); readExactly(channel, buffer); return buffer.array();
    }
  }
  private static void readExactly(FileChannel channel, ByteBuffer bytes) throws IOException, ProtocolException {
    while (bytes.hasRemaining()) if (channel.read(bytes) == -1) throw Wire.integrity("truncated payload metadata");
  }
  private static void write(FileChannel channel, ByteBuffer bytes) throws IOException {
    while (bytes.hasRemaining()) channel.write(bytes);
  }
  private static void syncDirectory(Path directory) throws IOException {
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) { channel.force(true); }
  }
  private static void discard(Path path, Credit credit) throws IOException {
    Files.delete(path); syncDirectory(path.getParent()); credit.release();
  }
}
