package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Records.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Independent Java V2 immutable input storage, not work admission or authorization. Blocking calls
 * belong outside transport event loops. A private local directory has one cooperative process
 * owner; no installed object is deleted here without an authoritative liveness proof.
 */
final class InputStore implements AutoCloseable {
  private static final byte[] MAGIC = {'P', 'S', 'J', 'V', '2', 'I', '0', '1'};
  private static final int METADATA_LIMIT = 8192;
  private static final int PREFIX = 12;
  private static final int CHECKSUM = 32;
  private static final int BLOCK = 8192;
  private static final String FORMAT = "pipestream-java-v2-input-store-2";
  private static final Set<String> ROOT_NAMES =
      Set.of("writer.lock", "policy.cbor", "pending", "objects");
  private static final Set<Path> OPEN_ROOTS = new HashSet<>();

  /**
   * Persistent file-length and file-name bounds, not filesystem block or RSS limits.
   *
   * @param bytes aggregate object and temporary file bytes, including bounded headers
   * @param files aggregate file names, including both names during installation
   * @param objectBytes maximum input payload length
   * @param handles simultaneous receivers and readers
   */
  record Limits(long bytes, int files, long objectBytes, int handles) {
    /** Reject unusable or unrepresentable local bounds. */
    Limits {
      if (bytes < 1
          || files < 1
          || objectBytes < 0
          || objectBytes > (Long.MAX_VALUE - METADATA_LIMIT - PREFIX - CHECKSUM) / 2
          || handles < 1
          || handles > 128) throw new IllegalArgumentException("invalid V2 input storage limits");
    }
  }

  /**
   * Conservative charges; in-progress reception reserves both possible installation names.
   *
   * @param bytes charged complete file bytes
   * @param files charged file names
   * @param handles active receivers and readers
   */
  record Usage(long bytes, int files, int handles) {}

  /** Package-local observation points for actual filesystem interruption tests. */
  enum Phase {
    /** Staging bytes and header were synchronized. */
    RECEIVED,
    /** The immutable object name was linked. */
    LINKED,
    /** The linked object was synchronized. */
    OBJECT_SYNCED,
    /** The staging name was removed. */
    STAGING_REMOVED,
    /** Recovery completed its retained-file audit. */
    RECOVERY_AUDITED
  }

  /** Fault observation only; never a processing callback or authorization policy. */
  @FunctionalInterface
  interface Probe {
    /**
     * Observe one completed filesystem boundary.
     *
     * @param phase reached boundary
     * @throws IOException injected filesystem failure
     */
    void reached(Phase phase) throws IOException;
  }

  private record Envelope(UUID store, Commitments.Context context, InputHeader header) {}

  private record Inspected(Envelope envelope, long offset, long size) {}

  private final Path root;
  private final Limits limits;
  private final UUID identity;
  private final UUID authorityIdentity;
  private final FileChannel lockChannel;
  private final FileLock lock;
  private final Probe probe;
  private long bytes;
  private int files;
  private int handles;
  private boolean closed;

  private InputStore(
      Path root,
      Limits limits,
      UUID identity,
      UUID authorityIdentity,
      FileChannel channel,
      FileLock lock,
      Probe probe) {
    this.root = root;
    this.limits = limits;
    this.identity = identity;
    this.authorityIdentity = authorityIdentity;
    this.lockChannel = channel;
    this.lock = lock;
    this.probe = probe;
  }

  /**
   * Install a new private directory; existing directories are never adopted or reset.
   *
   * @param directory new directory under an existing local parent
   * @param limits immutable storage policy
   * @return exclusively owned input store
   * @throws IOException for existing storage, unsupported filesystem or installation failure
   */
  static InputStore initialize(Path directory, Limits limits) throws IOException {
    return initialize(directory, limits, null);
  }

  /**
   * Initialize with a package-local interruption observer.
   *
   * @param directory new directory
   * @param limits immutable policy
   * @param probe fault observer, or null
   * @return exclusively owned store
   * @throws IOException installation failure
   */
  static InputStore initialize(Path directory, Limits limits, Probe probe) throws IOException {
    return initialize(directory, limits, null, probe);
  }

  /**
   * Install immutable storage for exactly one database installation. The database must separately
   * commit this input store's identity before it may reference an object. Neither step admits work.
   *
   * @param directory new input-store directory
   * @param limits immutable file policy
   * @param authorityIdentity persistent database installation identity, not a protocol issuer name
   * @return exclusively owned input store
   * @throws IOException installation failure
   */
  static InputStore initializeForAuthority(Path directory, Limits limits, UUID authorityIdentity)
      throws IOException {
    Objects.requireNonNull(authorityIdentity);
    if (authorityIdentity.equals(new UUID(0, 0)))
      throw new IllegalArgumentException("zero authority installation identity");
    return initialize(directory, limits, authorityIdentity, null);
  }

  private static InputStore initialize(
      Path directory, Limits limits, UUID authorityIdentity, Probe probe) throws IOException {
    Objects.requireNonNull(limits);
    Path requested = directory.toAbsolutePath().normalize();
    Files.createDirectory(
        requested,
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    sync(requested.getParent());
    return acquire(requested, limits, true, authorityIdentity, probe);
  }

  /**
   * Recover exactly this policy and format; inspect all retained bodies before removing abandoned
   * temporary names or exposing new capacity.
   *
   * @param directory existing input store
   * @param limits exact retained policy
   * @return audited exclusive store
   * @throws IOException for missing, locked or incompatible storage
   */
  static InputStore open(Path directory, Limits limits) throws IOException {
    return open(directory, limits, null);
  }

  /**
   * Recover with a package-local interruption observer.
   *
   * @param directory existing directory
   * @param limits exact retained policy
   * @param probe fault observer, or null
   * @return audited exclusive store
   * @throws IOException recovery failure
   */
  static InputStore open(Path directory, Limits limits, Probe probe) throws IOException {
    Objects.requireNonNull(limits);
    return acquire(directory.toAbsolutePath().normalize(), limits, false, null, probe);
  }

  private static InputStore acquire(
      Path requested, Limits limits, boolean initialize, UUID configuredAuthority, Probe probe)
      throws IOException {
    if (!Files.isDirectory(requested, LinkOption.NOFOLLOW_LINKS))
      throw new IOException("V2 input store requires a real existing directory");
    Path root = requested.toRealPath();
    synchronized (OPEN_ROOTS) {
      // Opening then closing a competing lock channel can release another channel's POSIX lock.
      if (!OPEN_ROOTS.add(root)) throw new IOException("V2 input store is already open");
    }
    FileChannel channel = null;
    FileLock lock = null;
    try {
      Path lockPath = root.resolve("writer.lock");
      channel =
          initialize
              ? create(lockPath)
              : FileChannel.open(lockPath, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
      try {
        lock = channel.tryLock();
      } catch (OverlappingFileLockException failure) {
        throw new IOException("V2 input store is already locked", failure);
      }
      if (lock == null) throw new IOException("V2 input store is already locked");
      UUID identity;
      UUID authorityIdentity;
      if (initialize) {
        identity = UUID.randomUUID();
        authorityIdentity = configuredAuthority;
        try (FileChannel policy = create(root.resolve("policy.cbor"))) {
          writeAll(policy, ByteBuffer.wrap(policy(limits, identity, authorityIdentity)));
          policy.force(true);
        }
        Files.createDirectory(root.resolve("pending"));
        Files.createDirectory(root.resolve("objects"));
        sync(root);
      } else {
        byte[] retained = bounded(root.resolve("policy.cbor"), 4096);
        try {
          Cbor.Reader in = new Cbor.Reader(retained, 4096);
          in.exact(8);
          if (!FORMAT.equals(in.text(128))) throw corrupt("unsupported input store format");
          ByteBuffer id = ByteBuffer.wrap(in.bytes(16));
          identity = new UUID(id.getLong(), id.getLong());
          if (in.nullable()) authorityIdentity = null;
          else {
            ByteBuffer authority = ByteBuffer.wrap(in.bytes(16));
            authorityIdentity = new UUID(authority.getLong(), authority.getLong());
            if (authorityIdentity.equals(new UUID(0, 0)))
              throw corrupt("zero authority installation identity");
          }
          in.number();
          in.number();
          in.number();
          in.number();
          in.bytes(32);
          in.end();
          if (!Arrays.equals(retained, policy(limits, identity, authorityIdentity)))
            throw corrupt("input store identity, policy or checksum differs");
        } catch (ProtocolError failure) {
          throw corrupt("invalid input store policy", failure);
        }
      }
      InputStore store =
          new InputStore(root, limits, identity, authorityIdentity, channel, lock, probe);
      store.recover();
      return store;
    } catch (IOException | RuntimeException failure) {
      if (lock != null)
        try {
          lock.release();
        } catch (IOException cleanup) {
          failure.addSuppressed(cleanup);
        }
      if (channel != null)
        try {
          channel.close();
        } catch (IOException cleanup) {
          failure.addSuppressed(cleanup);
        }
      if (channel == null || !channel.isOpen())
        synchronized (OPEN_ROOTS) {
          OPEN_ROOTS.remove(root);
        }
      throw failure;
    }
  }

  /**
   * Inspect accounting without granting admission or a read lease.
   *
   * @return current conservative charges
   */
  synchronized Usage usage() {
    return new Usage(bytes, files, handles);
  }

  /**
   * Get the persistent local store identity for an eventual authority-store binding.
   *
   * @return installation identity, not a protocol principal
   */
  UUID identity() {
    return identity;
  }

  /**
   * Get the immutable database installation this root was created for.
   *
   * @return database identity, or empty for standalone storage which cannot later be adopted
   */
  Optional<UUID> authorityIdentity() {
    return Optional.ofNullable(authorityIdentity);
  }

  /**
   * Verify and synchronize the retained ownership claim before an authority transaction uses it.
   * The caller must keep this store's monitor through that transaction to exclude concurrent close.
   *
   * @param expected exact database installation identity
   * @throws IOException closed storage, unbound or mismatched ownership, changed policy or failed
   *     sync
   */
  synchronized void verifyAuthority(UUID expected) throws IOException {
    ensureOpen();
    if (authorityIdentity == null || !authorityIdentity.equals(expected))
      throw corrupt("input store belongs to a different or no authority installation");
    if (!Arrays.equals(
        bounded(root.resolve("policy.cbor"), 4096), policy(limits, identity, authorityIdentity)))
      throw corrupt("retained input policy changed");
    forceFile(root.resolve("policy.cbor"));
    sync(root);
  }

  /**
   * Reserve bounded reception after the caller validates authorization, membership and policy.
   * Neither local producer is implicitly authorized by this file-storage primitive.
   *
   * @param context already authenticated session context
   * @param header structurally valid immutable input header
   * @param selected negotiated connection limits
   * @param nowNanos monotonic reception start
   * @return single-use receiver
   * @throws IOException for filesystem failure or a closed store
   */
  synchronized Receiver begin(
      Commitments.Context context,
      InputHeader header,
      Messages.Capabilities selected,
      long nowNanos)
      throws IOException {
    ensureOpen();
    Envelope envelope = envelope(context, header);
    if (header.parameters().input().length() > limits.objectBytes())
      throw ProtocolError.limit("input exceeds local object ceiling");
    ObjectStream.Payload verifier =
        new ObjectStream.Payload(
            header.parameters().input().length(),
            header.parameters().input().sha256(),
            selected,
            nowNanos);
    byte[] metadata = metadata(envelope);
    long size = add(add(PREFIX + CHECKSUM, metadata.length), header.parameters().input().length());
    long reservation = multiply(size, 2);
    reserve(reservation, 2);
    Path path = root.resolve("pending").resolve(UUID.randomUUID() + ".part");
    FileChannel output = null;
    try {
      output = create(path);
      writeAll(output, ByteBuffer.allocate(PREFIX).put(MAGIC).putInt(metadata.length).flip());
      writeAll(output, ByteBuffer.wrap(metadata));
      writeAll(output, ByteBuffer.wrap(Commitments.sha256().digest(metadata)));
      return new Receiver(path, output, envelope, verifier, size);
    } catch (IOException | RuntimeException failure) {
      if (output != null)
        try {
          output.close();
        } catch (IOException cleanup) {
          failure.addSuppressed(cleanup);
        }
      try {
        // CREATE_NEW failure does not grant ownership of an already existing temporary name.
        if (output != null) {
          Files.deleteIfExists(path);
          sync(root.resolve("pending"));
        }
        release(reservation, 2);
      } catch (IOException cleanup) {
        failure.addSuppressed(cleanup);
      }
      handles--;
      throw failure;
    }
  }

  /**
   * Find and fully verify an immutable object for exactly this context and header. This is not an
   * operation-replay receipt; an authority must still resolve operation identity and admission.
   *
   * @param context expected session identity
   * @param header exact input parameters
   * @return verified object, if installed
   * @throws IOException for corruption, filesystem failure or a closed store
   */
  synchronized Optional<Stored> find(Commitments.Context context, InputHeader header)
      throws IOException {
    ensureOpen();
    Envelope expected = envelope(context, header);
    Path path = objectPath(expected);
    if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return Optional.empty();
    inspect(path, expected, true);
    // Visibility after an interrupted installation does not prove its force or directory sync.
    forceFile(path);
    sync(root.resolve("objects"));
    return Optional.of(new Stored(expected, path));
  }

  /** One bounded immutable reception; finish means actual transport FIN, not admission. */
  final class Receiver implements AutoCloseable {
    private final Path path;
    private final FileChannel channel;
    private final Envelope envelope;
    private final ObjectStream.Payload verifier;
    private final long size;
    private boolean linked;
    private boolean ended;

    private Receiver(
        Path path,
        FileChannel channel,
        Envelope envelope,
        ObjectStream.Payload verifier,
        long size) {
      this.path = path;
      this.channel = channel;
      this.envelope = envelope;
      this.verifier = verifier;
      this.size = size;
    }

    /**
     * Hash and write supplied bytes without retaining a payload-sized array.
     *
     * @param input payload bytes, consumed on success
     * @param nowNanos monotonic payload-progress observation
     * @throws IOException for file failure or a closed receiver
     */
    synchronized void write(ByteBuffer input, long nowNanos) throws IOException {
      if (ended) throw new IOException("input receiver is closed");
      try {
        verifier.feed(input.duplicate(), nowNanos);
        writeAll(channel, input);
      } catch (IOException | RuntimeException failure) {
        abandon(failure);
        throw failure;
      }
    }

    /**
     * Drive idle and lifetime expiry even when no more payload bytes arrive.
     *
     * @param nowNanos current monotonic time
     * @throws IOException cleanup failure or closed receiver
     */
    synchronized void checkDeadline(long nowNanos) throws IOException {
      if (ended) throw new IOException("input receiver is closed");
      try {
        verifier.checkDeadline(nowNanos);
      } catch (RuntimeException failure) {
        abandon(failure);
        throw failure;
      }
    }

    /**
     * Verify actual FIN, force all bytes, and install without replacing any existing object. A
     * returned object is not an admission receipt or a restartable job.
     *
     * @param nowNanos monotonic transport FIN observation
     * @return durably installed immutable object
     * @throws IOException for failed installation or a closed receiver
     */
    synchronized Stored finish(long nowNanos) throws IOException {
      if (ended) throw new IOException("input receiver is closed");
      try {
        verifier.finish(nowNanos);
        channel.force(true);
        reached(Phase.RECEIVED);
        synchronized (InputStore.this) {
          ensureOpen();
          Path target = objectPath(envelope);
          try {
            Files.createLink(target, path);
            linked = true;
            reached(Phase.LINKED);
          } catch (FileAlreadyExistsException duplicate) {
            inspect(target, envelope, true);
            // An earlier attempt may have linked every byte but failed its directory force.
            try (FileChannel retained =
                FileChannel.open(target, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
              retained.force(true);
            }
          }
          sync(root.resolve("objects"));
          reached(Phase.OBJECT_SYNCED);
          Stored stored = new Stored(envelope, target);
          close();
          return stored;
        }
      } catch (IOException | RuntimeException failure) {
        abandon(failure);
        throw failure;
      }
    }

    private void abandon(Exception failure) {
      try {
        close();
      } catch (IOException cleanup) {
        failure.addSuppressed(cleanup);
      }
    }

    /**
     * Abort unfinished reception or release its staging name after installation. Accounting is
     * refunded only after deletion is synchronized. Installed objects are never removed here.
     *
     * @throws IOException if close, deletion or synchronization fails
     */
    @Override
    public synchronized void close() throws IOException {
      if (ended) return;
      verifier.abort();
      channel.close();
      Files.deleteIfExists(path);
      reached(Phase.STAGING_REMOVED);
      sync(root.resolve("pending"));
      synchronized (InputStore.this) {
        release(linked ? size : multiply(size, 2), linked ? 1 : 2);
        handles--;
      }
      ended = true;
    }
  }

  /** Immutable installed bytes. Possession is neither authorization nor an admission promise. */
  final class Stored {
    private final Envelope envelope;
    private final Path path;

    private Stored(Envelope envelope, Path path) {
      this.envelope = envelope;
      this.path = path;
    }

    /**
     * Returns the original immutable input header.
     *
     * @return original immutable input header
     */
    InputHeader header() {
      return envelope.header();
    }

    /**
     * Returns the owner-qualified context, not a bearer credential.
     *
     * @return owner-qualified context, not a bearer credential
     */
    Commitments.Context context() {
      return envelope.context();
    }

    /**
     * Returns the exact payload length without the private storage header.
     *
     * @return exact payload length without the private storage header
     */
    long length() {
      return envelope.header().parameters().input().length();
    }

    /**
     * Returns a bounded opaque local reference, never a caller-controlled pathname.
     *
     * @return bounded opaque local reference, never a caller-controlled pathname
     */
    String reference() {
      return path.getFileName().toString();
    }

    /**
     * Verify retained bytes on one descriptor, then expose a bounded payload-only reader. The
     * higher authority layer must validate its current read or execution authorization first.
     *
     * @return reader whose close releases a handle
     * @throws IOException for corruption, closed storage or file failure
     */
    InputStream openStream() throws IOException {
      synchronized (InputStore.this) {
        pin();
        FileChannel input = null;
        try {
          input = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
          Inspected inspected = inspect(input, envelope, true);
          input.position(inspected.offset());
          return new Reader(input, length());
        } catch (IOException | RuntimeException failure) {
          if (input != null)
            try {
              input.close();
            } catch (IOException cleanup) {
              failure.addSuppressed(cleanup);
            }
          handles--;
          throw failure;
        }
      }
    }
  }

  private final class Reader extends InputStream {
    private final InputStream input;
    private long remaining;
    private boolean ended;

    Reader(FileChannel channel, long length) {
      input = Channels.newInputStream(channel);
      remaining = length;
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      return read(one, 0, 1) == -1 ? -1 : Byte.toUnsignedInt(one[0]);
    }

    @Override
    public synchronized int read(byte[] buffer, int offset, int length) throws IOException {
      Objects.checkFromIndexSize(offset, length, buffer.length);
      if (ended) throw new IOException("input reader is closed");
      if (length == 0) return 0;
      if (remaining == 0) return -1;
      int count = input.read(buffer, offset, (int) Math.min(length, remaining));
      if (count < 0) throw new EOFException("retained input shortened during read");
      remaining -= count;
      return count;
    }

    @Override
    public synchronized void close() throws IOException {
      if (ended) return;
      input.close();
      synchronized (InputStore.this) {
        handles--;
      }
      ended = true;
    }
  }

  /**
   * Close only when no receive/read handle can still reference this owner's files.
   *
   * @throws IOException if handles remain or lock release fails
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) return;
    if (handles != 0) throw new IOException("V2 input store still has active handles");
    lock.release();
    lockChannel.close();
    closed = true;
    synchronized (OPEN_ROOTS) {
      OPEN_ROOTS.remove(root);
    }
  }

  private void recover() throws IOException {
    Set<String> names = new HashSet<>();
    try (var entries = Files.newDirectoryStream(root)) {
      for (Path entry : entries) {
        String name = entry.getFileName().toString();
        if (!ROOT_NAMES.contains(name)) throw corrupt("foreign input-store entry");
        names.add(name);
      }
    }
    if (!names.equals(ROOT_NAMES)) throw corrupt("incomplete input-store installation");
    for (String name : ROOT_NAMES) {
      Path path = root.resolve(name);
      boolean directory = name.equals("pending") || name.equals("objects");
      if (directory
          ? !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)
          : !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
        throw corrupt("input-store entry has wrong file type");
    }
    // Complete validation precedes cleanup. A malformed live object must not be hidden by deletion.
    for (String namespace : new String[] {"objects", "pending"}) {
      try (var entries = Files.newDirectoryStream(root.resolve(namespace))) {
        for (Path path : entries) {
          String name = path.getFileName().toString();
          boolean object = namespace.equals("objects");
          if (!name.matches(
                  object
                      ? "[0-9a-f]{64}\\.input"
                      : "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.part")
              || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
            throw corrupt("unknown input-store file");
          long size = Files.size(path);
          if (size > add(limits.objectBytes(), METADATA_LIMIT + PREFIX + CHECKSUM))
            throw corrupt("input-store file exceeds local ceiling");
          if (object) {
            Inspected inspected = inspect(path, null, true);
            if (!objectPath(inspected.envelope()).equals(path))
              throw corrupt("input-store filename contradicts identity");
          }
          bytes = add(bytes, size);
          if (files == limits.files() || bytes > limits.bytes())
            throw ProtocolError.limit("retained input storage exceeds policy");
          files++;
        }
      }
    }
    reached(Phase.RECOVERY_AUDITED);
    // A complete policy may have survived a failed initial force; re-establish it before use.
    forceFile(root.resolve("policy.cbor"));
    sync(root);
    try (var entries = Files.newDirectoryStream(root.resolve("pending"))) {
      for (Path path : entries) {
        long size = Files.size(path);
        Files.delete(path);
        sync(root.resolve("pending"));
        release(size, 1);
      }
    }
  }

  private Envelope envelope(Commitments.Context context, InputHeader header) {
    Objects.requireNonNull(context);
    Objects.requireNonNull(header);
    if (context.generation() != header.generation())
      throw new ProtocolError(ProtocolError.Code.CONFLICT, "input generation differs from context");
    Wire.encodeRecord(header, Wire.HEADER_LIMIT);
    return new Envelope(identity, context, header);
  }

  private Path objectPath(Envelope envelope) {
    byte[] digest = Commitments.sha256().digest(metadata(envelope));
    return root.resolve("objects").resolve(HexFormat.of().formatHex(digest) + ".input");
  }

  private static byte[] metadata(Envelope envelope) {
    Cbor.Writer out = new Cbor.Writer(METADATA_LIMIT);
    out.array(5);
    out.bytes(uuid(envelope.store()));
    out.text(envelope.context().authority(), 128);
    out.text(envelope.context().owner(), 128);
    out.number(envelope.context().generation());
    RecordCodec.write(out, envelope.header());
    return out.finish();
  }

  private Inspected inspect(Path path, Envelope expected, boolean hash) throws IOException {
    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
      throw corrupt("input object is not a regular file");
    try (FileChannel input =
        FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
      return inspect(input, expected, hash);
    }
  }

  private Inspected inspect(FileChannel input, Envelope expected, boolean hash) throws IOException {
    try {
      ByteBuffer prefix = ByteBuffer.allocate(PREFIX);
      readAll(input, prefix);
      prefix.flip();
      byte[] magic = new byte[8];
      prefix.get(magic);
      int count = prefix.getInt();
      if (!Arrays.equals(magic, MAGIC) || count < 1 || count > METADATA_LIMIT)
        throw corrupt("invalid input object header");
      byte[] encoded = new byte[count];
      readAll(input, ByteBuffer.wrap(encoded));
      byte[] checksum = new byte[CHECKSUM];
      readAll(input, ByteBuffer.wrap(checksum));
      if (!MessageDigest.isEqual(checksum, Commitments.sha256().digest(encoded)))
        throw corrupt("input metadata checksum differs");
      Cbor.Reader in = new Cbor.Reader(encoded, METADATA_LIMIT);
      in.exact(5);
      ByteBuffer storeId = ByteBuffer.wrap(in.bytes(16));
      UUID store = new UUID(storeId.getLong(), storeId.getLong());
      Commitments.Context context =
          new Commitments.Context(in.text(128), in.text(128), in.number());
      InputHeader header = RecordCodec.inputHeader(in);
      in.end();
      Envelope envelope = new Envelope(store, context, header);
      if (!store.equals(identity)
          || context.generation() != header.generation()
          || (expected != null && !envelope.equals(expected))
          || !Arrays.equals(encoded, metadata(envelope)))
        throw corrupt("input object identity differs");
      Wire.encodeRecord(header, Wire.HEADER_LIMIT);
      Input descriptor = header.parameters().input();
      long offset = PREFIX + CHECKSUM + (long) count;
      long size = add(offset, descriptor.length());
      if (descriptor.length() > limits.objectBytes() || input.size() != size)
        throw corrupt("input object length differs");
      if (hash) {
        MessageDigest digest = Commitments.sha256();
        ByteBuffer block = ByteBuffer.allocate(BLOCK);
        long remaining = descriptor.length();
        while (remaining != 0) {
          block.clear().limit((int) Math.min(BLOCK, remaining));
          readAll(input, block);
          remaining -= block.position();
          block.flip();
          digest.update(block);
        }
        if (!MessageDigest.isEqual(digest.digest(), descriptor.sha256().bytes()))
          throw corrupt("retained input digest differs");
      }
      return new Inspected(envelope, offset, size);
    } catch (ProtocolError failure) {
      throw corrupt("invalid retained input", failure);
    }
  }

  private static byte[] policy(Limits limits, UUID identity, UUID authorityIdentity) {
    Cbor.Writer committed = new Cbor.Writer(1024);
    policyFields(committed, limits, identity, authorityIdentity, 7);
    Cbor.Writer out = new Cbor.Writer(1024);
    policyFields(out, limits, identity, authorityIdentity, 8);
    out.bytes(Commitments.sha256().digest(committed.finish()));
    return out.finish();
  }

  private static void policyFields(
      Cbor.Writer out, Limits limits, UUID identity, UUID authorityIdentity, int count) {
    out.array(count);
    out.text(FORMAT, 128);
    out.bytes(uuid(identity));
    if (authorityIdentity == null) out.nil();
    else out.bytes(uuid(authorityIdentity));
    out.number(limits.bytes());
    out.number(limits.files());
    out.number(limits.objectBytes());
    out.number(limits.handles());
  }

  private static byte[] uuid(UUID identity) {
    return ByteBuffer.allocate(16)
        .putLong(identity.getMostSignificantBits())
        .putLong(identity.getLeastSignificantBits())
        .array();
  }

  private void ensureOpen() throws IOException {
    if (closed) throw new IOException("V2 input store is closed");
  }

  private void reached(Phase phase) throws IOException {
    if (probe != null) probe.reached(phase);
  }

  private void pin() throws IOException {
    ensureOpen();
    if (handles >= limits.handles()) throw ProtocolError.limit("input handle capacity exhausted");
    handles++;
  }

  private void reserve(long addedBytes, int addedFiles) throws IOException {
    if (addedBytes > limits.bytes() - bytes || addedFiles > limits.files() - files)
      throw ProtocolError.limit("input storage capacity exhausted");
    pin();
    bytes += addedBytes;
    files += addedFiles;
  }

  private void release(long releasedBytes, int releasedFiles) {
    if (releasedBytes > bytes || releasedFiles > files)
      throw new IllegalStateException("input accounting underflow");
    bytes -= releasedBytes;
    files -= releasedFiles;
  }

  private static FileChannel create(Path path) throws IOException {
    return FileChannel.open(
        path,
        Set.of(
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE,
            StandardOpenOption.READ,
            LinkOption.NOFOLLOW_LINKS),
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
  }

  private static byte[] bounded(Path path, int maximum) throws IOException {
    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
      throw corrupt("missing regular policy file");
    try (FileChannel input =
        FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
      long length = input.size();
      if (length < 1 || length > maximum) throw corrupt("invalid policy length");
      byte[] value = new byte[(int) length];
      readAll(input, ByteBuffer.wrap(value));
      return value;
    }
  }

  private static void readAll(FileChannel channel, ByteBuffer bytes) throws IOException {
    while (bytes.hasRemaining())
      if (channel.read(bytes) <= 0) throw new EOFException("truncated input file");
  }

  private static void writeAll(FileChannel channel, ByteBuffer bytes) throws IOException {
    while (bytes.hasRemaining())
      if (channel.write(bytes) <= 0) throw new IOException("input file made no progress");
  }

  private static void sync(Path directory) throws IOException {
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }

  private static void forceFile(Path path) throws IOException {
    try (FileChannel channel =
        FileChannel.open(path, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
      channel.force(true);
    }
  }

  private static long add(long left, long right) {
    try {
      return Math.addExact(left, right);
    } catch (ArithmeticException overflow) {
      throw ProtocolError.limit("input size overflow");
    }
  }

  private static long multiply(long value, long factor) {
    try {
      return Math.multiplyExact(value, factor);
    } catch (ArithmeticException overflow) {
      throw ProtocolError.limit("input reservation overflow");
    }
  }

  private static IOException corrupt(String detail) {
    return new IOException("V2 input storage: " + detail);
  }

  private static IOException corrupt(String detail, Throwable cause) {
    return new IOException("V2 input storage: " + detail, cause);
  }
}
