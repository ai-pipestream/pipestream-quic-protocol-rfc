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
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Independent Java V2 immutable input storage and durable output funding, not work admission or
 * authorization. Blocking calls belong outside transport event loops. A private local directory has
 * one cooperative process owner; no installed object or reservation is deleted here without an
 * authoritative liveness proof.
 */
final class InputStore implements AutoCloseable {
  private static final byte[] MAGIC = {'P', 'S', 'J', 'V', '2', 'I', '0', '1'};
  private static final byte[] FUNDING_MAGIC = {'P', 'S', 'J', 'V', '2', 'R', '0', '1'};
  private static final int METADATA_LIMIT = 8192;
  private static final int PREFIX = 12;
  private static final int CHECKSUM = 32;
  private static final int BLOCK = 8192;
  private static final int OUTPUT_OVERHEAD = PREFIX + METADATA_LIMIT + CHECKSUM;
  private static final String FORMAT = "pipestream-java-v2-input-store-4";
  private static final Set<String> ROOT_NAMES =
      Set.of(
          "writer.lock",
          "policy.cbor",
          "pending",
          "objects",
          "reservations",
          "outputs",
          "output-pending");
  private static final Set<Path> OPEN_ROOTS = new HashSet<>();

  /**
   * Persistent file-length and file-name bounds, not filesystem block or RSS limits.
   *
   * @param bytes aggregate file bytes and funded output allowances, including bounded headers
   * @param files aggregate file names and funded output names, including installation names
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
   * Conservative charges; reception reserves both installation names and output funding reserves
   * future output bytes and names even before an executor produces them.
   *
   * @param bytes charged file bytes and funded allowances
   * @param files charged file names and funded allowances
   * @param handles live or callback-reserved receive, read and write handles
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
    /** An output funding record's staging bytes were synchronized. */
    FUNDING_RECEIVED,
    /** An immutable output funding name was linked. */
    FUNDING_LINKED,
    /** The output funding namespace was synchronized. */
    FUNDING_SYNCED,
    /** The output funding staging name was removed. */
    FUNDING_STAGING_REMOVED,
    /** A complete output and its final digest were synchronized before installation. */
    OUTPUT_RECEIVED,
    /** The immutable output name was linked. */
    OUTPUT_LINKED,
    /** The output namespace was synchronized. */
    OUTPUT_SYNCED,
    /** The output staging name was removed. */
    OUTPUT_STAGING_REMOVED,
    /** Every orphan target passed identity, allowance and installed-payload verification. */
    OUTPUT_RECLAIM_AUDITED,
    /** One eligible orphan staging name was removed. */
    OUTPUT_RECLAIM_PENDING_REMOVED,
    /** One eligible immutable orphan name was removed. */
    OUTPUT_RECLAIM_INSTALLED_REMOVED,
    /** Both orphan namespaces were synchronized before their slots became reusable. */
    OUTPUT_RECLAIM_SYNCED,
    /** One eligible input name was removed, before directory synchronization. */
    INPUT_RECLAIM_REMOVED,
    /** Eligible input removal is synchronized, before physical accounting is refunded. */
    INPUT_RECLAIM_SYNCED,
    /** One terminal output staging alias was removed. */
    OUTPUT_RETENTION_PENDING_REMOVED,
    /** One terminal installed output was removed. */
    OUTPUT_RETENTION_INSTALLED_REMOVED,
    /** Both terminal output namespaces are synchronized before funding removal. */
    OUTPUT_RETENTION_NAMES_SYNCED,
    /** Terminal output funding was unlinked before directory synchronization. */
    OUTPUT_FUNDING_REMOVED,
    /** Funding removal is synchronized, before physical quota refund. */
    OUTPUT_FUNDING_SYNCED,
    /** Recovery completed its retained-file audit. */
    RECOVERY_AUDITED,
    /** Recovered input and funding absence is synchronized before capacity becomes usable. */
    RECOVERY_RELEASES_SYNCED
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

  private record Funding(Envelope envelope, long size, long chargedBytes, int chargedFiles) {}

  private final Path root;
  private final Limits limits;
  private final UUID identity;
  private final UUID authorityIdentity;
  private final FileChannel lockChannel;
  private final FileLock lock;
  private final Probe probe;
  private final OutputStore outputs;
  private long bytes;
  private int files;
  private int handles;
  private final Map<Path, Integer> inputReaders = new HashMap<>();
  private final Map<Path, Integer> inputReceivers = new HashMap<>();
  private final Map<Path, Long> inputRemovals = new HashMap<>();
  // Preserve prepaid output charges across a same-process interrupted funding removal.
  private final Map<Path, Funding> outputRemovals = new HashMap<>();
  private boolean closed;
  private Object resultService;

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
    this.outputs = new OutputStore(this, root);
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
        Files.createDirectory(root.resolve("reservations"));
        Files.createDirectory(root.resolve("outputs"));
        Files.createDirectory(root.resolve("output-pending"));
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
   * Refuse an admission whose callback can never obtain its minimum physical I/O geometry. Current
   * handle occupancy is transient and is not an admission reservation.
   *
   * @param parameters exact requested processing mode and output budget
   * @throws IOException closed storage
   */
  synchronized void requireExecutionHandles(AdmitParameters parameters) throws IOException {
    ensureOpen();
    int required =
        1 + (parameters.mode() == 0 ? 0 : 1) + (parameters.outputs().count() == 0 ? 0 : 1);
    if (limits.handles() < required)
      throw ProtocolError.limit(
          "callback input, dependency and output handle policy is insufficient");
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
    return begin(context, header, selected, nowNanos, null);
  }

  /**
   * Receive with one previously reserved callback handle. The credit grants capacity only; the
   * authority must still validate the current producer fence before each operation.
   *
   * @param context already authenticated session context
   * @param header structurally valid immutable input header
   * @param selected negotiated connection limits
   * @param nowNanos monotonic reception start
   * @param credit this store's unborrowed credit, or null for ordinary reception
   * @return single-use receiver which returns borrowed capacity only after safe cleanup
   * @throws IOException filesystem failure or closed storage
   */
  synchronized Receiver begin(
      Commitments.Context context,
      InputHeader header,
      Messages.Capabilities selected,
      long nowNanos,
      ReceiverCredit credit)
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
    reserve(reservation, 2, credit);
    Path target = objectPath(envelope);
    inputReceivers.merge(target, 1, Integer::sum);
    Path path = root.resolve("pending").resolve(UUID.randomUUID() + ".part");
    FileChannel output = null;
    try {
      output = create(path);
      writeAll(output, ByteBuffer.allocate(PREFIX).put(MAGIC).putInt(metadata.length).flip());
      writeAll(output, ByteBuffer.wrap(metadata));
      writeAll(output, ByteBuffer.wrap(Commitments.sha256().digest(metadata)));
      return new Receiver(path, output, envelope, verifier, size, credit);
    } catch (IOException | RuntimeException failure) {
      try {
        if (output != null) output.close();
        // CREATE_NEW failure does not grant ownership of an already existing temporary name.
        if (output != null) {
          Files.deleteIfExists(path);
          sync(root.resolve("pending"));
        }
        release(reservation, 2);
        returnReceiver(credit);
        unpinReceiver(target);
      } catch (IOException cleanup) {
        // Uncertain physical close or unsynchronized deletion retains both the
        // namespace charge and its handle. Recovery must establish safe reuse.
        failure.addSuppressed(cleanup);
      }
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

  /**
   * Durably fund an input admission's entire output byte and count budget. The same quota governs
   * ordinary input reception, so it cannot consume these retained allowances. Installation does not
   * admit work: a failed authority transaction leaves an orphan charged until an authoritative
   * liveness proof permits reclamation.
   *
   * <p>Each possible output funds two payload copies and two bounded private headers/names, enough
   * for pending and installed files concurrently. This reserves file-length/name capacity, not
   * filesystem blocks or device free space. Output writers consume these prepaid allowances and
   * enforce the private header bound; immutable files alone do not publish a successful result.
   *
   * @param context already authenticated session context
   * @param header immutable input admission parameters, including the output budget
   * @return durable immutable funding identity, not an admission receipt
   * @throws IOException for file failure, corruption or a closed store
   * @throws ProtocolError for capacity exhaustion or a changed operation's parameters
   */
  synchronized Reservation reserveOutputs(Commitments.Context context, InputHeader header)
      throws IOException {
    Optional<Reservation> existing = findReservation(context, header);
    if (existing.isPresent()) return existing.get();
    Envelope expected = envelope(context, header);
    byte[] encoded = fundingMetadata(expected);
    Funding funding = funding(expected, PREFIX + CHECKSUM + encoded.length);
    long installationBytes = add(funding.chargedBytes(), funding.size());
    int installationFiles = funding.chargedFiles() + 1;
    reserve(installationBytes, installationFiles);
    Path staging = root.resolve("pending").resolve(UUID.randomUUID() + ".part");
    Path target = fundingPath(expected);
    boolean created = false;
    boolean retained = false;
    try {
      try (FileChannel output = create(staging)) {
        created = true;
        writeAll(
            output, ByteBuffer.allocate(PREFIX).put(FUNDING_MAGIC).putInt(encoded.length).flip());
        writeAll(output, ByteBuffer.wrap(encoded));
        writeAll(output, ByteBuffer.wrap(Commitments.sha256().digest(encoded)));
        output.force(true);
        reached(Phase.FUNDING_RECEIVED);
        try {
          Files.createLink(target, staging);
          retained = true;
          reached(Phase.FUNDING_LINKED);
        } catch (FileAlreadyExistsException duplicate) {
          inspectFunding(target, expected);
          forceFile(target);
          retained = true;
        }
        sync(root.resolve("reservations"));
        reached(Phase.FUNDING_SYNCED);
      }
      Files.delete(staging);
      reached(Phase.FUNDING_STAGING_REMOVED);
      sync(root.resolve("pending"));
      release(funding.size(), 1);
      return new Reservation(expected, target);
    } catch (IOException | RuntimeException failure) {
      try {
        if (created) {
          Files.deleteIfExists(staging);
          sync(root.resolve("pending"));
        }
        // A linked record stays funded even when its installation sync or subsequent cleanup fails.
        release(retained ? funding.size() : installationBytes, retained ? 1 : installationFiles);
      } catch (IOException cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    } finally {
      handles--;
    }
  }

  /**
   * Verify and synchronize funding for this exact admission operation. A changed header under the
   * same operation identity is a conflict, not a new reservation. Input admission originators own
   * their input producer; unlike cancellation and retry, they cannot target the other producer.
   *
   * @param context authenticated session identity
   * @param header exact immutable admission header
   * @return verified funding if installed
   * @throws IOException for corruption, failed synchronization or a closed store
   * @throws ProtocolError for changed parameters under the retained operation identity
   */
  synchronized Optional<Reservation> findReservation(
      Commitments.Context context, InputHeader header) throws IOException {
    ensureOpen();
    Envelope expected = envelope(context, header);
    Path path = fundingPath(expected);
    if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return Optional.empty();
    inspectFunding(path, expected);
    // A visible link from an interrupted attempt is not proof its file and directory were forced.
    forceFile(path);
    sync(root.resolve("reservations"));
    return Optional.of(new Reservation(expected, path));
  }

  /**
   * Start one output using the admission's prepaid byte/name allowance and shared handle pool. The
   * authority must check current execution ownership before calling this storage primitive. This
   * call never implicitly recycles existing slots; replacement workers require explicit
   * authority-proven reclamation, and live physical output handles prevent that reclamation.
   *
   * @param context authenticated retained session
   * @param header exact admitted input intent
   * @param lease current worker identity; its observation timestamp is not a file identity
   * @param index output index within the admitted count ceiling
   * @param length exact number of bytes to produce
   * @param contentType bounded printable content type
   * @param objectLimit retained per-object execution ceiling
   * @return bounded streaming writer, not a successful publication
   * @throws IOException storage corruption or installation failure
   */
  synchronized OutputStore.Writer beginOutput(
      Commitments.Context context,
      InputHeader header,
      ExecutionStore.Lease lease,
      int index,
      long length,
      String contentType,
      long objectLimit)
      throws IOException {
    ensureOpen();
    return outputs.begin(context, header, lease, index, length, contentType, objectLimit);
  }

  /**
   * Reserve one physical output-writer handle before invoking an admitted callback. The authority
   * must first prove current execution ownership; this credit is not authorization. Matching
   * sequential output writers borrow the same handle, so unrelated reception or readers cannot
   * spend it. A positive admitted output count is required; bytes and names stay admission-funded.
   *
   * @param context exact retained session
   * @param header immutable admitted input
   * @param lease current committed worker identity, ignoring renewable expiry observations
   * @return credit to close after all borrowed writers have physically closed
   * @throws IOException missing/corrupt funding or closed storage
   * @throws ProtocolError exhausted shared handles, zero output count or an existing credit/writer
   */
  synchronized OutputStore.WriterCredit reserveOutputWriter(
      Commitments.Context context, InputHeader header, ExecutionStore.Lease lease)
      throws IOException {
    ensureOpen();
    return outputs.reserveWriter(context, header, lease);
  }

  /**
   * Reserve one sequential output-reader handle before a branch callback starts. This credit grants
   * no result or dependency access; the authority must validate each selected object.
   *
   * @return store-bound capacity held until every borrowed reader closes
   * @throws IOException closed storage
   */
  synchronized OutputStore.ReaderCredit reserveOutputReader() throws IOException {
    ensureOpen();
    return outputs.reserveReader();
  }

  /**
   * Claim the single result-delivery registry for this exclusive storage owner.
   *
   * @param service local registry identity
   * @throws IOException closed storage
   */
  synchronized void claimResults(Object service) throws IOException {
    ensureOpen();
    Objects.requireNonNull(service);
    if (resultService != null) throw new IllegalStateException("result service already attached");
    resultService = service;
  }

  /**
   * Release a stopped, fully drained delivery registry.
   *
   * @param service exact previously attached identity
   */
  synchronized void releaseResults(Object service) {
    if (resultService != service) throw new IllegalStateException("foreign result service");
    resultService = null;
  }

  /**
   * Open one exact published object with a single full verification on its pinned descriptor.
   * Caller holds this monitor through the current authorization and availability transaction.
   *
   * @param context authenticated session
   * @param header retained admission
   * @param producer historical producing identity, not execution authority
   * @param expected committed object descriptor
   * @return verified payload-only reader whose close releases its physical pin
   * @throws IOException missing or contradictory retained storage
   */
  synchronized InputStream openPublishedOutput(
      Commitments.Context context,
      InputHeader header,
      ExecutionStore.Lease producer,
      Output expected)
      throws IOException {
    ensureOpen();
    return outputs.openPublished(context, header, producer, expected);
  }

  /**
   * Reserve one sequential child-input receiver before invoking an expanding callback. Bytes and
   * names are charged for each actual reception; this grants no producer or admission authority.
   *
   * @return store-bound handle capacity held until every borrowed receiver safely closes
   * @throws IOException closed storage
   */
  synchronized ReceiverCredit reserveInputReceiver() throws IOException {
    pin();
    return new ReceiverCredit();
  }

  /**
   * Fully verify an installed output for the exact worker identity without publishing or rerunning.
   *
   * @param context expected retained session
   * @param header exact input intent
   * @param lease producing worker identity, ignoring its renewable observation timestamp
   * @param index expected output index
   * @return verified immutable output when present
   * @throws IOException missing funding, corruption or synchronization failure
   */
  synchronized Optional<OutputStore.Stored> findOutput(
      Commitments.Context context, InputHeader header, ExecutionStore.Lease lease, int index)
      throws IOException {
    ensureOpen();
    return outputs.find(context, header, lease, index);
  }

  /**
   * Require the exact finished output set, never a silently truncated prefix or a live writer.
   *
   * @param context expected retained session
   * @param header exact admitted input
   * @param lease producing worker identity
   * @param count complete contiguous result count
   * @throws IOException malformed storage or inconsistent retained output set
   */
  synchronized void verifyOutputCount(
      Commitments.Context context, InputHeader header, ExecutionStore.Lease lease, int count)
      throws IOException {
    ensureOpen();
    outputs.verifyCount(context, header, lease, count);
  }

  /**
   * Recycle only unpinned output slots from strictly older execution identities. The authority must
   * supply its newly committed current EXECUTING job lease, holding this monitor through the claim
   * commit and this call. A historical lease or inferred absence is not deletion authority; no
   * successful terminal work may be claimed again. Runtime I/O must continue checking ownership.
   * Funding remains charged, and no current, future, foreign or published output is reclaimed.
   *
   * @param context exact retained session
   * @param header immutable admitted input
   * @param currentCommittedLease current durable replacement execution fence, not a credential
   * @throws IOException corruption or incomplete synchronized deletion
   * @throws ProtocolError live physical handles or output identity not strictly older
   */
  synchronized void reclaimOutputs(
      Commitments.Context context, InputHeader header, ExecutionStore.Lease currentCommittedLease)
      throws IOException {
    ensureOpen();
    outputs.reclaim(context, header, currentCommittedLease);
  }

  /**
   * Read one checked funding record by an internally derived bounded reference during recovery.
   *
   * @param reference opaque funding filename, not an arbitrary path
   * @return exact immutable funding identity
   * @throws IOException missing or corrupt funding
   */
  synchronized Reservation outputFunding(String reference) throws IOException {
    ensureOpen();
    if (reference == null || !reference.matches("[0-9a-f]{64}\\.funding"))
      throw corrupt("invalid output funding reference");
    Path path = root.resolve("reservations").resolve(reference);
    Funding retained = inspectFunding(path, null);
    return new Reservation(retained.envelope(), path);
  }

  /**
   * Derive an output funding name even after an authorized removal.
   *
   * @param context exact session
   * @param header admitted input
   * @return immutable funding filename
   * @throws IOException closed installation
   */
  synchronized String outputReference(Commitments.Context context, InputHeader header)
      throws IOException {
    ensureOpen();
    return fundingPath(envelope(context, header)).getFileName().toString();
  }

  /**
   * Check physical output liveness while the caller holds this monitor through its decision.
   *
   * @param context exact session
   * @param header admitted input
   * @return whether a reader, writer or borrowed callback credit pins the funding
   * @throws IOException closed installation
   */
  synchronized boolean outputInUse(Commitments.Context context, InputHeader header)
      throws IOException {
    ensureOpen();
    return outputs.inUse(outputReference(context, header));
  }

  /**
   * Audit remaining terminal outputs against durable metadata, including interrupted removal. The
   * paired authority must validate release eligibility before permitting missing bytes.
   *
   * @param context exact session
   * @param job retained job and release evidence
   * @param view retained terminal outcome
   * @throws IOException missing promised bytes, contradictory identity or manifest
   */
  synchronized void verifyRetainedOutputs(Commitments.Context context, JobRecord job, WorkView view)
      throws IOException {
    ensureOpen();
    outputs.retentionTargets(context, job, view);
  }

  /**
   * Remove terminal output names and funding under committed, revalidated release evidence. The
   * caller holds this monitor and the paired metadata writer transaction throughout.
   *
   * @param context exact session
   * @param job settled job with committed output eligibility
   * @param view immutable terminal outcome
   * @return false while physical dependencies remain, true after synchronized funding removal
   * @throws IOException corruption or incomplete physical deletion
   */
  synchronized boolean reclaimOutput(Commitments.Context context, JobRecord job, WorkView view)
      throws IOException {
    ensureOpen();
    if (job.outputReleaseAt() == null || !job.outputsLive())
      throw corrupt("output removal lacks live committed eligibility");
    if (outputInUse(context, job.input())) return false;
    outputs.reclaimTerminal(context, job, view);
    Envelope expected = envelope(context, job.input());
    Path target = fundingPath(expected);
    if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
      Funding retained = inspectFunding(target, expected);
      Funding charged = outputRemovals.putIfAbsent(target, retained);
      if (charged != null && !charged.equals(retained))
        throw corrupt("pending output funding removal changed identity or charge");
      Files.delete(target);
      reached(Phase.OUTPUT_FUNDING_REMOVED);
    }
    sync(root.resolve("reservations"));
    reached(Phase.OUTPUT_FUNDING_SYNCED);
    Funding charged = outputRemovals.remove(target);
    if (charged != null) release(charged.chargedBytes(), charged.chargedFiles());
    return true;
  }

  /**
   * Pin an output descriptor in the same pool as input reception and reads.
   *
   * @throws IOException closed storage
   */
  synchronized void pinOutput() throws IOException {
    pin();
  }

  /** Release one closed output descriptor without refunding its durable byte allowance. */
  synchronized void unpinOutput() {
    if (handles == 0) throw new IllegalStateException("output handle accounting underflow");
    handles--;
  }

  /**
   * Observe physical readers of one exact immutable input. A caller making a reclamation decision
   * must hold this store's monitor through the decision and filesystem operation. This is only a
   * physical-liveness gate: an unpinned input can still have durable work or parent dependencies.
   * Receive credits and readers of other inputs do not pin this object.
   *
   * @param context exact owner-qualified session
   * @param header immutable input identity
   * @return whether acquisition or an unclosed reader still holds this object's descriptor
   * @throws IOException closed storage
   */
  synchronized boolean inputPinned(Commitments.Context context, InputHeader header)
      throws IOException {
    ensureOpen();
    return inputReaders.containsKey(objectPath(envelope(context, header)));
  }

  /**
   * Observe descriptors and active installations for one exact input. A receiver remains live
   * through synchronized staging cleanup, so a delayed duplicate FIN cannot recreate an object
   * while a collector holds this monitor and acts on an unused result. Unborrowed receive credits
   * have no object identity and are not included. Durable eligibility is still required separately.
   *
   * @param context exact owner-qualified session
   * @param header immutable input identity
   * @return whether a reader or receiver still owns this object's physical lifecycle
   * @throws IOException closed storage
   */
  synchronized boolean inputInUse(Commitments.Context context, InputHeader header)
      throws IOException {
    ensureOpen();
    Path target = objectPath(envelope(context, header));
    return inputReaders.containsKey(target) || inputReceivers.containsKey(target);
  }

  /**
   * Derive the immutable input name without assuming its physical object remains retained.
   *
   * @param context exact owner-qualified session
   * @param header immutable input identity
   * @return bounded installation-qualified filename, not an arbitrary caller path
   * @throws IOException closed storage
   */
  synchronized String inputReference(Commitments.Context context, InputHeader header)
      throws IOException {
    ensureOpen();
    return objectPath(envelope(context, header)).getFileName().toString();
  }

  /**
   * Remove one input only under the authority's already committed, revalidated release evidence.
   * The caller must hold this monitor and its paired metadata writer transaction throughout. A
   * missing name is acceptable only because that durable evidence permits interrupted deletion.
   * Same-process unlink/sync failures keep their exact charge until synchronization succeeds.
   *
   * @param context checked owner-qualified session
   * @param header exact admitted input
   * @return false while a reader or receiver still owns the object, true after synchronized absence
   * @throws IOException corrupt retained object or incomplete deletion/synchronization
   */
  synchronized boolean reclaimInput(Commitments.Context context, InputHeader header)
      throws IOException {
    ensureOpen();
    Envelope expected = envelope(context, header);
    Path target = objectPath(expected);
    if (inputInUse(context, header)) return false;
    if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
      Inspected retained = inspect(target, expected, true);
      Long charged = inputRemovals.putIfAbsent(target, retained.size());
      if (charged != null && charged.longValue() != retained.size())
        throw corrupt("pending input removal changed physical size");
      Files.delete(target);
      reached(Phase.INPUT_RECLAIM_REMOVED);
    }
    sync(root.resolve("objects"));
    reached(Phase.INPUT_RECLAIM_SYNCED);
    Long charged = inputRemovals.remove(target);
    if (charged != null) release(charged, 1);
    return true;
  }

  /**
   * Observe an output installation boundary for actual interruption tests.
   *
   * @param phase completed filesystem boundary
   * @throws IOException injected failure
   */
  void outputPhase(Phase phase) throws IOException {
    reached(phase);
  }

  /** Durable funding identity, not a bearer credential or a release capability. */
  final class Reservation {
    private final Envelope envelope;
    private final Path path;

    private Reservation(Envelope envelope, Path path) {
      this.envelope = envelope;
      this.path = path;
    }

    /**
     * Get the immutable funded admission parameters.
     *
     * @return exact original input header
     */
    InputHeader header() {
      return envelope.header();
    }

    /**
     * Get the owner-qualified identity whose future outputs are funded.
     *
     * @return immutable context, not authorization
     */
    Commitments.Context context() {
      return envelope.context();
    }

    /**
     * Get a bounded opaque reference suitable for an authority's atomic job record.
     *
     * @return installed funding filename, never a caller-selected path
     */
    String reference() {
      return path.getFileName().toString();
    }
  }

  /** One store-bound receive handle, reusable by only one physical receiver at a time. */
  final class ReceiverCredit implements AutoCloseable {
    private final InputStore source = InputStore.this;
    private boolean borrowed;
    private boolean closed;

    private ReceiverCredit() {}

    private void borrow(InputStore expected) {
      if (source != expected || closed || borrowed)
        throw new ProtocolError(
            ProtocolError.Code.CONFLICT, "input receiver credit is foreign, closed or borrowed");
      borrowed = true;
    }

    private void returned() {
      if (closed || !borrowed) throw new IllegalStateException("input receiver credit underflow");
      borrowed = false;
    }

    /**
     * Release reserved capacity after its receiver has safely closed. A borrowed credit remains
     * charged and refuses release; repeated close after successful release is harmless.
     *
     * @throws IOException a physical receiver or uncertain cleanup still owns the credit
     */
    @Override
    public void close() throws IOException {
      synchronized (InputStore.this) {
        if (closed) return;
        if (borrowed) throw new IOException("input receiver credit is still borrowed");
        returnReceiver(null);
        closed = true;
      }
    }
  }

  /** One bounded immutable reception; finish means actual transport FIN, not admission. */
  final class Receiver implements AutoCloseable {
    private final Path path;
    private final FileChannel channel;
    private final Envelope envelope;
    private final ObjectStream.Payload verifier;
    private final long size;
    private final ReceiverCredit credit;
    private boolean linked;
    private boolean ended;

    private Receiver(
        Path path,
        FileChannel channel,
        Envelope envelope,
        ObjectStream.Payload verifier,
        long size,
        ReceiverCredit credit) {
      this.path = path;
      this.channel = channel;
      this.envelope = envelope;
      this.verifier = verifier;
      this.size = size;
      this.credit = credit;
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
        returnReceiver(credit);
        unpinReceiver(objectPath(envelope));
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
        inputReaders.merge(path, 1, Integer::sum);
        FileChannel input = null;
        try {
          input = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
          Inspected inspected = inspect(input, envelope, true);
          input.position(inspected.offset());
          return new Reader(input, length(), path);
        } catch (IOException | RuntimeException | Error failure) {
          boolean released = input == null;
          if (input != null)
            try {
              input.close();
            } catch (IOException cleanup) {
              failure.addSuppressed(cleanup);
            } finally {
              released = !input.isOpen();
            }
          // An unresolved close must not manufacture capacity or permission to delete this input.
          if (released) unpinInput(path);
          throw failure;
        }
      }
    }
  }

  private final class Reader extends InputStream {
    private final InputStream input;
    private final FileChannel channel;
    private final Path path;
    private long remaining;
    private boolean ended;

    Reader(FileChannel channel, long length, Path path) {
      this.channel = channel;
      input = Channels.newInputStream(channel);
      remaining = length;
      this.path = path;
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
      IOException failure = null;
      try {
        input.close();
      } catch (IOException close) {
        failure = close;
      }
      if (channel.isOpen()) {
        if (failure != null) throw failure;
        throw new IOException("input read descriptor remained open");
      }
      synchronized (InputStore.this) {
        unpinInput(path);
      }
      ended = true;
      if (failure != null) throw failure;
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
    if (resultService != null) throw new IOException("V2 result service still attached");
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
      boolean directory =
          name.equals("pending")
              || name.equals("objects")
              || name.equals("reservations")
              || name.equals("outputs")
              || name.equals("output-pending");
      if (directory
          ? !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)
          : !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
        throw corrupt("input-store entry has wrong file type");
    }
    // Complete validation precedes cleanup. A malformed live object must not be hidden by deletion.
    for (String namespace : new String[] {"reservations", "objects", "pending"}) {
      try (var entries = Files.newDirectoryStream(root.resolve(namespace))) {
        for (Path path : entries) {
          String name = path.getFileName().toString();
          boolean object = namespace.equals("objects");
          boolean reservation = namespace.equals("reservations");
          if (!name.matches(
                  object
                      ? "[0-9a-f]{64}\\.input"
                      : reservation
                          ? "[0-9a-f]{64}\\.funding"
                          : "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.part")
              || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
            throw corrupt("unknown input-store file");
          if (reservation) {
            Funding funding = inspectFunding(path, null);
            chargeRetained(funding.chargedBytes(), funding.chargedFiles());
            continue;
          }
          long size = Files.size(path);
          if (size > add(limits.objectBytes(), METADATA_LIMIT + PREFIX + CHECKSUM))
            throw corrupt("input-store file exceeds local ceiling");
          if (object) {
            Inspected inspected = inspect(path, null, true);
            if (!objectPath(inspected.envelope()).equals(path))
              throw corrupt("input-store filename contradicts identity");
          }
          chargeRetained(size, 1);
        }
      }
    }
    outputs.audit();
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
    outputs.cleanupPending();
    // Reconstructed absence can follow an interrupted unlink in the previous process. Force
    // both namespaces even when empty before returning any reusable physical capacity.
    sync(root.resolve("objects"));
    sync(root.resolve("reservations"));
    reached(Phase.RECOVERY_RELEASES_SYNCED);
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

  private Path fundingPath(Envelope envelope) {
    Cbor.Writer key = new Cbor.Writer(METADATA_LIMIT);
    key.array(8);
    key.text("pipestream-java-v2-output-funding-1", 128);
    key.bytes(uuid(envelope.store()));
    if (authorityIdentity == null) key.nil();
    else key.bytes(uuid(authorityIdentity));
    key.text(envelope.context().authority(), 128);
    key.text(envelope.context().owner(), 128);
    key.number(envelope.context().generation());
    key.number(envelope.header().parameters().work().producer());
    key.bytes(envelope.header().operation().bytes());
    byte[] digest = Commitments.sha256().digest(key.finish());
    return root.resolve("reservations").resolve(HexFormat.of().formatHex(digest) + ".funding");
  }

  private byte[] fundingMetadata(Envelope envelope) {
    Cbor.Writer out = new Cbor.Writer(METADATA_LIMIT);
    out.array(6);
    out.bytes(uuid(envelope.store()));
    if (authorityIdentity == null) out.nil();
    else out.bytes(uuid(authorityIdentity));
    out.text(envelope.context().authority(), 128);
    out.text(envelope.context().owner(), 128);
    out.number(envelope.context().generation());
    RecordCodec.write(out, envelope.header());
    return out.finish();
  }

  private static Funding funding(Envelope envelope, long size) {
    OutputBudget budget = envelope.header().parameters().outputs();
    long outputBytes = add(budget.totalBytes(), multiply(budget.count(), OUTPUT_OVERHEAD));
    return new Funding(envelope, size, add(size, multiply(outputBytes, 2)), 1 + 2 * budget.count());
  }

  private Funding inspectFunding(Path path, Envelope expected) throws IOException {
    Funding retained;
    try {
      byte[] complete = bounded(path, OUTPUT_OVERHEAD);
      if (complete.length < PREFIX + CHECKSUM + 1) throw corrupt("truncated output funding");
      ByteBuffer prefix = ByteBuffer.wrap(complete);
      byte[] magic = new byte[8];
      prefix.get(magic);
      int count = prefix.getInt();
      if (!Arrays.equals(magic, FUNDING_MAGIC)
          || count < 1
          || count > METADATA_LIMIT
          || complete.length != PREFIX + CHECKSUM + count)
        throw corrupt("invalid output funding header");
      byte[] encoded = Arrays.copyOfRange(complete, PREFIX, PREFIX + count);
      byte[] checksum = Arrays.copyOfRange(complete, PREFIX + count, complete.length);
      if (!MessageDigest.isEqual(checksum, Commitments.sha256().digest(encoded)))
        throw corrupt("output funding checksum differs");
      Cbor.Reader in = new Cbor.Reader(encoded, METADATA_LIMIT);
      in.exact(6);
      ByteBuffer storeId = ByteBuffer.wrap(in.bytes(16));
      UUID store = new UUID(storeId.getLong(), storeId.getLong());
      UUID authority = null;
      if (!in.nullable()) {
        ByteBuffer authorityId = ByteBuffer.wrap(in.bytes(16));
        authority = new UUID(authorityId.getLong(), authorityId.getLong());
      }
      Commitments.Context context =
          new Commitments.Context(in.text(128), in.text(128), in.number());
      InputHeader header = RecordCodec.inputHeader(in);
      in.end();
      Envelope envelope = new Envelope(store, context, header);
      if (!store.equals(identity)
          || !Objects.equals(authority, authorityIdentity)
          || context.generation() != header.generation()
          || !Arrays.equals(encoded, fundingMetadata(envelope))
          || !fundingPath(envelope).equals(path)) throw corrupt("output funding identity differs");
      Wire.encodeRecord(header, Wire.HEADER_LIMIT);
      retained = funding(envelope, complete.length);
    } catch (ProtocolError failure) {
      throw corrupt("invalid retained output funding", failure);
    }
    if (expected != null && !expected.equals(retained.envelope()))
      throw new ProtocolError(
          ProtocolError.Code.CONFLICT, "output funding operation parameters differ");
    return retained;
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

  private void unpinInput(Path path) {
    Integer readers = inputReaders.get(path);
    if (readers == null || readers == 0 || handles == 0)
      throw new IllegalStateException("input reader accounting underflow");
    if (readers == 1) inputReaders.remove(path);
    else inputReaders.put(path, readers - 1);
    handles--;
  }

  private void unpinReceiver(Path path) {
    Integer receivers = inputReceivers.get(path);
    if (receivers == null || receivers == 0)
      throw new IllegalStateException("input receiver identity accounting underflow");
    if (receivers == 1) inputReceivers.remove(path);
    else inputReceivers.put(path, receivers - 1);
  }

  private void reserve(long addedBytes, int addedFiles) throws IOException {
    reserve(addedBytes, addedFiles, null);
  }

  private void reserve(long addedBytes, int addedFiles, ReceiverCredit credit) throws IOException {
    ensureOpen();
    if (addedBytes > limits.bytes() - bytes || addedFiles > limits.files() - files)
      throw ProtocolError.limit("input storage capacity exhausted");
    if (credit == null) pin();
    else credit.borrow(this);
    bytes += addedBytes;
    files += addedFiles;
  }

  private void returnReceiver(ReceiverCredit credit) {
    if (credit == null) {
      if (handles == 0) throw new IllegalStateException("input handle accounting underflow");
      handles--;
    } else credit.returned();
  }

  private void chargeRetained(long addedBytes, int addedFiles) {
    if (addedBytes > limits.bytes() - bytes || addedFiles > limits.files() - files)
      throw ProtocolError.limit("retained input storage exceeds policy");
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
