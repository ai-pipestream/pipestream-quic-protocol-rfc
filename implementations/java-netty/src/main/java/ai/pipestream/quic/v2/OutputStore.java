package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Records.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Streaming immutable output files within {@link InputStore}'s prepaid allowance and handle pool.
 * Installation is not publication, and local worker identity is not authorization. An installed
 * slot is never recycled here, including for a replacement lease; reclamation requires a separate
 * authoritative liveness proof. Blocking calls belong outside transport event loops.
 */
final class OutputStore {
  private static final byte[] MAGIC = {'P', 'S', 'J', 'V', '2', 'O', '0', '1'};
  private static final int PREFIX = 12;
  private static final int CHECKSUM = 32;
  private static final int METADATA_LIMIT = 8192;
  private static final int OVERHEAD = PREFIX + CHECKSUM + METADATA_LIMIT;
  private static final int BLOCK = 8192;

  private record Identity(
      UUID store,
      UUID authority,
      Commitments.Context context,
      InputHeader header,
      long attempt,
      long lease,
      int index) {
    Identity {
      Objects.requireNonNull(store);
      Objects.requireNonNull(authority);
      Objects.requireNonNull(context);
      Objects.requireNonNull(header);
      Checks.id(attempt);
      Checks.id(lease);
      Checks.range(index, 0, 255);
      ProtocolError.require(
          context.generation() == header.generation(), "output generation differs");
      Wire.encodeRecord(header, Wire.HEADER_LIMIT);
    }
  }

  private record Metadata(
      Identity identity, long length, String contentType, long objectLimit, Digest sha256) {
    Metadata {
      Objects.requireNonNull(identity);
      Checks.number(length);
      Checks.number(objectLimit);
      Checks.label(contentType);
      Objects.requireNonNull(sha256);
      ProtocolError.require(
          length <= objectLimit
              && objectLimit <= identity.header().parameters().outputs().totalBytes(),
          "output exceeds its retained object ceiling");
    }
  }

  private record Inspected(Metadata metadata, long offset, long size) {}

  private record Slot(String funding, int index) {}

  private record Used(long bytes, int count) {}

  private record Ticket(String funding, Metadata metadata) {}

  private final InputStore owner;
  private final Path root;
  // Only live writers, bounded by the owner's <=128 shared handles. Installed state stays on disk.
  private final Map<String, Ticket> active = new HashMap<>();
  // Guarded by owner. Visible staging removal alone does not make the slot durably reusable.
  private boolean pendingSyncRequired;

  /**
   * Attach to the one owning input/funding installation; creates no independent resource pool.
   *
   * @param owner exclusive owner and shared quota
   * @param root already selected private store root
   */
  OutputStore(InputStore owner, Path root) {
    this.owner = owner;
    this.root = root;
  }

  /**
   * Start one prepaid output; caller holds the owner monitor and has checked execution permission.
   *
   * @param context retained session
   * @param header exact input intent
   * @param lease producing worker fence
   * @param index output slot
   * @param length exact payload length
   * @param contentType bounded type label
   * @param objectLimit retained per-object maximum
   * @return single-use writer
   * @throws IOException file creation, retained corruption or cleanup failure
   */
  Writer begin(
      Commitments.Context context,
      InputHeader header,
      ExecutionStore.Lease lease,
      int index,
      long length,
      String contentType,
      long objectLimit)
      throws IOException {
    InputStore.Reservation funding = funding(context, header, lease);
    OutputBudget budget = header.parameters().outputs();
    Checks.range(index, 0, 255);
    Checks.number(length);
    Checks.number(objectLimit);
    Checks.label(contentType);
    if (index >= budget.count() || length > objectLimit || objectLimit > budget.totalBytes())
      throw ProtocolError.limit("output index or length exceeds funded budget");
    if (pendingSyncRequired) syncPending();
    Identity identity = identity(context, header, lease, index);
    Metadata metadata =
        new Metadata(identity, length, contentType, objectLimit, new Digest(new byte[32]));
    String reference = name(funding.reference(), index);
    Path staging = pending(reference);
    if (active.containsKey(reference)
        || Files.exists(installed(reference), LinkOption.NOFOLLOW_LINKS))
      throw conflict("output slot already has a writer or immutable object");
    Used used = installed(funding, identity, -1);
    long promised = used.bytes();
    for (Ticket ticket : active.values()) {
      if (!ticket.funding().equals(funding.reference())) continue;
      requireWorker(ticket.metadata().identity(), identity);
      promised = add(promised, ticket.metadata().length());
    }
    verifyPending(funding, false);
    if (promised > budget.totalBytes() || length > budget.totalBytes() - promised)
      throw ProtocolError.limit("output aggregate exceeds funded byte budget");
    byte[] encoded = encode(metadata);
    owner.pinOutput();
    FileChannel channel = null;
    try {
      channel = create(staging);
      writeHeader(channel, encoded);
      active.put(reference, new Ticket(funding.reference(), metadata));
      return new Writer(reference, channel, metadata, PREFIX + CHECKSUM + encoded.length);
    } catch (IOException | RuntimeException failure) {
      boolean closed = true;
      if (channel != null) {
        try {
          channel.close();
        } catch (IOException cleanup) {
          failure.addSuppressed(cleanup);
        }
        closed = !channel.isOpen();
        if (closed) {
          pendingSyncRequired = true;
          try {
            Files.deleteIfExists(staging);
            syncPending();
          } catch (IOException cleanup) {
            failure.addSuppressed(cleanup);
          }
        }
      }
      if (closed) {
        active.remove(reference);
        owner.unpinOutput();
      }
      throw failure;
    }
  }

  /**
   * Verify and synchronize an exact immutable output; ignores only the lease observation expiry.
   *
   * @param context expected session
   * @param header original input
   * @param lease producing lease identity
   * @param index output index
   * @return verified output if present
   * @throws IOException corruption or synchronization failure
   */
  Optional<Stored> find(
      Commitments.Context context, InputHeader header, ExecutionStore.Lease lease, int index)
      throws IOException {
    InputStore.Reservation funding = funding(context, header, lease);
    Checks.range(index, 0, 255);
    if (index >= header.parameters().outputs().count()) return Optional.empty();
    Identity expected = identity(context, header, lease, index);
    String reference = name(funding.reference(), index);
    Path path = installed(reference);
    if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return Optional.empty();
    Metadata metadata = inspect(path, null, false).metadata();
    validateFunding(metadata, funding);
    if (!metadata.identity().equals(expected))
      throw conflict("output belongs to another worker identity");
    // Do not hash an untrusted declared body until its complete intent matches funded bounds.
    inspect(path, metadata, true);
    force(path);
    sync(root.resolve("outputs"));
    return Optional.of(new Stored(reference, metadata));
  }

  /**
   * Require exactly a finished contiguous set, with no surplus object or active/staged writer.
   *
   * @param context expected session
   * @param header original input
   * @param lease producing worker
   * @param count complete output count
   * @throws IOException corrupt files or orphan staging
   */
  void verifyCount(
      Commitments.Context context, InputHeader header, ExecutionStore.Lease lease, int count)
      throws IOException {
    InputStore.Reservation funding = funding(context, header, lease);
    Checks.range(count, 0, 256);
    if (count > header.parameters().outputs().count())
      throw ProtocolError.limit("published count exceeds admitted output budget");
    if (pendingSyncRequired) syncPending();
    for (Ticket ticket : active.values())
      if (ticket.funding().equals(funding.reference()))
        throw conflict("output set still has a live physical writer");
    verifyPending(funding, true);
    Identity worker = identity(context, header, lease, 0);
    Used used = installed(funding, worker, count);
    if (used.count() != count)
      throw new ProtocolError(ProtocolError.Code.NOT_READY, "output set is incomplete");
  }

  /** One streaming callback output. Close releases a handle, not its admission's funding. */
  final class Writer implements AutoCloseable {
    private final String reference;
    private final FileChannel channel;
    private final Metadata initial;
    private final long offset;
    private final MessageDigest digest = Commitments.sha256();
    private long written;
    private boolean ended;

    private Writer(String reference, FileChannel channel, Metadata initial, long offset) {
      this.reference = reference;
      this.channel = channel;
      this.initial = initial;
      this.offset = offset;
    }

    /**
     * Returns the output context.
     *
     * <p>Write and hash a bounded caller buffer without retaining it or allocating a payload copy.
     *
     * @param bytes bytes consumed on success
     * @throws IOException write or cleanup failure
     */
    synchronized void write(ByteBuffer bytes) throws IOException {
      if (ended) throw new IOException("output writer is closed");
      Objects.requireNonNull(bytes);
      try {
        if (bytes.remaining() > initial.length() - written)
          throw ProtocolError.limit("output exceeds declared length");
        int count = bytes.remaining();
        digest.update(bytes.duplicate());
        writeAll(channel, bytes);
        written += count;
      } catch (IOException | RuntimeException failure) {
        abandon(failure);
        throw failure;
      }
    }

    /**
     * Returns the admitted input identity.
     *
     * <p>Verify exact length and persisted SHA-256, then install without replacing any existing
     * slot. The returned object is still unpublished until the authority's fenced metadata
     * commitment.
     *
     * @return immutable stored descriptor
     * @throws IOException verification, force, installation or cleanup failure
     */
    synchronized Stored finish() throws IOException {
      if (ended) throw new IOException("output writer is closed");
      try {
        if (written != initial.length())
          throw new ProtocolError(
              ProtocolError.Code.INTEGRITY_ERROR, "output ended before its declared length");
        Metadata complete =
            new Metadata(
                initial.identity(),
                initial.length(),
                initial.contentType(),
                initial.objectLimit(),
                new Digest(digest.digest()));
        byte[] encoded = encode(complete);
        if (PREFIX + CHECKSUM + encoded.length != offset)
          throw corrupt("output header geometry changed during finalization");
        channel.position(0);
        writeHeader(channel, encoded);
        channel.position(0);
        inspect(channel, complete, true);
        channel.force(true);
        owner.outputPhase(InputStore.Phase.OUTPUT_RECEIVED);
        synchronized (owner) {
          InputStore.Reservation funding = owner.outputFunding(parse(reference).funding());
          validateFunding(complete, funding);
          Path target = installed(reference);
          try {
            Files.createLink(target, pending(reference));
          } catch (FileAlreadyExistsException duplicate) {
            throw conflict("immutable output slot was already installed");
          }
          owner.outputPhase(InputStore.Phase.OUTPUT_LINKED);
          sync(root.resolve("outputs"));
          owner.outputPhase(InputStore.Phase.OUTPUT_SYNCED);
          Stored result = new Stored(reference, complete);
          close();
          return result;
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
     * Returns the producing attempt.
     *
     * <p>Close the physical writer and remove only its staging name. An installed object and the
     * admission's full allowance stay charged; interrupted staging cleanup requires recovery.
     *
     * @throws IOException descriptor close or synchronized cleanup failure
     */
    @Override
    public synchronized void close() throws IOException {
      if (ended) return;
      IOException failure = null;
      try {
        channel.close();
      } catch (IOException close) {
        failure = close;
      }
      if (channel.isOpen()) {
        if (failure != null) throw failure;
        throw new IOException("output descriptor remained open");
      }
      synchronized (owner) {
        pendingSyncRequired = true;
        try {
          Files.deleteIfExists(pending(reference));
          owner.outputPhase(InputStore.Phase.OUTPUT_STAGING_REMOVED);
          syncPending();
        } catch (IOException cleanup) {
          if (failure == null) failure = cleanup;
          else failure.addSuppressed(cleanup);
        } finally {
          active.remove(reference);
          owner.unpinOutput();
          ended = true;
        }
      }
      if (failure != null) throw failure;
    }
  }

  /** Immutable installed output. Possession alone grants neither publication nor result access. */
  final class Stored {
    private final String reference;
    private final Metadata metadata;

    private Stored(String reference, Metadata metadata) {
      this.reference = reference;
      this.metadata = metadata;
    }

    /**
     * Returns the owner-qualified output session context.
     *
     * @return producing owner-qualified context, not a credential
     */
    Commitments.Context context() {
      return metadata.identity().context();
    }

    /**
     * Returns the original input header.
     *
     * @return exact original admitted input header
     */
    InputHeader header() {
      return metadata.identity().header();
    }

    /**
     * Returns the producing wire attempt.
     *
     * @return producing wire attempt
     */
    long attempt() {
      return metadata.identity().attempt();
    }

    /**
     * Returns the local lease number.
     *
     * @return producing local acquisition counter, not its renewable expiry observation
     */
    long leaseNumber() {
      return metadata.identity().lease();
    }

    /**
     * Returns the output index.
     *
     * @return contiguous-publication candidate index
     */
    int index() {
      return metadata.identity().index();
    }

    /**
     * Returns the payload byte length.
     *
     * @return exact payload bytes without the private header
     */
    long length() {
      return metadata.length();
    }

    /**
     * Returns the payload digest.
     *
     * @return independently verified payload commitment
     */
    Digest sha256() {
      return metadata.sha256();
    }

    /**
     * Returns the declared content type.
     *
     * @return original bounded printable content type
     */
    String contentType() {
      return metadata.contentType();
    }

    /**
     * Returns the installed reference name.
     *
     * @return opaque installed filename, never a caller-selected path
     */
    String reference() {
      return reference;
    }

    /**
     * Pin and verify one file descriptor before returning a bounded payload-only stream. Current
     * result-read/execution authorization and availability remain the authority caller's duty.
     *
     * @return verified bounded reader whose close releases the shared handle
     * @throws IOException changed identity, corruption, missing funding or failed read
     */
    InputStream openStream() throws IOException {
      synchronized (owner) {
        owner.verifyAuthority(metadata.identity().authority());
        validateFunding(metadata, owner.outputFunding(parse(reference).funding()));
        owner.pinOutput();
        FileChannel input = null;
        try {
          input =
              FileChannel.open(
                  installed(reference), StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
          Inspected value = inspect(input, metadata, true);
          input.position(value.offset());
          return new Reader(input, metadata.length());
        } catch (IOException | RuntimeException failure) {
          if (input != null) {
            try {
              input.close();
            } catch (IOException cleanup) {
              failure.addSuppressed(cleanup);
            }
          }
          // Do not turn a failed physical close into a reusable shared descriptor allowance.
          if (input == null || !input.isOpen()) owner.unpinOutput();
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
      return read(one, 0, 1) < 0 ? -1 : Byte.toUnsignedInt(one[0]);
    }

    @Override
    public synchronized int read(byte[] bytes, int offset, int length) throws IOException {
      Objects.checkFromIndexSize(offset, length, bytes.length);
      if (ended) throw new IOException("output reader is closed");
      if (length == 0) return 0;
      if (remaining == 0) return -1;
      int count = input.read(bytes, offset, (int) Math.min(length, remaining));
      if (count < 0) throw new EOFException("retained output shortened during read");
      remaining -= count;
      return count;
    }

    @Override
    public synchronized void close() throws IOException {
      if (ended) return;
      input.close();
      synchronized (owner) {
        owner.unpinOutput();
      }
      ended = true;
    }
  }

  /**
   * Audit all installed bodies and reconstructed funding before any abandoned staging is removed.
   *
   * @throws IOException malformed names, foreign identity, missing funding or corrupt bodies
   */
  void audit() throws IOException {
    for (String namespace : new String[] {"outputs", "output-pending"}) {
      try (var entries = Files.newDirectoryStream(root.resolve(namespace))) {
        for (Path path : entries) {
          Slot slot = parse(path.getFileName().toString());
          InputStore.Reservation funding = owner.outputFunding(slot.funding());
          if (slot.index() >= funding.header().parameters().outputs().count()
              || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
            throw corrupt("output name lacks funded regular-file slot");
          if (namespace.equals("outputs")) {
            Metadata value = inspect(path, null, false).metadata();
            validateFunding(value, funding);
            if (value.identity().index() != slot.index())
              throw corrupt("output name differs from metadata index");
          } else {
            if (Files.size(path)
                > add(funding.header().parameters().outputs().totalBytes(), OVERHEAD))
              throw corrupt("output staging exceeds funded geometry");
            Path target = installed(path.getFileName().toString());
            if (Files.exists(target, LinkOption.NOFOLLOW_LINKS) && !Files.isSameFile(target, path))
              throw corrupt("output staging and installed names are not the same immutable file");
          }
        }
      }
    }
    try (var entries = Files.newDirectoryStream(root.resolve("reservations"))) {
      for (Path path : entries) auditFunding(owner.outputFunding(path.getFileName().toString()));
    }
    // Establish every funded aggregate before reading any retained payload. A malicious collection
    // of individually bounded headers must not multiply body-hashing work beyond its allowance.
    try (var entries = Files.newDirectoryStream(root.resolve("outputs"))) {
      for (Path path : entries) {
        Slot slot = parse(path.getFileName().toString());
        InputStore.Reservation funding = owner.outputFunding(slot.funding());
        Metadata value = inspect(path, null, false).metadata();
        validateFunding(value, funding);
        if (value.identity().index() != slot.index())
          throw corrupt("output name differs from metadata index");
        inspect(path, value, true);
      }
    }
  }

  /**
   * Remove only exclusively orphaned staging after the complete store audit. No installed output,
   * descriptor or durable allowance is reclaimed, and no capacity counter is refunded.
   *
   * @throws IOException synchronized cleanup failure
   */
  void cleanupPending() throws IOException {
    if (!active.isEmpty()) throw corrupt("cannot recover staging with active physical writers");
    pendingSyncRequired = true;
    try (var entries = Files.newDirectoryStream(root.resolve("output-pending"))) {
      for (Path path : entries) {
        pendingSyncRequired = true;
        Files.delete(path);
        syncPending();
      }
    }
    // Also covers a previous process's visible but not durably synchronized last deletion.
    syncPending();
  }

  private void syncPending() throws IOException {
    sync(root.resolve("output-pending"));
    pendingSyncRequired = false;
  }

  private InputStore.Reservation funding(
      Commitments.Context context, InputHeader header, ExecutionStore.Lease lease)
      throws IOException {
    Objects.requireNonNull(context);
    Objects.requireNonNull(header);
    Objects.requireNonNull(lease);
    owner.verifyAuthority(lease.installation());
    if (!context.owner().equals(lease.owner())
        || context.generation() != lease.generation()
        || header.generation() != context.generation()
        || !header.parameters().work().equals(lease.work()))
      throw new ProtocolError(
          ProtocolError.Code.CONFLICT, "output worker and admitted context differ");
    return owner
        .findReservation(context, header)
        .orElseThrow(() -> new IOException("output lacks installed admission funding"));
  }

  private Identity identity(
      Commitments.Context context, InputHeader header, ExecutionStore.Lease lease, int index) {
    return new Identity(
        owner.identity(),
        lease.installation(),
        context,
        header,
        lease.attempt(),
        lease.number(),
        index);
  }

  private Used installed(InputStore.Reservation funding, Identity expected, int count)
      throws IOException {
    long total = 0;
    int found = 0;
    String prefix = funding.reference().substring(0, 64) + "-";
    try (var entries = Files.newDirectoryStream(root.resolve("outputs"))) {
      for (Path path : entries) {
        if (!path.getFileName().toString().startsWith(prefix)) continue;
        Slot slot = parse(path.getFileName().toString());
        if (slot.index() >= funding.header().parameters().outputs().count())
          throw corrupt("output index exceeds its funding record");
        if (count >= 0 && slot.index() >= count)
          throw conflict("output set contains an unpublished suffix");
        Metadata value = inspect(path, null, false).metadata();
        validateFunding(value, funding);
        if (slot.index() != value.identity().index()) throw corrupt("output slot identity differs");
        requireWorker(value.identity(), expected);
        total = add(total, value.length());
        found++;
      }
    }
    if (total > funding.header().parameters().outputs().totalBytes())
      throw corrupt("installed outputs exceed prepaid byte budget");
    return new Used(total, found);
  }

  private void verifyPending(InputStore.Reservation funding, boolean refuseAll) throws IOException {
    String prefix = funding.reference().substring(0, 64) + "-";
    try (var entries = Files.newDirectoryStream(root.resolve("output-pending"))) {
      for (Path path : entries) {
        String name = path.getFileName().toString();
        if (!name.startsWith(prefix)) continue;
        Slot slot = parse(name);
        if (slot.index() >= funding.header().parameters().outputs().count()
            || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
          throw corrupt("invalid output staging slot");
        if (refuseAll || !active.containsKey(name))
          throw conflict("output staging requires writer completion or exclusive recovery");
      }
    }
  }

  private void auditFunding(InputStore.Reservation funding) throws IOException {
    OutputBudget budget = funding.header().parameters().outputs();
    long payloads = 0, physical = 0;
    Identity producer = null;
    for (int index = 0; index < budget.count(); index++) {
      String reference = name(funding.reference(), index);
      Path target = installed(reference), staging = pending(reference);
      if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
        Inspected inspected = inspect(target, null, false);
        Metadata value = inspected.metadata();
        validateFunding(value, funding);
        if (value.identity().index() != index) throw corrupt("retained output slot index differs");
        if (producer != null && !sameWorker(producer, value.identity()))
          throw corrupt("funded outputs mix execution ownership generations");
        producer = value.identity();
        payloads = add(payloads, value.length());
        physical = add(physical, inspected.size());
      }
      if (Files.exists(staging, LinkOption.NOFOLLOW_LINKS))
        physical = add(physical, Files.size(staging));
    }
    long capacity = multiply(add(budget.totalBytes(), multiply(budget.count(), OVERHEAD)), 2);
    if (payloads > budget.totalBytes() || physical > capacity)
      throw corrupt("retained outputs exceed prepaid byte or staging allowance");
  }

  private void validateFunding(Metadata metadata, InputStore.Reservation funding)
      throws IOException {
    Identity identity = metadata.identity();
    if (!identity.store().equals(owner.identity())
        || !owner.authorityIdentity().filter(identity.authority()::equals).isPresent()
        || !identity.context().equals(funding.context())
        || !identity.header().equals(funding.header())
        || identity.index() >= funding.header().parameters().outputs().count())
      throw corrupt("output identity contradicts its authority or funding");
  }

  private static boolean sameWorker(Identity left, Identity right) {
    return left.store().equals(right.store())
        && left.authority().equals(right.authority())
        && left.context().equals(right.context())
        && left.header().equals(right.header())
        && left.attempt() == right.attempt()
        && left.lease() == right.lease();
  }

  private static void requireWorker(Identity retained, Identity expected) {
    if (!sameWorker(retained, expected))
      throw conflict("funded output slot belongs to another worker");
  }

  private static String name(String funding, int index) {
    return funding.substring(0, 64) + "-" + index + ".output";
  }

  private static Slot parse(String reference) throws IOException {
    if (!reference.matches("[0-9a-f]{64}-(0|[1-9][0-9]{0,2})\\.output"))
      throw corrupt("invalid output filename");
    int index = Integer.parseInt(reference.substring(65, reference.length() - 7));
    if (index > 255) throw corrupt("output filename index exceeds schema");
    return new Slot(reference.substring(0, 64) + ".funding", index);
  }

  private Path installed(String reference) {
    return root.resolve("outputs").resolve(reference);
  }

  private Path pending(String reference) {
    return root.resolve("output-pending").resolve(reference);
  }

  private static byte[] encode(Metadata metadata) {
    Identity identity = metadata.identity();
    Cbor.Writer out = new Cbor.Writer(METADATA_LIMIT);
    out.array(13);
    out.bytes(uuid(identity.store()));
    out.bytes(uuid(identity.authority()));
    out.text(identity.context().authority(), 128);
    out.text(identity.context().owner(), 128);
    out.number(identity.context().generation());
    RecordCodec.write(out, identity.header());
    out.number(identity.attempt());
    out.number(identity.lease());
    out.number(identity.index());
    out.number(metadata.length());
    out.text(metadata.contentType(), 128);
    out.number(metadata.objectLimit());
    out.bytes(metadata.sha256().bytes());
    return out.finish();
  }

  private Inspected inspect(Path path, Metadata expected, boolean hash) throws IOException {
    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
      throw corrupt("output is not a regular file");
    try (FileChannel input =
        FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
      return inspect(input, expected, hash);
    }
  }

  private Inspected inspect(FileChannel input, Metadata expected, boolean hash) throws IOException {
    try {
      ByteBuffer prefix = ByteBuffer.allocate(PREFIX);
      readAll(input, prefix);
      prefix.flip();
      byte[] magic = new byte[8];
      prefix.get(magic);
      int count = prefix.getInt();
      if (!Arrays.equals(magic, MAGIC) || count < 1 || count > METADATA_LIMIT)
        throw corrupt("invalid output header");
      byte[] encoded = new byte[count], checksum = new byte[CHECKSUM];
      readAll(input, ByteBuffer.wrap(encoded));
      readAll(input, ByteBuffer.wrap(checksum));
      if (!MessageDigest.isEqual(checksum, Commitments.sha256().digest(encoded)))
        throw corrupt("output metadata checksum differs");
      Cbor.Reader in = new Cbor.Reader(encoded, METADATA_LIMIT);
      in.exact(13);
      UUID store = uuid(in.bytes(16)), authority = uuid(in.bytes(16));
      Commitments.Context context =
          new Commitments.Context(in.text(128), in.text(128), in.number());
      InputHeader header = RecordCodec.inputHeader(in);
      Identity identity =
          new Identity(
              store,
              authority,
              context,
              header,
              in.number(),
              in.number(),
              (int) Checks.range(in.number(), 0, 255));
      Metadata metadata =
          new Metadata(identity, in.number(), in.text(128), in.number(), new Digest(in.bytes(32)));
      in.end();
      if (!identity.store().equals(owner.identity())
          || !owner.authorityIdentity().filter(identity.authority()::equals).isPresent()
          || expected != null && !metadata.equals(expected)
          || !Arrays.equals(encoded, encode(metadata)))
        throw corrupt("output metadata identity differs");
      long offset = PREFIX + CHECKSUM + (long) count;
      long size = add(offset, metadata.length());
      if (input.size() != size) throw corrupt("output payload geometry differs");
      if (hash) {
        MessageDigest digest = Commitments.sha256();
        ByteBuffer block = ByteBuffer.allocate(BLOCK);
        long remaining = metadata.length();
        while (remaining > 0) {
          block.clear().limit((int) Math.min(BLOCK, remaining));
          readAll(input, block);
          remaining -= block.position();
          block.flip();
          digest.update(block);
        }
        if (!MessageDigest.isEqual(metadata.sha256().bytes(), digest.digest()))
          throw corrupt("retained output digest differs");
      }
      return new Inspected(metadata, offset, size);
    } catch (ProtocolError invalid) {
      throw corrupt("invalid retained output", invalid);
    }
  }

  private static byte[] uuid(UUID value) {
    return ByteBuffer.allocate(16)
        .putLong(value.getMostSignificantBits())
        .putLong(value.getLeastSignificantBits())
        .array();
  }

  private static UUID uuid(byte[] value) {
    ByteBuffer bytes = ByteBuffer.wrap(value);
    return new UUID(bytes.getLong(), bytes.getLong());
  }

  private static void writeHeader(FileChannel channel, byte[] encoded) throws IOException {
    writeAll(channel, ByteBuffer.allocate(PREFIX).put(MAGIC).putInt(encoded.length).flip());
    writeAll(channel, ByteBuffer.wrap(encoded));
    writeAll(channel, ByteBuffer.wrap(Commitments.sha256().digest(encoded)));
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

  private static void readAll(FileChannel channel, ByteBuffer bytes) throws IOException {
    while (bytes.hasRemaining())
      if (channel.read(bytes) <= 0) throw new EOFException("truncated output file");
  }

  private static void writeAll(FileChannel channel, ByteBuffer bytes) throws IOException {
    while (bytes.hasRemaining())
      if (channel.write(bytes) <= 0) throw new IOException("output write made no progress");
  }

  private static void sync(Path directory) throws IOException {
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }

  private static void force(Path path) throws IOException {
    try (FileChannel channel =
        FileChannel.open(path, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
      channel.force(true);
    }
  }

  private static long add(long left, long right) {
    if (left < 0 || right < 0 || right > Long.MAX_VALUE - left)
      throw ProtocolError.limit("output size overflow");
    return left + right;
  }

  private static long multiply(long value, long factor) {
    if (value < 0 || factor < 0 || factor != 0 && value > Long.MAX_VALUE / factor)
      throw ProtocolError.limit("output funding overflow");
    return value * factor;
  }

  private static ProtocolError conflict(String detail) {
    return new ProtocolError(ProtocolError.Code.CONFLICT, detail);
  }

  private static IOException corrupt(String detail) {
    return new IOException("V2 output storage: " + detail);
  }

  private static IOException corrupt(String detail, Throwable cause) {
    return new IOException("V2 output storage: " + detail, cause);
  }
}
