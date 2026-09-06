package ai.pipestream.quic;

import java.sql.Connection;
import java.sql.SQLException;

/** Remaining fixed-image writes under the pinned SQLite WAL and Unix WAL-index layout. */
final class SealedCompletionReservations {
  private static final long WAL_HEADER = 32, FRAME_HEADER = 24, MAX_SECTOR = 65536;
  private final Connection connection;
  private final Model model;
  private final long usableWal;
  private long remaining;

  record Model(long pageSize) {
    Model {
      if (pageSize < 512 || pageSize > 65536 || Long.bitCount(pageSize) != 1) {
        throw new IllegalArgumentException("unsupported SQLite completion page size");
      }
    }

    long acquisition() { return stage(256); }
    long publication(int kind) {
      // PROCESS can also retire its preallocated future; REHYDRATE cannot.
      return kind == SealedJobs.PROCESS ? stage(112, 256, 256) : stage(112, 256);
    }
    long conversion(int descriptorCapacity) { return stage(128, 112, descriptorCapacity, 32, 256); }
    long future(int descriptorCapacity) {
      return conversion(descriptorCapacity) + acquisition() + publication(SealedJobs.REHYDRATE);
    }
    long admission(int descriptorCapacity) {
      return acquisition() + publication(SealedJobs.PROCESS) + future(descriptorCapacity);
    }

    long stage(int... images) {
      long pages = 0;
      for (int bytes : images) {
        if (bytes < 1 || bytes > SealedSqliteImages.MAX_BYTES) throw new IllegalArgumentException("invalid completion image size");
        // A slice either begins in a leaf and enters overflow at offset zero,
        // or begins partway through overflow. In either case ceil(n/(p-4))+1
        // covers every dirty page, without assuming the BLOB's row offset.
        pages += Math.ceilDiv((long) bytes, pageSize - 4) + 1;
      }
      long frame = pageSize + FRAME_HEADER;
      // SQLite 3.53.4 walFrames reuses same-transaction spill frames. The
      // final commit page may repeat, followed by full sector-boundary padding.
      return WAL_HEADER + (pages + 1 + Math.ceilDiv(MAX_SECTOR, frame)) * frame;
    }

    long usableWal(SealedSessionStore.FileLimits limits) {
      // 32 KiB index regions, rounded to 64 KiB by the guarded Unix VFS.
      long regions = limits.sharedMemoryBytes() / 32768;
      long frames = 4062 + (regions - 1) * 4096;
      return Math.min(limits.walBytes(), WAL_HEADER + frames * (pageSize + FRAME_HEADER));
    }
  }

  private SealedCompletionReservations(Connection connection, Model model,
      SealedSessionStore.FileLimits limits) throws SQLException, ProtocolException {
    this.connection = connection;
    this.model = model;
    this.usableWal = model.usableWal(limits);
    this.remaining = SealedJobs.completionBytes(connection, model);
    install();
  }

  static SealedCompletionReservations protect(Connection connection, SealedSessionStore.FileLimits limits)
      throws SQLException, ProtocolException {
    long page;
    try (var statement = connection.createStatement()) {
      try (var rows = statement.executeQuery("PRAGMA page_size")) {
        if (!rows.next()) throw new SQLException("missing SQLite page geometry");
        page = rows.getLong(1);
      }
      try (var rows = statement.executeQuery("PRAGMA journal_mode")) {
        if (!rows.next() || !"wal".equalsIgnoreCase(rows.getString(1))) throw new SQLException("completion funding requires WAL");
      }
      try (var rows = statement.executeQuery("PRAGMA auto_vacuum")) {
        if (!rows.next() || rows.getInt(1) != 0) throw new SQLException("unsupported completion auto-vacuum geometry");
      }
    }
    return new SealedCompletionReservations(connection, new Model(page), limits);
  }

  Model model() { return model; }

  /** Called after validating the stage, before its first image or row write. */
  void adjust(long delta) throws SQLException, ProtocolException {
    try { remaining = Math.addExact(remaining, delta); }
    catch (ArithmeticException overflow) { throw Wire.limit("SQLite completion reservation overflow"); }
    if (remaining < 0) throw Wire.integrity("completion stage released unreserved capacity");
    install();
  }

  private void install() throws SQLException, ProtocolException {
    if (remaining > usableWal - WAL_HEADER) throw Wire.limit("insufficient SQLite completion headroom");
    // Actual file length can include a rolled-back tail. Only SQLite knows its
    // next committed append point; the VFS bounds every write at that offset.
    SealedSqliteImages.walCeiling(connection, usableWal - remaining);
  }

  void verify() throws SQLException, ProtocolException {
    if (remaining != SealedJobs.completionBytes(connection, model)) {
      throw Wire.integrity("completion reservation differs from committed job stages");
    }
  }
}
