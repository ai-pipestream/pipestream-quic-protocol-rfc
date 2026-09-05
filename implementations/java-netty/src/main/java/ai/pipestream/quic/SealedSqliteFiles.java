package ai.pipestream.quic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConnection;

/** Local immutable file policy and bootstrap for JDBC's bounded Unix VFS. */
final class SealedSqliteFiles {
  private static final String VFS = "pipestream-java-bounded-unix-v1";
  private static final byte[] MAGIC = {'P', 'S', 'J', 'D', 'B', '0', '0', '1'};
  private static final Object BOOT_LOCK = new Object();
  private static SQLiteConnection bootstrap;
  private final Path database;
  private final SealedSessionStore.FileLimits limits;
  private final byte[] policy;

  private SealedSqliteFiles(Path database, SealedSessionStore.FileLimits limits, byte[] policy) {
    this.database = database; this.limits = limits; this.policy = policy;
  }

  static synchronized SealedSqliteFiles open(Path input, SealedSessionStore.FileLimits requested)
      throws IOException, SQLException {
    if (!System.getProperty("os.name").equals("Linux")) throw new IOException("bounded JDBC storage currently requires Linux");
    Path absolute = input.toAbsolutePath().normalize();
    Path parent = absolute.getParent();
    if (parent == null) throw new IOException("database must have a parent directory");
    Path ancestor = parent;
    while (!Files.exists(ancestor)) ancestor = ancestor.getParent();
    ancestor = ancestor.toRealPath();
    Files.createDirectories(parent);
    Path path = parent.toRealPath().resolve(absolute.getFileName());
    for (String reserved : new String[]{"-wal", "-shm", "-journal", ".psjlimits", ".psjlock"}) {
      if (path.getFileName().toString().endsWith(reserved)) throw new IOException("reserved database filename suffix");
    }
    Path policyPath = sidecar(path, ".psjlimits");
    Path lockPath = sidecar(path, ".psjlock");
    regularLength(lockPath, 0);
    try (FileChannel lockChannel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        var lock = lockChannel.lock()) {
      if (!lock.isValid()) throw new IOException("database policy lock is invalid");
      if (!Files.exists(policyPath, LinkOption.NOFOLLOW_LINKS)) {
        for (String suffix : new String[]{"", "-wal", "-journal", "-shm"}) {
          if (regularLength(sidecar(path, suffix), Long.MAX_VALUE) != 0) {
            throw new IOException("nonempty JDBC store lacks file policy; conversion refused");
          }
        }
        byte[] bytes = encode(requested == null ? SealedSessionStore.FileLimits.defaults() : requested);
        try (FileChannel out = FileChannel.open(policyPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
          ByteBuffer buffer = ByteBuffer.wrap(bytes);
          while (buffer.hasRemaining()) out.write(buffer);
          out.force(true);
        }
      }
      byte[] bytes = readPolicy(policyPath);
      ByteBuffer values = ByteBuffer.wrap(bytes, 8, 32);
      SealedSessionStore.FileLimits actual;
      try {
        actual = new SealedSessionStore.FileLimits(values.getLong(), values.getLong(), values.getLong(), values.getLong());
      } catch (IllegalArgumentException invalid) { throw new IOException("invalid retained file policy", invalid); }
      if (requested != null && !requested.equals(actual)) throw new IOException("JDBC file policy cannot change on reopen");
      try (FileChannel file = FileChannel.open(policyPath, StandardOpenOption.WRITE)) { file.force(true); }
      for (Path directory = path.getParent(); ; directory = directory.getParent()) {
        try (FileChannel file = FileChannel.open(directory, StandardOpenOption.READ)) { file.force(true); }
        if (directory.equals(ancestor)) break;
        if (directory.getParent() == null) throw new IOException("policy directory escaped existing ancestor");
      }
      SealedSqliteFiles files = new SealedSqliteFiles(path, actual, bytes);
      files.usage();
      synchronized (BOOT_LOCK) { ensureBootstrap(); }
      return files;
    }
  }

  Path path() { return database; }
  SealedSessionStore.FileLimits limits() { return limits; }

  SealedSessionStore.FileUsage usage() throws IOException {
    verifyPolicy();
    return new SealedSessionStore.FileUsage(regularLength(database, limits.databaseBytes()),
        regularLength(sidecar(database, "-wal"), limits.walBytes()),
        regularLength(sidecar(database, "-journal"), limits.journalBytes()),
        regularLength(sidecar(database, "-shm"), limits.sharedMemoryBytes()));
  }

  Connection connect() throws SQLException {
    try { usage(); }
    catch (IOException invalid) { throw new SQLException("invalid JDBC file policy or layout", invalid); }
    // Registration owns a bounded native ticket until xOpen has its own reference.
    // The bootstrap connection and its management functions never escape this class.
    synchronized (BOOT_LOCK) {
      ensureBootstrap();
      try (var register = bootstrap.prepareStatement("SELECT pipestream_guard_register(?,?,?,?,?,?)")) {
        register.setString(1, database.toString());
        register.setLong(2, limits.databaseBytes()); register.setLong(3, limits.walBytes());
        register.setLong(4, limits.journalBytes()); register.setLong(5, limits.sharedMemoryBytes());
        register.setBytes(6, policy);
        try (var result = register.executeQuery()) {
          if (!result.next() || result.getInt(1) != 1) throw new SQLException("VFS registration did not acknowledge policy");
        }
      }
    }
    Connection connection = null;
    try {
      connection = DriverManager.getConnection("jdbc:sqlite:" + database.toUri().toASCIIString() + "?vfs=" + VFS);
      return connection;
    } finally {
      synchronized (BOOT_LOCK) {
        try (var unregister = bootstrap.prepareStatement("SELECT pipestream_guard_unregister(?)")) {
          unregister.setString(1, database.toString());
          try (var result = unregister.executeQuery()) {
            if (!result.next() || result.getInt(1) != 1) throw new SQLException("VFS registration ticket was not released");
          }
        } catch (SQLException failure) {
          if (connection != null) try { connection.close(); } catch (SQLException close) { failure.addSuppressed(close); }
          throw failure;
        }
      }
    }
  }

  private void verifyPolicy() throws IOException {
    if (!Arrays.equals(policy, readPolicy(sidecar(database, ".psjlimits")))) throw new IOException("JDBC file policy changed");
  }

  private static byte[] readPolicy(Path path) throws IOException {
    if (regularLength(path, 72) != 72) throw new IOException("JDBC file policy length mismatch");
    byte[] bytes;
    try (var input = Files.newInputStream(path)) { bytes = input.readNBytes(73); }
    if (bytes.length != 72 || !Arrays.equals(Arrays.copyOf(bytes, 8), MAGIC)
        || !MessageDigest.isEqual(SealedWork.sha256().digest(Arrays.copyOf(bytes, 40)), Arrays.copyOfRange(bytes, 40, 72))) {
      throw new IOException("JDBC file policy checksum or version mismatch");
    }
    return bytes;
  }

  private static byte[] encode(SealedSessionStore.FileLimits limits) {
    byte[] bytes = new byte[72];
    ByteBuffer.wrap(bytes).put(MAGIC).putLong(limits.databaseBytes()).putLong(limits.walBytes())
        .putLong(limits.journalBytes()).putLong(limits.sharedMemoryBytes());
    System.arraycopy(SealedWork.sha256().digest(Arrays.copyOf(bytes, 40)), 0, bytes, 40, 32);
    return bytes;
  }

  private static Path sidecar(Path path, String suffix) { return path.resolveSibling(path.getFileName() + suffix); }

  private static long regularLength(Path path, long limit) throws IOException {
    java.util.Map<String, Object> attrs;
    try { attrs = Files.readAttributes(path, "unix:isRegularFile,nlink,size", LinkOption.NOFOLLOW_LINKS); }
    catch (java.nio.file.NoSuchFileException missing) { return 0; }
    if (!Boolean.TRUE.equals(attrs.get("isRegularFile")) || ((Number) attrs.get("nlink")).longValue() > 1) {
      throw new IOException("JDBC file is not a private regular file");
    }
    long size = ((Number) attrs.get("size")).longValue();
    if (size < 0 || size > limit) throw new IOException("JDBC file exceeds its retained limit");
    return size;
  }

  static boolean isFull(SQLException failure) { return (failure.getErrorCode() & 255) == 13; }

  private static void ensureBootstrap() throws SQLException {
    if (bootstrap != null) {
      if (bootstrap.isClosed()) throw new SQLException("bounded VFS bootstrap was closed");
      return;
    }
    SQLiteConfig config = new SQLiteConfig();
    config.enableLoadExtension(true);
    SQLiteConnection connection = (SQLiteConnection) config.createConnection("jdbc:sqlite::memory:");
    try {
      Path directory = Files.createTempDirectory("pipestream-jdbc-native-");
      Path library = directory.resolve("libpipestream_sqlite.so");
      try {
        try (var input = SealedSqliteFiles.class.getResourceAsStream("/ai/pipestream/quic/native/libpipestream_sqlite.so")) {
          if (input == null) throw new IOException("bounded JDBC native library is absent; no fallback permitted");
          Files.copy(input, library);
        }
        try (var load = connection.prepareStatement("SELECT load_extension(?, 'sqlite3_pipestream_init')")) {
          load.setString(1, library.toString());
          try (var result = load.executeQuery()) { if (!result.next()) throw new SQLException("native VFS load returned no row"); }
        }
      } finally {
        Files.deleteIfExists(library);
        Files.delete(directory);
      }
      if (connection.getDatabase().enable_load_extension(false) != 0) throw new SQLException("cannot disable SQLite extension loading");
      bootstrap = connection;
    } catch (IOException | SQLException | RuntimeException failure) {
      try { connection.close(); } catch (SQLException close) { failure.addSuppressed(close); }
      throw new SQLException("bounded JDBC VFS bootstrap failed", failure);
    }
  }
}
