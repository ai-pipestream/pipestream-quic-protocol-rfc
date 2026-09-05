# JDBC SQLite file guard

This C extension enforces SQLite file-length limits only. The PipeStream codec,
session state machine, network service, and executor remain independent Java
implementations. It contains no protocol code and imports no Rust code.

Maven uses CMake to build `libpipestream_sqlite.so` and places it under
`ai/pipestream/quic/native/` in both the library and shaded JAR. Runtime loading
uses Xerial JDBC's existing SQLite extension API, not an additional JNI bridge
or a second linked SQLite engine. Only one private in-memory bootstrap connection
can register paths. Normal store connections cannot call its management
functions or load extensions; the default SQLite VFS is unchanged.

The build fetches SQLite 3.53.4's official amalgamation:

- Source: <https://www.sqlite.org/2026/sqlite-amalgamation-3530400.zip>
- SHA-256: `1e71ddf93849c6a6ecf58b827c0692073d2dd7ee40196158068f7b29f422e87d`
- License: SQLite [public-domain dedication](https://www.sqlite.org/copyright.html).

The runtime extension uses only `sqlite3.h` and `sqlite3ext.h`. CTest separately
compiles the amalgamation into its standalone test executable to invoke real
SQLite file methods against the built extension. That test engine is neither
packaged nor linked into the runtime JAR. Java integration tests independently
exercise the extension through the actual pinned `sqlite-jdbc:3.53.4.0` driver.
The extension refuses a different SQLite version. Updating that dependency
requires updating this pin and rechecking the Unix VFS and its growth controls.

`pipestream-java-bounded-unix-v1` wraps the bundled `unix` VFS on 64-bit Linux.
It delegates locking and synchronization, but bounds writes, truncates and
shared-memory growth, disables database mmap and preallocation, and declines
unaudited file controls and temporary/foreign files. Policy and layout are
checked before opening registered files. The Java store also sets a main-page
cap and in-memory temporary storage. Direct raw JDBC or external filesystem
writers are outside the private-directory, cooperating-writer boundary.

Each connection open holds a short registration ticket until `xOpen` owns a
reference. Every opened main/WAL/journal file retains that reference until
`xClose`. A fixed 64-entry registry is reclaimed at the last reference, not at
the lifetime of a historical Java store object. There is no filesystem I/O under
the registry lock or bootstrap registration lock. The extension returns
`SQLITE_OK_LOAD_PERMANENTLY` so registered VFS callbacks remain valid after the
loading connection closes. See SQLite's [extension lifetime rules](https://www.sqlite.org/loadext.html#persistent_loadable_extensions),
[file methods](https://www.sqlite.org/c3ref/io_methods.html), and
[file controls](https://www.sqlite.org/c3ref/c_fcntl_begin_atomic_write.html).

Run the native address/undefined-behavior sanitizer gate independently of the JVM:

```sh
cmake -S native -B target/native-sanitize -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_FLAGS='-fsanitize=address,undefined -fno-omit-frame-pointer' \
  -DBUILD_TESTING=ON
cmake --build target/native-sanitize --parallel 2
ctest --test-dir target/native-sanitize --output-on-failure --no-tests=error
```

The tests invoke actual main/WAL/journal/shared-memory file methods, including
overflow and negative-offset refusal, forced-off mmap, size hints and chunk
rounding, forbidden temporary files, and VFS lifetime after bootstrap close.
These are file-length limits, not filesystem allocation quotas, future
completion-space reservations, a storage-latency bound, or a power-loss proof.
