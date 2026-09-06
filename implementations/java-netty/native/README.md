# JDBC SQLite file guard

This C extension enforces SQLite file-length limits and supplies fixed-size
BLOB writes and connection-local WAL ceilings. The PipeStream codec,
session state machine, reservation calculations, network service, and executor remain independent Java
implementations. It contains no protocol code and imports no Rust code.

Maven uses CMake to build `libpipestream_sqlite.so` and places it under
`ai/pipestream/quic/native/` in both the library and shaded JAR. Runtime loading
uses Xerial JDBC's existing SQLite extension API, not an additional JNI bridge
or a second linked SQLite engine. Only one private in-memory bootstrap connection
can register paths. Normal store connections cannot call its management
functions or load extensions; the default SQLite VFS is unchanged.

SQLite's [automatic-extension callback](https://www.sqlite.org/c3ref/auto_extension.html)
installs `pipestream_blob_replace` and `pipestream_wal_ceiling` only on connections
whose main database uses the guarded VFS. Both are DIRECTONLY and require an
explicit main writer transaction. The BLOB helper accepts an exact-size image,
bounded at 16 MiB, and uses the [incremental BLOB API](https://www.sqlite.org/c3ref/blob_open.html)
without SQL row replacement. It does not run SQL UPDATE triggers, maintain an
index over image bytes, or enforce the Java state codec's invariants. Java must
validate the image and roll back the transaction on any subsequent failure.

Each main handle owns a reference-counted atomic WAL ceiling, shared only with
its own journal/WAL handles. SQLite's original journal filename supplies the
[main-file association](https://www.sqlite.org/c3ref/database_file_object.html);
synthetic filenames cannot be used for that API. Another connection's ceiling
cannot relax this writer's bound. The ceiling also bounds WAL truncation and
size hints, cannot exceed the immutable policy, and survives rollback until
explicitly changed or the connection closes. Java admission computes and applies
the remaining-stage reservations with this primitive. The setter refuses
nonzero reserved page bytes or sectors larger than its 64 KiB cost allowance.

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

`pipestream-java-bounded-unix-v2` wraps the bundled `unix` VFS on 64-bit Linux.
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
They also exercise exact BLOB replacement and rollback, bounded arguments, and
ceiling changes through real SQLite connections. JDBC tests cover three page
sizes, fixed main-page counts, connection isolation, late WAL opens, invalid
transaction contexts and DIRECTONLY restrictions.
These native primitives enforce file lengths, not filesystem allocation quotas,
a storage-latency bound, or a power-loss proof. The Java transaction layer supplies
the [completion-reservation model](../../../docs/standards/java-completion-reservations.md).
