# Java QUIC transport extension

This source-pinned dependency extension is being developed for Java V2's
control/data reservation. It is not an upstream Netty release, a published Maven
artifact, or evidence that Java implements durable work/results. The main Java
reference selects this extension through its exact Maven coordinates and still
advertises V2 Core only.

## Source and artifact boundary

- Netty baseline: `e0789d32c72f46fd2e7c99b6fdbbf7e2f4409e44`
  (`netty-4.2.17.Final`).
- quiche baseline: `4f347477006bf7f928335d28f05056013f70b87e`.
- The Netty native build pins BoringSSL
  `0226f30467f540a3f62ef48d453f93927da199b6`. This differs from the quiche
  checkout's BoringSSL submodule,
  `f1c75347daa2ea81a941e953f2263e0a4d970c8d`; tests using the latter are not
  tests of the exact Netty native TLS build.
- Modified classes/native artifacts use the group `ai.pipestream.transport`
  and version `4.2.17.Final-pipestream.1`. Other Netty modules remain official
  `io.netty` 4.2.17.Final dependencies. The native library name uses
  `netty_quiche42_pipestream`, not the official library's name.

The Java packages remain `io.netty.handler.codec.quic`. The extension must
replace, not accompany, the official QUIC classes/native dependencies on an
application classpath. Its distinct coordinates identify the modified build;
they do not make two copies of the same Java classes interoperable.

Only reviewed patches, the Rust dependency lock and accompanying upstream notices
belong here. Upstream
repositories and build checkouts belong in reference-code, not in the research
notes or this product repository. Original source licenses remain in the source
trees and patches; verbatim copies accompany the bundle in [licenses](licenses/README.md).
The two patch files preserve unified-diff blank-line prefixes byte for byte.
Their local Git attributes permit that required trailing space; whitespace checks
for the actual source changes pass in both patched reference repositories.

## Accounting contract

The quiche extension exposes an initial connection-credit replenishment window
independently of advertised initial MAX_DATA. Explicit windows are clamped to the
configured maximum at connection creation, independent of setter order. Unset
configuration preserves upstream defaults. Autotuning and stream-window lower
bounds still apply, so these settings alone are not an absolute receive-memory
bound.

The send accounting sums each live stream's written offset minus its highest
contiguous ACK offset, with saturating arithmetic. Packet emission and local
write completion do not release the charge. ACKed bytes behind a gap remain
charged; resets release cleared send buffers. This is a logical retained-data
span, not RSS or an allocation counter. Metadata, TLS, datagrams and partial
backing-buffer prefixes require separate limits. The copying path can retain
one acknowledged backing-buffer prefix of up to 4095 bytes per send stream.

`STREAM_SEND_BUFFER_LIMITS` is a connection option installed before registration.
Ordinary streams use the aggregate ceiling `total - reserved`; locally selected
`USE_RESERVED_SEND_BUFFER` streams may use `total`. The current stream setting
applies at each native write, including automatic queued-write retries. No
temporary global allowance is exposed to unrelated stream flushes. Java's signed
counter boundary fails closed on saturation.

The extension does not reserve congestion-window capacity or peer credit, prove
network delivery, bound application queues, or provide control deadlines. Those
remain explicit responsibilities of the Java object/control owner and its
end-to-end acceptance tests.

## Rebuild and verification

The Linux x86_64 entry point is `bash build.sh` from this directory. It requires
Git, Maven, Cargo, a C/C++ build toolchain, CMake, Perl and Go. It verifies the
checked-in patch/lock checksums, fetches exact source revisions into a new
directory under `/work/reference-code`, and runs the native reactor verification
with an isolated Maven cache. After verification it installs the modified
artifacts only in that cache and prints its absolute path as the sole stdout
line; progress goes to stderr. A failed build returns no repository path.
`PIPESTREAM_REFERENCE_CODE_ROOT` may select another existing reference-code root.

From the Java reference directory:

```bash
transport_repository=$(bash transport/build.sh)
mvn "-Dmaven.repo.local=$transport_repository" verify
```

The full conformance command performs that bootstrap once and uses the returned
repository for both the reference and external Java example. Developers can reuse
the captured path for focused builds of the same pinned dependency. The POM
always names the extension explicitly; there is no fallback to official QUIC or
system-path dependency. Runtime tests check the unique class/native resources
and exact manifest revision/patch identity.

Every invocation retains its complete build directory and verification log on
success or failure. Nothing is installed in the global Maven cache or published.
Netty's native build owns and deletes generated source directories under its
`target`; never configure those properties to point at retained source checkouts.

The final bundle passed a fresh rebuild on 2026-09-07: 294 native Java tests in
31 fresh XML reports, with no failures, errors or skips. Generated manifests
identify the expected upstream revisions, both patch hashes and the distinct
native-library name. Commands, artifact hashes, red-to-green history and scoped
lint limitations are recorded in the
[verification evidence](../../../conformance/results/durable-work-v2-java-native-credit-2026-09-07.txt).
This is a repeatable source-pinned build, not a claim of bit-identical binaries
across toolchains and timestamps. Source-level Rust tests, native Java transport
tests, full RFC regression tests and V2 durable-object resource tests are separate
gates. Passing one does not imply the others; the Java V2 object/control owner
and its complete durable-profile integration remain open.
